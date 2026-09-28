# Distributed-Database

Hello! I'm really interested in distributed systems and pursuing a career in infrastructure, so I recently embarked on building a **Redis-like distributed key-value store** using Golang(Go) and deploying it on Amazon Web Services (AWS).

I decided to use **single-leader replication**, where one parent owns every write, child nodes serve reads,and new children can join a running cluster and catch up automatically.

It uses the Go standard library, ships as a Docker image, and runs live on AWS as a 3-node cluster of Docker containers on EC2.

## Why this design?

A database that lives on one server has two big problems. First, it can only handle so much traffic (One machine has a fixed amount of CPU and memory). Second, if it goes down, everything goes down!

The obvious fix is to add more servers. But if every server accepts writes, two servers can accept conflicting writes to the same key at the same moment, and then you have to decide who wins. That is one of the hardest problems in distributed systems.

Most applications are read-heavy. Roughly 80% of requests just read data, so I used a design that takes advantage of that:

- One parent node handles every write. This node is the single source of truth, so writes can never conflict.
- Many child nodes. Each holds a full copy of the data and answers reads. If you need more read capacity, just simply add more children!

TLDR: I implemented single-leader replication. It's the same idea behind PostgreSQL read replicas, MySQL replication, and Redis replicas.

## How it works

Every running copy of the program is one node. The same binary can start as either a parent or a child, depending on command-line flags and environment variables. Nodes talk to each other over plain HTTP with JSON bodies.

### Reads: any node, in parallel

Any node answers GET /get?key=... straight from its own in-memory map.Many HTTP requests are handled at the same time (Go runs each one in its own goroutine), so the map is protected by a sync.RWMutex (a read-write lock). Reads take the read lock, which lets any number of them run at once. Writes take the write lock, which gives them exclusive access. These locks help me prevent two goroutines from touching the same map at the same time (race condition).

### Writes: the parent stores, then fans out

When the parent receives POST /put, it:

1. stores the key under the write lock
2. starts one goroutine per child that sends the change to that child's /replicate endpoint, and replies to the client right away without waiting for the children (asynchronous replication)

Writes sent to a child are forwarded to the parent

### Deletes: redirect up, replicate down

If a client sends DELETE to a child, the child responds with a 307 Temporary Redirect pointing to the parent. The parent deletes the key and sends the delete to every child with an {X-Replication: true} header. When a child sees that header, it knows the request came from the parent, so it applies the delete locally and stops. Without that marker, the child would redirect the parent's own delete back to the parent, which leads to a loop.

### Joining a running cluster

Children don't have to be listed ahead of time. When a new child starts, it:

1. finds its own address
2. registers with the parent (POST /addChild)
3. pulls a full snapshot of the parent's data (GET /display)

It registers before syncing. If it synced first, any write that arrived between the sync and the registration would be missed. This is the same idea as auto-scaling in the cloud: start another container, and it joins the cluster and fills itself in.

## Measuring it

To check that adding nodes really adds read capacity, I wrote a load tester in Go (`tests/stresstest.go`). It sends 10,000 `GET` requests, spreads them round-robin across the nodes, and times the whole run. Building it taught me several core Go concurrency tools:

- a **buffered channel used as a semaphore**, to cap how many requests are in flight at once,
- a **`sync.WaitGroup`**, to wait until every request goroutine has finished,
- **`sync/atomic`**, for a counter that many goroutines can safely increment, and
- a shared **`http.Transport`**, which reuses TCP connections instead of opening a new one for every request.
  **Result:** 10,000 reads took **1.74 s on one node and 0.62 s across three**, which is a **2.8× throughput increase** (5,747 → 16,047 requests/second).

These numbers come from my laptop, so all three nodes shared one CPU. That makes the result conservative. Next, I'm re-running the benchmarks with one node per EC2 instance and a separate load-generator machine, and adding latency percentiles (p50/p99) and replication-lag measurements.

## Deployment

I packaged the node as a **multi-stage Docker image**. The first stage uses the full Go toolchain to compile a static Linux binary. The second stage copies only that binary into a small Alpine image, so the final image doesn't carry the compiler.

On AWS, the cluster runs on an **EC2 t3.micro** instance (Amazon Linux 2023). I build the image on the instance and start three containers on a shared Docker network, where they find each other by container name (`parent`, `child1`, `child2`). The **security group** only allows SSH and the three node ports from my own IP, so the database isn't open to the public internet.

All three nodes currently share one VM. That's a real cloud deployment, but not real fault tolerance, because if that one machine fails, the whole cluster goes down. Spreading the nodes across separate instances is on my roadmap.

## Quick start

```bash
go run . -parent -port 8080                                                  # parent
PARENT_NODE=localhost:8080 SELF_ADDRESS=localhost:8081 go run . -port 8081   # child
PARENT_NODE=localhost:8080 SELF_ADDRESS=localhost:8082 go run . -port 8082   # child

curl -X POST localhost:8081/put -d '{"key":"name","value":"ben"}'   # write via a child (forwarded)
curl "localhost:8082/get?key=name"                                  # read from another child
curl -X DELETE "localhost:8080/delete?key=name"
curl localhost:8082/display                                         # dump a node's data
```

With Docker (containers find each other by name on a shared network):

```bash
docker build -t distdb . && docker network create distnet
docker run -d --name parent --network distnet -p 8080:8080 distdb -parent -port 8080
docker run -d --name child1 --network distnet -p 8081:8080 \
  -e PARENT_NODE=parent:8080 -e SELF_ADDRESS=child1:8080 distdb -port 8080
docker run -d --name child2 --network distnet -p 8082:8080 \
  -e PARENT_NODE=parent:8080 -e SELF_ADDRESS=child2:8080 distdb -port 8080
```

## What I learned

**Go**

- Structs, methods with pointer receivers, maps, slices, and constructor functions
- Writing HTTP servers and clients with `net/http`: handlers, custom requests, headers, redirects, and proxying
- JSON encoding and decoding, and Go's explicit error handling
- Concurrency: goroutines, `sync.RWMutex`, `sync.WaitGroup`, `sync/atomic`, and channels as semaphores
- Gotchas like passing loop variables into goroutines, and why you can't copy a mutex
- Interfaces and type assertions (`addr.(*net.IPNet)`) when walking network interfaces
- Using the race detector to find bugs that normal testing misses
- Multi-stage Docker builds for Go
  **Distributed systems**
- Single-leader replication, and why a single writer avoids conflicts
- Synchronous vs. asynchronous replication, and the latency vs. consistency trade-off
- Eventual consistency, and the difference between replication _lag_ and replica _divergence_
- Message reordering, and why systems need version numbers or logical clocks
- Idempotency, and using a marker header to prevent replication loops
- Cluster membership, bootstrapping a new replica, and state transfer
- The limits of this design: the parent is a write bottleneck and a single point of failure

## Next steps

- [ ] **Persistence:** append-only log and snapshots, so a node survives a restart
- [ ] **Health checks:** heartbeat children and drop dead ones from the replica set
- [ ] **Parent failover:** remove the single point of failure with leader election (**Raft**)
- [ ] **Sharding:** consistent hashing to scale _writes_, not just reads
- [ ] **Tunable consistency:** quorum reads/writes (`R + W > N`)
- [ ] **Multi-machine deploy:** one node per EC2 instance (or ECS), with EC2 load-test numbers
- [ ] **Real tests:** table-driven `httptest` unit tests under `go test -race`, plus fixes
      for the two bugs above (per-key sequence numbers; listen before registering)

## License

[MIT](LICENSE)
