package main

import (
	"bytes"         // bytes.NewBuffer: wrap JSON bytes as a request body
	"encoding/json" // decodes/encodes JSON <-> Go values
	"flag"          // command-line flag parsing
	"fmt"           // log.Fatal prints an error and exits
	"io"            // io.ReadAll/ io.NopCloser
	"log"
	"net"
	"net/http"
	"os"      // os.Getenv: for reading env vars
	"strings" // strings.Split: parse comma-separated child list
	"sync"    // gives sync.RWMutex, which allows many goroutines to read data at same time, but gives only one goroutine exclusive access when it needs to write
)

// Node represents single server in distributed database.
// Every running instance of this program IS one Node.
type Node struct {
	// true -> accept writes and is source of truth
	// false -> hold read-only copy and serve reads
	isParent bool
	// actual key-value store
	data map[string]string
	// sync.RWMutex is a "read-write mutex":
	// 		- Call mu.RLock()/mu.RUnlock() around READS (many readers allowed at once)
	// 		- Call mu.Lock()/mu.Unlock() around WRITES (one writer with exclusive access)
	mu sync.RWMutex
	// list of child addresses this node replicates to.
	childNodes []string
	// address of this node's parent
	parentNode string
	// how OTHER nodes can reach THIS node over network
	selfAddress string
}

// NewNode is a constructor, and it returns a *Node.
// Go has no 'new'/ 'constructor' keyword
// Return POINTER to a Node so...
// 1. every handler shares SAME node and its data
// 2. lock (mu) works: copying a mutex breaks it
func NewNode(isParent bool, parentNode string, childNodes []string, selfAddress string) *Node {
	// &Node{...} creates a Node and immediately takes its address
	return &Node{
		data:        make(map[string]string),
		isParent:    isParent,
		parentNode:  parentNode,
		childNodes:  childNodes,
		selfAddress: selfAddress,
		// mu's zero value is a usable unlocked mutex -> don't set it
	}
}

// Get handles read requests: GET /get?key=...
// The (n *Node) receiver binds this METHOD to a specific node
func (n *Node) Get(w http.ResponseWriter, r *http.Request) {
	// r.URL.Query() parses "?key=..." part of URL into a lookup table
	key := r.URL.Query().Get("key")

	if key == "" {
		http.Error(w, "Missing key in request", http.StatusBadRequest)
		return
	}

	// Take a READ lock: many Get requests can hold this at the same time
	// However, if a writer is mid-write, we wait until it's done
	n.mu.RLock()

	// reading map returns TWO things -
	// 		1. value -> stored value
	// 		2. exists -> bool
	value, exists := n.data[key]

	n.mu.RUnlock()

	if !exists {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
	// Fprintf is like printf, but writes to w
	fmt.Fprintf(w, "Value: %s\n", value)
}

// Put handles write requests: POST /put with a JSON body {"key":"...","value":"..."}
func (n *Node) Put(w http.ResponseWriter, r *http.Request) {

	// CHILD BRANCH: child must not store writes -> forwards to the parent
	if !n.isParent {
		// a child with no known parent can't forward. 500 = our side can't proceed
		if n.parentNode == "" {
			http.Error(w, "Parent node not available", http.StatusInternalServerError)
			return
		}

		// r.Body is a one-time stream that gets drained
		// read it fully into a byte slice so we can
		// a) forward those bytes, b) rebuild body if needed
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read request body", http.StatusInternalServerError)
			return
		}

		// reset the request body so it can be read again
		r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

		// Build a NEW POST request to the parent's /put
		// use http.NewRequest (not http.Post) b/c I need to set headers.
		url := "http://" + n.parentNode + "/put"
		req, err := http.NewRequest("POST", url, bytes.NewBuffer(bodyBytes))
		if err != nil {
			http.Error(w, "Failed to create request to parent node", http.StatusInternalServerError)
			return
		}

		// copies incoming request's headers onto forwarded request
		req.Header = r.Header.Clone()
		// tells next server who connected to this server
		req.Header.Set("X-Forwarded-For", r.RemoteAddr)

		// http.Client is the thing that actually SENDS a custom request.
		// client.Do(req) performs it and returns the parent's response
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			http.Error(w, "Failed to forward request to parent node", http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		// Read parent's response body so we can relay it
		responseBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			http.Error(w, "Failed to read response from parent node", http.StatusInternalServerError)
			return
		}

		// relay parent's response back to the original client
		// copy every header (headers are map[string][]string -> one key, may values)
		for k, v := range resp.Header {
			for _, vv := range v {
				w.Header().Add(k, vv)
			}
		}

		w.WriteHeader(resp.StatusCode)
		w.Write(responseBytes)
		return // stop here so we don't fall through into the parent code.

	}

	// PARENT BRANCH

	// decode JSON body into a Go map
	var body map[string]string

	// json.NewDecoder(r.Body) wraps request body in a JSON decoder
	// .Decode(&body) reads JSON from request and fills in body (POINTER)
	// JSON -> Go
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}
	// defer schedules r.Body.Close() to run when this function returns universally
	// frees resources of the body
	defer r.Body.Close()

	key, keyOk := body["key"]
	value, valueOk := body["value"]

	if !keyOk || !valueOk {
		http.Error(w, "Missing key or value in request", http.StatusBadRequest)
		return
	}

	n.mu.Lock()
	n.data[key] = value // the actual store
	n.mu.Unlock()

	n.replicateToChildren(key, value)

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Stored: %s -> %s\n", key, value)

}

// Delete
// handles: DELETE /delete?key=...
// behaves differently for parent vs. child, and for client vs replicated deletes
func (n *Node) Delete(w http.ResponseWriter, r *http.Request) {
	// recall: HTTP request both a method and path (GET /item)
	// this handler deletes data, so request must use the DELETE method.
	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// key comes from URL (?key=...)
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing key in request", http.StatusBadRequest)
		return
	}

	// Replication push from parent? header tells us.
	// r.Header.Get returns "" if header is absent, so this is false for ordinary client requests and true only for parent-steamped ones.
	isReplication := r.Header.Get("X-Replication") == "true"

	// CHILD BRANCH
	if !n.isParent {
		if !isReplication {
			// real client is deleting on child -> bounce the client to the parent with 307 temporary redirect
			if n.parentNode == "" {
				http.Error(w, "Parent node not available", http.StatusInternalServerError)
				return
			}
			http.Redirect(w, r, "http://"+n.parentNode+"/delete?key="+key, http.StatusTemporaryRedirect)
			return
		}
		// if its not a replication, this delete came FROM the parent (X-Replication: true)

		// in this case, it is a replication, so  just apply locally and don't forward
		n.mu.Lock()
		delete(n.data, key)
		n.mu.Unlock()
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "Replicated deletion of key: %s\n", key)
		return
	}

	// PARENT branch: client initiated delete at source of truth
	n.mu.Lock()
	delete(n.data, key)
	n.mu.Unlock()

	// Fan out deletion to every child (stamped as replication)
	n.replicateDeletionToChildren(key)
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Deleted key: %s\n", key)
}

// DisplayData handles: Get /display
// returns entire key-value store as one JSON object.
func (n *Node) DisplayData(w http.ResponseWriter, r *http.Request) {
	// use READ lock
	n.mu.RLock()
	defer n.mu.RUnlock()

	// Tell the caller the body is JSON. Headers must be set BEFORE WriteHeader.
	w.Header().Set("Content-Type", "application/json")

	// Go -> JSON
	// json.Marshal turns a Go value into a []byte of JSON, which is opposite of Decode
	// It returns (bytes, error); we must check the error
	jsonData, err := json.Marshal(n.data)
	if err != nil {
		// 500 = "something broke on OUR side," not the caller's fault.
		http.Error(w, "Error encoding data to JSON", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	// w.Write sends raw bytes as the response body (we already have JSON bytes).
	w.Write(jsonData)
}

// Replicate is Child node's inbox: POST /replicate with {"key":"...","value":"..."}
// The parent calls this on each child to "push out" a write
func (n *Node) Replicate(w http.ResponseWriter, r *http.Request) {
	// Only children accept replicated data
	if n.isParent {
		http.Error(w, "Parent node cannot receive replication data!", http.StatusBadRequest)
		return
	}

	// Put
	// 1. decode body (JSON -> Go object)
	// 2. validate
	// 3. store
	var body map[string]string
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "Invalid replication payload", http.StatusBadRequest)
		return
	}

	key, keyOk := body["key"]
	value, valueOk := body["value"]
	if !keyOk || !valueOk {
		http.Error(w, "Missing key or value in replication data", http.StatusBadRequest)
		return
	}

	n.mu.Lock()
	n.data[key] = value
	n.mu.Unlock()

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Replicated: %s -> %s\n", key, value)
}

// replicateToChildren fans a single write out to EVERY child, concurrently
func (n *Node) replicateToChildren(key, value string) {
	// iterate over each child address
	// range gives (index, value)
	for _, childAddr := range n.childNodes {
		// go func(...){...}(childaddr) launches an immediately invoked anonymous function in a NEW goroutine.
		go func(addr string) {
			// build JSON body
			replicationData := map[string]string{"key": key, "value": value}
			// json.Marhsal returns (bytes, error), turns Go -> JSON
			jsonData, _ := json.Marshal(replicationData)

			// http.Post(url, contentType, body) sends a POST
			// bytes.NewBuffer turns []byte into io.Reader that Post can read the body from
			resp, err := http.Post("http://"+addr+"/replicate", "application/json", bytes.NewBuffer(jsonData))
			if err != nil {
				log.Printf("Failed to replicate to %s: %v", addr, err)
				return // NOTE: on error, resp is nil — we must NOT touch resp.Body here.
			}
			defer resp.Body.Close()
		}(childAddr) // childAddr is passed in as "addr"

	}
}

// replicateDeletionToChildren: fans deletion to every child, concurrently
// like replicateToChildren, but sends a DELETE (no body)
// marks it as replication so children apply-and-stop instead of redirecting
func (n *Node) replicateDeletionToChildren(key string) {
	for _, childAddr := range n.childNodes {
		go func(addr string) {
			// Build a DELETE request
			// deletes carry key in URL, not a body, so body arg is nil
			req, err := http.NewRequest(http.MethodDelete, "http://"+addr+"/delete?key="+key, nil)
			if err != nil {
				log.Printf("Failed to create DELETE request for %s: %v", addr, err)
				return
			}

			// mark as replication delete
			req.Header.Set("X-Replication", "true")

			// Do(req) sends our custom request
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				log.Printf("Failed to replicate deletion to %s: %v", addr, err)
				return // resp is nil on error — correctly, we don't touch it
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				log.Printf("Replication to %s failed with status: %s", addr, resp.Status)
			}
		}(childAddr)
	}
}

// GetSelfAddress() finds an address other nodes can actually reach this one at, and appends the given port. 
// Returns (address, nil) or ("", error)
func GetSelfAddress(port string) (string, error){
	// InterfaceAddrs returns ALL of this machine's network addresses.
	// It returns (addrs, error)
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	var ipAddr string
	for _, addr := range addrs{
		// addr is a net.Addr (INTERFACE). Its concrete type is *net.IPNet.
		// TYPE ASSERTION asks "is concrete value behind addr a *net.IPNet?"
		//   ipNet -> the concrete value if yes
		//   ok    -> false if not (then we skip; no panic)
		// Also require it not to be a loopback address
		if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback(){
			// To4() returns non-nill only for IPv4 addresses. take first IPv4, non-loopback address and stop
			if ipNet.IP.To4() != nil{
				ipAddr = ipNet.IP.String() // e.g. "192.168.1.42"
				break
			}
		}
	}

	if ipAddr == "" {
		return "", fmt.Errorf("could not determine self IP address")
	}

	// Sprintf builds a string (unlike Printf, which prints it): "192.168.1.42:8081".
	return fmt.Sprintf("%s:%s", ipAddr, port), nil
}


// where execution begins
func main() {
	// Command-line flags
	// flag.Bool/flag.String(name, defaultValue, helpText) return POINTERS (*bool, *string), not values.
	// The pointed-to value is empty until flag.Parse() runs.
	isParent := flag.Bool("parent", false, "Set to true if this is the parent node")
	childNodes := flag.String("childNodes", "", "Comma-separated list of child node addresses (parent only)")
	port := flag.String("port", "8080", "Port to run this node on")

	// flag.Parse() reads the command-line arguments and fills in the flag variables above.
	flag.Parse()

	// Environment variables
	parentNodeEnv := os.Getenv("PARENT_NODE")   // who my parent is (children set this)
	selfAddressEnv := os.Getenv("SELF_ADDRESS") // how others reach me

	// Decide our own address
	var selfAddress string
	if selfAddressEnv != "" {
		selfAddress = selfAddressEnv
	} else {
		// Auto-detect routable addr
		addr, err := GetSelfAddress(*port)
		if err != nil {
			log.Fatalf("Failed to get self address: %v", err)
		}
		selfAddress = addr

	}

	// Turn the comma-separated child list into a []string slice ---
	var childNodeList []string
	if *childNodes != "" {
		childNodeList = strings.Split(*childNodes, ",")
	}

	node := NewNode(*isParent, parentNodeEnv, childNodeList, selfAddress)

	// Get
	http.HandleFunc("/get", node.Get)
	// Put
	http.HandleFunc("/put", node.Put)
	// Delete
	http.HandleFunc("/delete", node.Delete)
	// Display
	http.HandleFunc("/display", node.DisplayData)
	// Health Check
	// Fprintln writes text to first arg -> w is response so text goes back to caller
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Distributed-Database node is alive!")
	})
	// Replicate
	http.HandleFunc("/replicate", node.Replicate)

	fmt.Printf("Node running on port %s (Parent: %v, Parent Node: %s, Child Nodes: %v)\n", *port, *isParent, node.parentNode, childNodeList)
	fmt.Println("Self address detected as:", selfAddress)

	// Serve on the CONFIGURED port. log.Fatal prints the error and exits(1)
	// if ListenAndServe ever returns (it only returns on failure).
	log.Fatal(http.ListenAndServe(":"+*port, nil))
}
