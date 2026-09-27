package main

import (
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// benchmarkRequests fires numRequests concurrent GETs, spread across numPorts nodes
// reports total + average time
func benchmarkRequests(numRequests int, numPorts int) {
	// WaitGroup: lets main() wait until every request goroutine has finished.
	var wg sync.WaitGroup


	concurrency := 5     // max requests in flight at once (semaphore size)
	ports := []int{8080, 8081, 8082} // the node ports to hit 
	key := "name"

	// Seed the RNG
	rand.Seed(time.Now().UnixNano())

	// Buffered channel used as a SEMAPHORE: it holds up to concurrency tokens.
	// sending blocks when full, so no more than concurrency requests run at once
	// if concurency was 0, then it would be a unbufferred channel
	requestCh := make(chan struct{}, concurrency)


	// A shared counter, incremented atomically, to round-robin across ports.
	var portIndex uint64 = 0
	portsLen := uint64(numPorts)

	// A custom Transport that pools/reuses connections instead of a new TCP handshake per request. 
	transport := &http.Transport{
		MaxIdleConnsPerHost: 100,
	}
	client := &http.Client{
		Transport: transport,
	}

	selectedPorts := ports[:numPorts] // use the first numPorts entries
	start := time.Now()

	for i := 0; i < numRequests; i++ {
		wg.Add(1)                // register one more goroutine with the WaitGroup
		requestCh <- struct{}{}  // acquire a semaphore slot (blocks if 5 are busy)
		go func() {
			defer wg.Done() // mark this goroutine done when it returns

			// increment port index by 1 and stores new value in idx
			// Atomic means concurrent goroutines can increment it safely
			idx := atomic.AddUint64(&portIndex, 1)

			// selects a port using remainder operator
			// cycles in round-robin order
			port := selectedPorts[idx%portsLen]

			url := fmt.Sprintf("http://localhost:%d/get?key=%s", port, key)
			resp, err := client.Get(url)
			if err != nil {
				fmt.Println("Error:", err)
			} else {
				resp.Body.Close() // MUST close, or connections leak and pooling breaks
			}
			<-requestCh // release the semaphore slot so another request can start
		}()
	}

	wg.Wait() // block here until all numRequests goroutines have finished
	elapsed := time.Since(start)
	fmt.Printf("Total time for %d requests using %d ports: %s\n", numRequests, numPorts, elapsed)
	fmt.Printf("Average time per request: %s\n\n", elapsed/time.Duration(numRequests))
}


func main() {
	numRequests := 10000
	numPorts := 3 
	benchmarkRequests(numRequests, numPorts)
}

