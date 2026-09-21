package main

import (
	"fmt"
	"net/http"
	"sync" // gives sync.RWMutex, which allows many goroutines to read data at same time, but gives only one goroutine exclusive access when it needs to write
)

// Node represents single server in distributed database.
// Every running instance of this program IS one Node.
type Node struct{
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
		data: make(map[string]string),
		isParent: isParent,
		parentNode: parentNode,
		childNodes: childNodes,
		selfAddress: selfAddress,
		// mu's zero value is a usable unlocked mutex -> don't set it 


	}
}




// where execution begins
func main(){

	// http.HandleFunc registers "handler" for URL path.
	//    w http.ResponseWriter -> I WRITE my response into this.
	//    r *http.Request -> the incoming request I READ from
	// r is a pointer to a request


	// Fprintln writes text to first arg -> w is response so text goes back to caller
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request){
		fmt.Fprintln(w, "Distributed-Database node is alive!")
	})

	fmt.Println("Listening on http://localhost:8080")

	// ListenAndServe starts web server and blocks here forever, 
	// handling requests, until program is stopped or it errors.
	// nil means "use default router (one HandleFunc registered on)"
	http.ListenAndServe(":8080", nil)


}