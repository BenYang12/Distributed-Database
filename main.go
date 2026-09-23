package main

import (
	"bytes"         // bytes.NewBuffer: wrap JSON bytes as a request body
	"encoding/json" // decodes/encodes JSON <-> Go values
	"flag"          // command-line flag parsing
	"fmt"           // log.Fatal prints an error and exits
	"log"
	"net/http"
	"os"      // os.Getenv: for reading env vars
	"strings" // strings.Split: parse comma-separated child list
	"sync"    // gives sync.RWMutex, which allows many goroutines to read data at same time, but gives only one goroutine exclusive access when it needs to write
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

// Get handles read requests: GET /get?key=...
// The (n *Node) receiver binds this METHOD to a specific node
func (n *Node) Get(w http.ResponseWriter, r *http.Request){
	// r.URL.Query() parses "?key=..." part of URL into a lookup table
	key := r.URL.Query().Get("key")

	if key == "" {
		// http.Error writes an error message AND sets the HTTP status code,
		// then we return early so we don't keep processing a bad request.
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
func (n *Node) Put(w http.ResponseWriter, r *http.Request){
	// decode JSON body into a Go map
	var body map[string]string

    // json.NewDecoder(r.Body) wraps request body in a JSON decoder
	// .Decode(&body) reads JSON and fills in body (POINTER)
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
	fmt.Fprintf(w,"Stored: %s -> %s\n", key, value)

}

// Delete 
// handles: DELETE /delete?key=...
func (n *Node) Delete(w http.ResponseWriter, r *http.Request){
	// Go's default router filters by path, not HTTP method
	// http.MethodDelete is just constant string "DELETE."
	if r.Method != http.MethodDelete{
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// key comes from URL (?key=...)
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing key in request", http.StatusBadRequest)
		return
	}

	// Deleting mutates map -> needs WRITE lock
	n.mu.Lock()

	// delete() is go Builtin. 
	// if key isn't in map, this is a harmless no-op
	delete(n.data,key)
	n.mu.Unlock()

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
func (n *Node) Replicate(w http.ResponseWriter, r *http.Request){
	// Only children accept replicated data
	if n.isParent{
		http.Error(w, "Parent node cannot receive replication data!", http.StatusBadRequest)
		return
	}

	// Put
	// 1. decode body (JSON -> Go object)
	// 2. validate
	// 3. store
	var body map[string]string
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil{
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
func (n *Node) replicateToChildren(key, value string){
	// iterate over each child address
	// range gives (index, value)
	for _, childAddr := range(n.childNodes){
		// go func(...){...}(childaddr) launches an immediately invoked anonymous function in a NEW goroutine.
		go func(addr string){
			// build JSON body
			replicationData := map[string]string{"key": key, "value": value}
			// json.Marhsal returns (bytes, error)
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


// where execution begins
func main(){
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
		selfAddress = "localhost:" + *port 
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
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request){
		fmt.Fprintln(w, "Distributed-Database node is alive!")
	})
	// Replicate
	http.HandleFunc("/replicate", node.Replicate)



	fmt.Printf("Node running on port %s (Parent: %v, Parent Node: %s, Child Nodes: %v)\n", *port, *isParent, node.parentNode, childNodeList)

	// Serve on the CONFIGURED port. log.Fatal prints the error and exits(1)
	// if ListenAndServe ever returns (it only returns on failure).
	log.Fatal(http.ListenAndServe(":"+*port, nil))
}