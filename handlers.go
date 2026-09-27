package main

// Client-facing HTTP handlers: Get, Put, Delete, DisplayData, Replicate

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

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
