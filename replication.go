package main

// fan-out helper functions

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
)

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
