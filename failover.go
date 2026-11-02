package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sort"
	"time"
)

func (n *Node) monitorParent(interval time.Duration, maxFailures int) {
	var peers []string
	failures := 0
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		n.mu.RLock()
		parent := n.parentNode
		n.mu.RUnlock()

		resp, err := healthClient.Get("http://" + parent + "/children")
		if err == nil {
			var children []string
			decodeErr := json.NewDecoder(resp.Body).Decode(&children)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK && decodeErr == nil {
				peers = children
				failures = 0
				continue
			}
		}

		failures++
		log.Printf("Failover: parent %s missed a check (%d/%d)", parent, failures, maxFailures)
		if failures < maxFailures {
			continue
		}

		var candidates []string
		for _, addr := range peers {
			if addr != parent {
				candidates = append(candidates, addr)
			}
		}
		if len(candidates) == 0 {
			continue
		}
		sort.Strings(candidates)
		newParent := candidates[0]
		peers = candidates
		failures = 0

		if newParent == n.selfAddress {
			n.promote(candidates[1:])
			return
		}

		log.Printf("Failover: following new parent %s", newParent)
		n.mu.Lock()
		n.parentNode = newParent
		n.mu.Unlock()
		if err := n.synchronizeData(); err != nil {
			log.Printf("Failover: sync with %s failed: %v", newParent, err)
		}
	}
}

func (n *Node) promote(children []string) {
	n.mu.Lock()
	n.isParent = true
	n.parentNode = ""
	n.childNodes = children
	n.mu.Unlock()

	log.Printf("Failover: promoted to parent, children are %v", children)
	go n.monitorChildren(2*time.Second, 3)
}
