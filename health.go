package main

import (
	"log"
	"net/http"
	"time"
)

var healthClient = &http.Client{Timeout: 1 * time.Second}

func isAlive(addr string) bool {
	resp, err := healthClient.Get("http://" + addr + "/")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func (n *Node) monitorChildren(interval time.Duration, maxFailures int) {
	failures := make(map[string]int)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		n.mu.RLock()
		children := append([]string(nil), n.childNodes...)
		n.mu.RUnlock()

		var dead []string
		for _, addr := range children {
			if isAlive(addr) {
				if failures[addr] > 0 {
					log.Printf("Health: %s is back", addr)
				}
				failures[addr] = 0
				continue
			}
			failures[addr]++
			log.Printf("Health: %s missed a check (%d/%d)", addr, failures[addr], maxFailures)
			if failures[addr] >= maxFailures {
				dead = append(dead, addr)
			}
		}

		if len(dead) > 0 {
			n.removeChildren(dead)
			for _, addr := range dead {
				delete(failures, addr)
			}
		}
	}
}

func (n *Node) removeChildren(dead []string) {
	isDead := make(map[string]bool)
	for _, addr := range dead {
		isDead[addr] = true
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	alive := make([]string, 0, len(n.childNodes))
	for _, addr := range n.childNodes {
		if !isDead[addr] {
			alive = append(alive, addr)
		}
	}
	n.childNodes = alive
	log.Printf("Health: removed %v, children are now %v", dead, alive)
}
