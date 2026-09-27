package main

// membership and addressing

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
)

// GetSelfAddress() finds an address other nodes can actually reach this one at, and appends the given port.
// Returns (address, nil) or ("", error)
func GetSelfAddress(port string) (string, error) {
	// InterfaceAddrs returns ALL of this machine's network addresses.
	// It returns (addrs, error)
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	var ipAddr string
	for _, addr := range addrs {
		// addr is a net.Addr (INTERFACE). Its concrete type is *net.IPNet.
		// TYPE ASSERTION asks "is concrete value behind addr a *net.IPNet?"
		//   ipNet -> the concrete value if yes
		//   ok    -> false if not (then we skip; no panic)
		// Also require it not to be a loopback address
		if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			// To4() returns non-nill only for IPv4 addresses. take first IPv4, non-loopback address and stop
			if ipNet.IP.To4() != nil {
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

// registerWithParent: tell parent "add me as a child"
// returns error (nil on success)
func (n *Node) registerWithParent() error {
	if n.selfAddress == "" {
		return fmt.Errorf("self address is not set")
	}

	// Body: {"childNode": "<my address>"}
	registrationData := map[string]string{"childNode": n.selfAddress}
	jsonData, _ := json.Marshal(registrationData) // json.Marshal: Go -> JSON

	// POST to parent's /addChild endpoint
	resp, err := http.Post("http://"+n.parentNode+"/addChild", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("registration failed: %s", string(bodyBytes))
	}
	return nil // success
}

// synchronizeData pulls parent's ENTIRE dataset and adopts it
// for freshly joined/lagging children
func (n *Node) synchronizeData() error {
	resp, err := http.Get("http://" + n.parentNode + "/display")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to synchronize data: %s", string(bodyBytes))
	}

	// Decode parent's whole map
	var data map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return err
	}

	// REPLACE local map under write lock
	n.mu.Lock()
	n.data = data
	n.mu.Unlock()
	return nil
}

// AddChildNode is Parent's endpoint: POST /addChild {"childNode": "<addr>"}
// A child calls this (via registerWithParent) to join the replication set
func (n *Node) AddChildNode(w http.ResponseWriter, r *http.Request) {
	if !n.isParent {
		http.Error(w, "Only parent nodes can add child nodes!", http.StatusBadRequest)
		return
	}

	var body map[string]string
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	newChild, ok := body["childNode"]
	if !ok || newChild == "" {
		http.Error(w, "Missing 'childNode' in request", http.StatusBadRequest)
		return
	}

	// append grows slice by one, returning new slice
	// do this under write lock
	n.mu.Lock()
	n.childNodes = append(n.childNodes, newChild)
	n.mu.Unlock()

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Child node added: %s\n", newChild)

}

// SetParentNode: POST /setParent {"parentNode": "<addr>"}
// Lets running child switch to a new parent at runtime
// building block for failover
func (n *Node) SetParentNode(w http.ResponseWriter, r *http.Request) {

	if n.isParent {
		http.Error(w, "Parent nodes cannot have a parent", http.StatusBadRequest)
		return
	}

	var body map[string]string
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// extract from go map
	newParent, ok := body["parentNode"]
	if !ok || newParent == "" {
		http.Error(w, "Missing 'parentNode' in request", http.StatusBadRequest)
		return
	}

	n.mu.Lock()
	n.parentNode = newParent
	n.mu.Unlock()

	// Announce to parent
	if err := n.registerWithParent(); err != nil {
		http.Error(w, "Failed to register with parent node: "+err.Error(), http.StatusInternalServerError)
		return
	}
	// synchronize Data
	if err := n.synchronizeData(); err != nil {
		http.Error(w, "Failed to synchronize data: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Parent node updated to: %s\n", newParent)

}
