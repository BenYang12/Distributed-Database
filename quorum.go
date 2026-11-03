package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type readReply struct {
	value   string
	version uint64
	found   bool
	ok      bool
}

func (n *Node) replicaSet() ([]string, error) {
	n.mu.RLock()
	isParent, parent, self := n.isParent, n.parentNode, n.selfAddress
	children := append([]string(nil), n.childNodes...)
	n.mu.RUnlock()

	if isParent {
		return append([]string{self}, children...), nil
	}

	resp, err := peerClient.Get("http://" + parent + "/children")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(&children); err != nil {
		return nil, err
	}
	return append([]string{parent}, children...), nil
}

func (n *Node) quorumGet(w http.ResponseWriter, key string, readQuorum int) {
	members, err := n.replicaSet()
	if err != nil {
		http.Error(w, "Could not find replicas: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	if readQuorum > len(members) {
		http.Error(w, fmt.Sprintf("r=%d but only %d replicas", readQuorum, len(members)), http.StatusBadRequest)
		return
	}

	replies := make(chan readReply, len(members))
	for _, addr := range members {
		go func(addr string) {
			resp, err := peerClient.Get("http://" + addr + "/get?key=" + url.QueryEscape(key))
			if err != nil {
				replies <- readReply{}
				return
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			version, _ := strconv.ParseUint(resp.Header.Get("X-Version"), 10, 64)
			replies <- readReply{
				value:   strings.TrimSuffix(strings.TrimPrefix(string(body), "Value: "), "\n"),
				version: version,
				found:   resp.StatusCode == http.StatusOK,
				ok:      resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNotFound,
			}
		}(addr)
	}

	var best readReply
	answered := 0
	for i := 0; i < len(members) && answered < readQuorum; i++ {
		reply := <-replies
		if !reply.ok {
			continue
		}
		answered++
		if reply.found && (!best.found || reply.version > best.version) {
			best = reply
		}
	}

	if answered < readQuorum {
		http.Error(w, fmt.Sprintf("Only %d of %d required replicas answered", answered, readQuorum), http.StatusServiceUnavailable)
		return
	}
	if !best.found {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	w.Header().Set("X-Version", strconv.FormatUint(best.version, 10))
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Value: %s\n", best.value)
}
