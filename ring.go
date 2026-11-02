package main

import (
	"fmt"
	"hash/crc32"
	"net/http"
	"sort"
)

type Ring struct {
	hashes []uint32
	owners map[uint32]string
}

func NewRing(shards []string, replicas int) *Ring {
	r := &Ring{owners: make(map[uint32]string)}
	for _, shard := range shards {
		for i := 0; i < replicas; i++ {
			h := crc32.ChecksumIEEE([]byte(fmt.Sprintf("%s#%d", shard, i)))
			r.hashes = append(r.hashes, h)
			r.owners[h] = shard
		}
	}
	sort.Slice(r.hashes, func(i, j int) bool { return r.hashes[i] < r.hashes[j] })
	return r
}

func (r *Ring) Owner(key string) string {
	h := crc32.ChecksumIEEE([]byte(key))
	i := sort.Search(len(r.hashes), func(i int) bool { return r.hashes[i] >= h })
	if i == len(r.hashes) {
		i = 0
	}
	return r.owners[r.hashes[i]]
}

func (n *Node) redirectToOwner(w http.ResponseWriter, r *http.Request, key string) bool {
	if n.ring == nil {
		return false
	}

	n.mu.RLock()
	myShard := n.parentNode
	if n.isParent {
		myShard = n.selfAddress
	}
	n.mu.RUnlock()

	owner := n.ring.Owner(key)
	if owner == myShard {
		return false
	}
	http.Redirect(w, r, "http://"+owner+r.URL.RequestURI(), http.StatusTemporaryRedirect)
	return true
}
