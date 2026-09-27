package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
)

// Entry point
// where execution begins
func main() {
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
		// Auto-detect routable addr
		addr, err := GetSelfAddress(*port)
		if err != nil {
			log.Fatalf("Failed to get self address: %v", err)
		}
		selfAddress = addr

	}

	// Turn the comma-separated child list into a []string slice ---
	var childNodeList []string
	if *childNodes != "" {
		childNodeList = strings.Split(*childNodes, ",")
	}

	node := NewNode(*isParent, parentNodeEnv, childNodeList, selfAddress)

	// A child must join the cluster before serving: register, then catch up
	if !node.isParent {
		if node.parentNode == "" {
			log.Fatal("Parent node address is not set")
		}
		// first, register so child can start receiving new writes
		if err := node.registerWithParent(); err != nil {
			log.Fatalf("Failed to register with parent node: %v", err)
		}
		// then sync to catch up on everything written before we joined.
		if err := node.synchronizeData(); err != nil {
			log.Fatalf("Failed to synchronize data: %v", err)
		}
	}

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
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Distributed-Database node is alive!")
	})
	// Replicate
	http.HandleFunc("/replicate", node.Replicate)
	http.HandleFunc("/setParent", node.SetParentNode)
	http.HandleFunc("/addChild", node.AddChildNode)

	fmt.Printf("Node running on port %s (Parent: %v, Parent Node: %s, Child Nodes: %v)\n", *port, *isParent, node.parentNode, childNodeList)
	fmt.Println("Self address detected as:", selfAddress)

	// Serve on the CONFIGURED port. log.Fatal prints the error and exits(1)
	// if ListenAndServe ever returns (it only returns on failure).
	log.Fatal(http.ListenAndServe(":"+*port, nil))
}
