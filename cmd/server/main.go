package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"

	"raft-kv-store/internal/raft"
	"raft-kv-store/internal/storage"
)

type PutRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func main() {
	port := flag.String("port", "8081", "Port to run the HTTP server on")
	nodeID := flag.String("id", "node1", "Unique node identifier")
	peersFlag := flag.String("peers", "", "Comma-separated peer URLs")
	flag.Parse()

	var peers []string
	if *peersFlag != "" {
		peers = strings.Split(*peersFlag, ",")
	}

	// 1. Initialize Storage Engine
	walPath := fmt.Sprintf("%s_data.wal", *nodeID)
	engine, err := storage.NewKVEngine(walPath)
	if err != nil {
		log.Fatalf("Failed to initialize storage: %v", err)
	}

	// 2. Initialize Raft Consensus Node
	raftNode := raft.NewRaftNode(*nodeID, peers)

	// --- Raft RPC Endpoints ---
	http.HandleFunc("/raft/request-vote", func(w http.ResponseWriter, r *http.Request) {
		var args raft.RequestVoteArgs
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		reply := raftNode.HandleRequestVote(args)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(reply)
	})

	http.HandleFunc("/raft/append-entries", func(w http.ResponseWriter, r *http.Request) {
		var args raft.AppendEntriesArgs
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		reply := raftNode.HandleAppendEntries(args)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(reply)
	})

	// --- INTERNAL: Replication Endpoint ---
	// Nodes call this on each other to keep their databases perfectly synced
	http.HandleFunc("/replicate", func(w http.ResponseWriter, r *http.Request) {
		var req PutRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}

		// Save the replicated data locally
		engine.Put(req.Key, req.Value)
		w.WriteHeader(http.StatusOK)
	})

	// --- Client KV Endpoints ---
	http.HandleFunc("/put", func(w http.ResponseWriter, r *http.Request) {
		var req PutRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}

		// 1. Save Locally
		if err := engine.Put(req.Key, req.Value); err != nil {
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}

		// 2. Broadcast (Replicate) to all other nodes in the background
		for _, peer := range peers {
			go func(peerURL string) {
				data, _ := json.Marshal(req)
				// Send the data to the peer's /replicate endpoint
				http.Post(peerURL+"/replicate", "application/json", bytes.NewBuffer(data))
			}(peer)
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "Successfully stored %s\n", req.Key)
	})

	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		val, err := engine.Get(key)
		if err != nil {
			http.Error(w, "Key not found", http.StatusNotFound)
			return
		}
		fmt.Fprintf(w, "%s\n", val)
	})

	fmt.Printf("Cluster Node %s running on port :%s...\n", *nodeID, *port)
	log.Fatal(http.ListenAndServe(":"+*port, nil))
}