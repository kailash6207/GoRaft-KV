package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// RequestVoteArgs is the payload sent during leader election.
type RequestVoteArgs struct {
	Term        int    `json:"term"`
	CandidateID string `json:"candidate_id"`
}

// RequestVoteReply is the response to a vote request.
type RequestVoteReply struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"vote_granted"`
}

// AppendEntriesArgs is used for both heartbeats and log replication.
type AppendEntriesArgs struct {
	Term     int    `json:"term"`
	LeaderID string `json:"leader_id"`
}

// AppendEntriesReply is the response from a follower to a heartbeat.
type AppendEntriesReply struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
}

// RaftNode represents an active member of the consensus group.
type RaftNode struct {
	mu          sync.Mutex
	ID          string
	Peers       []string // URLs of peer nodes, e.g., ["http://localhost:8082", "http://localhost:8083"]
	CurrentTerm int
	VotedFor    string
	State       State

	heartbeatTimer *time.Ticker
	electionTimer  *time.Timer
}

func NewRaftNode(id string, peers []string) *RaftNode {
	r := &RaftNode{
		ID:          id,
		Peers:       peers,
		CurrentTerm: 0,
		VotedFor:    "",
		State:       Follower,
	}
	r.resetElectionTimer()
	return r
}

func (r *RaftNode) resetElectionTimer() {
	if r.electionTimer != nil {
		r.electionTimer.Stop()
	}
	// Randomized timeout between 1500ms and 3000ms to avoid split votes
	d := time.Duration(1500+rand.Intn(1500)) * time.Millisecond
	r.electionTimer = time.AfterFunc(d, r.startElection)
}

func (r *RaftNode) startElection() {
	r.mu.Lock()
	r.State = Candidate
	r.CurrentTerm++
	r.VotedFor = r.ID
	currentTerm := r.CurrentTerm
	peers := make([]string, len(r.Peers))
	copy(peers, r.Peers)
	r.resetElectionTimer()
	r.mu.Unlock()

	fmt.Printf("[%s] Election triggered for Term %d\n", r.ID, currentTerm)

	votes := 1
	var voteMu sync.Mutex

	for _, peer := range peers {
		go func(peerURL string) {
			args := RequestVoteArgs{
				Term:        currentTerm,
				CandidateID: r.ID,
			}
			data, _ := json.Marshal(args)
			resp, err := http.Post(peerURL+"/raft/request-vote", "application/json", bytes.NewBuffer(data))
			if err != nil {
				return
			}
			defer resp.Body.Close()

			var reply RequestVoteReply
			if err := json.NewDecoder(resp.Body).Decode(&reply); err == nil && reply.VoteGranted {
				voteMu.Lock()
				votes++
				totalNodes := len(peers) + 1
				if votes > totalNodes/2 && r.State == Candidate {
					r.becomeLeader()
				}
				voteMu.Unlock()
			}
		}(peer)
	}
}

func (r *RaftNode) becomeLeader() {
	r.mu.Lock()
	r.State = Leader
	if r.electionTimer != nil {
		r.electionTimer.Stop()
	}
	r.mu.Unlock()

	fmt.Printf(">>> [%s] BECAME LEADER for Term %d <<<\n", r.ID, r.CurrentTerm)
	r.sendHeartbeats()
	r.heartbeatTimer = time.NewTicker(500 * time.Millisecond)
	go func() {
		for range r.heartbeatTimer.C {
			r.mu.Lock()
			if r.State != Leader {
				r.mu.Unlock()
				return
			}
			r.mu.Unlock()
			r.sendHeartbeats()
		}
	}()
}

func (r *RaftNode) sendHeartbeats() {
	r.mu.Lock()
	currentTerm := r.CurrentTerm
	leaderID := r.ID
	peers := make([]string, len(r.Peers))
	copy(peers, r.Peers)
	r.mu.Unlock()

	for _, peer := range peers {
		go func(peerURL string) {
			args := AppendEntriesArgs{
				Term:     currentTerm,
				LeaderID: leaderID,
			}
			data, _ := json.Marshal(args)
			resp, err := http.Post(peerURL+"/raft/append-entries", "application/json", bytes.NewBuffer(data))
			if err != nil {
				return
			}
			resp.Body.Close()
		}(peer)
	}
}

// HandleRequestVote handles incoming RequestVote RPCs.
func (r *RaftNode) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	r.mu.Lock()
	defer r.mu.Unlock()

	if args.Term > r.CurrentTerm {
		r.CurrentTerm = args.Term
		r.State = Follower
		r.VotedFor = ""
	}

	reply := RequestVoteReply{Term: r.CurrentTerm, VoteGranted: false}
	if args.Term == r.CurrentTerm && (r.VotedFor == "" || r.VotedFor == args.CandidateID) {
		r.VotedFor = args.CandidateID
		reply.VoteGranted = true
		r.resetElectionTimer()
		fmt.Printf("[%s] Voted for %s in Term %d\n", r.ID, args.CandidateID, args.Term)
	}

	return reply
}

// HandleAppendEntries handles incoming AppendEntries RPCs (Heartbeats).
func (r *RaftNode) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	r.mu.Lock()
	defer r.mu.Unlock()

	if args.Term < r.CurrentTerm {
		return AppendEntriesReply{Term: r.CurrentTerm, Success: false}
	}

	if args.Term > r.CurrentTerm || r.State == Candidate {
		r.CurrentTerm = args.Term
		r.State = Follower
		r.VotedFor = ""
	}

	r.resetElectionTimer()
	return AppendEntriesReply{Term: r.CurrentTerm, Success: true}
}