# 🌊 GoRaft-KV

A fault-tolerant, high-performance distributed Key-Value store built from scratch in **Go**, featuring a **Write-Ahead Log (WAL)** for durable disk persistence, **Raft consensus mechanisms** for leader election and state synchronization, and an interactive **Streamlit dashboard** for real-time monitoring and chaos testing.

---

## 🏛️ System Architecture

```text
                      +-----------------------------+
                      |   Streamlit Control Center  |
                      |    (Live Cluster Health)    |
                      +--------------+--------------+
                                     |  HTTP REST
                                     v
                 +-------------------+-------------------+
                 |                                       |
                 v                                       v
      +---------------------+                 +---------------------+
      |   Node 1 (Leader)   | <=== Heartbeat/ |  Node 2 (Follower)  |
      |   Port: 8081        |     Replication |  Port: 8082         |
      | +-----------------+ |                 | +-----------------+ |
      | | In-Memory Store | | ===> Sync ===>  | | In-Memory Store | |
      | +-----------------+ |                 | +-----------------+ |
      | | Write-Ahead Log | |                 | | Write-Ahead Log | |
      | +-----------------+ |                 | +-----------------+ |
      +---------------------+                 +---------------------+
                 ^                                       ^
                 |=========== Peer Consensus ===========|
                                     |
                                     v
                          +---------------------+
                          |  Node 3 (Follower)  |
                          |  Port: 8083         |
                          | +-----------------+ |
                          | | In-Memory Store | |
                          | +-----------------+ |
                          | | Write-Ahead Log | |
                          | +-----------------+ |
                          +---------------------+
🚀 Key Features
Write-Ahead Logging (WAL): Ensures strict persistence by flushing every mutate operation to disk (.wal) with fsync before modifying in-memory state, enabling instant state recovery after crashes.

Thread-Safe In-Memory Storage: High-throughput reads and writes powered by Go's sync.RWMutex.

Distributed Leader Election: Implements randomized election timeouts (1500ms–3000ms) to prevent split-vote scenarios, maintaining cluster quorum (> N/2).

Active State Replication: Replicates mutations across peer nodes via internal HTTP endpoints, ensuring high availability even during node failures.

Interactive Control Plane: Python Streamlit frontend featuring real-time node health monitoring, key-value mutations, and queries.

📂 Project Structure
Plaintext
GoRaft-KV/
├── cmd/
│   └── server/
│       └── main.go           # Server entry point, CLI flags, & HTTP routing
├── internal/
│   ├── raft/
│   │   └── consensus.go      # Raft state machine, timers, and RPC models
│   └── storage/
│       ├── engine.go         # Thread-safe KV engine & memory management
│       └── wal.go            # Write-Ahead Log implementation & disk sync
├── ui.py                     # Streamlit cluster health & testing dashboard
├── go.mod                    # Go module definitions
├── .gitignore                # Git ignore rules for WAL files
└── README.md                 # System documentation
⚙️ Getting Started
Prerequisites
Go (v1.21 or higher)

Python (v3.9 or higher)

1. Installation
Clone the repository:

Bash
git clone [https://github.com/kailash6207/GoRaft-KV.git](https://github.com/kailash6207/GoRaft-KV.git)
cd GoRaft-KV
Install Python dependencies for the UI:

Bash
pip install streamlit requests
🖥️ Running the Cluster
Open three separate terminal tabs and launch the cluster nodes:

Node 1 (Port 8081)
Bash
go run cmd/server/main.go -port=8081 -id=node1 -peers=http://localhost:8082,http://localhost:8083
Node 2 (Port 8082)
Bash
go run cmd/server/main.go -port=8082 -id=node2 -peers=http://localhost:8081,http://localhost:8083
Node 3 (Port 8083)
Bash
go run cmd/server/main.go -port=8083 -id=node3 -peers=http://localhost:8081,http://localhost:8082
📊 Launching the UI Dashboard
In a fourth terminal, launch the Streamlit frontend:

Bash
streamlit run ui.py
The control plane will automatically open at http://localhost:8501.

🧪 Testing & API Reference
Store a Key-Value Pair (Write)
Bash
Invoke-RestMethod -Uri "http://localhost:8081/put" -Method Post -ContentType "application/json" -Body '{"key":"system_check","value":"synchronized"}'
Retrieve a Value (Read)
Bash
Invoke-RestMethod -Uri "http://localhost:8081/get?key=system_check" -Method Get
Fault Tolerance & Chaos Testing
Store any key-value pair through the UI.

Terminate Node 1 in its terminal (Ctrl + C).

Refresh the UI: Node 1 will show as OFFLINE, but queries for the key will succeed without data loss by seamlessly routing to surviving nodes.

Developed by N.H. Kailash
