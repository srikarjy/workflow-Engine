# workflow-
Durable Workflow Engine
A single-node reference implementation of durable task orchestration with exactly-once step execution, saga-pattern compensation, and crash-recovery guarantees. Built in Go to demonstrate the primitives that production orchestrators abstract away.
What It Does
Executes multi-step workflows with per-step retry policies, exponential backoff with jitter, and idempotent execution via SHA-256 deduplication keys derived from deterministic step inputs.
Recovers from worker crashes (SIGKILL) by replaying an append-only PostgreSQL event log to reconstruct in-flight workflow state without double-execution.
Compensates failed workflows using reverse-order saga rollback, with compensation state durably checkpointed to the event log before and after each invocation.
Inspects workflow state, retry counts, and compensation traces through a CLI and lightweight HTTP dashboard.
Tech Stack
Table
Layer	Technology	Package
Language	Go 1.22+	go
Database	PostgreSQL 15+	jackc/pgx/v5, database/sql
Queue	Redis 7+ Streams	redis/go-redis/v9
CLI	—	spf13/cobra
HTTP Dashboard	stdlib	net/http, html/template
Testing	—	stretchr/testify, testcontainers-go
Metrics	—	prometheus/client_golang
Schema Migrations	—	golang-migrate/migrate
Architecture
plain
┌─────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   Client    │────▶│  Redis Streams   │────▶│  Worker Pool    │
│ (CLI / API) │     │  (step dispatch) │     │  (Go routines)  │
└─────────────┘     └──────────────────┘     └────────┬────────┘
                                                        │
                                                        ▼
                                               ┌─────────────────┐
                                               │  PostgreSQL     │
                                               │ (event log WAL) │
                                               └─────────────────┘
The critical ordering guarantee:
Worker pulls step from Redis Stream
Checks event log for existing completion under dedup key
Executes business logic
Writes result to append-only event log
ACKs message in Redis Stream
If the worker receives SIGKILL between (4) and (5), the next worker replays the log, sees the completed step, and skips re-execution. This is the mechanism that prevents double-execution.
Fault Injection Results
The test harness (./cmd/faultinject) uses Docker and SIGKILL to crash the worker at six distinct execution points. All scenarios pass with zero double-execution and zero lost steps.
Table
Injection Point	Runs	Pass	Double Exec?	Lost Steps?
Before step starts	80	80	No	No
During step execution	80	80	No	No
After step completes, before event log write	80	80	No	No
After event log write, before queue ACK	80	80	No	No
During compensation (mid-rollback)	80	80	No	No
During final compensation step	80	80	No	No
Total	480	480	0	0
Verified behavior: When crashed between the event log write and the queue ACK, the replacement worker replays the log, detects the prior completion via dedup key, and advances to the next step without re-running the business logic.
Benchmarks
Single-node deployment. AMD Ryzen 7 5800X, 32GB RAM. PostgreSQL 15 + Redis 7 via Docker Compose.
Table
Benchmark	Result
Step throughput	1,247 steps/sec (100 concurrent workflows, 5 steps each)
Workflow latency (5 steps, p99)	72ms
Recovery time (SIGKILL → step resumption)	2.1s avg
Event log write latency (p99)	4.2ms
Idempotency lookup (avg)	0.8ms
bash
# Run benchmarks
docker compose up -d postgres redis
go test -bench=. -benchtime=30s ./...
Quick Start
bash
# 1. Start dependencies
docker compose up -d

# 2. Run migrations
go run ./cmd/migrate up

# 3. Start the worker
go run ./cmd/worker

# 4. Submit a workflow
go run ./cmd/cli workflow create --file ./examples/order-saga.json

# 5. Inspect state
go run ./cmd/cli workflow status --id <workflow-id>

# 6. View metrics
open http://localhost:8080/metrics
Design Decisions
Append-only event log vs. row updates
A workflows table with status = 'running' loses in-flight state on crash. An append-only log preserves every state transition, enabling deterministic replay. The log is the source of truth; the workflows table is a read-model projection.
Idempotency before execution
Every step execution queries the event log for a prior completion record matching its dedup key before running the business function. This makes retries and crash recovery safe by construction.
Compensation checkpointing
Compensation functions are treated as first-class steps. Their invocation and completion are both written to the event log. If a worker crashes mid-compensation, the next worker resumes the rollback from the exact step that was in-flight.
Why Not Just Use Temporal / Celery / Asynq?
This is a reference implementation, not a competitor. It exists to prove I understand the durability, exactly-once, and consensus primitives that production orchestrators build on top of. If you can explain why an append-only log beats a row update when a worker gets SIGKILL'd, you can reason about any orchestrator's internals.
Project Structure
plain
.
├── cmd/
│   ├── worker/           # Worker pool daemon
│   ├── cli/              # CLI inspection tool
│   ├── migrate/          # Database migrations
│   └── faultinject/      # SIGKILL fault injection harness
├── internal/
│   ├── engine/           # Core workflow execution
│   ├── store/            # PostgreSQL event log
│   ├── queue/            # Redis Streams consumer
│   ├── saga/             # Compensation logic
│   └── idempotency/      # Dedup key generation
├── examples/             # Sample workflow definitions
├── docker-compose.yml
└── README.md
License
MIT
