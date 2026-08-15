# Durable Workflow Engine

A single-node reference implementation of a **durable workflow orchestration engine** with **exactly-once step execution**, **Saga-pattern compensation**, and **crash-recovery guarantees**.

Built in **Go** to demonstrate the low-level primitives that production workflow orchestrators (Temporal, Cadence, Durable Functions, etc.) abstract away.

---

## Features

- Executes multi-step workflows with configurable per-step retry policies.
- Supports exponential backoff with jitter for transient failures.
- Guarantees idempotent execution using SHA-256 deduplication keys derived from deterministic step inputs.
- Recovers from worker crashes (`SIGKILL`) by replaying an append-only PostgreSQL event log without double-executing completed steps.
- Implements reverse-order Saga compensation with durable checkpointing before and after every compensation step.
- Provides workflow inspection through both a CLI and lightweight HTTP dashboard.
- Exposes Prometheus metrics for monitoring and benchmarking.

---

## Tech Stack

| Layer | Technology | Packages |
|-------|------------|----------|
| Language | Go 1.22+ | `go` |
| Database | PostgreSQL 15+ | `jackc/pgx/v5`, `database/sql` |
| Queue | Redis 7 Streams | `redis/go-redis/v9` |
| CLI | Cobra | `spf13/cobra` |
| HTTP Dashboard | Go stdlib | `net/http`, `html/template` |
| Metrics | Prometheus | `prometheus/client_golang` |
| Testing | Testify, Testcontainers | `stretchr/testify`, `testcontainers-go` |
| Migrations | golang-migrate | `golang-migrate/migrate` |

---

# Architecture

```text
+-------------+     +------------------+     +-----------------+
|   Client    |---->|  Redis Streams   |---->|  Worker Pool    |
| (CLI / API) |     |  (Step Dispatch) |     |  (Go Routines)  |
+-------------+     +------------------+     +--------+--------+
                                                        |
                                                        v
                                               +-----------------+
                                               |  PostgreSQL     |
                                               |  Append-Only    |
                                               |   Event Log     |
                                               +-----------------+
```

---

## Execution Flow

```
1. Worker pulls a step from Redis Streams.
2. Computes the deterministic deduplication key.
3. Checks the PostgreSQL event log for prior completion.
4. Executes business logic only if no completion exists.
5. Persists completion to the append-only event log.
6. ACKs the Redis Stream message.
```

### Crash Safety

If a worker crashes **between writing the event log and acknowledging the queue**, the replacement worker:

1. Replays the event log.
2. Detects the completed step using the deduplication key.
3. Skips business execution.
4. Continues with the next workflow step.

This guarantees **exactly-once step execution** despite worker failures.

---

# Fault Injection Results

The `cmd/faultinject` harness crashes workers with `SIGKILL` at each of six execution points and verifies exactly-once behavior from the resulting event log. The crash is triggered in-process — a checkpoint (`internal/faultinject.Crash`) called at each named point self-terminates the worker the instant it's reached when `FAULT_INJECT` matches, so the timing is deterministic rather than guessed from outside via a delay. A replacement worker then reclaims the crashed worker's unacked message (or, for the compensation points, resumes the same workflow ID) and the harness checks the event log for exactly one completion — never zero (a lost step), never more than one (a double execution).

Measured results, 10 runs per point (60 total), against Postgres 15 + Redis 7 in Docker on an Apple M2 / 8 GB:

| Injection Point | Runs | Passed | Double Execution | Lost Steps |
|-----------------|----:|------:|:----------------:|:----------:|
| Before step execution | 10 | 10 | ❌ | ❌ |
| During step execution | 10 | 10 | ❌ | ❌ |
| After step completion, before event log write | 10 | 10 | ❌ | ❌ |
| After event log write, before queue ACK | 10 | 10 | ❌ | ❌ |
| During compensation | 10 | 10 | ❌ | ❌ |
| During final compensation step | 10 | 10 | ❌ | ❌ |
| **Total** | **60** | **60** | **0** | **0** |

Reproduce:

```bash
docker compose up -d postgres redis
go run ./cmd/migrate up
go build -o /tmp/worker ./cmd/worker
go build -o /tmp/faultinject ./cmd/faultinject
/tmp/faultinject -worker /tmp/worker -runs 10
```

`-runs 80` (480 total, matching the original target) works too, but each run opens its own short-lived Postgres connections from a fresh worker subprocess; at high concurrency against a single default-configured Postgres container this can exhaust `max_connections` and produce connection errors unrelated to the recovery logic itself. Lower `-runs`, or raise Postgres's `max_connections`, if you see that.

## Concurrent contention

The crash tests above prove exactly-once for one crash at a time. The harness's concurrent mode instead races multiple live worker processes (same Redis stream, same consumer group) for a single step message and checks the event log the same way — exactly one completion, no double executions, no lost steps. This proves the dedup-key idempotency holds under real concurrent load, not just crash recovery.

Measured: 10 runs, 4 workers each racing for one message, same environment as above — 10/10 exactly-one-completion, 0 double executions, 0 lost steps.

```bash
/tmp/faultinject -worker /tmp/worker -concurrent-workers 4 -concurrent-runs 10
```

### Verified Behavior

When the worker is terminated **after the event log write but before the Redis ACK**, the replacement worker reclaims the pending message, recognizes the completed step via its deduplication key, and advances the workflow without re-running the business logic. When it's terminated **mid-compensation**, a second run against the same workflow ID resumes and completes only the compensating steps that hadn't yet finished.

---

# Benchmarks

**Environment** (as measured for the numbers below — re-run `go test -bench` on your own hardware for your own numbers, see below)

- Apple M2, 8 GB RAM
- PostgreSQL 15, Redis 7 (Docker, default container resource limits)
- Go 1.25, `GOMAXPROCS=8`

| Benchmark | Result |
|-----------|--------|
| Step throughput, sequential (5-step workflow) | **962 steps/sec** |
| Step throughput, concurrent (`b.RunParallel`, GOMAXPROCS=8) | **2,884 steps/sec** |
| Workflow latency (5 steps, p50 / p99) | **4.4 ms / 19.1 ms** |
| Single-step latency via queue path (`ProcessStep`, p50 / p99) | **0.62 ms / 1.23 ms** |
| Recovery time (SIGKILL → step resumed and completed) | **~90 ms** (steady state; first run after a cold binary load was ~540 ms) |
| Event log write latency, avg (`AppendEvent`) | **0.30 ms** |
| Idempotency lookup, avg (`HasCompleted`) | **0.14 ms** |

These come from three different sources, not one uniform harness:
- Step throughput, workflow latency, and single-step latency are `go test -bench` output (`internal/engine`), run against a real Postgres connection — no mock.
- Event log write and idempotency lookup latency are `go test -bench` output (`internal/store`), same real connection.
- Recovery time isn't something a Go benchmark can measure (it spans two separate process lifetimes with a `SIGKILL` in between) — it's a shell loop that produces a step, crashes a worker on it via `-fault-saga`-style injection, starts a replacement, and times wall-clock from crash to the `step_completed` event landing. It's the mean of 5 runs, excluding the first (which pays one-time OS binary-load cost, not recovery cost).

Run the two Go benchmark suites yourself:

```bash
docker compose up -d postgres redis
go run ./cmd/migrate up
go test -run=^$ -bench=. -benchtime=3s ./internal/engine/... ./internal/store/...
```

They skip (not fail) if Postgres isn't reachable at `localhost:15432` with the default `docker-compose.yml` credentials; point them elsewhere with `BENCH_POSTGRES_DSN`.

---

# Quick Start

### 1. Start dependencies

```bash
docker compose up -d
```

### 2. Run database migrations

```bash
go run ./cmd/migrate up
```

### 3. Start the worker

```bash
go run ./cmd/worker
```

### 4. Submit a workflow

```bash
go run ./cmd/cli create --file ./examples/order-saga.json
```

### 5. Inspect workflow state

```bash
go run ./cmd/cli status --id <workflow-id>
```

### 6. View Prometheus metrics

```
http://localhost:8080/metrics
```

---

# Design Decisions

## Append-Only Event Log

Instead of mutating workflow rows with a `status` column, every state transition is appended to an immutable event log.

**Benefits**

- Crash-safe state reconstruction
- Deterministic replay
- Complete execution history
- Auditability
- Read-model projection support

The event log is the **source of truth**, while workflow tables are treated as projections.

---

## Idempotency Before Execution

Before executing any step, the engine queries the event log using a deterministic SHA-256 deduplication key.

If a matching completion already exists:

- Business logic is skipped.
- The workflow proceeds to the next step.

This makes retries and crash recovery safe by construction.

---

## Durable Saga Compensation

Compensation operations are modeled as first-class workflow steps.

Each compensation records:

1. Compensation started
2. Compensation completed

Both events are durably persisted.

If a worker crashes during rollback, the next worker resumes compensation from the exact unfinished step.

---

# Why Build This?

Production workflow engines like **Temporal**, **Cadence**, **Celery**, and **Asynq** hide significant distributed systems complexity.

This project is **not** intended to replace them.

Instead, it demonstrates an understanding of the underlying primitives:

- Durable execution
- Exactly-once processing
- Event sourcing
- Idempotency
- Crash recovery
- Saga compensation
- Deterministic replay

Understanding these mechanisms makes it easier to reason about how production orchestration frameworks operate internally.

---

# Project Structure

```text
.
├── cmd/
│   ├── worker/          # Worker pool daemon
│   ├── cli/             # CLI inspection tool
│   ├── migrate/         # Database migrations
│   └── faultinject/     # SIGKILL fault injection harness
│
├── internal/
│   ├── engine/          # Workflow execution engine
│   ├── store/           # PostgreSQL event log
│   ├── queue/           # Redis Streams consumer
│   ├── saga/            # Compensation engine
│   └── idempotency/     # Deduplication key generation
│
├── examples/            # Sample workflow definitions
├── docker-compose.yml
└── README.md
```

---

# License

MIT License
