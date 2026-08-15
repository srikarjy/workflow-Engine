# Durable Workflow Engine

A single-node reference implementation of a **durable workflow orchestration engine** with **exactly-once step execution**, **Saga-pattern compensation**, and **crash-recovery guarantees**.

Built in **Go** to demonstrate the low-level primitives that production workflow orchestrators (Temporal, Cadence, Durable Functions, etc.) abstract away.

---

## Features

- Define workflows as **YAML**, no Go required: each step is an HTTP call against your own services (see [Workflow Definitions](#workflow-definitions)).
- Executes multi-step workflows with configurable per-step retry policies, exponential backoff, and jitter for transient failures (`internal/httpstep`).
- Guarantees idempotent execution using SHA-256 deduplication keys derived from deterministic step inputs.
- Recovers from worker crashes (`SIGKILL`) by replaying an append-only PostgreSQL event log without double-executing completed steps.
- Implements reverse-order Saga compensation: on failure, each completed step's real rollback HTTP call is invoked (not just logged), most-recent-first, with durable checkpointing before and after every compensation step.
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
| Testing | Go stdlib `testing`, in-memory fakes | `internal/*/fake_store_test.go` |
| Migrations | golang-migrate | `golang-migrate/migrate` |
| Workflow definitions | YAML | `gopkg.in/yaml.v3` |

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

### 3. Start the worker (dashboard + metrics)

```bash
go run ./cmd/worker
```

### 4. Start a target service and run the example workflow

`examples/order-saga.yaml` calls out to a small demo HTTP service
(`cmd/exampleservice`) that stands in for your real order-fulfillment
endpoints — swap its URLs for your own services to run a real workflow.

```bash
go run ./cmd/exampleservice &
go run ./cmd/cli run --workflow examples/order-saga.yaml --input examples/order-input.json
```

### 5. Inspect workflow state

```bash
go run ./cmd/cli status --id <workflow-id>
```

### 6. View the dashboard and Prometheus metrics

The worker serves both on the address given by `-http` (default `:8080`):

```
http://localhost:8080/               # workflow list
http://localhost:8080/workflows/<id> # workflow detail: status + full event log
http://localhost:8080/metrics        # Prometheus metrics
```

Metrics exported: `workflow_steps_started_total`, `workflow_steps_completed_total`,
`workflow_steps_failed_total`, `workflow_steps_skipped_duplicate_total`,
`workflow_step_duration_seconds`, `workflow_compensations_started_total`,
`workflow_compensations_completed_total` — all labeled by step name, alongside
the standard Go runtime collectors.

### Securing a public deployment

By default the dashboard and `/metrics` are open (fine for `localhost`).
Before exposing the worker's `-http` port publicly, set:

```bash
export WORKFLOW_DASHBOARD_TOKEN=<some-random-secret>
go run ./cmd/worker  # -auth-token also works if you'd rather not use an env var
```

Every request then requires `Authorization: Bearer <token>` or gets `401`.
A shared, global rate limit (`-rate-limit-rps` / `-rate-limit-burst`,
default 5 rps / burst 20 across all clients combined) returns `429` once
exceeded, so a public URL can't be hammered into runaway compute or
database load. Leaving `WORKFLOW_DASHBOARD_TOKEN` unset logs a startup
warning rather than failing silently.

---

# Deploying

The included `Dockerfile` builds `cmd/worker` (the deployable service: the
worker pool plus the dashboard/metrics HTTP server) and applies migrations
automatically on container start via `docker-entrypoint.sh`, so there's
nothing to run manually before the app boots.

It reads its configuration entirely from environment variables, so it
deploys against **free-tier managed Postgres and Redis** with no self-hosted
database to pay for:

| Variable | Purpose |
|---|---|
| `DATABASE_URL` | Postgres connection string (e.g. from [Neon](https://neon.tech)) |
| `REDIS_URL` | Redis connection string — `redis://` or `rediss://` with embedded auth (e.g. from [Upstash](https://upstash.com)) |
| `WORKFLOW_DASHBOARD_TOKEN` | Bearer token required on every dashboard/`/metrics` request — **set this before deploying publicly** |
| `PORT` | HTTP port to bind (most PaaS hosts, e.g. Render, set this automatically) |

`render.yaml` is a ready-to-import [Render Blueprint](https://render.com/docs/blueprint-spec)
targeting Render's free web service plan (no credit card required; the
service spins down after 15 minutes idle and cold-starts on the next
request — the tradeoff for $0 hosting). `/healthz` is unauthenticated
specifically so the platform's health check doesn't need the bearer token.

```bash
# Local sanity check before deploying anywhere:
docker build -t workflow-engine .
docker run --rm -p 8080:8080 \
  -e DATABASE_URL=<your-postgres-url> \
  -e REDIS_URL=<your-redis-url> \
  -e WORKFLOW_DASHBOARD_TOKEN=<a-random-secret> \
  workflow-engine
```

---

# Workflow Definitions

A workflow is a YAML file: a name, an ordered list of steps, and an optional
default retry policy. Each step is an HTTP call — the engine POSTs the
current input as the JSON request body and treats the JSON response body as
the step's output, which becomes the next step's input. A non-2xx response
or network error triggers a retry (per policy) and, once retries are
exhausted, Saga compensation of every step that already completed.

```yaml
name: order-saga

retry:                       # default for any step that doesn't set its own
  max_attempts: 3
  initial_backoff: 100ms
  max_backoff: 2s
  jitter: true

steps:
  - name: reserve_inventory
    execute:
      method: POST
      url: http://localhost:9090/reserve
      timeout: 5s
    compensate:               # called (for real, not just logged) if a
      method: POST             # later step fails
      url: http://localhost:9090/release

  - name: charge_payment
    execute: {method: POST, url: http://localhost:9090/charge}
    compensate: {method: POST, url: http://localhost:9090/refund}
    retry: {max_attempts: 5}   # per-step override

  - name: update_analytics
    execute: {method: POST, url: http://localhost:9090/analytics}
    # no `compensate`: some steps (an analytics event) have nothing to undo
```

Run it against real Postgres, with full crash-recoverable execution and
Saga compensation on failure (this is `engine.ExecuteWorkflow` — the same
code path proven by the fault-injection results below, not a separate
"simple mode"):

```bash
go run ./cmd/cli run --workflow <file.yaml> --input <input.json> [--resume <workflow-id>]
```

`--resume` reuses a previous workflow ID (e.g. after a crash) instead of
starting a new run — every step already completed is skipped via the same
dedup-key check the worker uses.

**Retry, verified live** (not just unit-tested — see `internal/httpstep`
for the retry loop and its tests): pointing a step's URL at
`cmd/exampleservice`'s `?fail=N` query param (fails the first N requests to
that path, then succeeds) and running with `max_attempts: 3` shows two
`503`s in the service log followed by a success, and the workflow completes
normally. Setting `fail` above `max_attempts` instead exhausts the retry
budget, and the workflow's real compensation calls (e.g. `/release`) fire —
confirmed by grepping the example service's request log, not just checking
the event log for `compensation_completed` rows.

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

When a step fails, the engine walks every already-completed step in reverse
order and, for each one, actually invokes its rollback logic (its
`StepExecutor.Compensate` — an HTTP call to the `compensate` URL for
YAML-defined steps) between two durably persisted checkpoints:

1. `compensation_started`
2. `compensation_completed` — written only after the rollback call itself
   succeeds; if it fails, the error is returned and only the `started`
   event exists, so a resumed run retries the same rollback call rather
   than silently marking it done.

If a worker crashes during rollback, the next worker resumes compensation
from the exact unfinished step.

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
│   ├── worker/          # Worker pool daemon (dashboard + metrics HTTP server)
│   ├── cli/             # CLI: create/status/list/run
│   ├── migrate/         # Database migrations
│   ├── faultinject/     # SIGKILL fault injection harness
│   └── exampleservice/  # Throwaway HTTP target for examples/order-saga.yaml
│
├── internal/
│   ├── engine/          # Workflow execution engine
│   ├── store/           # PostgreSQL event log
│   ├── queue/           # Redis Streams consumer
│   ├── saga/            # Compensation engine
│   ├── idempotency/     # Deduplication key generation
│   ├── httpstep/        # HTTP-backed StepExecutor with retry/backoff
│   ├── workflowdef/     # YAML workflow definition parsing
│   ├── dashboard/       # HTTP workflow list/detail views
│   └── metrics/         # Prometheus metric definitions
│
├── examples/            # order-saga.yaml + order-input.json
├── docker-compose.yml
└── README.md
```

---

# License

MIT License
