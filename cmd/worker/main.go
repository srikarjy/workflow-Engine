// Command worker runs the workflow worker pool daemon.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/srikarjy/workflow_engine/internal/engine"
	"github.com/srikarjy/workflow_engine/internal/faultinject"
	"github.com/srikarjy/workflow_engine/internal/queue"
	"github.com/srikarjy/workflow_engine/internal/steps"
	"github.com/srikarjy/workflow_engine/internal/store"
)

func main() {
	var (
		postgresDSN = flag.String("postgres", "postgres://postgres:postgres@localhost:5432/workflow", "PostgreSQL DSN")
		redisAddr   = flag.String("redis", "localhost:6379", "Redis address")
		streamName  = flag.String("stream", "workflow-steps", "Redis stream name")
		groupName   = flag.String("group", "workers", "Consumer group name")
		workerID    = flag.String("worker-id", "", "Worker ID (auto-generated if empty)")
		numWorkers  = flag.Int("workers", 4, "Number of worker goroutines")
		blockTime   = flag.Duration("block", 5*time.Second, "Block time for XREADGROUP")
		count       = flag.Int64("count", 10, "Max messages per fetch")
		reclaimIdle = flag.Duration("reclaim-idle", 5*time.Second, "Reclaim pending messages idle at least this long, from any consumer (picks up work orphaned by a crashed worker)")

		faultSaga = flag.Bool("fault-saga", false, "Run a single hardcoded 2-success+1-failure saga directly (bypassing the queue) to exercise Saga compensation, then exit. Used by cmd/faultinject.")
		wfIDFlag  = flag.String("wf-id", "", "Workflow ID for -fault-saga (required); reuse the same ID across a crash and its replacement run to resume compensation")
	)
	flag.Parse()

	if *workerID == "" {
		*workerID = "worker-" + uuid.New().String()[:8]
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, err := store.New(ctx, *postgresDSN)
	if err != nil {
		log.Fatalf("Failed to create store: %v", err)
	}
	defer s.Close()

	registry := engine.NewStepRegistry()
	for _, step := range steps.OrderSagaSteps() {
		registry.Register(step)
	}

	if *faultSaga {
		wfID, err := uuid.Parse(*wfIDFlag)
		if err != nil {
			log.Fatalf("-fault-saga requires a valid -wf-id: %v", err)
		}
		e := engine.NewEngine(s, nil, *workerID, registry)
		runFaultSaga(ctx, e, wfID)
		return
	}

	// Connect to PostgreSQL (for the raw pool used elsewhere; store.New
	// above holds its own pool for the engine's queries).
	pool, err := pgxpool.New(ctx, *postgresDSN)
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer pool.Close()

	// Connect to Redis
	rdb := redis.NewClient(&redis.Options{
		Addr: *redisAddr,
	})
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}

	q := queue.NewClient(rdb, *streamName, *groupName)
	if err := q.EnsureGroup(ctx); err != nil {
		log.Fatalf("Failed to ensure consumer group: %v", err)
	}

	e := engine.NewEngine(s, q, *workerID, registry)

	// Start worker pool
	var wg sync.WaitGroup
	for i := 0; i < *numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			consumerName := *workerID + "-" + string(rune('a'+id))
			runWorker(ctx, e, q, consumerName, *blockTime, *count, *reclaimIdle)
		}(i)
	}

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("Shutdown signal received, stopping workers...")
	cancel()
	wg.Wait()
	log.Println("All workers stopped")
}

// runFaultSaga runs a fixed 2-step-success-then-1-step-failure workflow
// directly through Engine.ExecuteWorkflow against wfID, forcing Saga
// compensation. Invoked twice by cmd/faultinject against the same wfID: once
// under FAULT_INJECT (which self-crashes mid-compensation), and once without
// it as the replacement run that resumes and finishes compensation.
func runFaultSaga(ctx context.Context, e *engine.Engine, wfID uuid.UUID) {
	wfDef := &engine.WorkflowDefinition{
		Name: "fault-saga",
		Steps: []engine.StepExecutor{
			steps.NewLoggingStep("reserve_inventory"),
			steps.NewLoggingStep("charge_payment"),
			steps.NewFailingStep("create_shipment"),
		},
	}
	_, err := e.ExecuteWorkflow(ctx, wfID, wfDef, map[string]any{"data": "fault-saga-test"})
	if err != nil {
		log.Printf("fault-saga %s: %v (expected: the last step always fails to force compensation)", wfID, err)
	}
}

func runWorker(ctx context.Context, e *engine.Engine, q *queue.Client, consumer string, block time.Duration, count int64, reclaimIdle time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Pick up any message left pending by a consumer that crashed
			// before acking it, so crash recovery doesn't depend on a
			// specific consumer name coming back. Handle these right away:
			// ConsumeSteps below blocks for up to `block` waiting on brand
			// new messages, so folding reclaimed work into its result would
			// leave a reclaimed message sitting idle until that wait times
			// out.
			reclaimed, err := q.ReclaimPending(ctx, consumer, reclaimIdle)
			if err != nil {
				log.Printf("Worker %s: reclaim error: %v", consumer, err)
			}
			processMessages(ctx, e, q, consumer, reclaimed)

			msgs, err := q.ConsumeSteps(ctx, consumer, count, block)
			if err != nil {
				log.Printf("Worker %s: consume error: %v", consumer, err)
				time.Sleep(time.Second)
				continue
			}
			processMessages(ctx, e, q, consumer, msgs)
		}
	}
}

func processMessages(ctx context.Context, e *engine.Engine, q *queue.Client, consumer string, msgs []redis.XMessage) {
	for _, msg := range msgs {
		stepMsg, err := queue.ParseStepMessage(msg)
		if err != nil {
			log.Printf("Worker %s: parse error: %v", consumer, err)
			_ = q.Ack(ctx, msg.ID)
			continue
		}

		if err := e.ProcessStep(ctx, stepMsg); err != nil {
			log.Printf("Worker %s: process step error: %v", consumer, err)
			continue
		}

		faultinject.Crash("after_log_before_ack")

		if err := q.Ack(ctx, msg.ID); err != nil {
			log.Printf("Worker %s: ack error: %v", consumer, err)
		}
	}
}
