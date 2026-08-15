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
		postgresDSN = flag.String("postgres", "postgres://workflow:workflow@localhost:15432/workflow?sslmode=disable", "PostgreSQL DSN")
		redisAddr   = flag.String("redis", "localhost:6379", "Redis address")
		streamName  = flag.String("stream", "workflow-steps", "Redis stream name")
		groupName   = flag.String("group", "workers", "Consumer group name")
		workerID    = flag.String("worker-id", "", "Worker ID (auto-generated if empty)")
		numWorkers  = flag.Int("workers", 4, "Number of worker goroutines")
		blockTime   = flag.Duration("block", 5*time.Second, "Block time for XREADGROUP")
		count       = flag.Int64("count", 10, "Max messages per fetch")
		reclaimIdle = flag.Duration("reclaim-idle", 5*time.Second, "Reclaim pending messages idle at least this long, from any consumer (picks up work orphaned by a crashed worker)")
		maxMessages = flag.Int("max-messages", 0, "Exit after processing this many messages (0 = run forever)")

		faultSaga = flag.Bool("fault-saga", false, "Run a single hardcoded 2-success+1-failure saga directly (bypassing the queue) to exercise Saga compensation, then exit. Used by cmd/faultinject.")
		wfIDFlag  = flag.String("wf-id", "", "Workflow ID for -fault-saga (required); reuse the same ID across a crash and its replacement run to resume compensation")

		metalswBin      = flag.String("metalsw-bin", "", "Path to metalsw's compiled gpu_main binary; metalsw step registered only if set")
		metalswMetallib = flag.String("metalsw-metallib", "smith_waterman.metallib", "Path to metalsw's compiled Metal shader library")
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
	if *metalswBin != "" {
		registry.Register(steps.NewMetalSWStep(*metalswBin, *metalswMetallib))
	}
	// Register notification step (confidence-gated, idempotent)
	notificationConfig := steps.NotificationConfig{
		ConfidenceThreshold: 0.7, // Only notify if confidence >= 70%
		Channels:            []string{"email", "webhook"},
		EmailConfig: &steps.EmailNotificationConfig{
			To:      []string{"research-team@example.com"},
			Subject: "Research Workflow Complete: {{.workflow_id}}",
			Body:    "Workflow {{.workflow_id}} completed with confidence {{.confidence}}",
		},
		WebhookConfig: &steps.WebhookNotificationConfig{
			URL:    "https://webhook.example.com/notify",
			Method: "POST",
			Body:   `{"workflow_id": "{{.workflow_id}}", "confidence": {{.confidence}}}`,
		},
	}
	registry.Register(steps.NewNotificationStep("send_notification", notificationConfig, s))

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
			runWorker(ctx, e, q, consumerName, *blockTime, *count, *reclaimIdle, *maxMessages)
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

func runWorker(ctx context.Context, e *engine.Engine, q *queue.Client, consumer string, block time.Duration, count int64, reclaimIdle time.Duration, maxMessages int) {
	messagesProcessed := 0
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if maxMessages > 0 && messagesProcessed >= maxMessages {
				return
			}
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
			processed := processMessages(ctx, e, q, consumer, reclaimed)
			messagesProcessed += processed
			if maxMessages > 0 && messagesProcessed >= maxMessages {
				return
			}

			msgs, err := q.ConsumeSteps(ctx, consumer, count, block)
			if err != nil {
				log.Printf("Worker %s: consume error: %v", consumer, err)
				time.Sleep(time.Second)
				continue
			}
			if len(msgs) == 0 {
				continue
			}
			processed = processMessages(ctx, e, q, consumer, msgs)
			messagesProcessed += processed
			if maxMessages > 0 && messagesProcessed >= maxMessages {
				return
			}
		}
	}
}

func processMessages(ctx context.Context, e *engine.Engine, q *queue.Client, consumer string, msgs []redis.XMessage) int {
	processed := 0
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
			// Leave the message pending so a reclaim can retry the ack;
			// don't count it toward maxMessages.
			log.Printf("Worker %s: ack error: %v", consumer, err)
			return processed
		}
		processed++
	}
	return processed
}
