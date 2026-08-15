// Command worker runs the workflow worker pool daemon.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/srikarjy/workflow_engine/internal/dashboard"
	"github.com/srikarjy/workflow_engine/internal/engine"
	"github.com/srikarjy/workflow_engine/internal/faultinject"
	"github.com/srikarjy/workflow_engine/internal/queue"
	"github.com/srikarjy/workflow_engine/internal/steps"
	"github.com/srikarjy/workflow_engine/internal/store"
)

func main() {
	var (
		postgresDSN = flag.String("postgres", envOr("DATABASE_URL", "postgres://workflow:workflow@localhost:15432/workflow?sslmode=disable"), "PostgreSQL DSN. Defaults to $DATABASE_URL.")
		redisAddr   = flag.String("redis", envOr("REDIS_URL", "localhost:6379"), "Redis address (\"host:port\") or a full redis://.../rediss://... URL (e.g. from a managed provider like Upstash). Defaults to $REDIS_URL.")
		streamName  = flag.String("stream", "workflow-steps", "Redis stream name")
		groupName   = flag.String("group", "workers", "Consumer group name")
		workerID    = flag.String("worker-id", "", "Worker ID (auto-generated if empty)")
		numWorkers  = flag.Int("workers", 4, "Number of worker goroutines")
		blockTime   = flag.Duration("block", 5*time.Second, "Block time for XREADGROUP")
		count       = flag.Int64("count", 10, "Max messages per fetch")
		reclaimIdle = flag.Duration("reclaim-idle", 5*time.Second, "Reclaim pending messages idle at least this long, from any consumer (picks up work orphaned by a crashed worker)")
		httpAddr    = flag.String("http", ":"+envOr("PORT", "8080"), "Address to serve the dashboard (/) and Prometheus metrics (/metrics) on; empty disables it. Port defaults to $PORT (what most PaaS hosts, e.g. Render, require binding to).")
		maxMessages = flag.Int("max-messages", 0, "Exit after processing this many messages (0 = run forever)")

		authToken      = flag.String("auth-token", os.Getenv("WORKFLOW_DASHBOARD_TOKEN"), "Bearer token required on every dashboard/metrics request; empty disables auth (fine for local dev, NOT for a public deployment). Defaults to $WORKFLOW_DASHBOARD_TOKEN so it doesn't need to appear on the command line.")
		rateLimitRPS   = flag.Float64("rate-limit-rps", 5, "Sustained requests/sec allowed across all clients on the dashboard/metrics server; <=0 disables rate limiting")
		rateLimitBurst = flag.Int("rate-limit-burst", 20, "Burst allowance above -rate-limit-rps")

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

	// Connect to Redis. -redis accepts either a bare "host:port" (local
	// dev) or a full redis://.../rediss://... URL with embedded auth and
	// TLS, as managed providers like Upstash hand out.
	redisOpts, err := parseRedisAddr(*redisAddr)
	if err != nil {
		log.Fatalf("Invalid -redis value: %v", err)
	}
	rdb := redis.NewClient(redisOpts)
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}

	q := queue.NewClient(rdb, *streamName, *groupName)
	if err := q.EnsureGroup(ctx); err != nil {
		log.Fatalf("Failed to ensure consumer group: %v", err)
	}

	e := engine.NewEngine(s, q, *workerID, registry)

	if *httpAddr != "" {
		if *authToken == "" {
			log.Printf("WARNING: -auth-token / $WORKFLOW_DASHBOARD_TOKEN is unset — the dashboard and /metrics are unauthenticated. Fine for local dev, not for a public deployment.")
		}
		cfg := dashboard.Config{AuthToken: *authToken, RateLimitRPS: *rateLimitRPS, RateLimitBurst: *rateLimitBurst}
		srv := &http.Server{Addr: *httpAddr, Handler: dashboard.Handler(s, cfg)}
		go func() {
			log.Printf("dashboard and metrics listening on %s", *httpAddr)
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Printf("http server error: %v", err)
			}
		}()
		defer srv.Close()
	}

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

// envOr returns the environment variable key's value, or def if unset.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// parseRedisAddr accepts either a bare "host:port" or a full redis:// /
// rediss:// URL (the form managed providers like Upstash issue, with auth
// and TLS embedded) and returns the corresponding *redis.Options.
func parseRedisAddr(addr string) (*redis.Options, error) {
	if strings.Contains(addr, "://") {
		return redis.ParseURL(addr)
	}
	return &redis.Options{Addr: addr}, nil
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
