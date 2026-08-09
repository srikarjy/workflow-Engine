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
	"github.com/srikarjy/workflow_engine/internal/queue"
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
	)
	flag.Parse()

	if *workerID == "" {
		*workerID = "worker-" + uuid.New().String()[:8]
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Connect to PostgreSQL
	pool, err := pgxpool.New(ctx, *postgresDSN)
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer pool.Close()

	s, err := store.New(ctx, *postgresDSN)
	if err != nil {
		log.Fatalf("Failed to create store: %v", err)
	}
	defer s.Close()

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

	e := engine.NewEngine(s, q, *workerID)

	// Start worker pool
	var wg sync.WaitGroup
	for i := 0; i < *numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			consumerName := *workerID + "-" + string(rune('a'+id))
			runWorker(ctx, e, q, consumerName, *blockTime, *count)
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

func runWorker(ctx context.Context, e *engine.Engine, q *queue.Client, consumer string, block time.Duration, count int64) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			msgs, err := q.ConsumeSteps(ctx, consumer, count, block)
			if err != nil {
				log.Printf("Worker %s: consume error: %v", consumer, err)
				time.Sleep(time.Second)
				continue
			}

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

				if err := q.Ack(ctx, msg.ID); err != nil {
					log.Printf("Worker %s: ack error: %v", consumer, err)
				}
			}
		}
	}
}