// Command faultinject crashes workers with SIGKILL at various execution points.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/srikarjy/workflow_engine/internal/idempotency"
	"github.com/srikarjy/workflow_engine/internal/queue"
	"github.com/srikarjy/workflow_engine/internal/store"
)

type injectionPoint int

const (
	BeforeExecution injectionPoint = iota
	DuringExecution
	AfterCompletionBeforeEventLog
	AfterEventLogBeforeAck
	DuringCompensation
	DuringFinalCompensation
	injectionPointCount
)

var injectionNames = []string{
	"Before step execution",
	"During step execution",
	"After step completion, before event log write",
	"After event log write, before queue ACK",
	"During compensation",
	"During final compensation step",
}

type testResult struct {
	injectionPoint injectionPoint
	run            int
	passed         bool
	doubleExec     bool
	lostStep       bool
	duration       time.Duration
}

func main() {
	var (
		postgresDSN  = flag.String("postgres", "postgres://postgres:postgres@localhost:5432/workflow", "PostgreSQL DSN")
		redisAddr    = flag.String("redis", "localhost:6379", "Redis address")
		runsPerPoint = flag.Int("runs", 80, "Runs per injection point (total = runs * 6)")
		streamName   = flag.String("stream", "workflow-steps", "Redis stream name")
		groupName    = flag.String("group", "workers", "Consumer group name")
		workerPath   = flag.String("worker", "./worker", "Path to worker binary")
	)
	flag.Parse()

	totalRuns := *runsPerPoint * int(injectionPointCount)
	fmt.Printf("Starting fault injection suite: %d runs (%d per injection point)\n", totalRuns, *runsPerPoint)

	results := make(chan testResult, totalRuns)
	var wg sync.WaitGroup

	for ip := injectionPoint(0); ip < injectionPointCount; ip++ {
		for run := 0; run < *runsPerPoint; run++ {
			wg.Add(1)
			go func(ip injectionPoint, run int) {
				defer wg.Done()
				result := runInjectionTest(ip, run, *postgresDSN, *redisAddr, *streamName, *groupName, *workerPath)
				results <- result
			}(ip, run)
		}
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var passed, doubleExec, lostSteps int
	pointStats := make(map[injectionPoint]struct{ passed, doubleExec, lostSteps int })

	for result := range results {
		if result.passed {
			passed++
		}
		if result.doubleExec {
			doubleExec++
		}
		if result.lostStep {
			lostSteps++
		}

		stats := pointStats[result.injectionPoint]
		if result.passed {
			stats.passed++
		}
		if result.doubleExec {
			stats.doubleExec++
		}
		if result.lostStep {
			stats.lostSteps++
		}
		pointStats[result.injectionPoint] = stats
	}

	fmt.Println("\n| Injection Point | Runs | Passed | Double Execution | Lost Steps |")
	fmt.Println("|-----------------|----:|------:|:----------------:|:----------:|")
	for ip := injectionPoint(0); ip < injectionPointCount; ip++ {
		stats := pointStats[ip]
		runs := *runsPerPoint
		fmt.Printf("| %-47s | %4d | %6d | %16v | %10v |\n", injectionNames[ip], runs, stats.passed, stats.doubleExec > 0, stats.lostSteps > 0)
	}
	fmt.Printf("| **Total** | **%4d** | **%6d** | **%16v** | **%10v** |\n", totalRuns, passed, doubleExec > 0, lostSteps > 0)

	fmt.Printf("\nSummary: %d/%d passed, %d double executions, %d lost steps\n", passed, totalRuns, doubleExec, lostSteps)

	if doubleExec > 0 || lostSteps > 0 {
		os.Exit(1)
	}
}

func runInjectionTest(ip injectionPoint, run int, postgresDSN, redisAddr, streamName, groupName, workerPath string) testResult {
	start := time.Now()
	ctx := context.Background()

	testID := fmt.Sprintf("fault-%s-%d", injectionNames[ip], run)
	wfID := uuid.New()

	pool, err := pgxpool.New(ctx, postgresDSN)
	if err != nil {
		log.Printf("Test %s: failed to connect to DB: %v", testID, err)
		return testResult{injectionPoint: ip, run: run, passed: false}
	}
	defer pool.Close()

	s, err := store.New(ctx, postgresDSN)
	if err != nil {
		log.Printf("Test %s: failed to create store: %v", testID, err)
		return testResult{injectionPoint: ip, run: run, passed: false}
	}
	defer s.Close()

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	q := queue.NewClient(rdb, streamName, groupName)
	if err := q.EnsureGroup(ctx); err != nil {
		log.Printf("Test %s: failed to ensure group: %v", testID, err)
		return testResult{injectionPoint: ip, run: run, passed: false}
	}

	if err := s.CreateWorkflow(ctx, wfID, "fault-test", mustMarshal(map[string]any{"test": testID})); err != nil {
		log.Printf("Test %s: failed to create workflow: %v", testID, err)
		return testResult{injectionPoint: ip, run: run, passed: false}
	}

	stepName := "test_step"
	dedupKey, _ := idempotency.DedupKey(wfID.String(), stepName, map[string]any{"data": "test"})
	msg := queue.StepMessage{
		WorkflowID: wfID.String(),
		StepName:   stepName,
		Input:      map[string]any{"data": "test"},
		DedupKey:   dedupKey,
	}
	if err := q.ProduceStep(ctx, msg); err != nil {
		log.Printf("Test %s: failed to produce step: %v", testID, err)
		return testResult{injectionPoint: ip, run: run, passed: false}
	}

	workerID := fmt.Sprintf("fault-worker-%s", testID)
	cmd := exec.Command(workerPath,
		"-postgres", postgresDSN,
		"-redis", redisAddr,
		"-stream", streamName,
		"-group", groupName,
		"-worker-id", workerID,
		"-workers", "1",
	)

	switch ip {
	case BeforeExecution:
		cmd.Env = append(os.Environ(), "FAULT_INJECT=before_execution", "FAULT_SIGNAL=SIGKILL")
	case DuringExecution:
		cmd.Env = append(os.Environ(), "FAULT_INJECT=during_execution", "FAULT_SIGNAL=SIGKILL")
	case AfterCompletionBeforeEventLog:
		cmd.Env = append(os.Environ(), "FAULT_INJECT=after_completion_before_log", "FAULT_SIGNAL=SIGKILL")
	case AfterEventLogBeforeAck:
		cmd.Env = append(os.Environ(), "FAULT_INJECT=after_log_before_ack", "FAULT_SIGNAL=SIGKILL")
	case DuringCompensation:
		cmd.Env = append(os.Environ(), "FAULT_INJECT=during_compensation", "FAULT_SIGNAL=SIGKILL")
	case DuringFinalCompensation:
		cmd.Env = append(os.Environ(), "FAULT_INJECT=during_final_compensation", "FAULT_SIGNAL=SIGKILL")
	}

	if err := cmd.Start(); err != nil {
		log.Printf("Test %s: failed to start worker: %v", testID, err)
		return testResult{injectionPoint: ip, run: run, passed: false}
	}

	time.Sleep(2 * time.Second)

	if err := cmd.Process.Kill(); err != nil {
		log.Printf("Test %s: failed to kill worker: %v", testID, err)
	}
	cmd.Wait()

	replacementID := workerID + "-replacement"
	cmd2 := exec.Command(workerPath,
		"-postgres", postgresDSN,
		"-redis", redisAddr,
		"-stream", streamName,
		"-group", groupName,
		"-worker-id", replacementID,
		"-workers", "1",
	)

	if err := cmd2.Start(); err != nil {
		log.Printf("Test %s: failed to start replacement worker: %v", testID, err)
		return testResult{injectionPoint: ip, run: run, passed: false}
	}

	time.Sleep(3 * time.Second)
	cmd2.Process.Kill()
	cmd2.Wait()

	var completionCount int
	err = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM events
		WHERE workflow_id = $1 AND step_name = $2 AND event_type = 'step_completed'
	`, wfID, stepName).Scan(&completionCount)

	doubleExec := completionCount > 1
	lostStep := completionCount == 0

	passed := !doubleExec && !lostStep

	return testResult{
		injectionPoint: ip,
		run:            run,
		passed:         passed,
		doubleExec:     doubleExec,
		lostStep:       lostStep,
		duration:       time.Since(start),
	}
}

func mustMarshal(v any) []byte {
	data, _ := json.Marshal(v)
	return data
}