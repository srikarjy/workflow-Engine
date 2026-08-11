// Command faultinject crashes workers with SIGKILL at various execution
// points and verifies exactly-once behavior across the crash.
//
// Each injection point corresponds to a checkpoint the worker process
// itself calls via internal/faultinject.Crash: when the FAULT_INJECT
// environment variable matches the checkpoint name, the worker
// self-terminates with SIGKILL at that exact point in its own code, rather
// than being killed from outside after a guessed delay. That makes the
// crash timing deterministic and precisely synchronized with the code path
// under test.
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

// injectionEnvKeys are the FAULT_INJECT values internal/faultinject.Crash
// checks for at each corresponding checkpoint in the engine/saga/steps
// packages. Index-aligned with the injectionPoint constants above.
var injectionEnvKeys = []string{
	"before_execution",
	"during_execution",
	"after_completion_before_log",
	"after_log_before_ack",
	"during_compensation",
	"during_final_compensation",
}

// crashWaitTimeout bounds how long we wait for a worker to reach its
// checkpoint and self-crash. The checkpoint fires almost immediately once
// the code path is reached, so this is a generous safety net for a
// misconfigured run (e.g. checkpoint never reached), not the normal case.
const crashWaitTimeout = 10 * time.Second

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
		streamName   = flag.String("stream", "workflow-steps", "Redis stream name prefix (each run gets its own stream)")
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

func runInjectionTest(ip injectionPoint, run int, postgresDSN, redisAddr, baseStream, groupName, workerPath string) testResult {
	start := time.Now()

	if ip == DuringCompensation || ip == DuringFinalCompensation {
		return runCompensationInjectionTest(ip, run, postgresDSN, workerPath, start)
	}

	ctx := context.Background()
	testID := fmt.Sprintf("%s-%d-%d", injectionEnvKeys[ip], run, time.Now().UnixNano())
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

	// Each test gets its own stream so concurrent runs can never steal one
	// another's queued step message.
	streamName := baseStream + "-" + testID
	q := queue.NewClient(rdb, streamName, groupName)
	if err := q.EnsureGroup(ctx); err != nil {
		log.Printf("Test %s: failed to ensure group: %v", testID, err)
		return testResult{injectionPoint: ip, run: run, passed: false}
	}

	if err := s.CreateWorkflow(ctx, wfID, "fault-test", mustMarshal(map[string]any{"test": testID})); err != nil {
		log.Printf("Test %s: failed to create workflow: %v", testID, err)
		return testResult{injectionPoint: ip, run: run, passed: false}
	}

	// reserve_inventory is one of the demo steps cmd/worker always
	// registers (internal/steps.OrderSagaSteps), so ProcessStep can
	// actually resolve and execute it.
	stepName := "reserve_inventory"
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
	cmd.Env = append(os.Environ(), "FAULT_INJECT="+injectionEnvKeys[ip])

	if err := cmd.Start(); err != nil {
		log.Printf("Test %s: failed to start worker: %v", testID, err)
		return testResult{injectionPoint: ip, run: run, passed: false}
	}
	if !waitForExit(cmd, crashWaitTimeout) {
		log.Printf("Test %s: worker did not self-crash within %s (checkpoint never reached)", testID, crashWaitTimeout)
	}

	// Replacement worker: no FAULT_INJECT, reclaims the message the crashed
	// worker left pending (reclaim-idle=0 so it doesn't have to wait out the
	// default idle threshold) and drives it to completion.
	replacementID := workerID + "-replacement"
	cmd2 := exec.Command(workerPath,
		"-postgres", postgresDSN,
		"-redis", redisAddr,
		"-stream", streamName,
		"-group", groupName,
		"-worker-id", replacementID,
		"-workers", "1",
		"-reclaim-idle", "0s",
	)
	if err := cmd2.Start(); err != nil {
		log.Printf("Test %s: failed to start replacement worker: %v", testID, err)
		return testResult{injectionPoint: ip, run: run, passed: false}
	}
	time.Sleep(3 * time.Second)
	_ = cmd2.Process.Kill()
	_ = cmd2.Wait()

	var completionCount int
	err = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM events
		WHERE workflow_id = $1 AND step_name = $2 AND event_type = 'step_completed'
	`, wfID, stepName).Scan(&completionCount)
	if err != nil {
		log.Printf("Test %s: failed to count completions: %v", testID, err)
		return testResult{injectionPoint: ip, run: run, passed: false}
	}

	doubleExec := completionCount > 1
	lostStep := completionCount == 0

	return testResult{
		injectionPoint: ip,
		run:            run,
		passed:         !doubleExec && !lostStep,
		doubleExec:     doubleExec,
		lostStep:       lostStep,
		duration:       time.Since(start),
	}
}

// runCompensationInjectionTest exercises the Saga rollback path: it runs a
// fixed workflow (two steps succeed, the third always fails) through
// worker's -fault-saga mode against a specific wfID, which forces
// compensation of the two successful steps. FAULT_INJECT makes that first
// run self-crash mid-compensation; a second run against the same wfID (no
// FAULT_INJECT) resumes and finishes it.
func runCompensationInjectionTest(ip injectionPoint, run int, postgresDSN, workerPath string, start time.Time) testResult {
	ctx := context.Background()
	wfID := uuid.New()
	testID := wfID.String()

	pool, err := pgxpool.New(ctx, postgresDSN)
	if err != nil {
		log.Printf("Test %s: failed to connect to DB: %v", testID, err)
		return testResult{injectionPoint: ip, run: run, passed: false}
	}
	defer pool.Close()

	cmd := exec.Command(workerPath, "-postgres", postgresDSN, "-fault-saga", "-wf-id", wfID.String())
	cmd.Env = append(os.Environ(), "FAULT_INJECT="+injectionEnvKeys[ip])
	if err := cmd.Start(); err != nil {
		log.Printf("Test %s: failed to start fault-saga worker: %v", testID, err)
		return testResult{injectionPoint: ip, run: run, passed: false}
	}
	if !waitForExit(cmd, crashWaitTimeout) {
		log.Printf("Test %s: worker did not self-crash within %s (checkpoint never reached)", testID, crashWaitTimeout)
	}

	// Replacement run against the same wfID resumes compensation:
	// CreateWorkflow is idempotent and every step/compensation is gated on
	// the event log, so already-completed work is skipped.
	cmd2 := exec.Command(workerPath, "-postgres", postgresDSN, "-fault-saga", "-wf-id", wfID.String())
	if err := cmd2.Run(); err != nil {
		log.Printf("Test %s: replacement fault-saga run error: %v", testID, err)
	}

	// The workflow is reserve_inventory -> charge_payment -> (fails), so
	// compensation runs in reverse: compensate_charge_payment first (the
	// "during compensation" checkpoint), compensate_reserve_inventory last
	// (the "during final compensation" checkpoint).
	compStepName := "compensate_reserve_inventory"
	if ip == DuringCompensation {
		compStepName = "compensate_charge_payment"
	}

	var completionCount int
	err = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM events
		WHERE workflow_id = $1 AND step_name = $2 AND event_type = 'compensation_completed'
	`, wfID, compStepName).Scan(&completionCount)
	if err != nil {
		log.Printf("Test %s: failed to count compensation completions: %v", testID, err)
		return testResult{injectionPoint: ip, run: run, passed: false}
	}

	doubleExec := completionCount > 1
	lostStep := completionCount == 0

	return testResult{
		injectionPoint: ip,
		run:            run,
		passed:         !doubleExec && !lostStep,
		doubleExec:     doubleExec,
		lostStep:       lostStep,
		duration:       time.Since(start),
	}
}

// waitForExit waits for cmd to exit on its own (the expected outcome: it hit
// its FAULT_INJECT checkpoint and self-SIGKILLed). It returns false and
// force-kills the process if that doesn't happen within timeout.
func waitForExit(cmd *exec.Cmd, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		_ = cmd.Process.Kill()
		<-done
		return false
	}
}

func mustMarshal(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		return []byte("{}")
	}
	return data
}
