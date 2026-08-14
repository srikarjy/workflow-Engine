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
	// Metalsw-specific injection points
	MetalswBeforeExecution
	MetalswAfterExecutionBeforeParse
	MetalswAfterParseBeforeReturn
	// Concurrent workers test (not a crash injection point, but a mode)
	ConcurrentWorkers
	injectionPointCount
)

var injectionNames = []string{
	"Before step execution",
	"During step execution",
	"After step completion, before event log write",
	"After event log write, before queue ACK",
	"During compensation",
	"During final compensation step",
	"Metalsw: before gpu_main execution",
	"Metalsw: after gpu_main, before parse",
	"Metalsw: after parse, before return",
	"Concurrent workers racing for same queue",
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
	"metalsw_before_execution",
	"metalsw_after_execution_before_parse",
	"metalsw_after_parse_before_return",
	"", // ConcurrentWorkers doesn't use FAULT_INJECT
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
		postgresDSN       = flag.String("postgres", "postgres://workflow:workflow@localhost:15432/workflow?sslmode=disable", "PostgreSQL DSN")
		redisAddr         = flag.String("redis", "localhost:6379", "Redis address")
		runsPerPoint      = flag.Int("runs", 10, "Runs per injection point")
		streamName        = flag.String("stream", "workflow-steps", "Redis stream name prefix (each run gets its own stream)")
		groupName         = flag.String("group", "workers", "Consumer group name")
		workerPath        = flag.String("worker", "./worker", "Path to worker binary")
		metalswBin        = flag.String("metalsw-bin", "", "Path to metalsw's compiled gpu_main binary (required for metalsw injection points)")
		metalswMetallib   = flag.String("metalsw-metallib", "smith_waterman.metallib", "Path to metalsw's compiled Metal shader library")
		concurrentWorkers = flag.Int("concurrent-workers", 0, "Number of concurrent workers for contention test (0=disabled)")
		concurrentRuns    = flag.Int("concurrent-runs", 5, "Number of concurrent test runs")
	)
	flag.Parse()

	// Metalsw injection points require the binary
	metalswInjectionPoints := map[injectionPoint]bool{
		MetalswBeforeExecution: true,
		MetalswAfterExecutionBeforeParse: true,
		MetalswAfterParseBeforeReturn: true,
	}
	hasMetalswBin := *metalswBin != ""
	totalInjectionPoints := int(injectionPointCount)
	if !hasMetalswBin {
		totalInjectionPoints -= len(metalswInjectionPoints)
	}

	// ConcurrentWorkers is a special mode, not a crash injection point
	if *concurrentWorkers > 0 {
		runConcurrentWorkersTest(*postgresDSN, *redisAddr, *streamName, *groupName, *workerPath, *concurrentWorkers, *concurrentRuns)
		return
	}

	totalRuns := *runsPerPoint * totalInjectionPoints
	fmt.Printf("Starting fault injection suite: %d runs (%d per injection point, %d injection points)\n", totalRuns, *runsPerPoint, totalInjectionPoints)
	if !hasMetalswBin {
		fmt.Printf("Note: metalsw-bin not provided, skipping %d metalsw injection points\n", len(metalswInjectionPoints))
	}

	results := make(chan testResult, totalRuns)
	var wg sync.WaitGroup

	for ip := injectionPoint(0); ip < injectionPointCount; ip++ {
		if metalswInjectionPoints[ip] && !hasMetalswBin {
			continue
		}
		for run := 0; run < *runsPerPoint; run++ {
			wg.Add(1)
			go func(ip injectionPoint, run int) {
				defer wg.Done()
				result := runInjectionTest(ip, run, *postgresDSN, *redisAddr, *streamName, *groupName, *workerPath, *metalswBin, *metalswMetallib)
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

func runInjectionTest(ip injectionPoint, run int, postgresDSN, redisAddr, baseStream, groupName, workerPath, metalswBin, metalswMetallib string) testResult {
	start := time.Now()

	if ip == DuringCompensation || ip == DuringFinalCompensation {
		return runCompensationInjectionTest(ip, run, postgresDSN, workerPath, start)
	}

	// Metalsw injection points need special handling
	metalswInjectionPoints := map[injectionPoint]bool{
		MetalswBeforeExecution: true,
		MetalswAfterExecutionBeforeParse: true,
		MetalswAfterParseBeforeReturn: true,
	}
	if metalswInjectionPoints[ip] {
		return runMetalswInjectionTest(ip, run, postgresDSN, redisAddr, baseStream, groupName, workerPath, metalswBin, metalswMetallib, start)
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

// runMetalswInjectionTest exercises the metalsw step crash-recovery path.
// It queues a metalsw step (requires metalsw binary) and crashes at
// various points during its execution.
func runMetalswInjectionTest(ip injectionPoint, run int, postgresDSN, redisAddr, baseStream, groupName, workerPath, metalswBin, metalswMetallib string, start time.Time) testResult {
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

	if err := s.CreateWorkflow(ctx, wfID, "metalsw-fault-test", mustMarshal(map[string]any{"test": testID})); err != nil {
		log.Printf("Test %s: failed to create workflow: %v", testID, err)
		return testResult{injectionPoint: ip, run: run, passed: false}
	}

	// Use test FASTA files if available, or create minimal ones
	queryFasta := "testdata/query.fasta"
	dbFasta := "testdata/db.fasta"
	// Check if testdata exists
	if _, err := os.Stat(queryFasta); os.IsNotExist(err) {
		// Create minimal test FASTA files
		queryFasta = "/tmp/query_" + testID + ".fasta"
		dbFasta = "/tmp/db_" + testID + ".fasta"
		os.WriteFile(queryFasta, []byte(">query\nACDEFGHIKLMNPQRSTVWY\n"), 0644)
		os.WriteFile(dbFasta, []byte(">target1\nACDEFGHIKLMNPQRSTVWY\n>target2\nVWYACDEFGHIKLMNPQRST\n"), 0644)
	}

	stepName := "metalsw"
	dedupKey, _ := idempotency.DedupKey(wfID.String(), stepName, map[string]any{
		"query_fasta": queryFasta,
		"db_fasta":    dbFasta,
		"top_n":       5,
	})
	msg := queue.StepMessage{
		WorkflowID: wfID.String(),
		StepName:   stepName,
		Input: map[string]any{
			"query_fasta": queryFasta,
			"db_fasta":    dbFasta,
			"top_n":       5,
		},
		DedupKey: dedupKey,
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
		"-metalsw-bin", metalswBin,
		"-metalsw-metallib", metalswMetallib,
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
		"-metalsw-bin", metalswBin,
		"-metalsw-metallib", metalswMetallib,
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

// runConcurrentWorkersTest spawns N workers that all race to process
// messages from the SAME Redis stream (same stream name, same consumer group).
// This tests that the idempotency mechanism prevents double execution when
// multiple workers concurrently try to process the same step.
func runConcurrentWorkersTest(postgresDSN, redisAddr, streamName, groupName, workerPath string, numWorkers, numRuns int) {
	fmt.Printf("Starting concurrent workers test: %d workers racing for same queue, %d runs\n", numWorkers, numRuns)

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, postgresDSN)
	if err != nil {
		log.Fatalf("Failed to connect to DB: %v", err)
	}
	defer pool.Close()

	s, err := store.New(ctx, postgresDSN)
	if err != nil {
		log.Fatalf("Failed to create store: %v", err)
	}
	defer s.Close()

	// Use a SINGLE stream for all runs so workers race across runs too
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	q := queue.NewClient(rdb, streamName, groupName)
	if err := q.EnsureGroup(ctx); err != nil {
		log.Fatalf("Failed to ensure group: %v", err)
	}

	var wg sync.WaitGroup
	results := make(chan struct {
		run          int
		completions  int
		doubleExec   bool
		lostStep     bool
		workerCounts map[string]int
	}, numRuns)

	for run := 0; run < numRuns; run++ {
		wg.Add(1)
		go func(run int) {
			defer wg.Done()
			result := runSingleConcurrentTest(ctx, pool, s, q, workerPath, postgresDSN, redisAddr, numWorkers, streamName, groupName, run)
			results <- result
		}(run)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var totalCompletions, totalDoubleExec, totalLostSteps int
	runStats := make([]struct {
		completions  int
		doubleExec   bool
		lostStep     bool
		workerCounts map[string]int
	}, numRuns)

	for result := range results {
		runStats[result.run] = struct {
			completions  int
			doubleExec   bool
			lostStep     bool
			workerCounts map[string]int
		}{
			completions:  result.completions,
			doubleExec:   result.doubleExec,
			lostStep:     result.lostStep,
			workerCounts: result.workerCounts,
		}
		totalCompletions += result.completions
		if result.doubleExec {
			totalDoubleExec++
		}
		if result.lostStep {
			totalLostSteps++
		}
	}

	fmt.Println("\n| Run | Completions | Double Exec | Lost Step | Worker Distribution |")
	fmt.Println("|----:|------------:|:-----------:|:---------:|---------------------|")
	for i, stats := range runStats {
		workerDist := ""
		for worker, count := range stats.workerCounts {
			workerDist += fmt.Sprintf("%s=%d ", worker, count)
		}
		fmt.Printf("| %3d | %11d | %11v | %9v | %s |\n", i, stats.completions, stats.doubleExec, stats.lostStep, workerDist)
	}
	fmt.Printf("| **Total** | **%11d** | **%11v** | **%9v** |                     |\n", totalCompletions, totalDoubleExec > 0, totalLostSteps > 0)

	fmt.Printf("\nConcurrent workers test: %d runs, %d total completions, %d double executions, %d lost steps\n",
		numRuns, totalCompletions, totalDoubleExec, totalLostSteps)

	if totalDoubleExec > 0 || totalLostSteps > 0 {
		os.Exit(1)
	}
}

func runSingleConcurrentTest(ctx context.Context, pool *pgxpool.Pool, s store.EventLog, q *queue.Client, workerPath, postgresDSN, redisAddr string, numWorkers int, streamName, groupName string, run int) struct {
	run         int
	completions int
	doubleExec  bool
	lostStep    bool
	workerCounts map[string]int
} {
	wfID := uuid.New()
	testID := fmt.Sprintf("concurrent-%d-%d", run, time.Now().UnixNano())

	// Create a single workflow with ONE step that all workers will race for
	stepName := "reserve_inventory"
	if err := s.CreateWorkflow(ctx, wfID, "concurrent-test", mustMarshal(map[string]any{"test": testID})); err != nil {
		log.Printf("Run %d: failed to create workflow: %v", run, err)
		return struct {
			run         int
			completions int
			doubleExec  bool
			lostStep    bool
			workerCounts map[string]int
		}{run: run, doubleExec: true, lostStep: true}
	}

	dedupKey, _ := idempotency.DedupKey(wfID.String(), stepName, map[string]any{"data": "test"})
	msg := queue.StepMessage{
		WorkflowID: wfID.String(),
		StepName:   stepName,
		Input:      map[string]any{"data": "test"},
		DedupKey:   dedupKey,
	}
	// Produce exactly ONE message that all workers will race for
	if err := q.ProduceStep(ctx, msg); err != nil {
		log.Printf("Run %d: failed to produce step: %v", run, err)
		return struct {
			run         int
			completions int
			doubleExec  bool
			lostStep    bool
			workerCounts map[string]int
		}{run: run, doubleExec: true, lostStep: true}
	}

	// Start N workers all on the SAME stream and SAME consumer group
	// They will race to claim the single message
	var wg sync.WaitGroup
	workerCounts := make(map[string]int)
	var countsMu sync.Mutex

	workerDone := make(chan struct{})

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		workerID := fmt.Sprintf("concurrent-%s-w%d", testID, i)
		go func(workerID string) {
			defer wg.Done()
			cmd := exec.Command(workerPath,
				"-postgres", postgresDSN,
				"-redis", redisAddr,
				"-stream", streamName,
				"-group", groupName,
				"-worker-id", workerID,
				"-workers", "1",
				"-reclaim-idle", "0s",
			)
			// Run until the message is processed
			if err := cmd.Run(); err != nil {
				log.Printf("Worker %s exited with error: %v", workerID, err)
			}
			// Track which worker did the work by checking completions
			var count int
			_ = pool.QueryRow(ctx, `
				SELECT COUNT(*) FROM events
				WHERE workflow_id = $1 AND step_name = $2 AND event_type = 'step_completed'
				AND payload::jsonb @> '{"worker_id": "' + workerID + '"}'
			`, wfID, stepName).Scan(&count)
			countsMu.Lock()
			workerCounts[workerID] = count
			countsMu.Unlock()
		}(workerID)
	}

	// Wait for all workers to finish
	wg.Wait()
	close(workerDone)

	// Count total completions
	var completionCount int
	err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM events
		WHERE workflow_id = $1 AND step_name = $2 AND event_type = 'step_completed'
	`, wfID, stepName).Scan(&completionCount)
	if err != nil {
		log.Printf("Run %d: failed to count completions: %v", run, err)
		return struct {
			run         int
			completions int
			doubleExec  bool
			lostStep    bool
			workerCounts map[string]int
		}{run: run, doubleExec: true, lostStep: true}
	}

	doubleExec := completionCount > 1
	lostStep := completionCount == 0

	return struct {
		run         int
		completions int
		doubleExec  bool
		lostStep    bool
		workerCounts map[string]int
	}{
		run:          run,
		completions:  completionCount,
		doubleExec:   doubleExec,
		lostStep:     lostStep,
		workerCounts: workerCounts,
	}
}
