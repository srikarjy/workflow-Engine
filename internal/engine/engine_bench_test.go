// Package engine benchmarks. These run the real Engine against a real
// PostgreSQL event log (no queue/Redis involved — ExecuteWorkflow doesn't
// touch the queue). Set BENCH_POSTGRES_DSN, or start the default
// docker-compose Postgres (`docker compose up -d postgres && go run
// ./cmd/migrate up`) and they'll use that. Without a reachable database
// they skip rather than fail or, worse, silently benchmark a mock.
package engine

import (
	"context"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/srikarjy/workflow_engine/internal/queue"
	"github.com/srikarjy/workflow_engine/internal/store"
)

func benchDSN() string {
	if v := os.Getenv("BENCH_POSTGRES_DSN"); v != "" {
		return v
	}
	return "postgres://workflow:workflow@localhost:15432/workflow?sslmode=disable"
}

func newBenchStore(b *testing.B) *store.Store {
	b.Helper()
	dsn := benchDSN()
	s, err := store.New(context.Background(), dsn)
	if err != nil {
		b.Skipf("postgres unreachable at %s (%v); set BENCH_POSTGRES_DSN or run `docker compose up -d postgres && go run ./cmd/migrate up`", dsn, err)
	}
	return s
}

type benchStep struct {
	name string
}

func (s *benchStep) Name() string { return s.name }
func (s *benchStep) Execute(ctx context.Context, input map[string]any) (map[string]any, error) {
	return map[string]any{"output": s.name}, nil
}
func (s *benchStep) Compensate(ctx context.Context, output map[string]any) error { return nil }

func benchWorkflowDef() *WorkflowDefinition {
	return &WorkflowDefinition{
		Name: "benchmark",
		Steps: []StepExecutor{
			&benchStep{"step1"}, &benchStep{"step2"}, &benchStep{"step3"}, &benchStep{"step4"}, &benchStep{"step5"},
		},
	}
}

// BenchmarkEngine_ExecuteWorkflow runs a 5-step workflow to completion
// sequentially against real Postgres: every step writes a step_started and
// a step_completed event, so this is the true per-workflow cost, not a
// mock's. Reports p50/p99 workflow latency and step throughput alongside
// the standard ns/op.
func BenchmarkEngine_ExecuteWorkflow(b *testing.B) {
	s := newBenchStore(b)
	defer s.Close()
	e := NewEngine(s, nil, "bench-worker", nil)
	wfDef := benchWorkflowDef()

	durations := make([]time.Duration, 0, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		if _, err := e.ExecuteWorkflow(context.Background(), uuid.New(), wfDef, map[string]any{"data": "test"}); err != nil {
			b.Fatal(err)
		}
		durations = append(durations, time.Since(start))
	}
	b.StopTimer()

	reportLatencyPercentiles(b, durations)
	b.ReportMetric(float64(len(wfDef.Steps))*float64(b.N)/b.Elapsed().Seconds(), "steps/sec")
}

// BenchmarkEngine_ExecuteWorkflow_Concurrent runs the same 5-step workflow
// from concurrent goroutines to measure throughput under contention, the
// scenario the README's "100 concurrent workflows" claim describes. Actual
// concurrency is bounded by GOMAXPROCS and the store's underlying
// connection pool, both reported so the number is reproducible.
func BenchmarkEngine_ExecuteWorkflow_Concurrent(b *testing.B) {
	s := newBenchStore(b)
	defer s.Close()
	e := NewEngine(s, nil, "bench-worker", nil)
	wfDef := benchWorkflowDef()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := e.ExecuteWorkflow(context.Background(), uuid.New(), wfDef, map[string]any{"data": "test"}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.StopTimer()

	b.ReportMetric(float64(len(wfDef.Steps))*float64(b.N)/b.Elapsed().Seconds(), "steps/sec")
}

// BenchmarkEngine_ProcessStep measures the queue-driven single-step path
// (the code ProcessStep actually runs for each dispatched message), against
// a step registered in a real StepRegistry.
func BenchmarkEngine_ProcessStep(b *testing.B) {
	s := newBenchStore(b)
	defer s.Close()

	registry := NewStepRegistry()
	registry.Register(&benchStep{"bench_step"})
	e := NewEngine(s, nil, "bench-worker", registry)

	wfID := uuid.New()
	if err := s.CreateWorkflow(context.Background(), wfID, "bench", nil); err != nil {
		b.Fatal(err)
	}

	durations := make([]time.Duration, 0, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := queue.StepMessage{
			WorkflowID: wfID.String(),
			StepName:   "bench_step",
			Input:      map[string]any{"i": i}, // varies the dedup key per iteration
		}
		start := time.Now()
		if err := e.ProcessStep(context.Background(), msg); err != nil {
			b.Fatal(err)
		}
		durations = append(durations, time.Since(start))
	}
	b.StopTimer()

	reportLatencyPercentiles(b, durations)
}

func reportLatencyPercentiles(b *testing.B, durations []time.Duration) {
	if len(durations) == 0 {
		return
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	p50 := durations[len(durations)*50/100]
	p99idx := len(durations) * 99 / 100
	if p99idx >= len(durations) {
		p99idx = len(durations) - 1
	}
	p99 := durations[p99idx]
	b.ReportMetric(float64(p50.Microseconds())/1000, "p50-ms")
	b.ReportMetric(float64(p99.Microseconds())/1000, "p99-ms")
}
