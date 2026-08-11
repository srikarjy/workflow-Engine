// Package store benchmarks. These exercise a real PostgreSQL connection —
// set BENCH_POSTGRES_DSN, or start the default docker-compose Postgres
// (`docker compose up -d postgres && go run ./cmd/migrate up`) and they'll
// use that. Without a reachable database they skip rather than fail.
package store

import (
	"context"
	"os"
	"testing"

	"github.com/google/uuid"
)

func benchDSN() string {
	if v := os.Getenv("BENCH_POSTGRES_DSN"); v != "" {
		return v
	}
	return "postgres://workflow:workflow@localhost:15432/workflow?sslmode=disable"
}

func newBenchStore(b *testing.B) *Store {
	b.Helper()
	dsn := benchDSN()
	s, err := New(context.Background(), dsn)
	if err != nil {
		b.Skipf("postgres unreachable at %s (%v); set BENCH_POSTGRES_DSN or run `docker compose up -d postgres && go run ./cmd/migrate up`", dsn, err)
	}
	return s
}

func BenchmarkStore_CreateWorkflow(b *testing.B) {
	s := newBenchStore(b)
	defer s.Close()
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.CreateWorkflow(ctx, uuid.New(), "bench", nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStore_AppendEvent(b *testing.B) {
	s := newBenchStore(b)
	defer s.Close()
	ctx := context.Background()

	wfID := uuid.New()
	if err := s.CreateWorkflow(ctx, wfID, "bench", nil); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := s.AppendEvent(ctx, Event{
			WorkflowID: wfID,
			StepName:   "bench_step",
			Type:       EventStepStarted,
			DedupKey:   uuid.New().String(),
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStore_HasCompleted(b *testing.B) {
	s := newBenchStore(b)
	defer s.Close()
	ctx := context.Background()

	wfID := uuid.New()
	if err := s.CreateWorkflow(ctx, wfID, "bench", nil); err != nil {
		b.Fatal(err)
	}
	dedupKey := uuid.New().String()
	if _, err := s.AppendEvent(ctx, Event{
		WorkflowID: wfID, StepName: "bench_step", Type: EventStepCompleted, DedupKey: dedupKey,
	}); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.HasCompleted(ctx, dedupKey); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStore_ReplayEvents(b *testing.B) {
	s := newBenchStore(b)
	defer s.Close()
	ctx := context.Background()

	wfID := uuid.New()
	if err := s.CreateWorkflow(ctx, wfID, "bench", nil); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if _, err := s.AppendEvent(ctx, Event{
			WorkflowID: wfID, StepName: "bench_step", Type: EventStepStarted, DedupKey: uuid.New().String(),
		}); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.ReplayEvents(ctx, wfID); err != nil {
			b.Fatal(err)
		}
	}
}
