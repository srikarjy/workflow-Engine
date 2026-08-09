// Package engine benchmarks.
package engine

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/srikarjy/workflow_engine/internal/idempotency"
	"github.com/srikarjy/workflow_engine/internal/store"
)

// Mock store for benchmarking without database - thread-safe
type mockStore struct {
	mu        sync.RWMutex
	events    map[string][]store.Event
	workflows map[uuid.UUID]*store.Workflow
	completed map[string]bool
	counter   atomic.Int64
}

func newMockStore() *mockStore {
	return &mockStore{
		events:    make(map[string][]store.Event),
		workflows: make(map[uuid.UUID]*store.Workflow),
		completed: make(map[string]bool),
	}
}

func (m *mockStore) CreateWorkflow(ctx context.Context, id uuid.UUID, name string, input json.RawMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workflows[id] = &store.Workflow{ID: id, Name: name, Status: store.StatusRunning, Input: input}
	return nil
}

func (m *mockStore) GetWorkflow(ctx context.Context, id uuid.UUID) (store.Workflow, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	w, ok := m.workflows[id]
	if !ok {
		return store.Workflow{}, store.ErrNotFound
	}
	return *w, nil
}

func (m *mockStore) UpdateWorkflowStatus(ctx context.Context, id uuid.UUID, status store.WorkflowStatus) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if w, ok := m.workflows[id]; ok {
		w.Status = status
	}
	return nil
}

func (m *mockStore) AppendEvent(ctx context.Context, e store.Event) (store.Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := e.DedupKey
	if e.Type == store.EventStepCompleted || e.Type == store.EventCompensationCompleted {
		if m.completed[key] {
			return store.Event{}, store.ErrDuplicateEvent
		}
		m.completed[key] = true
	}
	e.ID = m.counter.Add(1)
	e.CreatedAt = time.Now()
	m.events[e.WorkflowID.String()] = append(m.events[e.WorkflowID.String()], e)
	return e, nil
}

func (m *mockStore) HasCompleted(ctx context.Context, dedupKey string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.completed[dedupKey], nil
}

func (m *mockStore) ReplayEvents(ctx context.Context, workflowID uuid.UUID) ([]store.Event, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.events[workflowID.String()], nil
}

type mockEngine struct {
	store *mockStore
}

func (e *mockEngine) ExecuteWorkflow(ctx context.Context, wfDef *WorkflowDefinition, input map[string]any) (uuid.UUID, error) {
	wfID := uuid.New()
	_ = e.store.CreateWorkflow(ctx, wfID, wfDef.Name, mustMarshal(input))
	for _, step := range wfDef.Steps {
		dedupKey, _ := idempotency.DedupKey(wfID.String(), step.Name(), input)
		completed, _ := e.store.HasCompleted(ctx, dedupKey)
		if completed {
			continue
		}
		output, _ := step.Execute(ctx, input)
		_, _ = e.store.AppendEvent(ctx, store.Event{
			WorkflowID: wfID,
			StepName:   step.Name(),
			Type:       store.EventStepCompleted,
			DedupKey:   dedupKey,
			Payload:    mustMarshal(output),
		})
		input = output
	}
	return wfID, nil
}

type benchStep struct {
	name string
}

func (b *benchStep) Name() string { return b.name }
func (b *benchStep) Execute(ctx context.Context, input map[string]any) (map[string]any, error) {
	return map[string]any{"output": b.name}, nil
}
func (b *benchStep) Compensate(ctx context.Context, output map[string]any) error {
	return nil
}

func BenchmarkEngine_ExecuteWorkflow(b *testing.B) {
	s := newMockStore()
	e := &mockEngine{store: s}

	wfDef := &WorkflowDefinition{
		Name: "benchmark",
		Steps: []StepExecutor{
			&benchStep{name: "step1"},
			&benchStep{name: "step2"},
			&benchStep{name: "step3"},
			&benchStep{name: "step4"},
			&benchStep{name: "step5"},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = e.ExecuteWorkflow(context.Background(), wfDef, map[string]any{"data": "test"})
	}
}