package engine

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/google/uuid"

	"github.com/srikarjy/workflow_engine/internal/store"
)

// fakeEventLog is an in-memory store.EventLog used to unit test Engine and
// Saga logic without a live PostgreSQL connection. It reproduces the two
// invariants the real Store enforces: CreateWorkflow is idempotent on id,
// and AppendEvent returns store.ErrDuplicateEvent on a second attempt to
// write a completion event (step_completed / compensation_completed) under
// the same dedup key — the in-memory equivalent of the partial unique index
// in migrations/0001_init.up.sql.
type fakeEventLog struct {
	mu        sync.Mutex
	workflows map[uuid.UUID]store.Workflow
	events    map[uuid.UUID][]store.Event
	completed map[string]bool
	nextID    int64
}

func newFakeEventLog() *fakeEventLog {
	return &fakeEventLog{
		workflows: make(map[uuid.UUID]store.Workflow),
		events:    make(map[uuid.UUID][]store.Event),
		completed: make(map[string]bool),
	}
}

func (f *fakeEventLog) CreateWorkflow(ctx context.Context, id uuid.UUID, name string, input json.RawMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, exists := f.workflows[id]; exists {
		return nil
	}
	f.workflows[id] = store.Workflow{ID: id, Name: name, Status: store.StatusRunning, Input: input}
	return nil
}

func (f *fakeEventLog) UpdateWorkflowStatus(ctx context.Context, id uuid.UUID, status store.WorkflowStatus) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if w, ok := f.workflows[id]; ok {
		w.Status = status
		f.workflows[id] = w
	}
	return nil
}

func (f *fakeEventLog) AppendEvent(ctx context.Context, e store.Event) (store.Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if e.Type == store.EventStepCompleted || e.Type == store.EventCompensationCompleted {
		if f.completed[e.DedupKey] {
			return store.Event{}, store.ErrDuplicateEvent
		}
		f.completed[e.DedupKey] = true
	}

	f.nextID++
	e.ID = f.nextID
	f.events[e.WorkflowID] = append(f.events[e.WorkflowID], e)
	return e, nil
}

func (f *fakeEventLog) HasCompleted(ctx context.Context, dedupKey string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.completed[dedupKey], nil
}

func (f *fakeEventLog) ReplayEvents(ctx context.Context, workflowID uuid.UUID) ([]store.Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]store.Event, len(f.events[workflowID]))
	copy(out, f.events[workflowID])
	return out, nil
}

// markCompleted simulates another worker having already committed a
// completion event for dedupKey, without going through AppendEvent — used
// to test the "we lost the race" tolerance branches in ProcessStep and
// ExecuteWorkflow.
func (f *fakeEventLog) markCompleted(dedupKey string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.completed[dedupKey] = true
}

// countEvents returns how many events of type t are recorded for stepName
// across all workflows, for asserting exactly-once (never 0, never >1).
func (f *fakeEventLog) countEvents(stepName string, t store.EventType) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, events := range f.events {
		for _, e := range events {
			if e.StepName == stepName && e.Type == t {
				n++
			}
		}
	}
	return n
}
