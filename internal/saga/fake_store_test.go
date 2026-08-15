package saga

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/google/uuid"

	"github.com/srikarjy/workflow_engine/internal/store"
)

// fakeEventLog is a minimal in-memory store.EventLog for unit testing
// ExecuteCompensation/CompensateStep without a live PostgreSQL connection.
// It enforces the same invariant the real Store does: a second AppendEvent
// for a completion event type (compensation_completed) under an
// already-used dedup key returns store.ErrDuplicateEvent.
type fakeEventLog struct {
	mu            sync.Mutex
	events        map[uuid.UUID][]store.Event
	completed     map[string]bool
	notifications map[string]bool
	nextID        int64
}

func newFakeEventLog() *fakeEventLog {
	return &fakeEventLog{
		events:        make(map[uuid.UUID][]store.Event),
		completed:     make(map[string]bool),
		notifications: make(map[string]bool),
	}
}

func (f *fakeEventLog) CreateWorkflow(ctx context.Context, id uuid.UUID, name string, input json.RawMessage) error {
	return nil
}

func (f *fakeEventLog) UpdateWorkflowStatus(ctx context.Context, id uuid.UUID, status store.WorkflowStatus) error {
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
	if e.Type == store.EventNotificationSent {
		if f.notifications[e.DedupKey] {
			return store.Event{}, store.ErrDuplicateEvent
		}
		f.notifications[e.DedupKey] = true
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

func (f *fakeEventLog) HasNotificationSent(ctx context.Context, dedupKey string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.notifications[dedupKey], nil
}

func (f *fakeEventLog) ReplayEvents(ctx context.Context, workflowID uuid.UUID) ([]store.Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]store.Event, len(f.events[workflowID]))
	copy(out, f.events[workflowID])
	return out, nil
}

func (f *fakeEventLog) eventsOfType(t store.EventType) []store.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []store.Event
	for _, events := range f.events {
		for _, e := range events {
			if e.Type == t {
				out = append(out, e)
			}
		}
	}
	return out
}
