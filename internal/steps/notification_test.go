package steps

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/srikarjy/workflow_engine/internal/store"
)

// notifFakeEventLog is a minimal in-memory store.EventLog for exercising the
// notification step's idempotency contract: HasNotificationSent reports true
// once a notification_sent event was appended under a dedup key.
type notifFakeEventLog struct {
	mu     sync.Mutex
	events []store.Event
	nextID int64
}

func (f *notifFakeEventLog) CreateWorkflow(ctx context.Context, id uuid.UUID, name string, input json.RawMessage) error {
	return nil
}

func (f *notifFakeEventLog) UpdateWorkflowStatus(ctx context.Context, id uuid.UUID, status store.WorkflowStatus) error {
	return nil
}

func (f *notifFakeEventLog) AppendEvent(ctx context.Context, e store.Event) (store.Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if e.Type == store.EventNotificationSent {
		for _, existing := range f.events {
			if existing.Type == store.EventNotificationSent && existing.DedupKey == e.DedupKey {
				return store.Event{}, store.ErrDuplicateEvent
			}
		}
	}
	f.nextID++
	e.ID = f.nextID
	f.events = append(f.events, e)
	return e, nil
}

func (f *notifFakeEventLog) HasCompleted(ctx context.Context, dedupKey string) (bool, error) {
	return false, nil
}

func (f *notifFakeEventLog) HasNotificationSent(ctx context.Context, dedupKey string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range f.events {
		if e.Type == store.EventNotificationSent && e.DedupKey == dedupKey {
			return true, nil
		}
	}
	return false, nil
}

func (f *notifFakeEventLog) ReplayEvents(ctx context.Context, workflowID uuid.UUID) ([]store.Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]store.Event, len(f.events))
	copy(out, f.events)
	return out, nil
}

func (f *notifFakeEventLog) sentEventCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, e := range f.events {
		if e.Type == store.EventNotificationSent {
			n++
		}
	}
	return n
}

func testEmailConfig() NotificationConfig {
	return NotificationConfig{
		ConfidenceThreshold: 0.7,
		Channels:            []string{"email"},
		EmailConfig: &EmailNotificationConfig{
			To:      []string{"team@example.com"},
			Subject: "done",
			Body:    "workflow complete",
		},
	}
}

func testInput(confidence float64) map[string]any {
	return map[string]any{
		"workflow_id": "5f6a2c1e-9b3d-4e8f-a1b2-c3d4e5f60718",
		"confidence":  confidence,
	}
}

func TestNotificationSendsAboveThreshold(t *testing.T) {
	fake := &notifFakeEventLog{}
	step := NewNotificationStep("notify", testEmailConfig(), fake)

	out, err := step.Execute(context.Background(), testInput(0.9))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if sent, _ := out["notification_sent"].(bool); !sent {
		t.Fatalf("expected notification_sent=true, got %v", out)
	}
	if n := fake.sentEventCount(); n != 1 {
		t.Fatalf("expected 1 notification_sent event, got %d", n)
	}
}

func TestNotificationSkippedBelowThreshold(t *testing.T) {
	fake := &notifFakeEventLog{}
	step := NewNotificationStep("notify", testEmailConfig(), fake)

	out, err := step.Execute(context.Background(), testInput(0.5))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if skipped, _ := out["notification_skipped"].(bool); !skipped {
		t.Fatalf("expected notification_skipped=true, got %v", out)
	}
	if n := fake.sentEventCount(); n != 0 {
		t.Fatalf("expected no notification_sent events, got %d", n)
	}
}

func TestNotificationIdempotentAcrossRetry(t *testing.T) {
	fake := &notifFakeEventLog{}
	step := NewNotificationStep("notify", testEmailConfig(), fake)
	input := testInput(0.9)

	if _, err := step.Execute(context.Background(), input); err != nil {
		t.Fatalf("first Execute: %v", err)
	}
	out, err := step.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("second Execute: %v", err)
	}
	if skipped, _ := out["notification_skipped"].(bool); !skipped {
		t.Fatalf("expected retry to be skipped as already sent, got %v", out)
	}
	if reason, _ := out["reason"].(string); reason != "already sent" {
		t.Fatalf("expected reason 'already sent', got %q", reason)
	}
	if n := fake.sentEventCount(); n != 1 {
		t.Fatalf("expected exactly 1 notification_sent event after retry, got %d", n)
	}
}

func TestNotificationAllChannelsFailedFailsStep(t *testing.T) {
	fake := &notifFakeEventLog{}
	// "email" with a nil EmailConfig fails, as does an unknown channel.
	config := NotificationConfig{
		ConfidenceThreshold: 0.7,
		Channels:            []string{"email", "carrier-pigeon"},
	}
	step := NewNotificationStep("notify", config, fake)

	_, err := step.Execute(context.Background(), testInput(0.9))
	if err == nil {
		t.Fatal("expected error when every channel fails")
	}
	if !strings.Contains(err.Error(), "all 2 channels failed") {
		t.Fatalf("unexpected error: %v", err)
	}
	// Nothing was delivered, so no event may exist: a retry must re-send.
	if n := fake.sentEventCount(); n != 0 {
		t.Fatalf("expected no notification_sent events after total failure, got %d", n)
	}
}

func TestNotificationPartialFailureStillRecordsSent(t *testing.T) {
	fake := &notifFakeEventLog{}
	config := testEmailConfig()
	config.Channels = []string{"email", "carrier-pigeon"}
	step := NewNotificationStep("notify", config, fake)

	out, err := step.Execute(context.Background(), testInput(0.9))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if sent, _ := out["notification_sent"].(bool); !sent {
		t.Fatalf("expected partial success to count as sent, got %v", out)
	}
	results, _ := out["results"].([]map[string]any)
	if len(results) != 2 {
		t.Fatalf("expected 2 per-channel results, got %v", out["results"])
	}
	if results[0]["status"] != "sent" || results[1]["status"] != "failed" {
		t.Fatalf("expected sent+failed statuses, got %v", results)
	}
	if n := fake.sentEventCount(); n != 1 {
		t.Fatalf("expected 1 notification_sent event, got %d", n)
	}
}

func TestNotificationRejectsInvalidWorkflowID(t *testing.T) {
	fake := &notifFakeEventLog{}
	step := NewNotificationStep("notify", testEmailConfig(), fake)

	input := testInput(0.9)
	input["workflow_id"] = "not-a-uuid"
	_, err := step.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error for invalid workflow_id")
	}
	if !strings.Contains(err.Error(), "invalid workflow_id") {
		t.Fatalf("unexpected error: %v", err)
	}
	if n := fake.sentEventCount(); n != 0 {
		t.Fatalf("expected no events, got %d", n)
	}
}
