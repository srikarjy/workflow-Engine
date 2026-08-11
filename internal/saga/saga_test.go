package saga

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/srikarjy/workflow_engine/internal/store"
)

func TestExecuteCompensation_RunsInReverseOrder(t *testing.T) {
	log := newFakeEventLog()
	wfID := uuid.New()

	plan := NewCompensationPlan()
	plan.AddStep("reserve_inventory", map[string]any{"a": 1}, "dedup-1")
	plan.AddStep("charge_payment", map[string]any{"b": 2}, "dedup-2")
	plan.AddStep("create_shipment", map[string]any{"c": 3}, "dedup-3")

	if err := plan.ExecuteCompensation(context.Background(), log, wfID); err != nil {
		t.Fatalf("ExecuteCompensation: %v", err)
	}

	completed := log.eventsOfType(store.EventCompensationCompleted)
	want := []string{"compensate_create_shipment", "compensate_charge_payment", "compensate_reserve_inventory"}
	if len(completed) != len(want) {
		t.Fatalf("got %d compensation_completed events, want %d", len(completed), len(want))
	}
	for i, name := range want {
		if completed[i].StepName != name {
			t.Errorf("compensation[%d].StepName = %q, want %q", i, completed[i].StepName, name)
		}
	}
}

func TestExecuteCompensation_EachStepExactlyOnce(t *testing.T) {
	log := newFakeEventLog()
	wfID := uuid.New()

	plan := NewCompensationPlan()
	plan.AddStep("reserve_inventory", map[string]any{"a": 1}, "dedup-1")
	plan.AddStep("charge_payment", map[string]any{"b": 2}, "dedup-2")

	if err := plan.ExecuteCompensation(context.Background(), log, wfID); err != nil {
		t.Fatalf("ExecuteCompensation: %v", err)
	}

	started := log.eventsOfType(store.EventCompensationStarted)
	completed := log.eventsOfType(store.EventCompensationCompleted)
	if len(started) != 2 {
		t.Errorf("compensation_started events = %d, want 2", len(started))
	}
	if len(completed) != 2 {
		t.Errorf("compensation_completed events = %d, want 2", len(completed))
	}
}

// TestExecuteCompensation_ResumesWithoutRepeatingFinishedSteps is the
// crash-recovery guarantee for the rollback path itself: re-running
// ExecuteCompensation for a plan where one step's compensation already
// completed must not attempt to compensate it a second time.
func TestExecuteCompensation_ResumesWithoutRepeatingFinishedSteps(t *testing.T) {
	log := newFakeEventLog()
	wfID := uuid.New()

	plan := NewCompensationPlan()
	plan.AddStep("reserve_inventory", map[string]any{"a": 1}, "dedup-1")
	plan.AddStep("charge_payment", map[string]any{"b": 2}, "dedup-2")

	// First run compensates both steps.
	if err := plan.ExecuteCompensation(context.Background(), log, wfID); err != nil {
		t.Fatalf("first run: %v", err)
	}
	firstRunCount := len(log.eventsOfType(store.EventCompensationCompleted))

	// A resumed run (e.g. a replacement worker after a crash) builds the
	// same plan and calls ExecuteCompensation again.
	plan2 := NewCompensationPlan()
	plan2.AddStep("reserve_inventory", map[string]any{"a": 1}, "dedup-1")
	plan2.AddStep("charge_payment", map[string]any{"b": 2}, "dedup-2")
	if err := plan2.ExecuteCompensation(context.Background(), log, wfID); err != nil {
		t.Fatalf("resumed run: %v", err)
	}

	secondRunCount := len(log.eventsOfType(store.EventCompensationCompleted))
	if secondRunCount != firstRunCount {
		t.Errorf("resumed run added %d more compensation_completed events, want 0 (already done)", secondRunCount-firstRunCount)
	}
}

func TestCompensateStep_IdempotentOnRepeatedCalls(t *testing.T) {
	log := newFakeEventLog()
	wfID := uuid.New()
	output := map[string]any{"x": 1}

	if err := CompensateStep(context.Background(), log, wfID, "reserve_inventory", output); err != nil {
		t.Fatalf("first call: %v", err)
	}
	if err := CompensateStep(context.Background(), log, wfID, "reserve_inventory", output); err != nil {
		t.Fatalf("second call should be a no-op, not an error: %v", err)
	}

	completed := log.eventsOfType(store.EventCompensationCompleted)
	if len(completed) != 1 {
		t.Errorf("compensation_completed events = %d, want exactly 1 across two calls", len(completed))
	}
}
