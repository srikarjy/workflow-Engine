package saga

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/srikarjy/workflow_engine/internal/store"
)

func TestExecuteCompensation_RunsInReverseOrder(t *testing.T) {
	log := newFakeEventLog()
	wfID := uuid.New()

	plan := NewCompensationPlan()
	plan.AddStep("reserve_inventory", map[string]any{"a": 1}, "dedup-1", nil)
	plan.AddStep("charge_payment", map[string]any{"b": 2}, "dedup-2", nil)
	plan.AddStep("create_shipment", map[string]any{"c": 3}, "dedup-3", nil)

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
	plan.AddStep("reserve_inventory", map[string]any{"a": 1}, "dedup-1", nil)
	plan.AddStep("charge_payment", map[string]any{"b": 2}, "dedup-2", nil)

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
	plan.AddStep("reserve_inventory", map[string]any{"a": 1}, "dedup-1", nil)
	plan.AddStep("charge_payment", map[string]any{"b": 2}, "dedup-2", nil)

	// First run compensates both steps.
	if err := plan.ExecuteCompensation(context.Background(), log, wfID); err != nil {
		t.Fatalf("first run: %v", err)
	}
	firstRunCount := len(log.eventsOfType(store.EventCompensationCompleted))

	// A resumed run (e.g. a replacement worker after a crash) builds the
	// same plan and calls ExecuteCompensation again.
	plan2 := NewCompensationPlan()
	plan2.AddStep("reserve_inventory", map[string]any{"a": 1}, "dedup-1", nil)
	plan2.AddStep("charge_payment", map[string]any{"b": 2}, "dedup-2", nil)
	if err := plan2.ExecuteCompensation(context.Background(), log, wfID); err != nil {
		t.Fatalf("resumed run: %v", err)
	}

	secondRunCount := len(log.eventsOfType(store.EventCompensationCompleted))
	if secondRunCount != firstRunCount {
		t.Errorf("resumed run added %d more compensation_completed events, want 0 (already done)", secondRunCount-firstRunCount)
	}
}

// TestExecuteCompensation_CallsRealCompensateFunc guards against the plan
// only recording compensation_started/compensation_completed events without
// ever running the step's actual rollback logic.
func TestExecuteCompensation_CallsRealCompensateFunc(t *testing.T) {
	log := newFakeEventLog()
	wfID := uuid.New()

	var calledWith []string
	compensate := func(name string) func(ctx context.Context, output map[string]any) error {
		return func(ctx context.Context, output map[string]any) error {
			calledWith = append(calledWith, name)
			return nil
		}
	}

	plan := NewCompensationPlan()
	plan.AddStep("reserve_inventory", map[string]any{"a": 1}, "dedup-1", compensate("reserve_inventory"))
	plan.AddStep("charge_payment", map[string]any{"b": 2}, "dedup-2", compensate("charge_payment"))

	if err := plan.ExecuteCompensation(context.Background(), log, wfID); err != nil {
		t.Fatalf("ExecuteCompensation: %v", err)
	}

	want := []string{"charge_payment", "reserve_inventory"} // reverse order
	if len(calledWith) != len(want) {
		t.Fatalf("Compensate called %d times, want %d: %v", len(calledWith), len(want), calledWith)
	}
	for i, name := range want {
		if calledWith[i] != name {
			t.Errorf("call[%d] = %q, want %q", i, calledWith[i], name)
		}
	}
}

// TestExecuteCompensation_StepWithNilCompensateIsNoop covers steps with
// nothing to roll back (e.g. an analytics event): passing nil must not panic
// and must still record compensation_completed.
func TestExecuteCompensation_StepWithNilCompensateIsNoop(t *testing.T) {
	log := newFakeEventLog()
	wfID := uuid.New()

	plan := NewCompensationPlan()
	plan.AddStep("update_analytics", map[string]any{}, "dedup-1", nil)

	if err := plan.ExecuteCompensation(context.Background(), log, wfID); err != nil {
		t.Fatalf("ExecuteCompensation: %v", err)
	}
	if len(log.eventsOfType(store.EventCompensationCompleted)) != 1 {
		t.Errorf("want 1 compensation_completed event for nil-Compensate step")
	}
}

// TestExecuteCompensation_FailedCompensateAbortsWithoutMarkingComplete
// ensures a rollback call that actually fails isn't silently recorded as
// having succeeded — and that a resumed run will retry it, matching the
// idempotent-retry model the rest of the engine uses for external calls.
func TestExecuteCompensation_FailedCompensateAbortsWithoutMarkingComplete(t *testing.T) {
	log := newFakeEventLog()
	wfID := uuid.New()

	failing := func(ctx context.Context, output map[string]any) error {
		return errors.New("release_inventory: service unavailable")
	}

	plan := NewCompensationPlan()
	plan.AddStep("reserve_inventory", map[string]any{"a": 1}, "dedup-1", failing)

	err := plan.ExecuteCompensation(context.Background(), log, wfID)
	if err == nil {
		t.Fatal("ExecuteCompensation: want error when Compensate fails, got nil")
	}
	if len(log.eventsOfType(store.EventCompensationCompleted)) != 0 {
		t.Error("compensation_completed recorded despite Compensate failing")
	}
	if len(log.eventsOfType(store.EventCompensationStarted)) != 1 {
		t.Error("compensation_started should still be recorded before the failing call")
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
