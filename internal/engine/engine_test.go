package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/srikarjy/workflow_engine/internal/idempotency"
	"github.com/srikarjy/workflow_engine/internal/queue"
	"github.com/srikarjy/workflow_engine/internal/store"
)

func TestProcessStep_ExecutesRegisteredStep(t *testing.T) {
	log := newFakeEventLog()
	step := &spyStep{name: "reserve_inventory"}
	registry := NewStepRegistry()
	registry.Register(step)
	e := NewEngine(log, nil, "test-worker", registry)

	wfID := uuid.New()
	if err := log.CreateWorkflow(context.Background(), wfID, "test", nil); err != nil {
		t.Fatal(err)
	}

	msg := queue.StepMessage{WorkflowID: wfID.String(), StepName: "reserve_inventory", Input: map[string]any{"data": "x"}}
	if err := e.ProcessStep(context.Background(), msg); err != nil {
		t.Fatalf("ProcessStep: %v", err)
	}

	if step.calls() != 1 {
		t.Errorf("step executed %d times, want 1", step.calls())
	}
	if got := log.countEvents("reserve_inventory", store.EventStepCompleted); got != 1 {
		t.Errorf("step_completed events = %d, want 1", got)
	}
}

func TestProcessStep_SkipsAlreadyCompletedStep(t *testing.T) {
	log := newFakeEventLog()
	step := &spyStep{name: "reserve_inventory"}
	registry := NewStepRegistry()
	registry.Register(step)
	e := NewEngine(log, nil, "test-worker", registry)

	wfID := uuid.New()
	input := map[string]any{"data": "x"}
	dedupKey, _ := idempotency.DedupKey(wfID.String(), "reserve_inventory", input)
	log.markCompleted(dedupKey) // simulate crash recovery: another run already finished this

	msg := queue.StepMessage{WorkflowID: wfID.String(), StepName: "reserve_inventory", Input: input, DedupKey: dedupKey}
	if err := e.ProcessStep(context.Background(), msg); err != nil {
		t.Fatalf("ProcessStep: %v", err)
	}

	if step.calls() != 0 {
		t.Errorf("business logic ran %d times for an already-completed step, want 0", step.calls())
	}
}

func TestProcessStep_UnregisteredStepReturnsError(t *testing.T) {
	log := newFakeEventLog()
	e := NewEngine(log, nil, "test-worker", NewStepRegistry())

	msg := queue.StepMessage{WorkflowID: uuid.New().String(), StepName: "nonexistent", Input: map[string]any{}}
	err := e.ProcessStep(context.Background(), msg)
	if !errors.Is(err, ErrStepNotRegistered) {
		t.Errorf("err = %v, want ErrStepNotRegistered", err)
	}
}

func TestProcessStep_ExecuteFailureRecordsStepFailed(t *testing.T) {
	log := newFakeEventLog()
	step := &spyStep{name: "charge_payment", failWith: errForced}
	registry := NewStepRegistry()
	registry.Register(step)
	e := NewEngine(log, nil, "test-worker", registry)

	wfID := uuid.New()
	msg := queue.StepMessage{WorkflowID: wfID.String(), StepName: "charge_payment", Input: map[string]any{}}
	err := e.ProcessStep(context.Background(), msg)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, errForced) {
		t.Errorf("err = %v, want it to wrap errForced", err)
	}
	if got := log.countEvents("charge_payment", store.EventStepFailed); got != 1 {
		t.Errorf("step_failed events = %d, want 1", got)
	}
	if got := log.countEvents("charge_payment", store.EventStepCompleted); got != 0 {
		t.Errorf("step_completed events = %d, want 0 for a failed step", got)
	}
}

// TestProcessStep_TeleratesLostCompletionRace covers the case where another
// worker commits the step_completed event between this worker's HasCompleted
// check and its own AppendEvent call — the store.ErrDuplicateEvent branch.
// ProcessStep must treat that as success, not surface it as an error.
func TestProcessStep_ToleratesLostCompletionRace(t *testing.T) {
	log := newFakeEventLog()
	step := &spyStep{name: "reserve_inventory"}
	registry := NewStepRegistry()
	registry.Register(step)
	e := NewEngine(log, nil, "test-worker", registry)

	wfID := uuid.New()
	input := map[string]any{"data": "x"}
	dedupKey, _ := idempotency.DedupKey(wfID.String(), "reserve_inventory", input)

	// Not completed yet when ProcessStep checks, so it proceeds to execute...
	// but by the time it tries to record completion, another worker's own
	// step_completed write has already landed. Appending that event directly
	// (rather than just flipping the completed flag) deterministically
	// simulates the race landing between this worker's check and its write,
	// while keeping the log itself realistic: exactly one completion event.
	step.outputFunc = func(in map[string]any) map[string]any {
		_, err := log.AppendEvent(context.Background(), store.Event{
			WorkflowID: wfID,
			StepName:   "reserve_inventory",
			Type:       store.EventStepCompleted,
			DedupKey:   dedupKey,
		})
		if err != nil {
			t.Fatalf("simulating the other worker's write: %v", err)
		}
		return map[string]any{"ok": true}
	}

	msg := queue.StepMessage{WorkflowID: wfID.String(), StepName: "reserve_inventory", Input: input, DedupKey: dedupKey}
	if err := e.ProcessStep(context.Background(), msg); err != nil {
		t.Fatalf("ProcessStep should tolerate a lost completion race, got: %v", err)
	}
	if got := log.countEvents("reserve_inventory", store.EventStepCompleted); got != 1 {
		t.Errorf("step_completed events = %d, want exactly 1 despite the race", got)
	}
}

func TestExecuteWorkflow_RunsStepsInOrderChainingOutput(t *testing.T) {
	log := newFakeEventLog()
	e := NewEngine(log, nil, "test-worker", nil)

	var order []string
	mk := func(name string) *spyStep {
		return &spyStep{name: name, outputFunc: func(in map[string]any) map[string]any {
			order = append(order, name)
			out := map[string]any{}
			for k, v := range in {
				out[k] = v
			}
			out[name] = true
			return out
		}}
	}
	s1, s2, s3 := mk("step1"), mk("step2"), mk("step3")
	wfDef := &WorkflowDefinition{Name: "wf", Steps: []StepExecutor{s1, s2, s3}}

	wfID, err := e.ExecuteWorkflow(context.Background(), uuid.New(), wfDef, map[string]any{"start": true})
	if err != nil {
		t.Fatalf("ExecuteWorkflow: %v", err)
	}
	if wfID == uuid.Nil {
		t.Fatal("expected a non-nil workflow ID")
	}

	want := []string{"step1", "step2", "step3"}
	if len(order) != len(want) {
		t.Fatalf("execution order = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Errorf("execution order = %v, want %v", order, want)
			break
		}
	}
}

// TestExecuteWorkflow_ResumesWithoutReExecutingCompletedSteps is the core
// crash-recovery guarantee: re-running a workflow whose first step already
// has a step_completed event in the log must not re-run that step's
// business logic, only continue from where it left off.
func TestExecuteWorkflow_ResumesWithoutReExecutingCompletedSteps(t *testing.T) {
	log := newFakeEventLog()
	e := NewEngine(log, nil, "test-worker", nil)

	step1 := &spyStep{name: "step1"}
	step2 := &spyStep{name: "step2"}
	wfDef := &WorkflowDefinition{Name: "wf", Steps: []StepExecutor{step1, step2}}
	input := map[string]any{"data": "x"}
	wfID := uuid.New()

	// First run completes both steps.
	if _, err := e.ExecuteWorkflow(context.Background(), wfID, wfDef, input); err != nil {
		t.Fatalf("first run: %v", err)
	}
	if step1.calls() != 1 || step2.calls() != 1 {
		t.Fatalf("first run: step1 calls=%d step2 calls=%d, want 1 and 1", step1.calls(), step2.calls())
	}

	// A "replacement worker" resumes the same workflow ID with the same
	// input, as cmd/faultinject does after a crash.
	if _, err := e.ExecuteWorkflow(context.Background(), wfID, wfDef, input); err != nil {
		t.Fatalf("resumed run: %v", err)
	}
	if step1.calls() != 1 || step2.calls() != 1 {
		t.Errorf("resumed run re-executed a completed step: step1 calls=%d step2 calls=%d, want 1 and 1", step1.calls(), step2.calls())
	}
}

// TestExecuteWorkflow_FailureCompensatesCompletedStepsInReverseOrder checks
// the Saga wiring end to end: when the last step fails, only the steps that
// actually completed get compensated, and in reverse order.
func TestExecuteWorkflow_FailureCompensatesCompletedStepsInReverseOrder(t *testing.T) {
	log := newFakeEventLog()
	e := NewEngine(log, nil, "test-worker", nil)

	step1 := &spyStep{name: "reserve_inventory"}
	step2 := &spyStep{name: "charge_payment"}
	step3 := &spyStep{name: "create_shipment", failWith: errForced}
	wfDef := &WorkflowDefinition{Name: "wf", Steps: []StepExecutor{step1, step2, step3}}
	wfID := uuid.New()

	_, err := e.ExecuteWorkflow(context.Background(), wfID, wfDef, map[string]any{})
	if err == nil {
		t.Fatal("expected an error from the failing final step")
	}

	events, _ := log.ReplayEvents(context.Background(), wfID)
	var compensationOrder []string
	for _, ev := range events {
		if ev.Type == store.EventCompensationCompleted {
			compensationOrder = append(compensationOrder, ev.StepName)
		}
	}
	want := []string{"compensate_charge_payment", "compensate_reserve_inventory"}
	if len(compensationOrder) != len(want) {
		t.Fatalf("compensation order = %v, want %v", compensationOrder, want)
	}
	for i := range want {
		if compensationOrder[i] != want[i] {
			t.Errorf("compensation order = %v, want %v", compensationOrder, want)
			break
		}
	}

	if step3.calls() != 1 {
		t.Errorf("failing step executed %d times, want 1", step3.calls())
	}
}
