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

	// Guards against compensation being log-only: the steps' actual
	// Compensate logic (not just the compensation_* events) must run.
	if step1.compensateCalls() != 1 {
		t.Errorf("step1.Compensate called %d times, want 1", step1.compensateCalls())
	}
	if step2.compensateCalls() != 1 {
		t.Errorf("step2.Compensate called %d times, want 1", step2.compensateCalls())
	}
}

// TestExecuteAgentic_RouterDecidesNextStep verifies that an agentic workflow
// uses the Router function to dynamically decide the next step based on
// the previous step's output.
func TestExecuteAgentic_RouterDecidesNextStep(t *testing.T) {
	log := newFakeEventLog()
	registry := NewStepRegistry()
	var steps []string
	mk := func(name string) *spyStep {
		return &spyStep{name: name, outputFunc: func(in map[string]any) map[string]any {
			steps = append(steps, name)
			out := map[string]any{}
			for k, v := range in {
				out[k] = v
			}
			out[name] = true
			return out
		}}
	}
	s1 := mk("retrieve_papers")
	s2 := mk("analyze_evidence")
	s3 := mk("synthesize_conclusion")
	registry.Register(s1)
	registry.Register(s2)
	registry.Register(s3)

	e := NewEngine(log, nil, "test-worker", registry)

	wfID := uuid.New()

	// Router that chains: retrieve -> analyze -> synthesize -> done
	calls := 0
	router := func(ctx context.Context, _ uuid.UUID, prevStep string, prevOutput map[string]any) (string, map[string]any, error) {
		calls++
		switch prevStep {
		case "retrieve_papers":
			return "analyze_evidence", prevOutput, nil
		case "analyze_evidence":
			return "synthesize_conclusion", prevOutput, nil
		case "synthesize_conclusion":
			return "", nil, nil // workflow complete
		default:
			return "", nil, errors.New("unexpected step")
		}
	}

	_, err := e.ExecuteAgentic(context.Background(), wfID, AgenticWorkflowConfig{
		StartStepName: "retrieve_papers",
		StartInput:    map[string]any{"query": "test"},
		Router:        router,
	})
	if err != nil {
		t.Fatalf("ExecuteAgentic: %v", err)
	}

	// Verify all three steps executed in order
	want := []string{"retrieve_papers", "analyze_evidence", "synthesize_conclusion"}
	if len(steps) != len(want) {
		t.Fatalf("steps = %v, want %v", steps, want)
	}
	for i := range want {
		if steps[i] != want[i] {
			t.Errorf("steps = %v, want %v", steps, want)
			break
		}
	}
	if calls != 3 {
		t.Errorf("router called %d times, want 3", calls)
	}
}

// TestExecuteAgentic_ResumesWithoutReExecutingCompletedSteps verifies
// crash recovery works for agentic workflows: a resumed run skips
// already-completed steps and continues from the router's decision.
func TestExecuteAgentic_ResumesWithoutReExecutingCompletedSteps(t *testing.T) {
	log := newFakeEventLog()
	registry := NewStepRegistry()

	step1 := &spyStep{name: "retrieve_papers"}
	step2 := &spyStep{name: "analyze_evidence"}
	step3 := &spyStep{name: "synthesize_conclusion"}
	registry.Register(step1)
	registry.Register(step2)
	registry.Register(step3)

	e := NewEngine(log, nil, "test-worker", registry)

	router := func(ctx context.Context, _ uuid.UUID, prevStep string, prevOutput map[string]any) (string, map[string]any, error) {
		switch prevStep {
		case "retrieve_papers":
			return "analyze_evidence", prevOutput, nil
		case "analyze_evidence":
			return "synthesize_conclusion", prevOutput, nil
		case "synthesize_conclusion":
			return "", nil, nil
		default:
			return "", nil, errors.New("unexpected step")
		}
	}

	wfID := uuid.New()

	// First run completes all steps.
	if _, err := e.ExecuteAgentic(context.Background(), wfID, AgenticWorkflowConfig{
		StartStepName: "retrieve_papers",
		StartInput:    map[string]any{"query": "test"},
		Router:        router,
	}); err != nil {
		t.Fatalf("first run: %v", err)
	}
	if step1.calls() != 1 || step2.calls() != 1 || step3.calls() != 1 {
		t.Fatalf("first run: calls=%d/%d/%d, want 1/1/1", step1.calls(), step2.calls(), step3.calls())
	}

	// Resume same workflow ID - should skip all completed steps.
	if _, err := e.ExecuteAgentic(context.Background(), wfID, AgenticWorkflowConfig{
		StartStepName: "retrieve_papers",
		StartInput:    map[string]any{"query": "test"},
		Router:        router,
	}); err != nil {
		t.Fatalf("resumed run: %v", err)
	}
	if step1.calls() != 1 || step2.calls() != 1 || step3.calls() != 1 {
		t.Errorf("resumed run re-executed: calls=%d/%d/%d, want 1/1/1", step1.calls(), step2.calls(), step3.calls())
	}
}

// TestExecuteAgentic_FailureCompensatesCompletedSteps verifies that
// if a step fails, the agentic workflow triggers Saga compensation
// for previously completed steps.
func TestExecuteAgentic_FailureCompensatesCompletedSteps(t *testing.T) {
	log := newFakeEventLog()
	registry := NewStepRegistry()

	step1 := &spyStep{name: "retrieve_papers"}
	step2 := &spyStep{name: "analyze_evidence"}
	step3 := &spyStep{name: "synthesize_conclusion", failWith: errForced}
	registry.Register(step1)
	registry.Register(step2)
	registry.Register(step3)

	e := NewEngine(log, nil, "test-worker", registry)

	router := func(ctx context.Context, _ uuid.UUID, prevStep string, prevOutput map[string]any) (string, map[string]any, error) {
		switch prevStep {
		case "retrieve_papers":
			return "analyze_evidence", prevOutput, nil
		case "analyze_evidence":
			return "synthesize_conclusion", prevOutput, nil
		case "synthesize_conclusion":
			return "", nil, nil
		default:
			return "", nil, errors.New("unexpected step")
		}
	}

	wfID := uuid.New()

	_, err := e.ExecuteAgentic(context.Background(), wfID, AgenticWorkflowConfig{
		StartStepName: "retrieve_papers",
		StartInput:    map[string]any{"query": "test"},
		Router:        router,
	})
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
	want := []string{"compensate_analyze_evidence", "compensate_retrieve_papers"}
	if len(compensationOrder) != len(want) {
		t.Fatalf("compensation order = %v, want %v", compensationOrder, want)
	}
	for i := range want {
		if compensationOrder[i] != want[i] {
			t.Errorf("compensation order = %v, want %v", compensationOrder, want)
			break
		}
	}
}

// TestRouterDecisionCache_CachesAndReplaysDecisions verifies that the
// RouterDecisionCache caches routing decisions and replays them for
// identical contexts, avoiding re-calling the underlying router.
func TestRouterDecisionCache_CachesAndReplaysDecisions(t *testing.T) {
	log := newFakeEventLog()
	registry := NewStepRegistry()

	step1 := &spyStep{name: "step1"}
	step2 := &spyStep{name: "step2"}
	step3 := &spyStep{name: "step3"}
	registry.Register(step1)
	registry.Register(step2)
	registry.Register(step3)

	e := NewEngine(log, nil, "test-worker", registry)

	cache := NewRouterDecisionCache()
	routerCalls := 0

	router := func(ctx context.Context, _ uuid.UUID, prevStep string, prevOutput map[string]any) (string, map[string]any, error) {
		routerCalls++
		switch prevStep {
		case "step1":
			return "step2", prevOutput, nil
		case "step2":
			return "step3", prevOutput, nil
		case "step3":
			return "", nil, nil
		default:
			return "", nil, errors.New("unexpected step")
		}
	}

	wfID := uuid.New()

	// First run - should call router for each step
	if _, err := e.ExecuteAgentic(context.Background(), wfID, AgenticWorkflowConfig{
		StartStepName: "step1",
		StartInput:    map[string]any{"query": "test"},
		Router:        router,
		RouterCache:   cache,
	}); err != nil {
		t.Fatalf("first run: %v", err)
	}

	if routerCalls != 3 {
		t.Errorf("first run: router called %d times, want 3", routerCalls)
	}

	// Second run with same workflow ID and input - should replay all decisions from cache
	routerCalls = 0
	wfID2 := uuid.New()
	if _, err := e.ExecuteAgentic(context.Background(), wfID2, AgenticWorkflowConfig{
		StartStepName: "step1",
		StartInput:    map[string]any{"query": "test"},
		Router:        router,
		RouterCache:   cache,
	}); err != nil {
		t.Fatalf("second run: %v", err)
	}

	if routerCalls != 0 {
		t.Errorf("second run: router called %d times, want 0 (cached)", routerCalls)
	}

	// Third run with different input - should call router again
	routerCalls = 0
	wfID3 := uuid.New()
	if _, err := e.ExecuteAgentic(context.Background(), wfID3, AgenticWorkflowConfig{
		StartStepName: "step1",
		StartInput:    map[string]any{"query": "different"},
		Router:        router,
		RouterCache:   cache,
	}); err != nil {
		t.Fatalf("third run: %v", err)
	}

	if routerCalls != 3 {
		t.Errorf("third run (different input): router called %d times, want 3", routerCalls)
	}
}

// TestExecuteAgentic_CacheWorksWithCrashRecovery verifies that the
// RouterDecisionCache works correctly when a workflow crashes and
// is resumed - it should replay cached decisions for already-completed steps.
func TestExecuteAgentic_CacheWorksWithCrashRecovery(t *testing.T) {
	log := newFakeEventLog()
	registry := NewStepRegistry()

	step1 := &spyStep{name: "retrieve_papers"}
	step2 := &spyStep{name: "analyze_evidence"}
	step3 := &spyStep{name: "synthesize_conclusion"}
	registry.Register(step1)
	registry.Register(step2)
	registry.Register(step3)

	e := NewEngine(log, nil, "test-worker", registry)

	cache := NewRouterDecisionCache()
	routerCalls := 0

	router := func(ctx context.Context, _ uuid.UUID, prevStep string, prevOutput map[string]any) (string, map[string]any, error) {
		routerCalls++
		switch prevStep {
		case "retrieve_papers":
			return "analyze_evidence", prevOutput, nil
		case "analyze_evidence":
			return "synthesize_conclusion", prevOutput, nil
		case "synthesize_conclusion":
			return "", nil, nil
		default:
			return "", nil, errors.New("unexpected step")
		}
	}

	wfID := uuid.New()

	// First run completes all steps
	if _, err := e.ExecuteAgentic(context.Background(), wfID, AgenticWorkflowConfig{
		StartStepName: "retrieve_papers",
		StartInput:    map[string]any{"query": "test"},
		Router:        router,
		RouterCache:   cache,
	}); err != nil {
		t.Fatalf("first run: %v", err)
	}

	initialRouterCalls := routerCalls
	if initialRouterCalls != 3 {
		t.Errorf("first run: router called %d times, want 3", initialRouterCalls)
	}

	// Resume same workflow ID - should skip completed steps and use cached decisions
	routerCalls = 0
	if _, err := e.ExecuteAgentic(context.Background(), wfID, AgenticWorkflowConfig{
		StartStepName: "retrieve_papers",
		StartInput:    map[string]any{"query": "test"},
		Router:        router,
		RouterCache:   cache,
	}); err != nil {
		t.Fatalf("resumed run: %v", err)
	}

	// No router calls should be made since all steps are completed and cached
	if routerCalls != 0 {
		t.Errorf("resumed run: router called %d times, want 0 (all cached)", routerCalls)
	}

	// Verify steps were not re-executed
	if step1.calls() != 1 || step2.calls() != 1 || step3.calls() != 1 {
		t.Errorf("resumed run re-executed: calls=%d/%d/%d, want 1/1/1", step1.calls(), step2.calls(), step3.calls())
	}
}
