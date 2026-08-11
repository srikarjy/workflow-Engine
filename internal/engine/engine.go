// Package engine implements the workflow execution engine.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/google/uuid"

	"github.com/srikarjy/workflow_engine/internal/faultinject"
	"github.com/srikarjy/workflow_engine/internal/idempotency"
	"github.com/srikarjy/workflow_engine/internal/queue"
	"github.com/srikarjy/workflow_engine/internal/saga"
	"github.com/srikarjy/workflow_engine/internal/store"
)

// ErrStepNotRegistered is returned when a queue-dispatched step name has no
// corresponding executor in the engine's registry.
var ErrStepNotRegistered = errors.New("engine: step not registered")

// StepRegistry resolves step names to their executors. Queue messages carry
// only a step name (they cross a Redis stream as JSON), so a worker process
// needs this lookup to find the business logic to run for ProcessStep.
type StepRegistry struct {
	mu    sync.RWMutex
	steps map[string]StepExecutor
}

// NewStepRegistry creates an empty step registry.
func NewStepRegistry() *StepRegistry {
	return &StepRegistry{steps: make(map[string]StepExecutor)}
}

// Register adds a step executor under its own Name().
func (r *StepRegistry) Register(step StepExecutor) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.steps[step.Name()] = step
}

// Get looks up a step executor by name.
func (r *StepRegistry) Get(name string) (StepExecutor, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	step, ok := r.steps[name]
	return step, ok
}

// StepExecutor defines the interface for executing a workflow step.
type StepExecutor interface {
	Execute(ctx context.Context, input map[string]any) (map[string]any, error)
	Compensate(ctx context.Context, output map[string]any) error
	Name() string
}

// WorkflowDefinition defines a workflow as a sequence of steps.
type WorkflowDefinition struct {
	Name  string
	Steps []StepExecutor
}

// Engine coordinates workflow execution with exactly-once guarantees.
type Engine struct {
	store    store.EventLog
	queue    *queue.Client
	workerID string
	registry *StepRegistry
}

// NewEngine creates a new workflow engine. registry resolves step names for
// queue-dispatched messages processed via ProcessStep; pass NewStepRegistry()
// with the relevant steps registered, or nil if the engine is only used via
// ExecuteWorkflow (which carries its own StepExecutor instances directly).
func NewEngine(store store.EventLog, queue *queue.Client, workerID string, registry *StepRegistry) *Engine {
	if registry == nil {
		registry = NewStepRegistry()
	}
	return &Engine{
		store:    store,
		queue:    queue,
		workerID: workerID,
		registry: registry,
	}
}

// ExecuteWorkflow runs a workflow to completion with crash recovery. wfID
// identifies the workflow: pass uuid.New() to start a fresh run, or a
// previously used ID to resume one that crashed mid-flight — CreateWorkflow
// is idempotent, and every step (forward and compensating) below is gated
// on the event log, so steps already recorded as complete are skipped
// rather than re-run.
func (e *Engine) ExecuteWorkflow(ctx context.Context, wfID uuid.UUID, wfDef *WorkflowDefinition, input map[string]any) (uuid.UUID, error) {
	// Create workflow record (idempotent: a resumed run reuses the same ID)
	if err := e.store.CreateWorkflow(ctx, wfID, wfDef.Name, mustMarshal(input)); err != nil {
		return uuid.Nil, fmt.Errorf("engine: create workflow: %w", err)
	}

	plan := saga.NewCompensationPlan()
	stepInputs := input

	for _, step := range wfDef.Steps {
		dedupKey, err := idempotency.DedupKey(wfID.String(), step.Name(), stepInputs)
		if err != nil {
			return uuid.Nil, fmt.Errorf("engine: dedup key for %s: %w", step.Name(), err)
		}

		// Check if already completed (crash recovery)
		completed, err := e.store.HasCompleted(ctx, dedupKey)
		if err != nil {
			return uuid.Nil, fmt.Errorf("engine: check completion: %w", err)
		}
		if completed {
			// Replay: find the output from event log
			output, err := e.getStepOutput(ctx, wfID, step.Name())
			if err != nil {
				return uuid.Nil, fmt.Errorf("engine: get step output: %w", err)
			}
			plan.AddStep(step.Name(), output, dedupKey)
			stepInputs = output
			continue
		}

		// Record step started
		_, err = e.store.AppendEvent(ctx, store.Event{
			WorkflowID: wfID,
			StepName:   step.Name(),
			Type:       store.EventStepStarted,
			DedupKey:   dedupKey,
			Payload:    mustMarshal(stepInputs),
		})
		if err != nil {
			if errors.Is(err, store.ErrDuplicateEvent) {
				// Another worker started it, wait and check completion
				output, err := e.getStepOutput(ctx, wfID, step.Name())
				if err != nil {
					return uuid.Nil, err
				}
				plan.AddStep(step.Name(), output, dedupKey)
				stepInputs = output
				continue
			}
			return uuid.Nil, fmt.Errorf("engine: record step started: %w", err)
		}

		// Execute step business logic
		output, err := step.Execute(ctx, stepInputs)
		if err != nil {
			// Record failure
			_, _ = e.store.AppendEvent(ctx, store.Event{
				WorkflowID: wfID,
				StepName:   step.Name(),
				Type:       store.EventStepFailed,
				DedupKey:   dedupKey,
				Payload:    mustMarshal(map[string]any{"error": err.Error()}),
			})
			_ = e.store.UpdateWorkflowStatus(ctx, wfID, store.StatusFailed)

			// Trigger compensation for completed steps
			_ = plan.ExecuteCompensation(ctx, e.store, wfID)
			_ = e.store.UpdateWorkflowStatus(ctx, wfID, store.StatusCompensated)

			return uuid.Nil, fmt.Errorf("engine: execute step %s: %w", step.Name(), err)
		}

		// Record step completed (idempotent via dedup key unique index)
		_, err = e.store.AppendEvent(ctx, store.Event{
			WorkflowID: wfID,
			StepName:   step.Name(),
			Type:       store.EventStepCompleted,
			DedupKey:   dedupKey,
			Payload:    mustMarshal(output),
		})
		if err != nil {
			if errors.Is(err, store.ErrDuplicateEvent) {
				// Another worker completed it concurrently
				output, _ = e.getStepOutput(ctx, wfID, step.Name())
			} else {
				return uuid.Nil, fmt.Errorf("engine: record step completed: %w", err)
			}
		}

		plan.AddStep(step.Name(), output, dedupKey)
		stepInputs = output
	}

	// Workflow completed successfully
	_ = e.store.UpdateWorkflowStatus(ctx, wfID, store.StatusCompleted)
	_, _ = e.store.AppendEvent(ctx, store.Event{
		WorkflowID: wfID,
		StepName:   "",
		Type:       store.EventWorkflowCompleted,
		DedupKey:   "",
		Payload:    mustMarshal(stepInputs),
	})

	return wfID, nil
}

// ProcessStep pulls a step from the queue and executes it (worker mode).
// It follows the same exactly-once sequence as ExecuteWorkflow: check for a
// prior completion, record that execution started, run the step's business
// logic via the registry, then record completion. The unique index on
// dedup_key for completion events (see migrations/0001_init.up.sql) is what
// makes a race between two workers processing the same message safe.
func (e *Engine) ProcessStep(ctx context.Context, msg queue.StepMessage) error {
	dedupKey := msg.DedupKey
	if dedupKey == "" {
		var err error
		dedupKey, err = idempotency.DedupKey(msg.WorkflowID, msg.StepName, msg.Input)
		if err != nil {
			return err
		}
	}

	completed, err := e.store.HasCompleted(ctx, dedupKey)
	if err != nil {
		return err
	}
	if completed {
		log.Printf("worker %s: step %s already completed (dedup: %s)", e.workerID, msg.StepName, dedupKey)
		return nil
	}

	wfID, err := uuid.Parse(msg.WorkflowID)
	if err != nil {
		return err
	}

	step, ok := e.registry.Get(msg.StepName)
	if !ok {
		return fmt.Errorf("%w: %s", ErrStepNotRegistered, msg.StepName)
	}

	_, err = e.store.AppendEvent(ctx, store.Event{
		WorkflowID: wfID,
		StepName:   msg.StepName,
		Type:       store.EventStepStarted,
		DedupKey:   dedupKey,
		Payload:    mustMarshal(msg.Input),
	})
	if err != nil {
		if errors.Is(err, store.ErrDuplicateEvent) {
			// Should not happen for step_started (no unique constraint on
			// it), but treat as already-handled defensively.
			return nil
		}
		return fmt.Errorf("engine: record step started: %w", err)
	}

	faultinject.Crash("before_execution")

	output, execErr := step.Execute(ctx, msg.Input)
	if execErr != nil {
		_, _ = e.store.AppendEvent(ctx, store.Event{
			WorkflowID: wfID,
			StepName:   msg.StepName,
			Type:       store.EventStepFailed,
			DedupKey:   dedupKey,
			Payload:    mustMarshal(map[string]any{"error": execErr.Error()}),
		})
		return fmt.Errorf("engine: execute step %s: %w", msg.StepName, execErr)
	}

	faultinject.Crash("after_completion_before_log")

	_, err = e.store.AppendEvent(ctx, store.Event{
		WorkflowID: wfID,
		StepName:   msg.StepName,
		Type:       store.EventStepCompleted,
		DedupKey:   dedupKey,
		Payload:    mustMarshal(output),
	})
	if err != nil && !errors.Is(err, store.ErrDuplicateEvent) {
		return fmt.Errorf("engine: record step completed: %w", err)
	}

	log.Printf("worker %s: step %s completed (dedup: %s)", e.workerID, msg.StepName, dedupKey)
	return nil
}

// RecoverWorkflow replays a workflow's event log to reconstruct state.
func (e *Engine) RecoverWorkflow(ctx context.Context, wfID uuid.UUID) (map[string]any, error) {
	events, err := e.store.ReplayEvents(ctx, wfID)
	if err != nil {
		return nil, err
	}

	state := make(map[string]any)
	for _, evt := range events {
		switch evt.Type {
		case store.EventStepCompleted:
			var output map[string]any
			if err := json.Unmarshal(evt.Payload, &output); err == nil {
				state[evt.StepName] = output
			}
		case store.EventCompensationCompleted:
			delete(state, evt.StepName)
		}
	}
	return state, nil
}

func (e *Engine) getStepOutput(ctx context.Context, wfID uuid.UUID, stepName string) (map[string]any, error) {
	events, err := e.store.ReplayEvents(ctx, wfID)
	if err != nil {
		return nil, err
	}
	for _, evt := range events {
		if evt.StepName == stepName && evt.Type == store.EventStepCompleted {
			var output map[string]any
			if err := json.Unmarshal(evt.Payload, &output); err == nil {
				return output, nil
			}
		}
	}
	return nil, errors.New("step output not found in event log")
}

func mustMarshal(v any) json.RawMessage {
	data, _ := json.Marshal(v)
	return data
}
