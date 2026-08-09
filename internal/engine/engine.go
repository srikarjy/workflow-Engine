// Package engine implements the workflow execution engine.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/google/uuid"

	"github.com/srikarjy/workflow_engine/internal/idempotency"
	"github.com/srikarjy/workflow_engine/internal/queue"
	"github.com/srikarjy/workflow_engine/internal/saga"
	"github.com/srikarjy/workflow_engine/internal/store"
)

// StepExecutor defines the interface for executing a workflow step.
type StepExecutor interface {
	Execute(ctx context.Context, input map[string]any) (map[string]any, error)
	Compensate(ctx context.Context, output map[string]any) error
	Name() string
}

// WorkflowDefinition defines a workflow as a sequence of steps.
type WorkflowDefinition struct {
	Name string
	Steps []StepExecutor
}

// Engine coordinates workflow execution with exactly-once guarantees.
type Engine struct {
	store   *store.Store
	queue   *queue.Client
	workerID string
}

// NewEngine creates a new workflow engine.
func NewEngine(store *store.Store, queue *queue.Client, workerID string) *Engine {
	return &Engine{
		store:    store,
		queue:    queue,
		workerID: workerID,
	}
}

// ExecuteWorkflow runs a workflow to completion with crash recovery.
func (e *Engine) ExecuteWorkflow(ctx context.Context, wfDef *WorkflowDefinition, input map[string]any) (uuid.UUID, error) {
	wfID := uuid.New()

	// Create workflow record
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

	// This would look up the step executor from a registry
	// For now, we just record completion
	_, err = e.store.AppendEvent(ctx, store.Event{
		WorkflowID: wfID,
		StepName:   msg.StepName,
		Type:       store.EventStepCompleted,
		DedupKey:   dedupKey,
		Payload:    mustMarshal(map[string]any{"status": "completed", "worker": e.workerID}),
	})
	return err
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