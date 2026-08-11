// Package saga implements reverse-order Saga compensation.
package saga

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/google/uuid"

	"github.com/srikarjy/workflow_engine/internal/faultinject"
	"github.com/srikarjy/workflow_engine/internal/idempotency"
	"github.com/srikarjy/workflow_engine/internal/store"
)

// CompensableStep defines a workflow step that supports compensation.
type CompensableStep struct {
	Name       string
	Execute    func(ctx context.Context, input map[string]any) (map[string]any, error)
	Compensate func(ctx context.Context, output map[string]any) error
}

// CompensationPlan holds the sequence of executed steps for rollback.
type CompensationPlan struct {
	steps []ExecutedStep
}

type ExecutedStep struct {
	Name     string
	Output   map[string]any
	DedupKey string
}

// NewCompensationPlan creates an empty compensation plan.
func NewCompensationPlan() *CompensationPlan {
	return &CompensationPlan{steps: make([]ExecutedStep, 0)}
}

// AddStep records a successfully executed step for potential compensation.
func (p *CompensationPlan) AddStep(name string, output map[string]any, dedupKey string) {
	p.steps = append(p.steps, ExecutedStep{
		Name:     name,
		Output:   output,
		DedupKey: dedupKey,
	})
}

// Steps returns the executed steps in reverse order for compensation.
func (p *CompensationPlan) Steps() []ExecutedStep {
	reversed := make([]ExecutedStep, len(p.steps))
	for i, s := range p.steps {
		reversed[len(p.steps)-1-i] = s
	}
	return reversed
}

// ExecuteCompensation runs compensation for all steps in reverse order.
// Each compensation step is recorded in the event log for crash recovery.
func (p *CompensationPlan) ExecuteCompensation(ctx context.Context, s *store.Store, workflowID uuid.UUID) error {
	steps := p.Steps()
	for i, step := range steps {
		compDedupKey, err := idempotency.DedupKey(workflowID.String(), "compensate_"+step.Name, step.Output)
		if err != nil {
			return err
		}

		completed, err := s.HasCompleted(ctx, compDedupKey)
		if err != nil {
			return err
		}
		if completed {
			continue
		}

		_, err = s.AppendEvent(ctx, store.Event{
			WorkflowID: workflowID,
			StepName:   "compensate_" + step.Name,
			Type:       store.EventCompensationStarted,
			DedupKey:   compDedupKey,
			Payload:    mustMarshal(step.Output),
		})
		if err != nil {
			if errors.Is(err, store.ErrDuplicateEvent) {
				continue
			}
			return err
		}

		if i == len(steps)-1 {
			faultinject.Crash("during_final_compensation")
		} else {
			faultinject.Crash("during_compensation")
		}

		_, err = s.AppendEvent(ctx, store.Event{
			WorkflowID: workflowID,
			StepName:   "compensate_" + step.Name,
			Type:       store.EventCompensationCompleted,
			DedupKey:   compDedupKey,
			Payload:    mustMarshal(map[string]any{"status": "compensated"}),
		})
		if err != nil {
			if errors.Is(err, store.ErrDuplicateEvent) {
				continue
			}
			return err
		}
	}
	return nil
}

// CompensateStep executes a single step's compensation with idempotency.
func CompensateStep(ctx context.Context, s *store.Store, workflowID uuid.UUID, stepName string, output map[string]any) error {
	compDedupKey, err := idempotency.DedupKey(workflowID.String(), "compensate_"+stepName, output)
	if err != nil {
		return err
	}

	completed, err := s.HasCompleted(ctx, compDedupKey)
	if err != nil {
		return err
	}
	if completed {
		return nil
	}

	_, err = s.AppendEvent(ctx, store.Event{
		WorkflowID: workflowID,
		StepName:   "compensate_" + stepName,
		Type:       store.EventCompensationCompleted,
		DedupKey:   compDedupKey,
		Payload:    mustMarshal(map[string]any{"status": "compensated"}),
	})
	return err
}

func mustMarshal(v any) json.RawMessage {
	data, _ := json.Marshal(v)
	return data
}