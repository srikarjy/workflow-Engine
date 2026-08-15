// Package engine implements the workflow execution engine.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/srikarjy/workflow_engine/internal/faultinject"
	"github.com/srikarjy/workflow_engine/internal/idempotency"
	"github.com/srikarjy/workflow_engine/internal/metrics"
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
			plan.AddStep(step.Name(), output, dedupKey, step.Compensate)
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
				plan.AddStep(step.Name(), output, dedupKey, step.Compensate)
				stepInputs = output
				continue
			}
			return uuid.Nil, fmt.Errorf("engine: record step started: %w", err)
		}
		metrics.StepsStarted.WithLabelValues(step.Name()).Inc()

		// Execute step business logic
		start := time.Now()
		output, err := step.Execute(ctx, stepInputs)
		metrics.StepDuration.WithLabelValues(step.Name()).Observe(time.Since(start).Seconds())
		if err != nil {
			metrics.StepsFailed.WithLabelValues(step.Name()).Inc()
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
		metrics.StepsCompleted.WithLabelValues(step.Name()).Inc()

		plan.AddStep(step.Name(), output, dedupKey, step.Compensate)
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

// StepRouter decides the next step name and input based on the previous
// step's output. Return (nil, nil) to signal workflow completion.
type StepRouter func(ctx context.Context, wfID uuid.UUID, previousStepName string, previousOutput map[string]any) (nextStepName string, nextInput map[string]any, err error)

// AgenticWorkflowConfig configures an agentic (dynamic) workflow execution.
type AgenticWorkflowConfig struct {
	StartStepName string
	StartInput    map[string]any
	Router        StepRouter
	MaxSteps      int                  // safety limit to prevent infinite loops (default 100)
	RouterCache   *RouterDecisionCache // optional cache for routing decisions
}

// RouterDecisionCache caches routing decisions to avoid re-asking a model
// for the same (wfID, stepName, stepInput) context. It is safe for concurrent
// use.
type RouterDecisionCache struct {
	mu    sync.Mutex
	cache map[string]RouterDecision
}

type RouterDecision struct {
	NextStepName string
	NextInput    map[string]any
}

// NewRouterDecisionCache creates a new empty router decision cache.
func NewRouterDecisionCache() *RouterDecisionCache {
	return &RouterDecisionCache{cache: make(map[string]RouterDecision)}
}

func (c *RouterDecisionCache) decisionKey(stepName string, stepInput map[string]any) (string, error) {
	inputBytes, err := json.Marshal(stepInput)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s|%s", stepName, string(inputBytes)), nil
}

// Get returns the cached decision if present, or (nil, false) if not cached.
func (c *RouterDecisionCache) Get(stepName string, stepInput map[string]any) (RouterDecision, bool, error) {
	key, err := c.decisionKey(stepName, stepInput)
	if err != nil {
		return RouterDecision{}, false, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	decision, ok := c.cache[key]
	return decision, ok, nil
}

// Set caches a routing decision.
func (c *RouterDecisionCache) Set(stepName string, stepInput map[string]any, decision RouterDecision) error {
	key, err := c.decisionKey(stepName, stepInput)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache[key] = decision
	return nil
}

// CachedRouter wraps a StepRouter with a RouterDecisionCache. If a decision
// for the given context is cached, it returns the cached decision without
// calling the underlying router. Otherwise, it calls the router and caches
// the result.
func CachedRouter(router StepRouter, cache *RouterDecisionCache) StepRouter {
	return func(ctx context.Context, wfID uuid.UUID, previousStepName string, previousOutput map[string]any) (string, map[string]any, error) {
		if cache == nil {
			return router(ctx, wfID, previousStepName, previousOutput)
		}
		decision, ok, err := cache.Get(previousStepName, previousOutput)
		if err != nil {
			return "", nil, err
		}
		if ok {
			return decision.NextStepName, decision.NextInput, nil
		}
		nextStepName, nextInput, err := router(ctx, wfID, previousStepName, previousOutput)
		if err != nil {
			return "", nil, err
		}
		if err := cache.Set(previousStepName, previousOutput, RouterDecision{
			NextStepName: nextStepName,
			NextInput:    nextInput,
		}); err != nil {
			return "", nil, err
		}
		return nextStepName, nextInput, nil
	}
}

// ExecuteAgentic runs a workflow where the next step is decided at runtime
// by the Router function, based on the previous step's output. It reuses
// the same exactly-once/idempotency/event-log machinery as ExecuteWorkflow.
// If RouterCache is provided, routing decisions are cached and replayed
// for identical contexts, avoiding re-asking a model.
func (e *Engine) ExecuteAgentic(ctx context.Context, wfID uuid.UUID, cfg AgenticWorkflowConfig) (uuid.UUID, error) {
	if cfg.Router == nil {
		return uuid.Nil, errors.New("engine: agentic workflow requires a Router function")
	}
	if cfg.StartStepName == "" {
		return uuid.Nil, errors.New("engine: agentic workflow requires a StartStepName")
	}
	maxSteps := cfg.MaxSteps
	if maxSteps <= 0 {
		maxSteps = 100
	}

	// Wrap router with cache if provided
	router := cfg.Router
	if cfg.RouterCache != nil {
		router = CachedRouter(cfg.Router, cfg.RouterCache)
	}

	// Create workflow record (idempotent: a resumed run reuses the same ID)
	if err := e.store.CreateWorkflow(ctx, wfID, "agentic-"+cfg.StartStepName, mustMarshal(cfg.StartInput)); err != nil {
		return uuid.Nil, fmt.Errorf("engine: create workflow: %w", err)
	}

	plan := saga.NewCompensationPlan()
	stepName := cfg.StartStepName
	stepInput := cfg.StartInput

	for stepCount := 0; stepCount < maxSteps; stepCount++ {
		dedupKey, err := idempotency.DedupKey(wfID.String(), stepName, stepInput)
		if err != nil {
			return uuid.Nil, fmt.Errorf("engine: dedup key for %s: %w", stepName, err)
		}

		// Check if already completed (crash recovery)
		completed, err := e.store.HasCompleted(ctx, dedupKey)
		if err != nil {
			return uuid.Nil, fmt.Errorf("engine: check completion: %w", err)
		}
		if completed {
			// Replay: find the output from event log
			output, err := e.getStepOutput(ctx, wfID, stepName)
			if err != nil {
				return uuid.Nil, fmt.Errorf("engine: get step output: %w", err)
			}
			if step, ok := e.registry.Get(stepName); ok {
				plan.AddStep(stepName, output, dedupKey, step.Compensate)
			} else {
				plan.AddStep(stepName, output, dedupKey, nil)
			}

			// Ask router for next step based on replayed output
			nextName, nextInput, err := router(ctx, wfID, stepName, output)
			if err != nil {
				return uuid.Nil, fmt.Errorf("engine: router error: %w", err)
			}
			if nextName == "" {
				break // workflow complete
			}
			stepName = nextName
			stepInput = nextInput
			continue
		}

		// Record step started
		_, err = e.store.AppendEvent(ctx, store.Event{
			WorkflowID: wfID,
			StepName:   stepName,
			Type:       store.EventStepStarted,
			DedupKey:   dedupKey,
			Payload:    mustMarshal(stepInput),
		})
		if err != nil {
			if errors.Is(err, store.ErrDuplicateEvent) {
				// Another worker started it, wait and check completion
				output, err := e.getStepOutput(ctx, wfID, stepName)
				if err != nil {
					return uuid.Nil, err
				}
				if step, ok := e.registry.Get(stepName); ok {
					plan.AddStep(stepName, output, dedupKey, step.Compensate)
				} else {
					plan.AddStep(stepName, output, dedupKey, nil)
				}

				nextName, nextInput, err := router(ctx, wfID, stepName, output)
				if err != nil {
					return uuid.Nil, fmt.Errorf("engine: router error: %w", err)
				}
				if nextName == "" {
					break
				}
				stepName = nextName
				stepInput = nextInput
				continue
			}
			return uuid.Nil, fmt.Errorf("engine: record step started: %w", err)
		}

		// Execute step business logic
		step, ok := e.registry.Get(stepName)
		if !ok {
			return uuid.Nil, fmt.Errorf("%w: %s", ErrStepNotRegistered, stepName)
		}

		faultinject.Crash("before_execution")

		output, execErr := step.Execute(ctx, stepInput)
		if execErr != nil {
			// Record failure
			_, _ = e.store.AppendEvent(ctx, store.Event{
				WorkflowID: wfID,
				StepName:   stepName,
				Type:       store.EventStepFailed,
				DedupKey:   dedupKey,
				Payload:    mustMarshal(map[string]any{"error": execErr.Error()}),
			})
			_ = e.store.UpdateWorkflowStatus(ctx, wfID, store.StatusFailed)

			// Trigger compensation for completed steps
			_ = plan.ExecuteCompensation(ctx, e.store, wfID)
			_ = e.store.UpdateWorkflowStatus(ctx, wfID, store.StatusCompensated)

			return uuid.Nil, fmt.Errorf("engine: execute step %s: %w", stepName, execErr)
		}

		faultinject.Crash("after_completion_before_log")

		// Record step completed (idempotent via dedup key unique index)
		_, err = e.store.AppendEvent(ctx, store.Event{
			WorkflowID: wfID,
			StepName:   stepName,
			Type:       store.EventStepCompleted,
			DedupKey:   dedupKey,
			Payload:    mustMarshal(output),
		})
		if err != nil {
			if errors.Is(err, store.ErrDuplicateEvent) {
				// Another worker completed it concurrently
				output, _ = e.getStepOutput(ctx, wfID, stepName)
			} else {
				return uuid.Nil, fmt.Errorf("engine: record step completed: %w", err)
			}
		}

		plan.AddStep(stepName, output, dedupKey, step.Compensate)

		// Ask router for next step
		nextName, nextInput, err := router(ctx, wfID, stepName, output)
		if err != nil {
			return uuid.Nil, fmt.Errorf("engine: router error: %w", err)
		}
		if nextName == "" {
			// Workflow complete
			break
		}
		stepName = nextName
		stepInput = nextInput
	}

	// Workflow completed successfully
	_ = e.store.UpdateWorkflowStatus(ctx, wfID, store.StatusCompleted)
	_, _ = e.store.AppendEvent(ctx, store.Event{
		WorkflowID: wfID,
		StepName:   "",
		Type:       store.EventWorkflowCompleted,
		DedupKey:   "",
		Payload:    mustMarshal(stepInput),
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
		metrics.StepsSkippedDuplicate.WithLabelValues(msg.StepName).Inc()
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
	metrics.StepsStarted.WithLabelValues(msg.StepName).Inc()

	faultinject.Crash("before_execution")

	start := time.Now()
	output, execErr := step.Execute(ctx, msg.Input)
	metrics.StepDuration.WithLabelValues(msg.StepName).Observe(time.Since(start).Seconds())
	if execErr != nil {
		metrics.StepsFailed.WithLabelValues(msg.StepName).Inc()
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
	metrics.StepsCompleted.WithLabelValues(msg.StepName).Inc()

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
