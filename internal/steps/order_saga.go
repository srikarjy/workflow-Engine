// Package steps provides StepExecutor implementations for the example
// order-saga workflow (see examples/order-saga.json). They are logging
// stand-ins rather than real integrations, since this project demonstrates
// engine mechanics rather than a real order-fulfillment system.
package steps

import (
	"context"
	"fmt"
	"log"

	"github.com/srikarjy/workflow_engine/internal/engine"
	"github.com/srikarjy/workflow_engine/internal/faultinject"
)

// loggingStep is a StepExecutor that logs when it runs and passes its input
// through as output, unchanged apart from a "status" marker.
type loggingStep struct {
	name string
}

// NewLoggingStep returns a StepExecutor named name that logs its execution
// and compensation instead of calling out to a real system.
func NewLoggingStep(name string) engine.StepExecutor {
	return &loggingStep{name: name}
}

func (s *loggingStep) Name() string { return s.name }

func (s *loggingStep) Execute(ctx context.Context, input map[string]any) (map[string]any, error) {
	log.Printf("step %s: executing with input %v", s.name, input)
	faultinject.Crash("during_execution")
	output := make(map[string]any, len(input)+1)
	for k, v := range input {
		output[k] = v
	}
	output["status"] = "completed"
	return output, nil
}

func (s *loggingStep) Compensate(ctx context.Context, output map[string]any) error {
	log.Printf("step %s: compensating output %v", s.name, output)
	return nil
}

// failingStep always fails. It is used to force Saga compensation in
// fault-injection tests of the rollback path.
type failingStep struct {
	name string
}

// NewFailingStep returns a StepExecutor named name whose Execute always
// returns an error, forcing compensation of any prior steps in the workflow.
func NewFailingStep(name string) engine.StepExecutor {
	return &failingStep{name: name}
}

func (s *failingStep) Name() string { return s.name }

func (s *failingStep) Execute(ctx context.Context, input map[string]any) (map[string]any, error) {
	return nil, fmt.Errorf("step %s: forced failure for fault-injection testing", s.name)
}

func (s *failingStep) Compensate(ctx context.Context, output map[string]any) error {
	return nil
}

// OrderSagaSteps returns the StepExecutors for every forward and
// compensating step referenced by examples/order-saga.json.
func OrderSagaSteps() []engine.StepExecutor {
	names := []string{
		"reserve_inventory", "release_inventory",
		"charge_payment", "refund_payment",
		"create_shipment", "cancel_shipment",
		"send_confirmation", "send_cancellation_notice",
		"update_analytics", "revert_analytics",
	}
	steps := make([]engine.StepExecutor, len(names))
	for i, name := range names {
		steps[i] = NewLoggingStep(name)
	}
	return steps
}
