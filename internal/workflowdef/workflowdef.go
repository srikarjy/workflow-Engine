// Package workflowdef parses YAML workflow definitions — a saga expressed
// as a sequence of HTTP calls, rather than compiled Go StepExecutors — and
// builds them into an engine.WorkflowDefinition backed by internal/httpstep.
package workflowdef

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/srikarjy/workflow_engine/internal/engine"
	"github.com/srikarjy/workflow_engine/internal/httpstep"
)

// RetryPolicy is the YAML form of httpstep.RetryPolicy.
type RetryPolicy struct {
	MaxAttempts    int           `yaml:"max_attempts"`
	InitialBackoff time.Duration `yaml:"initial_backoff"`
	MaxBackoff     time.Duration `yaml:"max_backoff"`
	Jitter         bool          `yaml:"jitter"`
}

// HTTPCall is the YAML form of httpstep.Call.
type HTTPCall struct {
	Method  string        `yaml:"method"`
	URL     string        `yaml:"url"`
	Timeout time.Duration `yaml:"timeout"`
}

// StepDef describes one saga step: what to call to execute it, what to
// call to compensate it (optional), and an optional retry override.
type StepDef struct {
	Name       string       `yaml:"name"`
	Execute    HTTPCall     `yaml:"execute"`
	Compensate *HTTPCall    `yaml:"compensate"`
	Retry      *RetryPolicy `yaml:"retry"`
}

// Definition is a parsed workflow: a name, an ordered list of steps, and a
// default retry policy applied to any step that doesn't set its own.
type Definition struct {
	Name  string       `yaml:"name"`
	Retry *RetryPolicy `yaml:"retry"`
	Steps []StepDef    `yaml:"steps"`
}

// Load reads and parses a workflow definition from a YAML file.
func Load(path string) (*Definition, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("workflowdef: read %s: %w", path, err)
	}
	var def Definition
	if err := yaml.Unmarshal(data, &def); err != nil {
		return nil, fmt.Errorf("workflowdef: parse %s: %w", path, err)
	}
	if def.Name == "" {
		return nil, fmt.Errorf("workflowdef: %s: missing top-level name", path)
	}
	if len(def.Steps) == 0 {
		return nil, fmt.Errorf("workflowdef: %s: no steps defined", path)
	}
	for i, s := range def.Steps {
		if s.Name == "" {
			return nil, fmt.Errorf("workflowdef: %s: step %d: missing name", path, i)
		}
		if s.Execute.URL == "" {
			return nil, fmt.Errorf("workflowdef: %s: step %s: missing execute.url", path, s.Name)
		}
	}
	return &def, nil
}

// Build converts the definition into an engine.WorkflowDefinition, resolving
// each step's retry policy as: step override, else workflow default, else
// httpstep.DefaultRetryPolicy (no retry).
func (d *Definition) Build() *engine.WorkflowDefinition {
	steps := make([]engine.StepExecutor, len(d.Steps))
	for i, s := range d.Steps {
		steps[i] = httpstep.New(
			s.Name,
			toCall(s.Execute),
			toCallPtr(s.Compensate),
			resolveRetry(s.Retry, d.Retry),
		)
	}
	return &engine.WorkflowDefinition{Name: d.Name, Steps: steps}
}

func resolveRetry(step, workflow *RetryPolicy) httpstep.RetryPolicy {
	policy := step
	if policy == nil {
		policy = workflow
	}
	if policy == nil {
		return httpstep.DefaultRetryPolicy
	}
	return httpstep.RetryPolicy{
		MaxAttempts:    policy.MaxAttempts,
		InitialBackoff: policy.InitialBackoff,
		MaxBackoff:     policy.MaxBackoff,
		Jitter:         policy.Jitter,
	}
}

func toCall(c HTTPCall) httpstep.Call {
	return httpstep.Call{Method: c.Method, URL: c.URL, Timeout: c.Timeout}
}

func toCallPtr(c *HTTPCall) *httpstep.Call {
	if c == nil {
		return nil
	}
	call := toCall(*c)
	return &call
}
