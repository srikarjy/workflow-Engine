// Package httpstep implements engine.StepExecutor by calling out to an HTTP
// endpoint, so a workflow step can be "whatever service this URL points at"
// instead of Go code compiled into the worker. Retry with exponential
// backoff and jitter lives here, inside a single Execute/Compensate call —
// the engine only ever sees one call that took a while and either
// succeeded or failed.
package httpstep

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"time"
)

// Call describes a single HTTP request a step (or its compensation) makes.
type Call struct {
	Method  string
	URL     string
	Timeout time.Duration
}

// RetryPolicy controls how many times, and with what backoff, a failed
// Execute call is retried before the step is considered failed.
type RetryPolicy struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Jitter         bool
}

// DefaultRetryPolicy makes a single attempt (no retry) — the safe default
// for a step definition that doesn't configure one.
var DefaultRetryPolicy = RetryPolicy{MaxAttempts: 1}

// Step is an engine.StepExecutor backed by HTTP calls.
type Step struct {
	name       string
	execute    Call
	compensate *Call
	retry      RetryPolicy
	client     *http.Client
}

// New returns a Step named name whose Execute calls execute (retrying per
// retry) and whose Compensate calls compensate. compensate may be nil for
// steps with no meaningful rollback (e.g. an analytics event).
func New(name string, execute Call, compensate *Call, retry RetryPolicy) *Step {
	if retry.MaxAttempts <= 0 {
		retry = DefaultRetryPolicy
	}
	return &Step{
		name:       name,
		execute:    execute,
		compensate: compensate,
		retry:      retry,
		client:     &http.Client{},
	}
}

func (s *Step) Name() string { return s.name }

// Execute calls s.execute with input as the JSON request body, retrying on
// error or non-2xx status per s.retry, and returns the JSON response body
// decoded as the step's output.
func (s *Step) Execute(ctx context.Context, input map[string]any) (map[string]any, error) {
	var lastErr error
	for attempt := 0; attempt < s.retry.MaxAttempts; attempt++ {
		if attempt > 0 {
			time.Sleep(s.backoff(attempt))
		}
		output, err := doCall(ctx, s.client, s.execute, input)
		if err == nil {
			return output, nil
		}
		lastErr = err
	}
	return nil, fmt.Errorf("httpstep: step %s: %w", s.name, lastErr)
}

// Compensate calls s.compensate with output as the JSON request body. It is
// a no-op if the step has no compensate call configured.
func (s *Step) Compensate(ctx context.Context, output map[string]any) error {
	if s.compensate == nil {
		return nil
	}
	_, err := doCall(ctx, s.client, *s.compensate, output)
	if err != nil {
		return fmt.Errorf("httpstep: compensate %s: %w", s.name, err)
	}
	return nil
}

// backoff returns the delay before the given retry attempt (1-indexed: the
// delay before the 2nd overall try), exponential in attempt and capped at
// MaxBackoff, with full jitter (uniform in [0, backoff)) when enabled.
func (s *Step) backoff(attempt int) time.Duration {
	initial := s.retry.InitialBackoff
	if initial <= 0 {
		initial = 100 * time.Millisecond
	}
	backoff := initial * time.Duration(1<<uint(attempt-1))
	if s.retry.MaxBackoff > 0 && backoff > s.retry.MaxBackoff {
		backoff = s.retry.MaxBackoff
	}
	if s.retry.Jitter && backoff > 0 {
		backoff = time.Duration(rand.Int63n(int64(backoff)))
	}
	return backoff
}

func doCall(ctx context.Context, client *http.Client, call Call, body map[string]any) (map[string]any, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal request body: %w", err)
	}

	callCtx := ctx
	if call.Timeout > 0 {
		var cancel context.CancelFunc
		callCtx, cancel = context.WithTimeout(ctx, call.Timeout)
		defer cancel()
	}

	method := call.Method
	if method == "" {
		method = http.MethodPost
	}
	req, err := http.NewRequestWithContext(callCtx, method, call.URL, bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request %s %s: %w", method, call.URL, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%s %s: status %d: %s", method, call.URL, resp.StatusCode, respBody)
	}

	if len(respBody) == 0 {
		return map[string]any{}, nil
	}
	var output map[string]any
	if err := json.Unmarshal(respBody, &output); err != nil {
		return nil, fmt.Errorf("decode response body: %w", err)
	}
	return output, nil
}
