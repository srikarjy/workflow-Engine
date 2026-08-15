package engine

import (
	"context"
	"errors"
	"sync/atomic"
)

// spyStep counts Execute calls (to assert business logic ran exactly the
// expected number of times — the whole point of exactly-once execution) and
// can be configured to fail, to exercise the failure/compensation paths.
type spyStep struct {
	name            string
	failWith        error
	execCount       atomic.Int32
	compensateCount atomic.Int32
	outputFunc      func(input map[string]any) map[string]any
}

func (s *spyStep) Name() string { return s.name }

func (s *spyStep) Execute(ctx context.Context, input map[string]any) (map[string]any, error) {
	s.execCount.Add(1)
	if s.failWith != nil {
		return nil, s.failWith
	}
	if s.outputFunc != nil {
		return s.outputFunc(input), nil
	}
	out := make(map[string]any, len(input)+1)
	for k, v := range input {
		out[k] = v
	}
	out[s.name] = "done"
	return out, nil
}

func (s *spyStep) Compensate(ctx context.Context, output map[string]any) error {
	s.compensateCount.Add(1)
	return nil
}

func (s *spyStep) calls() int { return int(s.execCount.Load()) }

func (s *spyStep) compensateCalls() int { return int(s.compensateCount.Load()) }

var errForced = errors.New("forced failure")
