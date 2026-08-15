// Package metrics defines the Prometheus metrics exported by a worker
// process, scraped via the HTTP handler mounted in cmd/worker at /metrics.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	StepsStarted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "workflow_steps_started_total",
		Help: "Total number of workflow steps started, labeled by step name.",
	}, []string{"step"})

	StepsCompleted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "workflow_steps_completed_total",
		Help: "Total number of workflow steps completed successfully, labeled by step name.",
	}, []string{"step"})

	StepsFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "workflow_steps_failed_total",
		Help: "Total number of workflow steps that returned an error, labeled by step name.",
	}, []string{"step"})

	StepsSkippedDuplicate = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "workflow_steps_skipped_duplicate_total",
		Help: "Total number of steps skipped because a completion event already existed for their dedup key (retry or crash recovery), labeled by step name.",
	}, []string{"step"})

	StepDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "workflow_step_duration_seconds",
		Help:    "Time spent executing a step's business logic, labeled by step name.",
		Buckets: prometheus.DefBuckets,
	}, []string{"step"})

	CompensationsStarted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "workflow_compensations_started_total",
		Help: "Total number of saga compensation steps started, labeled by step name.",
	}, []string{"step"})

	CompensationsCompleted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "workflow_compensations_completed_total",
		Help: "Total number of saga compensation steps completed, labeled by step name.",
	}, []string{"step"})
)
