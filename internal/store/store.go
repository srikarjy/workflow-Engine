// Package store persists the append-only workflow event log in PostgreSQL.
// Events are never updated or deleted; workflow state is derived by
// replaying a workflow's events in order.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type EventType string

const (
	EventStepStarted           EventType = "step_started"
	EventStepCompleted         EventType = "step_completed"
	EventStepFailed            EventType = "step_failed"
	EventCompensationStarted   EventType = "compensation_started"
	EventCompensationCompleted EventType = "compensation_completed"
	EventWorkflowCompleted     EventType = "workflow_completed"
	EventWorkflowFailed        EventType = "workflow_failed"
)

type WorkflowStatus string

const (
	StatusRunning      WorkflowStatus = "running"
	StatusCompensating WorkflowStatus = "compensating"
	StatusCompleted    WorkflowStatus = "completed"
	StatusCompensated  WorkflowStatus = "compensated"
	StatusFailed       WorkflowStatus = "failed"
)

// EventLog is the subset of *Store that Engine and Saga depend on. It exists
// so their logic can be unit tested against a fast in-memory fake instead of
// requiring a live PostgreSQL connection for every test; *Store satisfies it.
type EventLog interface {
	CreateWorkflow(ctx context.Context, id uuid.UUID, name string, input json.RawMessage) error
	UpdateWorkflowStatus(ctx context.Context, id uuid.UUID, status WorkflowStatus) error
	AppendEvent(ctx context.Context, e Event) (Event, error)
	HasCompleted(ctx context.Context, dedupKey string) (bool, error)
	ReplayEvents(ctx context.Context, workflowID uuid.UUID) ([]Event, error)
}

// ErrDuplicateEvent is returned by AppendEvent when a completion event with
// the same dedup key has already been committed by another worker. Callers
// treat this as "already done" rather than an error.
var ErrDuplicateEvent = errors.New("store: duplicate completion event")

// ErrNotFound is returned when a workflow lookup finds no row.
var ErrNotFound = errors.New("store: not found")

type Event struct {
	ID         int64
	WorkflowID uuid.UUID
	StepName   string
	Type       EventType
	DedupKey   string
	Payload    json.RawMessage
	CreatedAt  time.Time
}

type Workflow struct {
	ID        uuid.UUID
	Name      string
	Status    WorkflowStatus
	Input     json.RawMessage
	CreatedAt time.Time
	UpdatedAt time.Time
}

type Store struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() {
	s.pool.Close()
}

// CreateWorkflow inserts a new workflow row. It is idempotent on id: a
// resumed run after a crash reuses the same workflow ID, and re-inserting it
// is a no-op rather than a primary-key error.
func (s *Store) CreateWorkflow(ctx context.Context, id uuid.UUID, name string, input json.RawMessage) error {
	if input == nil {
		input = json.RawMessage("{}")
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO workflows (id, name, status, input)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (id) DO NOTHING
	`, id, name, StatusRunning, input)
	return err
}

func (s *Store) GetWorkflow(ctx context.Context, id uuid.UUID) (Workflow, error) {
	var w Workflow
	err := s.pool.QueryRow(ctx, `
		SELECT id, name, status, input, created_at, updated_at
		FROM workflows WHERE id = $1
	`, id).Scan(&w.ID, &w.Name, &w.Status, &w.Input, &w.CreatedAt, &w.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Workflow{}, ErrNotFound
	}
	return w, err
}

func (s *Store) UpdateWorkflowStatus(ctx context.Context, id uuid.UUID, status WorkflowStatus) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE workflows SET status = $2, updated_at = now() WHERE id = $1
	`, id, status)
	return err
}

// AppendEvent writes an event to the log. For completion event types
// (EventStepCompleted, EventCompensationCompleted), a unique index on
// dedup_key enforces exactly-once at the database level: if another
// worker already committed the same completion, this returns
// ErrDuplicateEvent instead of inserting a second row.
func (s *Store) AppendEvent(ctx context.Context, e Event) (Event, error) {
	if e.Payload == nil {
		e.Payload = json.RawMessage("{}")
	}
	err := s.pool.QueryRow(ctx, `
		INSERT INTO events (workflow_id, step_name, event_type, dedup_key, payload)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id, created_at
	`, e.WorkflowID, e.StepName, e.Type, e.DedupKey, e.Payload).Scan(&e.ID, &e.CreatedAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return Event{}, ErrDuplicateEvent
		}
		return Event{}, err
	}
	return e, nil
}

// HasCompleted reports whether a completion event (step_completed or
// compensation_completed) already exists for the given dedup key. The
// engine calls this before executing a step so that retries and crash
// recovery skip business logic that already ran to completion.
func (s *Store) HasCompleted(ctx context.Context, dedupKey string) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM events
			WHERE dedup_key = $1
			AND event_type IN ($2, $3)
		)
	`, dedupKey, EventStepCompleted, EventCompensationCompleted).Scan(&exists)
	return exists, err
}

// ReplayEvents returns all events for a workflow in commit order, the
// basis for deterministic crash recovery: a replacement worker replays
// this log to find out which steps already completed.
func (s *Store) ReplayEvents(ctx context.Context, workflowID uuid.UUID) ([]Event, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, workflow_id, step_name, event_type, dedup_key, payload, created_at
		FROM events
		WHERE workflow_id = $1
		ORDER BY id ASC
	`, workflowID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []Event
	for rows.Next() {
		var e Event
		if err := rows.Scan(&e.ID, &e.WorkflowID, &e.StepName, &e.Type, &e.DedupKey, &e.Payload, &e.CreatedAt); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, rows.Err()
}
