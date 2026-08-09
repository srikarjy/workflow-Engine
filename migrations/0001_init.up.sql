-- workflows is a projection/read-model over the append-only events table.
CREATE TABLE workflows (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    input JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- events is the append-only, source-of-truth event log. Rows are never
-- updated or deleted; workflow/step state is derived by replaying them
-- in (workflow_id, id) order.
CREATE TABLE events (
    id BIGSERIAL PRIMARY KEY,
    workflow_id UUID NOT NULL REFERENCES workflows(id),
    step_name TEXT NOT NULL,
    event_type TEXT NOT NULL,
    dedup_key TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX events_workflow_id_id_idx ON events (workflow_id, id);
CREATE INDEX events_dedup_key_idx ON events (dedup_key);

-- Enforces exactly-once completion at the database level: two workers
-- racing to complete the same step (identified by its deterministic
-- SHA-256 dedup key) can both attempt the insert, but only one commits.
CREATE UNIQUE INDEX events_dedup_key_completed_uk ON events (dedup_key)
    WHERE event_type IN ('step_completed', 'compensation_completed');
