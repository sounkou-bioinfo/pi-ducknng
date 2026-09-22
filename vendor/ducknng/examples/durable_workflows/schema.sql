-- Absurd-style checkpointed workflows for one DuckDB process.
--
-- The tables are deliberately separate from ducknng's transport/session state.
-- Workers manipulate them with single SQL statements, either locally, through
-- ducknng's query RPC, or through a remote DuckDB protocol such as Quack.

CREATE SCHEMA IF NOT EXISTS ducknng_workflow;

CREATE TABLE IF NOT EXISTS ducknng_workflow.schema_version (
    version INTEGER PRIMARY KEY,
    installed_at_ms BIGINT NOT NULL DEFAULT epoch_ms(current_timestamp)
);

INSERT INTO ducknng_workflow.schema_version(version)
VALUES (1)
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS ducknng_workflow.tasks (
    task_id UUID PRIMARY KEY,
    queue_name VARCHAR NOT NULL,
    task_name VARCHAR NOT NULL,
    input JSON NOT NULL,
    status VARCHAR NOT NULL DEFAULT 'ready',
    priority INTEGER NOT NULL DEFAULT 0,
    run_count UINTEGER NOT NULL DEFAULT 0,
    failure_count UINTEGER NOT NULL DEFAULT 0,
    max_failures UINTEGER NOT NULL DEFAULT 3,
    available_at_ms BIGINT NOT NULL DEFAULT epoch_ms(current_timestamp),
    waiting_event VARCHAR,
    lease_owner VARCHAR,
    lease_token UUID,
    lease_expires_at_ms BIGINT,
    result JSON,
    checkpoints JSON NOT NULL DEFAULT '{}',
    last_error VARCHAR,
    created_at_ms BIGINT NOT NULL DEFAULT epoch_ms(current_timestamp),
    updated_at_ms BIGINT NOT NULL DEFAULT epoch_ms(current_timestamp),
    CHECK (length(queue_name) BETWEEN 1 AND 128),
    CHECK (length(task_name) BETWEEN 1 AND 256),
    CHECK (length(CAST(input AS VARCHAR)) <= 1048576),
    CHECK (length(CAST(checkpoints AS VARCHAR)) <= 16777216),
    CHECK (max_failures BETWEEN 1 AND 1000000),
    CHECK (status IN ('ready', 'running', 'waiting', 'completed', 'failed', 'cancelled')),
    CHECK (waiting_event IS NULL OR length(waiting_event) BETWEEN 1 AND 256),
    CHECK (last_error IS NULL OR length(last_error) <= 8192),
    CHECK (
        (status = 'running'
         AND lease_owner IS NOT NULL
         AND lease_token IS NOT NULL
         AND lease_expires_at_ms IS NOT NULL)
        OR
        (status <> 'running'
         AND lease_owner IS NULL
         AND lease_token IS NULL
         AND lease_expires_at_ms IS NULL)
    )
);

CREATE TABLE IF NOT EXISTS ducknng_workflow.events (
    task_id UUID NOT NULL,
    event_name VARCHAR NOT NULL,
    value JSON NOT NULL,
    created_at_ms BIGINT NOT NULL DEFAULT epoch_ms(current_timestamp),
    PRIMARY KEY (task_id, event_name),
    CHECK (length(event_name) BETWEEN 1 AND 256),
    CHECK (length(CAST(value AS VARCHAR)) <= 1048576)
);

CREATE INDEX IF NOT EXISTS ducknng_workflow_tasks_claim
ON ducknng_workflow.tasks(queue_name, status, priority, available_at_ms);
