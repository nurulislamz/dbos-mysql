CREATE TABLE IF NOT EXISTS dbos_workflows (
    workflow_id           TEXT NOT NULL PRIMARY KEY,
    name                  TEXT NOT NULL,
    status                TEXT NOT NULL CHECK (status IN ('PENDING', 'SUCCESS', 'ERROR', 'MAX_RECOVERY_ATTEMPTS_EXCEEDED')),
    input                 TEXT NOT NULL,
    parent_workflow_id    TEXT,
    output                TEXT,
    error                 TEXT,
    claim_after_ms        INTEGER,
    lease_token           TEXT,
    recovery_attempts     INTEGER NOT NULL DEFAULT 1,
    max_recovery_attempts INTEGER NOT NULL,
    created_at_ms         INTEGER NOT NULL,
    updated_at_ms         INTEGER NOT NULL,
    completed_at_ms       INTEGER
) STRICT;

CREATE INDEX IF NOT EXISTS ix_dbos_workflows_claim
    ON dbos_workflows (claim_after_ms)
    WHERE claim_after_ms IS NOT NULL;

CREATE TABLE IF NOT EXISTS dbos_steps (
    workflow_id       TEXT NOT NULL REFERENCES dbos_workflows (workflow_id) ON DELETE CASCADE,
    step_id           INTEGER NOT NULL,
    step_name         TEXT NOT NULL,
    output            TEXT,
    error             TEXT,
    child_workflow_id TEXT,
    created_at_ms     INTEGER NOT NULL,
    PRIMARY KEY (workflow_id, step_id),
    CHECK (output IS NULL OR error IS NULL)
) STRICT;
