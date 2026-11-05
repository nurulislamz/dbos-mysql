CREATE TABLE IF NOT EXISTS workflow_status (
    workflow_uuid         TEXT NOT NULL PRIMARY KEY,
    name                  TEXT NOT NULL,
    status                TEXT NOT NULL CHECK (status IN ('PENDING', 'SUCCESS', 'ERROR', 'MAX_RECOVERY_ATTEMPTS_EXCEEDED')),
    inputs                TEXT NOT NULL,
    parent_workflow_id    TEXT,
    output                TEXT,
    error                 TEXT,
    serialization         TEXT NOT NULL,
    claim_after           INTEGER,
    lease_token           TEXT,
    recovery_attempts     INTEGER NOT NULL DEFAULT 1,
    max_recovery_attempts INTEGER NOT NULL,
    created_at            INTEGER NOT NULL,
    updated_at            INTEGER NOT NULL,
    completed_at          INTEGER
) STRICT;

CREATE INDEX IF NOT EXISTS ix_workflow_status_claim
    ON workflow_status (claim_after)
    WHERE claim_after IS NOT NULL;

CREATE TABLE IF NOT EXISTS operation_outputs (
    workflow_uuid         TEXT NOT NULL REFERENCES workflow_status (workflow_uuid) ON DELETE CASCADE,
    function_id           INTEGER NOT NULL,
    function_name         TEXT NOT NULL,
    output                TEXT,
    error                 TEXT,
    child_workflow_id     TEXT,
    serialization         TEXT NOT NULL,
    completed_at_epoch_ms INTEGER NOT NULL,
    PRIMARY KEY (workflow_uuid, function_id),
    CHECK (output IS NULL OR error IS NULL)
) STRICT;
