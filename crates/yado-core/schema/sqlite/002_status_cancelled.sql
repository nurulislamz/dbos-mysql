-- SQLite cannot alter a CHECK, so this is its documented table rebuild.
-- The runner has foreign keys off, or DROP TABLE would empty every ON DELETE CASCADE child.
-- The runner also restores this table's indexes and triggers afterwards.
CREATE TABLE workflow_status_new (
    workflow_uuid         TEXT NOT NULL PRIMARY KEY,
    name                  TEXT NOT NULL,
    status                TEXT NOT NULL CHECK (status IN ('PENDING', 'SUCCESS', 'ERROR', 'MAX_RECOVERY_ATTEMPTS_EXCEEDED', 'CANCELLED')),
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

INSERT INTO workflow_status_new (rowid, workflow_uuid, name, status, inputs, parent_workflow_id, output, error, serialization, claim_after, lease_token, recovery_attempts, max_recovery_attempts, created_at, updated_at, completed_at)
SELECT rowid, workflow_uuid, name, status, inputs, parent_workflow_id, output, error, serialization, claim_after, lease_token, recovery_attempts, max_recovery_attempts, created_at, updated_at, completed_at
FROM workflow_status;

DROP TABLE workflow_status;

ALTER TABLE workflow_status_new RENAME TO workflow_status;
