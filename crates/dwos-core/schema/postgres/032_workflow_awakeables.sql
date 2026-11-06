CREATE TABLE IF NOT EXISTS workflow_awakeables (
    awakeable_id TEXT PRIMARY KEY,
    workflow_uuid TEXT NOT NULL REFERENCES workflow_status (workflow_uuid) ON DELETE CASCADE,
    completed_at_epoch_ms BIGINT,
    value TEXT,
    serialization TEXT
);
CREATE INDEX IF NOT EXISTS ix_workflow_awakeables_workflow ON workflow_awakeables (workflow_uuid);
