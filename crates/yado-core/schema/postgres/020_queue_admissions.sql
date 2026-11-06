CREATE TABLE IF NOT EXISTS workflow_queue_admissions (
    queue_name TEXT NOT NULL,
    partition_key TEXT,
    admitted_at_ms BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_queue_admissions_time ON workflow_queue_admissions (queue_name, admitted_at_ms);
