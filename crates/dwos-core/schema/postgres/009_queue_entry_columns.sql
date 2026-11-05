ALTER TABLE workflow_queue_entries ADD COLUMN priority BIGINT NOT NULL DEFAULT 0;
ALTER TABLE workflow_queue_entries ADD COLUMN partition_key TEXT;
ALTER TABLE workflow_queue_entries ADD COLUMN deduplication_id TEXT;
ALTER TABLE workflow_queue_entries ADD COLUMN admitted_at_ms BIGINT;
ALTER TABLE workflow_queue_entries ADD COLUMN delay_until_ms BIGINT;
CREATE INDEX IF NOT EXISTS ix_workflow_queue_entries_order ON workflow_queue_entries (queue_name, priority, sequence);
CREATE INDEX IF NOT EXISTS ix_workflow_queue_entries_dedup ON workflow_queue_entries (queue_name, deduplication_id);
CREATE INDEX IF NOT EXISTS ix_workflow_queue_entries_admitted ON workflow_queue_entries (queue_name, admitted_at_ms);
