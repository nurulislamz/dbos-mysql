ALTER TABLE workflow_queue_entries ADD COLUMN debounced BIGINT NOT NULL DEFAULT 0;
ALTER TABLE workflow_queue_entries ADD COLUMN debounce_deadline_ms BIGINT;
