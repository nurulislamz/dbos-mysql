CREATE INDEX IF NOT EXISTS ix_workflow_status_parent ON workflow_status (parent_workflow_id);
CREATE INDEX IF NOT EXISTS ix_workflow_status_forked_from ON workflow_status (forked_from);
