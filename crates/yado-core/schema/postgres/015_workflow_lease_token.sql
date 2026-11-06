CREATE INDEX IF NOT EXISTS ix_workflow_status_lease_token ON workflow_status (lease_token) WHERE lease_token IS NOT NULL;
