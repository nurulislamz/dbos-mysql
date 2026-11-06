ALTER TABLE workflow_status ADD KEY ix_workflow_status_lease_token (lease_token);
