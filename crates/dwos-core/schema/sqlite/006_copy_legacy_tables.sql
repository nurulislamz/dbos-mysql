-- The old tables stay in place. Old rows use a JSON layout that neither current format reads,
-- so they get their own tag and recovery never claims them.
INSERT INTO workflow_status (workflow_uuid, name, status, inputs, parent_workflow_id, output, error, serialization, claim_after, lease_token, recovery_attempts, max_recovery_attempts, created_at, updated_at, completed_at)
SELECT workflow_id, name, status, input, parent_workflow_id, output, error, 'dbos_mysql_json',
       claim_after_ms, lease_token, recovery_attempts, max_recovery_attempts, created_at_ms, updated_at_ms, completed_at_ms
FROM dbos_workflows AS old
WHERE NOT EXISTS (SELECT 1 FROM workflow_status AS new_row WHERE new_row.workflow_uuid = old.workflow_id);

INSERT INTO operation_outputs (workflow_uuid, function_id, function_name, output, error, child_workflow_id, serialization, completed_at_epoch_ms)
SELECT workflow_id, step_id, step_name, output, error, child_workflow_id, 'dbos_mysql_json', created_at_ms
FROM dbos_steps AS old
WHERE NOT EXISTS (SELECT 1 FROM operation_outputs AS new_row WHERE new_row.workflow_uuid = old.workflow_id AND new_row.function_id = old.step_id);
