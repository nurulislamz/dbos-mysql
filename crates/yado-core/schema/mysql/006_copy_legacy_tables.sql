-- The old tables stay in place. Old rows use a JSON layout that neither current format reads,
-- so they get their own tag and recovery never claims them.
START TRANSACTION;

INSERT INTO workflow_status (workflow_uuid, name, status, inputs, parent_workflow_id, output, error, serialization, claim_after, lease_token, recovery_attempts, max_recovery_attempts, created_at, updated_at, completed_at)
SELECT workflow_id, name, status, input, parent_workflow_id, output, error, 'dbos_mysql_json',
       CAST(UNIX_TIMESTAMP(claim_after) * 1000 AS SIGNED), lease_token, recovery_attempts, max_recovery_attempts,
       CAST(UNIX_TIMESTAMP(created_at) * 1000 AS SIGNED), CAST(UNIX_TIMESTAMP(updated_at) * 1000 AS SIGNED),
       CAST(UNIX_TIMESTAMP(completed_at) * 1000 AS SIGNED)
FROM dbos_workflows old
WHERE NOT EXISTS (SELECT 1 FROM workflow_status new_row WHERE new_row.workflow_uuid = old.workflow_id);

INSERT INTO operation_outputs (workflow_uuid, function_id, function_name, output, error, child_workflow_id, serialization, completed_at_epoch_ms)
SELECT workflow_id, step_id, step_name, output, error, child_workflow_id, 'dbos_mysql_json',
       CAST(UNIX_TIMESTAMP(created_at) * 1000 AS SIGNED)
FROM dbos_steps old
WHERE NOT EXISTS (SELECT 1 FROM operation_outputs new_row WHERE new_row.workflow_uuid = old.workflow_id AND new_row.function_id = old.step_id);

COMMIT;
