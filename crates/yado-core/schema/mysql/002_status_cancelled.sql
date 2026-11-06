ALTER TABLE workflow_status
    DROP CHECK ck_workflow_status_status,
    ADD CONSTRAINT ck_workflow_status_status CHECK (status IN ('PENDING', 'SUCCESS', 'ERROR', 'MAX_RECOVERY_ATTEMPTS_EXCEEDED', 'CANCELLED'));
