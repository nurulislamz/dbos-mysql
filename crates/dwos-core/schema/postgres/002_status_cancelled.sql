ALTER TABLE workflow_status
    DROP CONSTRAINT workflow_status_status_check,
    ADD CONSTRAINT workflow_status_status_check CHECK (status IN ('PENDING', 'SUCCESS', 'ERROR', 'MAX_RECOVERY_ATTEMPTS_EXCEEDED', 'CANCELLED'));
