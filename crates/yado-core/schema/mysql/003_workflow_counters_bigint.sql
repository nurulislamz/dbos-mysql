ALTER TABLE workflow_status
    MODIFY recovery_attempts BIGINT NOT NULL DEFAULT 1,
    MODIFY max_recovery_attempts BIGINT NOT NULL;
