CREATE TABLE IF NOT EXISTS workflow_status (
    workflow_uuid         VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    name                  VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    status                VARCHAR(40)  CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    inputs                LONGTEXT     CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    parent_workflow_id    VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL,
    output                LONGTEXT     CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL,
    error                 LONGTEXT     CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL,
    serialization         VARCHAR(40)  CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    claim_after           BIGINT       NULL,
    lease_token           CHAR(36)     CHARACTER SET ascii COLLATE ascii_bin NULL,
    recovery_attempts     BIGINT       NOT NULL DEFAULT 1,
    max_recovery_attempts BIGINT       NOT NULL,
    created_at            BIGINT       NOT NULL,
    updated_at            BIGINT       NOT NULL,
    completed_at          BIGINT       NULL,
    PRIMARY KEY (workflow_uuid),
    KEY ix_workflow_status_claim (claim_after),
    CONSTRAINT ck_workflow_status_status
        CHECK (status IN ('PENDING', 'SUCCESS', 'ERROR', 'MAX_RECOVERY_ATTEMPTS_EXCEEDED', 'CANCELLED'))
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS operation_outputs (
    workflow_uuid         VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    function_id           BIGINT       NOT NULL,
    function_name         VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    output                LONGTEXT     CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL,
    error                 LONGTEXT     CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL,
    child_workflow_id     VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL,
    serialization         VARCHAR(40)  CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    completed_at_epoch_ms BIGINT       NOT NULL,
    PRIMARY KEY (workflow_uuid, function_id),
    CONSTRAINT fk_operation_outputs_workflow
        FOREIGN KEY (workflow_uuid) REFERENCES workflow_status (workflow_uuid) ON DELETE CASCADE,
    CONSTRAINT ck_operation_outputs_result
        CHECK (output IS NULL OR error IS NULL)
) ENGINE=InnoDB;
