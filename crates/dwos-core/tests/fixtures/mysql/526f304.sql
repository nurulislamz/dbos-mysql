CREATE TABLE IF NOT EXISTS dbos_workflows (
    workflow_id           VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    name                  VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    status                VARCHAR(40)  CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    input                 LONGTEXT     CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    parent_workflow_id    VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL,
    output                LONGTEXT     CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL,
    error                 LONGTEXT     CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL,
    claim_after           DATETIME(6)  NULL,
    lease_token           CHAR(36)     CHARACTER SET ascii COLLATE ascii_bin NULL,
    recovery_attempts     INT UNSIGNED NOT NULL DEFAULT 1,
    max_recovery_attempts INT UNSIGNED NOT NULL,
    created_at            DATETIME(6)  NOT NULL,
    updated_at            DATETIME(6)  NOT NULL,
    completed_at          DATETIME(6)  NULL,
    PRIMARY KEY (workflow_id),
    KEY ix_dbos_workflows_claim (claim_after),
    CONSTRAINT ck_dbos_workflows_status
        CHECK (status IN ('PENDING', 'SUCCESS', 'ERROR', 'MAX_RECOVERY_ATTEMPTS_EXCEEDED'))
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dbos_steps (
    workflow_id       VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    step_id           INT UNSIGNED NOT NULL,
    step_name         VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    output            LONGTEXT     CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL,
    error             LONGTEXT     CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL,
    child_workflow_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL,
    created_at        DATETIME(6)  NOT NULL,
    PRIMARY KEY (workflow_id, step_id),
    CONSTRAINT fk_dbos_steps_workflow
        FOREIGN KEY (workflow_id) REFERENCES dbos_workflows (workflow_id) ON DELETE CASCADE,
    CONSTRAINT ck_dbos_steps_result
        CHECK (output IS NULL OR error IS NULL)
) ENGINE=InnoDB;
