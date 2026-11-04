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

CREATE TABLE IF NOT EXISTS notifications (
    message_id               BIGINT NOT NULL AUTO_INCREMENT,
    destination_uuid         VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    topic                    VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    message                  LONGTEXT     CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    created_at_epoch_ms      BIGINT       NOT NULL,
    message_uuid             VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    serialization            VARCHAR(40)  CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    consumed                 BIGINT       NOT NULL DEFAULT 0,
    consumed_by_function_id  BIGINT       NULL,
    PRIMARY KEY (message_id),
    UNIQUE KEY uq_notifications_message (destination_uuid, message_uuid),
    KEY ix_notifications_pending (destination_uuid, topic, consumed, message_id),
    CONSTRAINT fk_notifications_workflow
        FOREIGN KEY (destination_uuid) REFERENCES workflow_status (workflow_uuid) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS workflow_events (
    workflow_uuid  VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    `key`          VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    value          LONGTEXT     CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    serialization  VARCHAR(40)  CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    PRIMARY KEY (workflow_uuid, `key`),
    CONSTRAINT fk_workflow_events_workflow
        FOREIGN KEY (workflow_uuid) REFERENCES workflow_status (workflow_uuid) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS workflow_timeouts (
    workflow_uuid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL PRIMARY KEY,
    timeout_ms BIGINT,
    deadline_epoch_ms BIGINT NOT NULL,
    KEY ix_workflow_timeouts_deadline (deadline_epoch_ms),
    CONSTRAINT fk_workflow_timeouts_workflow
        FOREIGN KEY (workflow_uuid) REFERENCES workflow_status (workflow_uuid) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS workflow_event_history (
    workflow_uuid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    event_key VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    revision BIGINT NOT NULL,
    created_at_epoch_ms BIGINT NOT NULL,
    value LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    serialization VARCHAR(40) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    PRIMARY KEY (workflow_uuid, event_key, revision),
    CONSTRAINT fk_workflow_event_history_workflow
        FOREIGN KEY (workflow_uuid) REFERENCES workflow_status (workflow_uuid) ON DELETE CASCADE
) ENGINE=InnoDB;
