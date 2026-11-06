CREATE TABLE IF NOT EXISTS workflow_awakeables (
    awakeable_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    workflow_uuid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    completed_at_epoch_ms BIGINT NULL,
    value LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL,
    serialization VARCHAR(40) CHARACTER SET ascii COLLATE ascii_bin NULL,
    PRIMARY KEY (awakeable_id),
    CONSTRAINT fk_workflow_awakeables_workflow FOREIGN KEY (workflow_uuid)
        REFERENCES workflow_status (workflow_uuid) ON DELETE CASCADE
) ENGINE=InnoDB;
