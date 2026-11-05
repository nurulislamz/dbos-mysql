CREATE TABLE IF NOT EXISTS workflow_attributes (
    workflow_uuid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    attr_key VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    attr_value LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    PRIMARY KEY (workflow_uuid, attr_key),
    CONSTRAINT fk_workflow_attributes_workflow FOREIGN KEY (workflow_uuid)
        REFERENCES workflow_status (workflow_uuid) ON DELETE CASCADE
) ENGINE=InnoDB;
