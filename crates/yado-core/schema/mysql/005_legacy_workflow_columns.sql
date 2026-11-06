ALTER TABLE dbos_workflows
    ADD COLUMN parent_workflow_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL,
    ADD COLUMN completed_at DATETIME(6) NULL;
