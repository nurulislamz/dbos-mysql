ALTER TABLE workflow_queue_entries
    ADD COLUMN priority BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN partition_key VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL,
    ADD COLUMN deduplication_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL,
    ADD COLUMN admitted_at_ms BIGINT NULL,
    ADD COLUMN delay_until_ms BIGINT NULL,
    ADD KEY ix_workflow_queue_entries_order (queue_name, priority, sequence),
    ADD KEY ix_workflow_queue_entries_dedup (queue_name, deduplication_id),
    ADD KEY ix_workflow_queue_entries_admitted (queue_name, admitted_at_ms);
