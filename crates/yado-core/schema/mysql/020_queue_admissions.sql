CREATE TABLE IF NOT EXISTS workflow_queue_admissions (
    queue_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    partition_key VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL,
    admitted_at_ms BIGINT NOT NULL,
    KEY ix_queue_admissions_time (queue_name, admitted_at_ms)
);
