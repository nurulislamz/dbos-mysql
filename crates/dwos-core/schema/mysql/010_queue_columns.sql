ALTER TABLE workflow_queues
    ADD COLUMN worker_concurrency BIGINT NULL,
    ADD COLUMN rate_limit BIGINT NULL,
    ADD COLUMN rate_period_ms BIGINT NULL,
    ADD COLUMN partition_concurrency BIGINT NULL,
    ADD COLUMN partition_worker_concurrency BIGINT NULL,
    ADD COLUMN partition_rate_limit BIGINT NULL,
    ADD COLUMN partition_rate_period_ms BIGINT NULL,
    ADD COLUMN polling_interval_ms BIGINT NULL,
    ADD COLUMN application_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL,
    ADD COLUMN deleted BIGINT NOT NULL DEFAULT 0;
