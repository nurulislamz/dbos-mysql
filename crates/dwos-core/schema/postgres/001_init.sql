CREATE TABLE IF NOT EXISTS workflow_status (
    workflow_uuid         TEXT PRIMARY KEY,
    name                  TEXT NOT NULL,
    status                TEXT NOT NULL CHECK (status IN ('PENDING', 'SUCCESS', 'ERROR', 'MAX_RECOVERY_ATTEMPTS_EXCEEDED', 'CANCELLED')),
    inputs                TEXT NOT NULL,
    parent_workflow_id    TEXT,
    output                TEXT,
    error                 TEXT,
    serialization         TEXT NOT NULL,
    claim_after           BIGINT,
    lease_token           TEXT,
    recovery_attempts     BIGINT NOT NULL DEFAULT 1,
    max_recovery_attempts BIGINT NOT NULL,
    created_at            BIGINT NOT NULL,
    updated_at            BIGINT NOT NULL,
    completed_at          BIGINT
);

CREATE INDEX IF NOT EXISTS ix_workflow_status_claim
    ON workflow_status (claim_after)
    WHERE claim_after IS NOT NULL;

CREATE TABLE IF NOT EXISTS operation_outputs (
    workflow_uuid         TEXT NOT NULL REFERENCES workflow_status (workflow_uuid) ON DELETE CASCADE,
    function_id           BIGINT NOT NULL,
    function_name         TEXT NOT NULL,
    output                TEXT,
    error                 TEXT,
    child_workflow_id     TEXT,
    serialization         TEXT NOT NULL,
    completed_at_epoch_ms BIGINT NOT NULL,
    PRIMARY KEY (workflow_uuid, function_id),
    CHECK (output IS NULL OR error IS NULL)
);

CREATE TABLE IF NOT EXISTS notifications (
    message_id               BIGSERIAL PRIMARY KEY,
    destination_uuid         TEXT NOT NULL REFERENCES workflow_status (workflow_uuid) ON DELETE CASCADE,
    topic                    TEXT NOT NULL,
    message                  TEXT NOT NULL,
    created_at_epoch_ms      BIGINT NOT NULL,
    message_uuid             TEXT NOT NULL,
    serialization            TEXT NOT NULL,
    consumed                 BIGINT NOT NULL DEFAULT 0,
    consumed_by_function_id  BIGINT,
    CHECK (consumed IN (0, 1)),
    UNIQUE (destination_uuid, message_uuid)
);

CREATE INDEX IF NOT EXISTS ix_notifications_pending
    ON notifications (destination_uuid, topic, consumed, message_id);

CREATE TABLE IF NOT EXISTS workflow_events (
    workflow_uuid  TEXT NOT NULL REFERENCES workflow_status (workflow_uuid) ON DELETE CASCADE,
    "key"          TEXT NOT NULL,
    value          TEXT NOT NULL,
    serialization  TEXT NOT NULL,
    PRIMARY KEY (workflow_uuid, "key")
);
