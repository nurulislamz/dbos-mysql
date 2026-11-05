CREATE TABLE IF NOT EXISTS workflow_schedules (
    schedule_name TEXT PRIMARY KEY,
    workflow_name TEXT NOT NULL,
    cron TEXT NOT NULL,
    queue_name TEXT NOT NULL,
    inputs TEXT NOT NULL,
    serialization TEXT NOT NULL,
    max_recovery_attempts BIGINT NOT NULL,
    last_fired_epoch_ms BIGINT
);
