CREATE TABLE IF NOT EXISTS workflow_schedules (
    schedule_name TEXT NOT NULL PRIMARY KEY,
    workflow_name TEXT NOT NULL,
    cron TEXT NOT NULL,
    queue_name TEXT NOT NULL,
    inputs TEXT NOT NULL,
    serialization TEXT NOT NULL,
    max_recovery_attempts INTEGER NOT NULL,
    last_fired_epoch_ms INTEGER
) STRICT;
