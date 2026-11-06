CREATE TABLE IF NOT EXISTS workflow_streams (
    workflow_uuid TEXT NOT NULL REFERENCES workflow_status (workflow_uuid) ON DELETE CASCADE,
    stream_key TEXT NOT NULL,
    stream_offset BIGINT NOT NULL,
    value TEXT,
    serialization TEXT NOT NULL,
    PRIMARY KEY (workflow_uuid, stream_key, stream_offset)
);
