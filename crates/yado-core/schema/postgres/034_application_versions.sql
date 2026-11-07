CREATE TABLE IF NOT EXISTS application_versions (
    version_name TEXT NOT NULL PRIMARY KEY,
    version_id TEXT NOT NULL,
    version_timestamp BIGINT NOT NULL,
    created_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_application_versions_timestamp ON application_versions (version_timestamp);
