CREATE TABLE IF NOT EXISTS application_versions (
    version_name TEXT NOT NULL PRIMARY KEY,
    version_id TEXT NOT NULL,
    version_timestamp INTEGER NOT NULL,
    created_at INTEGER NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS ix_application_versions_timestamp ON application_versions (version_timestamp);
