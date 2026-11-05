CREATE TABLE IF NOT EXISTS workflow_attributes (
    workflow_uuid TEXT NOT NULL REFERENCES workflow_status (workflow_uuid) ON DELETE CASCADE,
    attr_key TEXT NOT NULL,
    attr_value TEXT NOT NULL,
    PRIMARY KEY (workflow_uuid, attr_key)
) STRICT;
