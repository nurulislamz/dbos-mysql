CREATE TABLE IF NOT EXISTS workflow_attribute_lookup (
    workflow_uuid TEXT NOT NULL REFERENCES workflow_status (workflow_uuid) ON DELETE CASCADE,
    attr_key TEXT NOT NULL,
    attr_value TEXT NOT NULL,
    PRIMARY KEY (workflow_uuid, attr_key)
) STRICT;
DELETE FROM workflow_attribute_lookup;
INSERT INTO workflow_attribute_lookup (workflow_uuid, attr_key, attr_value) SELECT workflow_uuid, j.key, workflow_status.attributes -> j.fullkey FROM workflow_status, json_each(workflow_status.attributes) AS j WHERE length(j.key) <= 255;
CREATE INDEX ix_workflow_attributes_lookup ON workflow_attribute_lookup (attr_key, substr(attr_value, 1, 255));
