CREATE TABLE IF NOT EXISTS workflow_attribute_lookup (
    workflow_uuid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    attr_key VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    attr_value LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    PRIMARY KEY (workflow_uuid, attr_key),
    CONSTRAINT fk_workflow_attribute_lookup_workflow FOREIGN KEY (workflow_uuid)
        REFERENCES workflow_status (workflow_uuid) ON DELETE CASCADE
) ENGINE=InnoDB;
DELETE FROM workflow_attribute_lookup;
INSERT INTO workflow_attribute_lookup (workflow_uuid, attr_key, attr_value) SELECT workflow_uuid, j.attr_key, CAST(JSON_EXTRACT(workflow_status.attributes, CONCAT('$.', JSON_QUOTE(j.attr_key))) AS CHAR CHARACTER SET utf8mb4) FROM workflow_status JOIN JSON_TABLE(JSON_KEYS(workflow_status.attributes), '$[*]' COLUMNS (attr_key TEXT PATH '$')) AS j WHERE CHAR_LENGTH(j.attr_key) <= 255;
CREATE INDEX ix_workflow_attributes_lookup ON workflow_attribute_lookup (attr_key, attr_value(255));
