CREATE TABLE IF NOT EXISTS workflow_streams (
    workflow_uuid VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    stream_key VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    stream_offset BIGINT NOT NULL,
    value LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL,
    serialization VARCHAR(40) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    PRIMARY KEY (workflow_uuid, stream_key, stream_offset),
    CONSTRAINT fk_workflow_streams_workflow FOREIGN KEY (workflow_uuid)
        REFERENCES workflow_status (workflow_uuid) ON DELETE CASCADE
) ENGINE=InnoDB;
