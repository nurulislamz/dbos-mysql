CREATE TABLE IF NOT EXISTS application_versions (
    version_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    version_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    version_timestamp BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (version_name),
    KEY ix_application_versions_timestamp (version_timestamp)
) ENGINE=InnoDB;
