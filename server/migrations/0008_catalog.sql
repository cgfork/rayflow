CREATE TABLE IF NOT EXISTS catalog (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    uuid CHAR(36) NOT NULL,
    url TEXT NOT NULL,
    size BIGINT UNSIGNED NOT NULL,
    format VARCHAR(255) NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    deleted_at DATETIME(6) NOT NULL DEFAULT '1970-01-01 00:00:00.000000' COMMENT 'Unix epoch means active',
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id),
    UNIQUE KEY uk_catalog_uuid (uuid)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin COMMENT = 'physical object metadata';

CREATE TABLE IF NOT EXISTS file (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    uuid CHAR(36) NOT NULL,
    fullname VARCHAR(512) NOT NULL,
    filename VARCHAR(255) NOT NULL,
    catalog_uuid CHAR(36) NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    deleted_at DATETIME(6) NOT NULL DEFAULT '1970-01-01 00:00:00.000000' COMMENT 'Unix epoch means active',
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id),
    UNIQUE KEY uk_file_uuid (uuid),
    UNIQUE KEY uk_file_fullname_deleted_at (fullname, deleted_at),
    KEY idx_file_catalog_uuid (catalog_uuid)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin COMMENT = 'logical file names referencing catalog UUIDs';
