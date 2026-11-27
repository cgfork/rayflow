CREATE TABLE IF NOT EXISTS asset (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    uuid CHAR(36) NOT NULL,
    name VARCHAR(255) NOT NULL,
    version BIGINT UNSIGNED NOT NULL,
    catalog_uuid CHAR(36) NULL,
    sha256 BINARY(32) NOT NULL,
    size BIGINT UNSIGNED NOT NULL,
    format VARCHAR(255) NOT NULL,
    upload_id CHAR(36) NULL,
    source_url VARCHAR(750) NOT NULL,
    created_by VARCHAR(64) NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    published_at DATETIME(6) NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_asset_uuid (uuid),
    UNIQUE KEY uk_asset_name_version (name, version),
    UNIQUE KEY uk_asset_catalog_uuid (catalog_uuid)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin COMMENT = 'immutable asset revisions and pending uploads';
