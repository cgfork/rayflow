CREATE TABLE IF NOT EXISTS asset_promotion (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    uuid CHAR(36) NOT NULL,
    url VARCHAR(750) NOT NULL,
    expires_at DATETIME(6) NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_asset_promotion_uuid (uuid),
    KEY idx_asset_promotion_expires_at (expires_at)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin COMMENT = 'private asset copies awaiting publication or removal';
