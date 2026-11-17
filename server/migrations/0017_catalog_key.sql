-- Preserve catalog UUIDs and file references. Existing clients can use the URL
-- as their initial key, or rename it with update_catalog after upgrading.
ALTER TABLE catalog
    ADD COLUMN `key` VARCHAR(750) NULL COMMENT 'unique object key' AFTER uuid;

UPDATE catalog SET `key` = url, updated_at = updated_at;

ALTER TABLE catalog
    MODIFY COLUMN `key` VARCHAR(750) NOT NULL COMMENT 'unique object key',
    ADD UNIQUE KEY uk_catalog_key (`key`);
