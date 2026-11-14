-- Index the full URL with its deletion timestamp. At 750 utf8mb4 characters,
-- the composite key fits within InnoDB's 3072-byte index limit.
-- Duplicate URL/deletion pairs and URLs longer than 750 characters must be
-- resolved before applying this migration; preserve their UUID/file references.
ALTER TABLE catalog
    MODIFY COLUMN url VARCHAR(750) NOT NULL,
    ADD UNIQUE KEY uk_catalog_url_deleted_at (url, deleted_at);
