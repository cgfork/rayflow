-- Preserve all rows. Resolve duplicate URLs across deletion timestamps before
-- upgrading; the unique constraint fails rather than merging catalog identities.
ALTER TABLE catalog
    DROP INDEX uk_catalog_url_deleted_at,
    ADD UNIQUE KEY uk_catalog_url (url),
    DROP COLUMN deleted_at;
