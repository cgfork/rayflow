-- Preserve all rows and references. Full names must now be unique across all
-- rows. Keep this separate so a failed file migration can be retried after the
-- catalog migration has committed.
ALTER TABLE file
    DROP INDEX uk_file_fullname_deleted_at,
    ADD UNIQUE KEY uk_file_fullname (fullname),
    DROP COLUMN deleted_at;
