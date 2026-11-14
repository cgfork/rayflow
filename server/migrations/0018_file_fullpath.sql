-- Preserve logical names and their unique constraint while renaming the column.
ALTER TABLE file
    CHANGE COLUMN fullname fullpath VARCHAR(512) NOT NULL,
    DROP INDEX uk_file_fullname,
    ADD UNIQUE KEY uk_file_fullpath (fullpath);
