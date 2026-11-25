-- The ALTER commits atomically in MySQL before the migration ID is recorded.
SET @rayflow_migration_sql = IF(
    EXISTS(SELECT 1 FROM information_schema.TABLES
           WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'job_result'),
    'ALTER TABLE job_result
        ADD COLUMN filename VARCHAR(255) NOT NULL DEFAULT ''result.json'' AFTER job_uuid,
        DROP INDEX uk_job_result_job_uuid,
        ADD UNIQUE KEY uk_job_artifact_job_filename (job_uuid, filename),
        RENAME INDEX uk_job_result_catalog_uuid TO uk_job_artifact_catalog_uuid,
        COMMENT = ''job artifact owner and catalog association'',
        RENAME TO job_artifact',
    'SELECT filename FROM job_artifact LIMIT 0'
);
-- #[toasty::breakpoint]
PREPARE rayflow_migration FROM @rayflow_migration_sql;
-- #[toasty::breakpoint]
EXECUTE rayflow_migration;
-- #[toasty::breakpoint]
DEALLOCATE PREPARE rayflow_migration;
