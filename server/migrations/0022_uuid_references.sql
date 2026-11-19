-- MySQL commits DDL even if a later statement fails. Inspect the schema before
-- each DDL step so an unrecorded migration can resume without replacing UUIDs.
-- Conditional schema changes cannot be expressed through ORM model operations.
SET @rayflow_migration_sql = IF(
    EXISTS(SELECT 1 FROM information_schema.COLUMNS
           WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'run_config' AND COLUMN_NAME = 'uuid'),
    'SELECT 1',
    'ALTER TABLE run_config ADD COLUMN uuid CHAR(36) NULL'
);
-- #[toasty::breakpoint]
PREPARE rayflow_migration FROM @rayflow_migration_sql;
-- #[toasty::breakpoint]
EXECUTE rayflow_migration;
-- #[toasty::breakpoint]
DEALLOCATE PREPARE rayflow_migration;
-- #[toasty::breakpoint]
UPDATE run_config SET uuid = UUID(), updated_at = updated_at WHERE uuid IS NULL;
-- #[toasty::breakpoint]
SET @rayflow_migration_sql = IF(
    EXISTS(SELECT 1 FROM information_schema.STATISTICS
           WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'run_config' AND INDEX_NAME = 'uk_run_config_uuid'),
    'ALTER TABLE run_config MODIFY COLUMN uuid CHAR(36) NOT NULL',
    'ALTER TABLE run_config MODIFY COLUMN uuid CHAR(36) NOT NULL, ADD UNIQUE KEY uk_run_config_uuid (uuid)'
);
-- #[toasty::breakpoint]
PREPARE rayflow_migration FROM @rayflow_migration_sql;
-- #[toasty::breakpoint]
EXECUTE rayflow_migration;
-- #[toasty::breakpoint]
DEALLOCATE PREPARE rayflow_migration;
-- #[toasty::breakpoint]
SET @rayflow_migration_sql = IF(
    EXISTS(SELECT 1 FROM information_schema.COLUMNS
           WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'node_config' AND COLUMN_NAME = 'uuid'),
    'SELECT 1',
    'ALTER TABLE node_config ADD COLUMN uuid CHAR(36) NULL'
);
-- #[toasty::breakpoint]
PREPARE rayflow_migration FROM @rayflow_migration_sql;
-- #[toasty::breakpoint]
EXECUTE rayflow_migration;
-- #[toasty::breakpoint]
DEALLOCATE PREPARE rayflow_migration;
-- #[toasty::breakpoint]
UPDATE node_config SET uuid = UUID(), updated_at = updated_at WHERE uuid IS NULL;
-- #[toasty::breakpoint]
SET @rayflow_migration_sql = IF(
    EXISTS(SELECT 1 FROM information_schema.STATISTICS
           WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'node_config' AND INDEX_NAME = 'uk_node_config_uuid'),
    'ALTER TABLE node_config MODIFY COLUMN uuid CHAR(36) NOT NULL',
    'ALTER TABLE node_config MODIFY COLUMN uuid CHAR(36) NOT NULL, ADD UNIQUE KEY uk_node_config_uuid (uuid)'
);
-- #[toasty::breakpoint]
PREPARE rayflow_migration FROM @rayflow_migration_sql;
-- #[toasty::breakpoint]
EXECUTE rayflow_migration;
-- #[toasty::breakpoint]
DEALLOCATE PREPARE rayflow_migration;
-- #[toasty::breakpoint]
-- All three references are replaced by one atomic ALTER below. If it already
-- committed, skip numeric joins as well as DDL to preserve the converted IDs.
SET @rayflow_legacy_job_references = EXISTS(
    SELECT 1 FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'job'
      AND COLUMN_NAME = 'node_run_id' AND DATA_TYPE = 'bigint'
);
-- #[toasty::breakpoint]
SET @rayflow_migration_sql = IF(
    @rayflow_legacy_job_references AND NOT EXISTS(
        SELECT 1 FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'job' AND COLUMN_NAME = 'node_run_uuid'),
    'ALTER TABLE job ADD COLUMN node_run_uuid CHAR(36) NULL',
    'SELECT 1'
);
-- #[toasty::breakpoint]
PREPARE rayflow_migration FROM @rayflow_migration_sql;
-- #[toasty::breakpoint]
EXECUTE rayflow_migration;
-- #[toasty::breakpoint]
DEALLOCATE PREPARE rayflow_migration;
-- #[toasty::breakpoint]
SET @rayflow_migration_sql = IF(
    @rayflow_legacy_job_references AND NOT EXISTS(
        SELECT 1 FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'job' AND COLUMN_NAME = 'node_config_uuid'),
    'ALTER TABLE job ADD COLUMN node_config_uuid CHAR(36) NULL',
    'SELECT 1'
);
-- #[toasty::breakpoint]
PREPARE rayflow_migration FROM @rayflow_migration_sql;
-- #[toasty::breakpoint]
EXECUTE rayflow_migration;
-- #[toasty::breakpoint]
DEALLOCATE PREPARE rayflow_migration;
-- #[toasty::breakpoint]
SET @rayflow_migration_sql = IF(
    @rayflow_legacy_job_references AND NOT EXISTS(
        SELECT 1 FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'job' AND COLUMN_NAME = 'run_config_uuid'),
    'ALTER TABLE job ADD COLUMN run_config_uuid CHAR(36) NULL',
    'SELECT 1'
);
-- #[toasty::breakpoint]
PREPARE rayflow_migration FROM @rayflow_migration_sql;
-- #[toasty::breakpoint]
EXECUTE rayflow_migration;
-- #[toasty::breakpoint]
DEALLOCATE PREPARE rayflow_migration;
-- #[toasty::breakpoint]
SET @rayflow_migration_sql = IF(
    @rayflow_legacy_job_references,
    'UPDATE job j LEFT JOIN node_run nr ON nr.id = j.node_run_id
     SET j.node_run_uuid = nr.uuid, j.updated_at = j.updated_at',
    'SELECT 1'
);
-- #[toasty::breakpoint]
PREPARE rayflow_migration FROM @rayflow_migration_sql;
-- #[toasty::breakpoint]
EXECUTE rayflow_migration;
-- #[toasty::breakpoint]
DEALLOCATE PREPARE rayflow_migration;
-- #[toasty::breakpoint]
-- Optional configurations used numeric zero for "none"; the current models
-- use the nil UUID. Missing nonzero references remain NULL and fail the ALTER.
SET @rayflow_migration_sql = IF(
    @rayflow_legacy_job_references,
    'UPDATE job j LEFT JOIN node_config nc ON nc.id = j.node_config_id
     SET j.node_config_uuid = CASE WHEN j.node_config_id = 0
         THEN ''00000000-0000-0000-0000-000000000000'' ELSE nc.uuid END,
         j.updated_at = j.updated_at',
    'SELECT 1'
);
-- #[toasty::breakpoint]
PREPARE rayflow_migration FROM @rayflow_migration_sql;
-- #[toasty::breakpoint]
EXECUTE rayflow_migration;
-- #[toasty::breakpoint]
DEALLOCATE PREPARE rayflow_migration;
-- #[toasty::breakpoint]
SET @rayflow_migration_sql = IF(
    @rayflow_legacy_job_references,
    'UPDATE job j LEFT JOIN run_config rc ON rc.id = j.run_config_id
     SET j.run_config_uuid = CASE WHEN j.run_config_id = 0
         THEN ''00000000-0000-0000-0000-000000000000'' ELSE rc.uuid END,
         j.updated_at = j.updated_at',
    'SELECT 1'
);
-- #[toasty::breakpoint]
PREPARE rayflow_migration FROM @rayflow_migration_sql;
-- #[toasty::breakpoint]
EXECUTE rayflow_migration;
-- #[toasty::breakpoint]
DEALLOCATE PREPARE rayflow_migration;
-- #[toasty::breakpoint]
SET @rayflow_migration_sql = IF(
    @rayflow_legacy_job_references,
    'ALTER TABLE job
        DROP INDEX idx_job_node_run_id,
        DROP INDEX idx_job_node_config_id,
        DROP INDEX idx_job_run_config_id,
        DROP COLUMN node_run_id,
        DROP COLUMN node_config_id,
        DROP COLUMN run_config_id,
        CHANGE COLUMN node_run_uuid node_run_id CHAR(36) NOT NULL,
        CHANGE COLUMN node_config_uuid node_config_id CHAR(36) NOT NULL,
        CHANGE COLUMN run_config_uuid run_config_id CHAR(36) NOT NULL,
        ADD KEY idx_job_node_run_id (node_run_id),
        ADD KEY idx_job_node_config_id (node_config_id),
        ADD KEY idx_job_run_config_id (run_config_id)',
    'SELECT 1'
);
-- #[toasty::breakpoint]
PREPARE rayflow_migration FROM @rayflow_migration_sql;
-- #[toasty::breakpoint]
EXECUTE rayflow_migration;
-- #[toasty::breakpoint]
DEALLOCATE PREPARE rayflow_migration;
