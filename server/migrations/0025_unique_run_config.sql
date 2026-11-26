ALTER TABLE run_config
    MODIFY COLUMN run_id CHAR(36) NULL DEFAULT NULL COMMENT 'owning run uuid';
-- #[toasty::breakpoint]
UPDATE run_config SET run_id = NULL, updated_at = updated_at WHERE run_id = '';
-- #[toasty::breakpoint]
-- Keep the snapshot previously selected for new jobs. Older jobs can still
-- resolve their original snapshot through its UUID.
UPDATE run_config AS config
JOIN (
    SELECT run_id, MAX(id) AS latest_id
    FROM run_config
    WHERE run_id IS NOT NULL
    GROUP BY run_id
    HAVING COUNT(*) > 1
) AS latest ON latest.run_id = config.run_id
SET config.run_id = NULL, config.updated_at = config.updated_at
WHERE config.id <> latest.latest_id;
-- #[toasty::breakpoint]
-- The ALTER may have committed before the migration ID was recorded.
SET @rayflow_migration_sql = IF(
    EXISTS(SELECT 1 FROM information_schema.STATISTICS
           WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'run_config'
             AND INDEX_NAME = 'uk_run_config_run_id'),
    'SELECT 1',
    'ALTER TABLE run_config
        DROP INDEX idx_run_config_run_id,
        ADD UNIQUE KEY uk_run_config_run_id (run_id)'
);
-- #[toasty::breakpoint]
PREPARE rayflow_migration FROM @rayflow_migration_sql;
-- #[toasty::breakpoint]
EXECUTE rayflow_migration;
-- #[toasty::breakpoint]
DEALLOCATE PREPARE rayflow_migration;
