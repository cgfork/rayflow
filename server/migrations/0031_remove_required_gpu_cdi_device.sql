-- Each ALTER commits independently. Skip columns already dropped by a prior attempt.
SET @rayflow_migration_sql = IF(
    EXISTS(SELECT 1 FROM information_schema.COLUMNS
           WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'node_config'
             AND COLUMN_NAME = 'required_gpu_cdi_device'),
    'ALTER TABLE node_config DROP COLUMN required_gpu_cdi_device',
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
    EXISTS(SELECT 1 FROM information_schema.COLUMNS
           WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'job'
             AND COLUMN_NAME = 'required_gpu_cdi_device'),
    'ALTER TABLE job DROP COLUMN required_gpu_cdi_device',
    'SELECT 1'
);
-- #[toasty::breakpoint]
PREPARE rayflow_migration FROM @rayflow_migration_sql;
-- #[toasty::breakpoint]
EXECUTE rayflow_migration;
-- #[toasty::breakpoint]
DEALLOCATE PREPARE rayflow_migration;
