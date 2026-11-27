-- Each ALTER commits independently. Skip columns already added by a prior attempt.
SET @rayflow_migration_sql = IF(
    EXISTS(SELECT 1 FROM information_schema.COLUMNS
           WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'node_config'
             AND COLUMN_NAME = 'required_gpu_memory_bytes'),
    'SELECT 1',
    'ALTER TABLE node_config ADD COLUMN required_gpu_memory_bytes BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT ''job GPU memory requirement in bytes'' AFTER required_memory_bytes'
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
             AND COLUMN_NAME = 'required_gpu_memory_bytes'),
    'SELECT 1',
    'ALTER TABLE job ADD COLUMN required_gpu_memory_bytes BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT ''GPU memory requirement in bytes'' AFTER required_memory_bytes'
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
           WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'agent_instance'
             AND COLUMN_NAME = 'gpu_memory_bytes'),
    'SELECT 1',
    'ALTER TABLE agent_instance ADD COLUMN gpu_memory_bytes BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT ''agent GPU memory budget in bytes'' AFTER memory_bytes'
);
-- #[toasty::breakpoint]
PREPARE rayflow_migration FROM @rayflow_migration_sql;
-- #[toasty::breakpoint]
EXECUTE rayflow_migration;
-- #[toasty::breakpoint]
DEALLOCATE PREPARE rayflow_migration;
