-- Each ALTER commits independently. Skip columns already added by a prior attempt.
SET @rayflow_migration_sql = IF(
    EXISTS(SELECT 1 FROM information_schema.COLUMNS
           WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'node_config'
             AND COLUMN_NAME = 'required_gpu_cdi_device'),
    'SELECT 1',
    'ALTER TABLE node_config ADD COLUMN required_gpu_cdi_device VARCHAR(255) COLLATE utf8mb4_bin NULL AFTER required_gpu_memory_bytes'
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
    'SELECT 1',
    'ALTER TABLE job ADD COLUMN required_gpu_cdi_device VARCHAR(255) COLLATE utf8mb4_bin NULL AFTER required_gpu_memory_bytes'
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
             AND COLUMN_NAME = 'assigned_gpu_cdi_device'),
    'SELECT 1',
    'ALTER TABLE job ADD COLUMN assigned_gpu_cdi_device VARCHAR(255) COLLATE utf8mb4_bin NULL AFTER required_gpu_cdi_device'
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
             AND COLUMN_NAME = 'gpu_devices'),
    'SELECT 1',
    'ALTER TABLE agent_instance ADD COLUMN gpu_devices JSON NULL AFTER gpu_memory_bytes'
);
-- #[toasty::breakpoint]
PREPARE rayflow_migration FROM @rayflow_migration_sql;
-- #[toasty::breakpoint]
EXECUTE rayflow_migration;
-- #[toasty::breakpoint]
DEALLOCATE PREPARE rayflow_migration;
