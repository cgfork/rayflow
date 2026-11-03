-- Conditional DDL is needed because retired columns are absent from Toasty models.
SET @rayflow_migration_sql = IF(
    EXISTS(SELECT 1 FROM information_schema.COLUMNS
           WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'job'
             AND COLUMN_NAME = 'assignment_version' AND COLUMN_DEFAULT IS NULL),
    'ALTER TABLE job ALTER COLUMN assignment_version SET DEFAULT 0',
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
             AND COLUMN_NAME = 'steal_count' AND COLUMN_DEFAULT IS NULL),
    'ALTER TABLE job ALTER COLUMN steal_count SET DEFAULT 0',
    'SELECT 1'
);
-- #[toasty::breakpoint]
PREPARE rayflow_migration FROM @rayflow_migration_sql;
-- #[toasty::breakpoint]
EXECUTE rayflow_migration;
-- #[toasty::breakpoint]
DEALLOCATE PREPARE rayflow_migration;
