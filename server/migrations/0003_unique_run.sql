-- Existing installations can have either ORM-generated or deployment index names.
SET @rayflow_migration_sql = IF(
    EXISTS(SELECT INDEX_NAME FROM information_schema.STATISTICS
           WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'run' AND NON_UNIQUE = 0
           GROUP BY INDEX_NAME
           HAVING GROUP_CONCAT(COLUMN_NAME ORDER BY COLUMN_NAME) = 'data_id,graph_id'),
    'SELECT 1',
    'ALTER TABLE run ADD UNIQUE KEY uk_run_data_graph (data_id, graph_id)'
);
-- #[toasty::breakpoint]
PREPARE rayflow_migration FROM @rayflow_migration_sql;
-- #[toasty::breakpoint]
EXECUTE rayflow_migration;
-- #[toasty::breakpoint]
DEALLOCATE PREPARE rayflow_migration;
