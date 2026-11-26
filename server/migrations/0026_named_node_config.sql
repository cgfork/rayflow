-- Existing configs keep their IDs and use the default name. The ALTER may
-- commit before its migration ID is recorded.
SET @rayflow_migration_sql = IF(
    EXISTS(SELECT 1 FROM information_schema.STATISTICS
           WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'node_config'
             AND INDEX_NAME = 'uk_node_config_node_version_name'),
    'SELECT name FROM node_config LIMIT 0',
    'ALTER TABLE node_config
        ADD COLUMN name VARCHAR(255) COLLATE utf8mb4_bin NOT NULL DEFAULT ''default'' COMMENT ''configuration name'' AFTER version,
        DROP INDEX uk_node_config_node_version,
        ADD UNIQUE KEY uk_node_config_node_version_name (node_id, version, name),
        COMMENT = ''named node configurations by version'''
);
-- #[toasty::breakpoint]
PREPARE rayflow_migration FROM @rayflow_migration_sql;
-- #[toasty::breakpoint]
EXECUTE rayflow_migration;
-- #[toasty::breakpoint]
DEALLOCATE PREPARE rayflow_migration;
