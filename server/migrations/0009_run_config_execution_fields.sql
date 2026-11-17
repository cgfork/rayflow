-- Execution version and revision live on run. Keep annotation rows and their
-- IDs so existing job.run_config_id references continue to resolve.
ALTER TABLE run_config
    DROP INDEX uk_run_config_run_revision,
    DROP COLUMN version,
    DROP COLUMN revision,
    COMMENT = 'run annotation snapshots referenced by jobs';
