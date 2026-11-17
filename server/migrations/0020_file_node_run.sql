-- Stage the ownership column separately so a failed legacy backfill can be retried.
ALTER TABLE file ADD COLUMN node_run_id CHAR(36) NULL;
