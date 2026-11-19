-- Add stable UUID identities to configuration rows and migrate job references.
ALTER TABLE run_config ADD COLUMN uuid CHAR(36) NULL;
UPDATE run_config SET uuid = UUID() WHERE uuid IS NULL;
ALTER TABLE run_config MODIFY COLUMN uuid CHAR(36) NOT NULL,
    ADD UNIQUE KEY uk_run_config_uuid (uuid);

ALTER TABLE node_config ADD COLUMN uuid CHAR(36) NULL;
UPDATE node_config SET uuid = UUID() WHERE uuid IS NULL;
ALTER TABLE node_config MODIFY COLUMN uuid CHAR(36) NOT NULL,
    ADD UNIQUE KEY uk_node_config_uuid (uuid);

ALTER TABLE job
    ADD COLUMN node_run_uuid CHAR(36) NULL,
    ADD COLUMN node_config_uuid CHAR(36) NULL,
    ADD COLUMN run_config_uuid CHAR(36) NULL;

UPDATE job j
JOIN node_run nr ON nr.id = j.node_run_id
SET j.node_run_uuid = nr.uuid;
UPDATE job j
JOIN node_config nc ON nc.id = j.node_config_id
SET j.node_config_uuid = nc.uuid;
UPDATE job j
JOIN run_config rc ON rc.id = j.run_config_id
SET j.run_config_uuid = rc.uuid;

ALTER TABLE job
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
    ADD KEY idx_job_run_config_id (run_config_id);
