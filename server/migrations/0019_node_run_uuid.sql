-- Keep numeric primary keys and task/job references intact while assigning public UUIDs.
ALTER TABLE node_run ADD COLUMN uuid CHAR(36) NULL;
UPDATE node_run SET uuid = UUID(), updated_at = updated_at WHERE uuid IS NULL;
ALTER TABLE node_run MODIFY COLUMN uuid CHAR(36) NOT NULL,
    ADD UNIQUE KEY uk_node_run_uuid (uuid);
