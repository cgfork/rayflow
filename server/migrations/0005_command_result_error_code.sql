ALTER TABLE command_result
    ADD COLUMN error_code VARCHAR(64) NULL AFTER graph_id,
    ADD COLUMN error_message TEXT NULL AFTER error_code;
-- #[toasty::breakpoint]
UPDATE command_result
SET error_code = 'RAYFLOW_LEGACY_COMMAND_ERROR', error_message = error;
-- #[toasty::breakpoint]
ALTER TABLE command_result
    MODIFY COLUMN error_code VARCHAR(64) NOT NULL,
    MODIFY COLUMN error_message TEXT NOT NULL,
    DROP COLUMN error;
