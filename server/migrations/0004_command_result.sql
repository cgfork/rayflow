CREATE TABLE IF NOT EXISTS command_result (
    id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment primary key',
    command_id     CHAR(36)        NOT NULL COMMENT 'failed command unique identifier',
    command_offset BIGINT UNSIGNED NOT NULL COMMENT 'failed command log offset',
    data_id        CHAR(36)        NOT NULL COMMENT 'data unique identifier',
    graph_id       CHAR(36)        NOT NULL COMMENT 'graph unique identifier',
    error          TEXT            NOT NULL COMMENT 'permanent command application error',
    created_at     DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT 'failure recording timestamp',
    PRIMARY KEY (id),
    UNIQUE KEY uk_command_result_command_id (command_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = 'permanently failed command results';
