-- Rayflow runtime backend schema.

CREATE TABLE IF NOT EXISTS task (
    id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment primary key',
    uuid       CHAR(36)        NOT NULL DEFAULT '' COMMENT 'task unique identifier',
    data_id    CHAR(36)        NOT NULL DEFAULT '' COMMENT 'data unique identifier',
    graph_id   CHAR(36)        NOT NULL DEFAULT '' COMMENT 'graph unique identifier',
    node_id    CHAR(36)        NOT NULL DEFAULT '' COMMENT 'node unique identifier',
    node_run_id BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'owning node run id',
    shard      INT UNSIGNED    NOT NULL DEFAULT 0 COMMENT 'shard index derived from data_id',
    created_at DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT 'creation timestamp',
    PRIMARY KEY (id),
    UNIQUE KEY uk_task_uuid (uuid),
    KEY idx_task_node_run_id (node_run_id),
    KEY idx_task_shard (shard)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = 'task record';

CREATE TABLE IF NOT EXISTS task_poll_offset (
    id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment primary key',
    shard      INT UNSIGNED    NOT NULL DEFAULT 0 COMMENT 'shard index',
    `offset`   BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'last acknowledged task offset',
    updated_at DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT 'last update timestamp',
    PRIMARY KEY (id),
    UNIQUE KEY uk_task_poll_offset_shard (shard)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = 'per-shard task offset';

CREATE TABLE IF NOT EXISTS run (
    id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment primary key',
    uuid       CHAR(36)        NOT NULL DEFAULT '' COMMENT 'run unique identifier',
    data_id    CHAR(36)        NOT NULL DEFAULT '' COMMENT 'data unique identifier',
    graph_id   CHAR(36)        NOT NULL DEFAULT '' COMMENT 'graph unique identifier',
    shard      INT UNSIGNED    NOT NULL DEFAULT 0 COMMENT 'shard index derived from data_id',
    version    BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'graph version under execution',
    revision   BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'run revision, incremented on rerun/dryrun',
    created_at DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT 'creation timestamp',
    updated_at DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT 'last update timestamp',
    PRIMARY KEY (id),
    UNIQUE KEY uk_run_uuid (uuid),
    UNIQUE KEY uk_run_data_graph (data_id, graph_id),
    KEY idx_run_shard (shard)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = 'dag execution run, one row per (data_id, graph_id)';

CREATE TABLE IF NOT EXISTS run_config (
    id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment primary key',
    run_id     CHAR(36)        NOT NULL DEFAULT '' COMMENT 'owning run uuid',
    revision   BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'run revision this config applies to',
    annotation JSON            NOT NULL COMMENT 'run annotation key/value map',
    created_at DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT 'creation timestamp',
    updated_at DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT 'last update timestamp',
    created_by VARCHAR(64)     NOT NULL DEFAULT '' COMMENT 'creator of the record',
    updated_by VARCHAR(64)     NOT NULL DEFAULT '' COMMENT 'last updater of the record',
    PRIMARY KEY (id),
    UNIQUE KEY uk_run_config_run_revision (run_id, revision),
    KEY idx_run_config_run_id (run_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = 'run annotation configuration, one row per run revision';

CREATE TABLE IF NOT EXISTS node_config (
    id               BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment primary key',
    node_id          CHAR(36)        NOT NULL DEFAULT '' COMMENT 'owning node identifier',
    version          BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'config version',
    annotation_match VARCHAR(255)    NOT NULL DEFAULT '' COMMENT 'run annotation match expression',
    config           JSON            NOT NULL COMMENT 'node configuration payload',
    created_at       DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT 'creation timestamp',
    updated_at       DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT 'last update timestamp',
    created_by       VARCHAR(64)     NOT NULL DEFAULT '' COMMENT 'creator of the record',
    updated_by       VARCHAR(64)     NOT NULL DEFAULT '' COMMENT 'last updater of the record',
    PRIMARY KEY (id),
    UNIQUE KEY uk_node_config_node_version (node_id, version),
    KEY idx_node_config_node_id (node_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = 'node configuration, one row per node version';

CREATE TABLE IF NOT EXISTS node_run (
    id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment primary key',
    run_id     BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'owning run id',
    data_id    CHAR(36)        NOT NULL DEFAULT '' COMMENT 'data unique identifier',
    graph_id   CHAR(36)        NOT NULL DEFAULT '' COMMENT 'graph unique identifier',
    node_id    CHAR(36)        NOT NULL DEFAULT '' COMMENT 'node unique identifier',
    shard      INT UNSIGNED    NOT NULL DEFAULT 0 COMMENT 'shard index derived from data_id',
    revision   BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'node revision, incremented on rerun/dryrun',
    status     VARCHAR(16)     NOT NULL DEFAULT 'Ready' COMMENT 'node status',
    created_at DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT 'creation timestamp',
    updated_at DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT 'last update timestamp',
    PRIMARY KEY (id),
    UNIQUE KEY uk_node_run_run_node_revision (run_id, node_id, revision),
    KEY idx_node_run_data_graph (data_id, graph_id),
    KEY idx_node_run_shard (shard)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = 'node status within a dag execution run, one row per revision';

CREATE TABLE IF NOT EXISTS command_entry (
    id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment primary key, used as the command offset',
    uuid       CHAR(36)        NOT NULL DEFAULT '' COMMENT 'command unique identifier',
    data_id    CHAR(36)        NOT NULL DEFAULT '' COMMENT 'data unique identifier',
    graph_id   CHAR(36)        NOT NULL DEFAULT '' COMMENT 'graph unique identifier',
    shard      INT UNSIGNED    NOT NULL DEFAULT 0 COMMENT 'shard index derived from data_id',
    command    JSON            NOT NULL COMMENT 'serialized command payload',
    created_at DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT 'creation timestamp',
    PRIMARY KEY (id),
    UNIQUE KEY uk_command_entry_uuid (uuid),
    KEY idx_command_entry_shard (shard)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = 'append-only command event log';

CREATE TABLE IF NOT EXISTS command_poll_offset (
    id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment primary key',
    shard      INT UNSIGNED    NOT NULL DEFAULT 0 COMMENT 'shard index',
    `offset`   BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'last acknowledged command offset',
    updated_at DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT 'last update timestamp',
    PRIMARY KEY (id),
    UNIQUE KEY uk_command_poll_offset_shard (shard)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = 'per-shard command offset';

CREATE TABLE IF NOT EXISTS task_event (
    id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment primary key, used as the event offset',
    uuid        CHAR(36)        NOT NULL DEFAULT '' COMMENT 'event unique identifier',
    task_id     CHAR(36)        NOT NULL DEFAULT '' COMMENT 'task unique identifier',
    data_id     CHAR(36)        NOT NULL DEFAULT '' COMMENT 'data unique identifier',
    graph_id    CHAR(36)        NOT NULL DEFAULT '' COMMENT 'graph unique identifier',
    node_id     CHAR(36)        NOT NULL DEFAULT '' COMMENT 'node unique identifier',
    node_run_id BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'owning node run id',
    shard       INT UNSIGNED    NOT NULL DEFAULT 0 COMMENT 'shard index derived from data_id',
    event       VARCHAR(16)     NOT NULL DEFAULT '' COMMENT 'task event name',
    created_at  DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT 'creation timestamp',
    PRIMARY KEY (id),
    UNIQUE KEY uk_task_event_uuid (uuid),
    KEY idx_task_event_task_id (task_id),
    KEY idx_task_event_shard (shard)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = 'append-only task event log';

CREATE TABLE IF NOT EXISTS task_event_poll_offset (
    id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment primary key',
    shard      INT UNSIGNED    NOT NULL DEFAULT 0 COMMENT 'shard index',
    `offset`   BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'last acknowledged task event offset',
    updated_at DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT 'last update timestamp',
    PRIMARY KEY (id),
    UNIQUE KEY uk_task_event_poll_offset_shard (shard)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = 'per-shard task event offset';
