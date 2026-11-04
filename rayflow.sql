-- Rayflow runtime backend schema.

CREATE TABLE IF NOT EXISTS graph (
    id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment primary key',
    uuid       CHAR(36)        NOT NULL DEFAULT '' COMMENT 'graph identifier shared by its versions',
    name       VARCHAR(255)    NOT NULL DEFAULT '' COMMENT 'graph name',
    version    BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'graph version',
    graph      JSON            NOT NULL COMMENT 'serialized graph definition including nodes and edges',
    updated_at DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT 'last update timestamp',
    updated_by VARCHAR(64)     NOT NULL DEFAULT '' COMMENT 'last updater of the record',
    created_at DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT 'creation timestamp',
    created_by VARCHAR(64)     NOT NULL DEFAULT '' COMMENT 'creator of the record',
    PRIMARY KEY (id),
    UNIQUE KEY uk_graph_uuid_version (uuid, version),
    KEY idx_graph_uuid (uuid)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = 'versioned graph definitions with audit metadata';

CREATE TABLE IF NOT EXISTS graph_draft (
    id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment primary key',
    uuid       CHAR(36)        NOT NULL DEFAULT '' COMMENT 'graph identifier',
    name       VARCHAR(255)    NOT NULL DEFAULT '' COMMENT 'graph name',
    graph      JSON            NOT NULL COMMENT 'serialized editable graph definition including nodes and edges',
    updated_at DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT 'last update timestamp',
    updated_by VARCHAR(64)     NOT NULL DEFAULT '' COMMENT 'last updater of the record',
    created_at DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT 'creation timestamp',
    created_by VARCHAR(64)     NOT NULL DEFAULT '' COMMENT 'creator of the record',
    PRIMARY KEY (id),
    UNIQUE KEY uk_graph_draft_uuid (uuid)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = 'editable graph drafts with audit metadata';

CREATE TABLE IF NOT EXISTS task (
    id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment primary key',
    uuid       CHAR(36)        NOT NULL DEFAULT '' COMMENT 'task unique identifier',
    data_id    CHAR(36)        NOT NULL DEFAULT '' COMMENT 'data unique identifier',
    graph_id   CHAR(36)        NOT NULL DEFAULT '' COMMENT 'graph unique identifier',
    node_id    CHAR(36)        NOT NULL DEFAULT '' COMMENT 'node unique identifier',
    node_run_id BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'owning node run id',
    created_at DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT 'creation timestamp',
    PRIMARY KEY (id),
    UNIQUE KEY uk_task_uuid (uuid),
    KEY idx_task_node_run_id (node_run_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = 'task record';

CREATE TABLE IF NOT EXISTS run (
    id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment primary key',
    uuid       CHAR(36)        NOT NULL DEFAULT '' COMMENT 'run unique identifier',
    data_id    CHAR(36)        NOT NULL DEFAULT '' COMMENT 'data unique identifier',
    graph_id   CHAR(36)        NOT NULL DEFAULT '' COMMENT 'graph unique identifier',
    version    BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'graph version under execution',
    revision   BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'run revision, incremented on rerun/dryrun',
    created_at DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT 'creation timestamp',
    updated_at DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT 'last update timestamp',
    PRIMARY KEY (id),
    UNIQUE KEY uk_run_uuid (uuid),
    UNIQUE KEY uk_run_data_graph (data_id, graph_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = 'dag execution run, one row per (data_id, graph_id)';

CREATE TABLE IF NOT EXISTS run_config (
    id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment primary key',
    run_id     CHAR(36)        NOT NULL DEFAULT '' COMMENT 'owning run uuid',
    version    BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'graph version',
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
    lease            BIGINT UNSIGNED NULL COMMENT 'required job lifetime in milliseconds; NULL returns a missing lease error',
    required_cpu_cores INT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'job CPU requirement in cores',
    required_memory_bytes BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'job memory requirement in bytes',
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
    run_id     CHAR(36)        NOT NULL DEFAULT '' COMMENT 'owning run uuid',
    data_id    CHAR(36)        NOT NULL DEFAULT '' COMMENT 'data unique identifier',
    graph_id   CHAR(36)        NOT NULL DEFAULT '' COMMENT 'graph unique identifier',
    version    BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'graph version used by the owning run',
    node_id    CHAR(36)        NOT NULL DEFAULT '' COMMENT 'node unique identifier',
    revision   BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'node revision, incremented on rerun/dryrun',
    status     VARCHAR(16)     NOT NULL DEFAULT 'Ready' COMMENT 'node status',
    ineffective INT UNSIGNED  NOT NULL DEFAULT 0 COMMENT 'invalidated by an upstream rerun or upgrade',
    created_at DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT 'creation timestamp',
    updated_at DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT 'last update timestamp',
    PRIMARY KEY (id),
    UNIQUE KEY uk_node_run_run_node_revision (run_id, node_id, revision),
    KEY idx_node_run_data_graph (data_id, graph_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = 'node status within a dag execution run, one row per revision';

CREATE TABLE IF NOT EXISTS agent_instance (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    uuid CHAR(36) NOT NULL,
    cpu_cores INT UNSIGNED NOT NULL,
    memory_bytes BIGINT UNSIGNED NOT NULL,
    active_jobs INT UNSIGNED NOT NULL,
    first_seen_at DATETIME(6) NOT NULL,
    last_seen_at DATETIME(6) NOT NULL,
    online_until DATETIME(6) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_agent_instance_uuid (uuid),
    KEY idx_agent_instance_online_until (online_until)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = 'agent observations for the web console; not used for scheduling';

CREATE TABLE IF NOT EXISTS job (
    id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment primary key',
    uuid           CHAR(36)        NOT NULL DEFAULT '' COMMENT 'job unique identifier',
    task_id        CHAR(36)        NOT NULL DEFAULT '' COMMENT 'task unique identifier',
    run_id         CHAR(36)        NOT NULL DEFAULT '' COMMENT 'owning run uuid',
    node_run_id    BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'owning node run id',
    node_config_id BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'referenced node config id',
    run_config_id  BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'referenced run config id',
    status         VARCHAR(16)     NOT NULL DEFAULT 'Scheduled' COMMENT 'Scheduled, Polled, Yielded, Succeeded, Failed, or Terminated',
    required_cpu_cores INT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'CPU requirement in cores',
    required_memory_bytes BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'memory requirement in bytes',
    agent_id       CHAR(36) NULL COMMENT 'agent assigned to execute the job',
    polled_at      DATETIME(6) NULL COMMENT 'most recent delivery to the agent',
    expired_at            DATETIME(6) NULL COMMENT 'job runtime expiry; reset on delivery from the pinned node-config lifetime',
    created_at     DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT 'creation timestamp',
    updated_at     DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT 'last update timestamp',
    PRIMARY KEY (id),
    UNIQUE KEY uk_job_uuid (uuid),
    KEY idx_job_status (status),
    KEY idx_job_agent_id (agent_id),
    KEY idx_job_polled_at (polled_at),
    KEY idx_job_expired_at (expired_at),
    KEY idx_job_task_id (task_id),
    KEY idx_job_run_id (run_id),
    KEY idx_job_node_run_id (node_run_id),
    KEY idx_job_node_config_id (node_config_id),
    KEY idx_job_run_config_id (run_config_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = 'job execution instance referencing its logical task, run, node run and configurations';

CREATE TABLE IF NOT EXISTS command_entry (
    id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment primary key, used as the command offset',
    uuid       CHAR(36)        NOT NULL DEFAULT '' COMMENT 'command unique identifier',
    data_id    CHAR(36)        NOT NULL DEFAULT '' COMMENT 'data unique identifier',
    graph_id   CHAR(36)        NOT NULL DEFAULT '' COMMENT 'graph unique identifier',
    command    JSON            NOT NULL COMMENT 'serialized command payload',
    created_at DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT 'creation timestamp',
    PRIMARY KEY (id),
    UNIQUE KEY uk_command_entry_uuid (uuid)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = 'append-only command event log';

CREATE TABLE IF NOT EXISTS command_result (
    id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment primary key',
    command_id     CHAR(36)        NOT NULL COMMENT 'failed command unique identifier',
    command_offset BIGINT UNSIGNED NOT NULL COMMENT 'failed command log offset',
    data_id        CHAR(36)        NOT NULL COMMENT 'data unique identifier',
    graph_id       CHAR(36)        NOT NULL COMMENT 'graph unique identifier',
    error_code     VARCHAR(64)     NOT NULL COMMENT 'stable Rayflow error code',
    error_message  TEXT            NOT NULL COMMENT 'permanent command application error message',
    created_at     DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT 'failure recording timestamp',
    PRIMARY KEY (id),
    UNIQUE KEY uk_command_result_command_id (command_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = 'permanently failed command results';

CREATE TABLE IF NOT EXISTS task_event (
    id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment primary key, used as the event offset',
    uuid        CHAR(36)        NOT NULL DEFAULT '' COMMENT 'event unique identifier',
    task_id     CHAR(36)        NOT NULL DEFAULT '' COMMENT 'task unique identifier',
    data_id     CHAR(36)        NOT NULL DEFAULT '' COMMENT 'data unique identifier',
    graph_id    CHAR(36)        NOT NULL DEFAULT '' COMMENT 'graph unique identifier',
    node_id     CHAR(36)        NOT NULL DEFAULT '' COMMENT 'node unique identifier',
    node_run_id BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'owning node run id',
    event       VARCHAR(16)     NOT NULL DEFAULT '' COMMENT 'task event name',
    created_at  DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT 'creation timestamp',
    PRIMARY KEY (id),
    UNIQUE KEY uk_task_event_uuid (uuid),
    KEY idx_task_event_task_id (task_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = 'append-only task event log';

CREATE TABLE IF NOT EXISTS poll_offset (
    name       VARCHAR(64)     NOT NULL COMMENT 'log name: command_entry, task, or task_event',
    `offset`   BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'last acknowledged row offset',
    gap_since  DATETIME(6)     NULL DEFAULT NULL COMMENT 'when the current missing-id gap was first observed',
    updated_at DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT 'last update timestamp',
    PRIMARY KEY (name)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = 'global append-only log offset and gap state';

CREATE TABLE IF NOT EXISTS leader_election (
    name       VARCHAR(128)    NOT NULL COMMENT 'coordinator role name',
    leader_id  VARCHAR(128)    NOT NULL DEFAULT '' COMMENT 'current lease holder, empty when unheld',
    expires_at DATETIME(6)     NOT NULL COMMENT 'lease expiry timestamp',
    updated_at DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT 'last acquisition or renewal timestamp',
    PRIMARY KEY (name)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = 'named coordinator leader leases';
