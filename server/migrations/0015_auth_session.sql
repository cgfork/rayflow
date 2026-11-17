ALTER TABLE auth_session
    ENGINE = InnoDB,
    CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
    ADD COLUMN id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment primary key' FIRST,
    DROP PRIMARY KEY,
    ADD PRIMARY KEY (id),
    ADD UNIQUE KEY uk_auth_session_token_hash (token_hash),
    MODIFY COLUMN token_hash VARCHAR(64) NOT NULL COMMENT 'SHA-256 digest of the browser session token',
    MODIFY COLUMN user_id VARCHAR(64) NOT NULL COMMENT 'authenticated user identity',
    MODIFY COLUMN expires_at DATETIME(6) NOT NULL COMMENT 'session expiry timestamp',
    COMMENT = 'browser authentication sessions';
