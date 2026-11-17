ALTER TABLE auth_login_flow
    ENGINE = InnoDB,
    CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
    ADD COLUMN id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment primary key' FIRST,
    DROP PRIMARY KEY,
    ADD PRIMARY KEY (id),
    ADD UNIQUE KEY uk_auth_login_flow_state_hash (state_hash),
    MODIFY COLUMN state_hash VARCHAR(64) NOT NULL COMMENT 'SHA-256 digest of the single-use OIDC state',
    MODIFY COLUMN browser_hash VARCHAR(64) NOT NULL COMMENT 'SHA-256 digest of the browser binding token',
    MODIFY COLUMN nonce TEXT NOT NULL COMMENT 'OIDC nonce for ID token validation',
    MODIFY COLUMN verifier TEXT NOT NULL COMMENT 'PKCE verifier for authorization code exchange',
    MODIFY COLUMN expires_at DATETIME(6) NOT NULL COMMENT 'login flow expiry timestamp',
    COMMENT = 'single-use browser-bound OIDC login flows';
