-- Preserve the stable user identity under user_id; sessions and audit records
-- continue to reference it. The new numeric ID is assigned by the database.
ALTER TABLE auth_user
    ENGINE = InnoDB,
    CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
    CHANGE COLUMN id user_id VARCHAR(64) NOT NULL COMMENT 'stable user identity derived from provider and subject',
    ADD COLUMN id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment primary key' FIRST,
    DROP PRIMARY KEY,
    ADD PRIMARY KEY (id),
    ADD UNIQUE KEY uk_auth_user_user_id (user_id),
    MODIFY COLUMN profile JSON NOT NULL COMMENT 'provider identity and user display profile',
    MODIFY COLUMN updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT 'last profile update timestamp',
    COMMENT = 'authenticated user directory';
