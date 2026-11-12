CREATE TABLE IF NOT EXISTS auth_user (
    id VARCHAR(64) NOT NULL PRIMARY KEY,
    profile JSON NOT NULL,
    updated_at TIMESTAMP(6) NOT NULL
);

CREATE TABLE IF NOT EXISTS auth_session (
    token_hash VARCHAR(64) NOT NULL PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    expires_at TIMESTAMP(6) NOT NULL,
    INDEX auth_session_expires_at (expires_at)
);

CREATE TABLE IF NOT EXISTS auth_login_flow (
    state_hash VARCHAR(64) NOT NULL PRIMARY KEY,
    browser_hash VARCHAR(64) NOT NULL,
    nonce TEXT NOT NULL,
    verifier TEXT NOT NULL,
    expires_at TIMESTAMP(6) NOT NULL,
    INDEX auth_login_flow_expires_at (expires_at)
);
