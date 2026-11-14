use jiff::Timestamp;
use serde_json::Value;

/// Durable user directory; user_id is the stable identity used by sessions and audit fields.
#[derive(Clone, toasty::Model)]
#[table = "auth_user"]
#[unique(user_id)]
pub struct UserRecord {
    #[key]
    #[auto]
    pub id: u64,
    #[column(type = varchar(64))]
    pub user_id: String,
    #[column(type = json)]
    pub profile: Value,
    pub updated_at: Timestamp,
}

/// Only a SHA-256 digest of the random browser credential is persisted.
#[derive(Clone, toasty::Model)]
#[table = "auth_session"]
#[unique(token_hash)]
pub struct Session {
    #[key]
    #[auto]
    pub id: u64,
    #[column(type = varchar(64))]
    pub token_hash: String,
    #[column(type = varchar(64))]
    pub user_id: String,
    #[index]
    pub expires_at: Timestamp,
}

/// Short-lived OIDC state, bound to a separate random browser cookie.
#[derive(Clone, toasty::Model)]
#[table = "auth_login_flow"]
#[unique(state_hash)]
pub struct LoginFlow {
    #[key]
    #[auto]
    pub id: u64,
    #[column(type = varchar(64))]
    pub state_hash: String,
    #[column(type = varchar(64))]
    pub browser_hash: String,
    #[column(type = text)]
    pub nonce: String,
    #[column(type = text)]
    pub verifier: String,
    #[index]
    pub expires_at: Timestamp,
}
