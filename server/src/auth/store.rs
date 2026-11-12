use jiff::Timestamp;
use serde_json::Value;

/// Durable user directory; stable IDs also fit existing 64-character audit fields.
#[derive(Clone, toasty::Model)]
#[table = "auth_user"]
pub struct UserRecord {
    #[key]
    #[column(type = varchar(64))]
    pub id: String,
    #[column(type = json)]
    pub profile: Value,
    pub updated_at: Timestamp,
}

/// Only a SHA-256 digest of the random browser credential is persisted.
#[derive(Clone, toasty::Model)]
#[table = "auth_session"]
pub struct Session {
    #[key]
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
pub struct LoginFlow {
    #[key]
    #[column(type = varchar(64))]
    pub state_hash: String,
    #[column(type = varchar(64))]
    pub browser_hash: String,
    pub nonce: String,
    pub verifier: String,
    #[index]
    pub expires_at: Timestamp,
}
