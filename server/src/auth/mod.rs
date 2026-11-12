//! Configurable company login with local, LDAP, and OpenID Connect adapters.
mod config;
mod oidc;
mod password;
mod store;

pub use config::{AuthConfig, LdapConfig, LocalUser, OidcConfig};
pub use password::PasswordProvider;
pub use store::{LoginFlow, Session, UserRecord};

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use axum::{
    Json, Router,
    extract::{Query, Request, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Redirect, Response},
    routing::{get, post},
};
use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use toasty::{Db, Executor, schema::Model};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct User {
    pub id: String,
    pub provider: String,
    pub subject: String,
    pub name: String,
    pub email: Option<String>,
}

impl User {
    pub fn new(provider: &str, subject: &str, name: String, email: Option<String>) -> Self {
        Self {
            id: digest(&serde_json::to_string(&(provider, subject)).unwrap()),
            provider: provider.into(),
            subject: subject.into(),
            name,
            email,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("Sign in to continue")]
    Unauthorized,
    #[error("Invalid login credentials or expired sign-in request")]
    InvalidLogin,
    #[error("Request is not allowed")]
    Forbidden,
    #[error("Authentication is temporarily unavailable")]
    Unavailable,
    #[error("Too many sign-in attempts; try again in a minute")]
    RateLimited,
    #[error("invalid authentication configuration: {0}")]
    Configuration(String),
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let status = match self {
            Self::Unauthorized | Self::InvalidLogin => StatusCode::UNAUTHORIZED,
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::Unavailable | Self::Configuration(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::RateLimited => StatusCode::TOO_MANY_REQUESTS,
        };
        let mut response = (
            status,
            Json(serde_json::json!({ "error": self.to_string() })),
        )
            .into_response();
        response
            .headers_mut()
            .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        if status == StatusCode::TOO_MANY_REQUESTS {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("60"));
        }
        response
    }
}

impl From<toasty::Error> for AuthError {
    fn from(error: toasty::Error) -> Self {
        tracing::error!(%error, "authentication storage failed");
        Self::Unavailable
    }
}

#[derive(Clone)]
pub struct Auth(Arc<Inner>);

struct Inner {
    config: AuthConfig,
    db: Db,
    passwords: Vec<Arc<dyn PasswordProvider>>,
    oidc: Option<oidc::OidcProvider>,
    attempts: Mutex<HashMap<String, (Instant, u32)>>,
    password_slots: tokio::sync::Semaphore,
}

impl Auth {
    pub(crate) fn config(&self) -> &AuthConfig {
        &self.0.config
    }

    pub async fn new(config: AuthConfig, db: Db) -> Result<Self, AuthError> {
        Self::with_providers(config, db, Vec::new()).await
    }

    /// Add company-specific password adapters alongside the configured built-ins.
    pub async fn with_providers(
        mut config: AuthConfig,
        db: Db,
        mut providers: Vec<Arc<dyn PasswordProvider>>,
    ) -> Result<Self, AuthError> {
        config
            .validate_with_providers(!providers.is_empty())
            .map_err(AuthError::Configuration)?;
        if config.enabled {
            config.public_url = config::web_url(&config.public_url)
                .map_err(AuthError::Configuration)?
                .origin()
                .ascii_serialization();
        }
        let mut oidc = None;
        if config.enabled {
            if !config.local_users.is_empty() {
                providers.push(Arc::new(
                    password::LocalProvider::new(config.local_users.clone()).await?,
                ));
            }
            if let Some(ldap) = &config.ldap {
                providers.push(Arc::new(password::LdapProvider(ldap.clone())));
            }
            if config.oidc.is_some() {
                oidc = Some(oidc::OidcProvider::new(&config).await?);
            }
        }
        let mut ids = std::collections::HashSet::new();
        if providers.iter().any(|provider| {
            provider.id().is_empty() || provider.id() == "oidc" || !ids.insert(provider.id())
        }) {
            return Err(AuthError::Configuration(
                "password provider IDs must be unique and cannot be oidc".into(),
            ));
        }
        Ok(Self(Arc::new(Inner {
            config,
            db,
            passwords: providers,
            oidc,
            attempts: Mutex::new(HashMap::new()),
            password_slots: tokio::sync::Semaphore::new(8),
        })))
    }

    fn secure(&self) -> bool {
        self.0.config.public_url.starts_with("https://")
    }
    fn session_cookie(&self) -> &'static str {
        if self.secure() {
            "__Host-rayflow_session"
        } else {
            "rayflow_session"
        }
    }
    fn flow_cookie(&self) -> &'static str {
        if self.secure() {
            "__Host-rayflow_login"
        } else {
            "rayflow_login"
        }
    }
    fn cookie(&self, name: &str, value: &str, age: u64) -> HeaderValue {
        HeaderValue::from_str(&format!(
            "{name}={value}; Path=/; HttpOnly; SameSite=Lax; Max-Age={age}{}",
            if self.secure() { "; Secure" } else { "" }
        ))
        .expect("generated cookie is ASCII")
    }

    fn csrf(&self, headers: &HeaderMap) -> Result<(), AuthError> {
        // A custom header requires CORS preflight. Rayflow does not enable cross-origin CORS.
        if headers
            .get("x-rayflow-request")
            .and_then(|v| v.to_str().ok())
            != Some("1")
        {
            return Err(AuthError::Forbidden);
        }
        if headers.get(header::ORIGIN).is_some_and(|origin| {
            origin.to_str().ok() != Some(self.0.config.public_url.trim_end_matches('/'))
        }) {
            return Err(AuthError::Forbidden);
        }
        Ok(())
    }

    fn limit(&self, key: &str) -> Result<(), AuthError> {
        let mut attempts = self.0.attempts.lock().map_err(|_| AuthError::Unavailable)?;
        attempts.retain(|_, (at, _)| at.elapsed() < Duration::from_secs(60));
        if attempts.len() >= 4096 && !attempts.contains_key(key) {
            return Err(AuthError::RateLimited);
        }
        let entry = attempts
            .entry(key.to_owned())
            .or_insert((Instant::now(), 0));
        if entry.1 >= 10 {
            return Err(AuthError::RateLimited);
        }
        entry.1 += 1;
        Ok(())
    }

    async fn user(&self, headers: &HeaderMap) -> Result<User, AuthError> {
        let token = cookie_value(headers, self.session_cookie()).ok_or(AuthError::Unauthorized)?;
        let mut db = self.0.db.clone();
        let session = Session::filter_by_token_hash(digest(token))
            .first()
            .exec(&mut db)
            .await?
            .filter(|session| session.expires_at > Timestamp::now())
            .ok_or(AuthError::Unauthorized)?;
        let user = UserRecord::filter_by_id(session.user_id)
            .first()
            .exec(&mut db)
            .await?
            .ok_or(AuthError::Unauthorized)?;
        serde_json::from_value(user.profile).map_err(|_| AuthError::Unavailable)
    }

    async fn revoke(&self, headers: &HeaderMap) -> Result<(), AuthError> {
        if let Some(token) = cookie_value(headers, self.session_cookie()) {
            Session::filter_by_token_hash(digest(token))
                .delete()
                .exec(&mut self.0.db.clone())
                .await?;
        }
        Ok(())
    }

    async fn establish(&self, headers: &HeaderMap, user: User) -> Result<HeaderValue, AuthError> {
        let mut db = self.0.db.clone();
        let now = Timestamp::now();
        let profile = serde_json::to_value(&user).map_err(|_| AuthError::Unavailable)?;
        // Toasty has no atomic upsert; concurrent first logins must create one user identity.
        let table = db
            .schema()
            .db
            .table(db.schema().mapping.model(UserRecord::id()).table)
            .name
            .replace('`', "``");
        toasty::sql::statement(format!("INSERT INTO `{table}` (id, profile, updated_at) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE profile = VALUES(profile), updated_at = VALUES(updated_at)"))
            .bind(user.id.clone()).bind(profile.to_string()).bind(now).exec(&mut db).await?;
        Session::filter(Session::fields().expires_at().le(now))
            .delete()
            .exec(&mut db)
            .await?;
        LoginFlow::filter(LoginFlow::fields().expires_at().le(now))
            .delete()
            .exec(&mut db)
            .await?;
        self.revoke(headers).await?;
        let token = random_token();
        Session::create()
            .token_hash(digest(&token))
            .user_id(user.id)
            .expires_at(now + self.0.config.session_ttl)
            .exec(&mut db)
            .await?;
        Ok(self.cookie(
            self.session_cookie(),
            &token,
            self.0.config.session_ttl.as_secs(),
        ))
    }

    async fn consume_flow(&self, state: &str, browser: &str) -> Result<LoginFlow, AuthError> {
        let mut db = self.0.db.clone();
        let mut tx = db.transaction().await?;
        let state_hash = digest(state);
        // Row locking is not exposed by the ORM. Lock before reading to make callbacks single-use.
        let table_id = tx.schema().mapping.model(LoginFlow::id()).table;
        let table = tx.schema().db.table(table_id).name.replace('`', "``");
        toasty::sql::query(format!(
            "SELECT state_hash FROM `{table}` WHERE state_hash = ? FOR UPDATE"
        ))
        .bind(state_hash.clone())
        .exec(&mut tx)
        .await?;
        let flow = LoginFlow::filter_by_state_hash(state_hash.clone())
            .first()
            .exec(&mut tx)
            .await?
            .filter(|flow| {
                flow.expires_at > Timestamp::now()
                    && same_secret(&flow.browser_hash, &digest(browser))
            })
            .ok_or(AuthError::InvalidLogin)?;
        LoginFlow::filter_by_state_hash(state_hash)
            .delete()
            .exec(&mut tx)
            .await?;
        tx.commit().await?;
        Ok(flow)
    }
}

pub(crate) fn routes(auth: Option<Auth>, enabled: bool) -> Router {
    Router::new()
        .route("/auth/providers", get(providers))
        .route("/auth/me", get(me))
        .route("/auth/login", post(login))
        .route("/auth/logout", post(logout))
        .route("/auth/oidc/login", get(oidc_login))
        .route("/auth/oidc/callback", get(oidc_callback))
        .layer(axum::extract::DefaultBodyLimit::max(16 * 1024))
        .layer(axum::middleware::from_fn(no_cache))
        .with_state(AuthState { auth, enabled })
}

#[derive(Clone)]
pub(crate) struct AuthState {
    pub auth: Option<Auth>,
    pub enabled: bool,
}

impl AuthState {
    fn required(&self) -> Result<&Auth, AuthError> {
        self.auth
            .as_ref()
            .filter(|auth| self.enabled && auth.0.config.enabled)
            .ok_or(AuthError::Unavailable)
    }
}

async fn no_cache(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response.headers_mut().insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    response
}

async fn providers(State(state): State<AuthState>) -> Result<Json<serde_json::Value>, AuthError> {
    if !state.enabled {
        return Ok(Json(
            serde_json::json!({ "enabled": false, "company_name": "Rayflow", "providers": [] }),
        ));
    }
    let auth = state.required()?;
    let mut providers: Vec<_> = auth
        .0
        .passwords
        .iter()
        .map(|p| serde_json::json!({ "id": p.id(), "label": p.label(), "kind": "password" }))
        .collect();
    if auth.0.oidc.is_some() {
        providers
            .push(serde_json::json!({ "id": "oidc", "label": "Company SSO", "kind": "redirect" }));
    }
    Ok(Json(
        serde_json::json!({ "enabled": true, "company_name": auth.0.config.company_name, "providers": providers }),
    ))
}

async fn me(State(state): State<AuthState>, headers: HeaderMap) -> Result<Json<User>, AuthError> {
    Ok(Json(state.required()?.user(&headers).await?))
}

#[derive(Deserialize)]
struct Credentials {
    provider: String,
    username: String,
    password: String,
}

async fn login(
    State(state): State<AuthState>,
    headers: HeaderMap,
    Json(credentials): Json<Credentials>,
) -> Result<Response, AuthError> {
    let auth = state.required()?;
    auth.csrf(&headers)?;
    if credentials.username.is_empty()
        || credentials.username.len() > 256
        || credentials.password.is_empty()
        || credentials.password.len() > 4096
    {
        return Err(AuthError::InvalidLogin);
    }
    auth.limit(&digest(&format!(
        "{}:{}",
        credentials.provider, credentials.username
    )))?;
    let _slot = auth
        .0
        .password_slots
        .try_acquire()
        .map_err(|_| AuthError::RateLimited)?;
    let provider = auth
        .0
        .passwords
        .iter()
        .find(|p| p.id() == credentials.provider)
        .ok_or(AuthError::InvalidLogin)?;
    let user = provider
        .authenticate(&credentials.username, &credentials.password)
        .await?
        .ok_or(AuthError::InvalidLogin)?;
    let cookie = auth.establish(&headers, user.clone()).await?;
    Ok(([(header::SET_COOKIE, cookie)], Json(user)).into_response())
}

async fn logout(State(state): State<AuthState>, headers: HeaderMap) -> Result<Response, AuthError> {
    let auth = state.required()?;
    auth.csrf(&headers)?;
    auth.revoke(&headers).await?;
    Ok((
        StatusCode::NO_CONTENT,
        [(
            header::SET_COOKIE,
            auth.cookie(auth.session_cookie(), "", 0),
        )],
    )
        .into_response())
}

async fn oidc_login(State(state): State<AuthState>) -> Result<Response, AuthError> {
    let auth = state.required()?;
    auth.limit("oidc-start")?;
    let (url, state, nonce, verifier) = auth.0.oidc.as_ref().ok_or(AuthError::Unavailable)?.start();
    let browser = random_token();
    let mut db = auth.0.db.clone();
    LoginFlow::filter(LoginFlow::fields().expires_at().le(Timestamp::now()))
        .delete()
        .exec(&mut db)
        .await?;
    LoginFlow::create()
        .state_hash(digest(&state))
        .browser_hash(digest(&browser))
        .nonce(nonce)
        .verifier(verifier)
        .expires_at(Timestamp::now() + Duration::from_secs(300))
        .exec(&mut db)
        .await?;
    Ok((
        [(
            header::SET_COOKIE,
            auth.cookie(auth.flow_cookie(), &browser, 300),
        )],
        Redirect::to(&url),
    )
        .into_response())
}

#[derive(Deserialize)]
struct Callback {
    state: String,
    code: Option<String>,
    error: Option<String>,
}

async fn oidc_callback(
    State(state): State<AuthState>,
    headers: HeaderMap,
    Query(callback): Query<Callback>,
) -> Result<Response, AuthError> {
    let auth = state.required()?;
    let result: Result<HeaderValue, AuthError> = async {
        let browser = cookie_value(&headers, auth.flow_cookie()).ok_or(AuthError::InvalidLogin)?;
        let flow = auth.consume_flow(&callback.state, browser).await?;
        if callback.error.is_some() {
            return Err(AuthError::InvalidLogin);
        }
        // Refresh discovery/JWKS for key rotation. The pinned issuer still verifies the token.
        let oidc = oidc::OidcProvider::new(&auth.0.config).await?;
        let user = oidc
            .finish(callback.code.ok_or(AuthError::InvalidLogin)?, flow)
            .await?;
        auth.establish(&headers, user).await
    }
    .await;
    let mut response = match result {
        Ok(cookie) => (
            [(header::SET_COOKIE, cookie)],
            Redirect::to(&auth.0.config.public_url),
        )
            .into_response(),
        Err(_) => Redirect::to(&format!(
            "{}/?login_error=1",
            auth.0.config.public_url.trim_end_matches('/')
        ))
        .into_response(),
    };
    response
        .headers_mut()
        .append(header::SET_COOKIE, auth.cookie(auth.flow_cookie(), "", 0));
    Ok(response)
}

/// Enforces login on the entire API, including direct requests that bypass the UI.
pub(crate) async fn require_auth(
    State(state): State<AuthState>,
    mut request: Request,
    next: Next,
) -> Result<Response, AuthError> {
    if !state.enabled {
        return Ok(next.run(request).await);
    }
    let auth = state.required()?;
    if let Some(header) = request.headers().get(header::AUTHORIZATION) {
        let token = header
            .to_str()
            .ok()
            .and_then(|header| header.strip_prefix("Bearer "))
            .ok_or(AuthError::Unauthorized)?;
        if !auth
            .0
            .config
            .agent_token
            .as_ref()
            .is_some_and(|expected| same_secret(expected, token))
        {
            return Err(AuthError::Unauthorized);
        }
        if !agent_route(request.method(), request.uri().path()) {
            return Err(AuthError::Forbidden);
        }
    } else {
        let user = auth.user(request.headers()).await?;
        if !matches!(
            *request.method(),
            Method::GET | Method::HEAD | Method::OPTIONS
        ) {
            auth.csrf(request.headers())?;
        }
        request.extensions_mut().insert(user);
    }
    let mut response = next.run(request).await;
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    Ok(response)
}

fn agent_route(method: &Method, path: &str) -> bool {
    if *method == Method::POST {
        return matches!(
            path,
            "/jobs/poll"
                | "/job/cancel"
                | "/job/yield"
                | "/job/succeed"
                | "/job/fail"
                | "/job/terminate"
        );
    }
    *method == Method::GET
        && (path == "/graphs"
            || path
                .strip_prefix("/graphs/")
                .is_some_and(|id| Uuid::parse_str(id).is_ok()))
}

fn cookie_value<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    let mut values = headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .filter_map(|part| part.trim().split_once('='))
        .filter(|(key, _)| *key == name)
        .map(|(_, value)| value);
    let value = values.next()?;
    if values.next().is_some() || value.len() != 64 || !value.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return None;
    }
    Some(value)
}

fn random_token() -> String {
    format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
}
fn digest(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}
fn same_secret(left: &str, right: &str) -> bool {
    bool::from(Sha256::digest(left.as_bytes()).ct_eq(&Sha256::digest(right.as_bytes())))
}

#[cfg(test)]
mod tests;
