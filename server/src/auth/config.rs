use std::collections::HashSet;
use std::time::Duration;

use argon2::PasswordHash;
use serde::Deserialize;
use url::Url;

#[derive(Debug, thiserror::Error)]
pub enum AuthConfigError {
    #[error("invalid authentication URL")]
    InvalidUrl(#[source] url::ParseError),
    #[error(
        "authentication URLs require HTTPS (HTTP is allowed only on loopback) and no credentials, query, or fragment"
    )]
    UnsafeUrl,
    #[error("auth.public_url must be an origin without a path")]
    PublicUrlPath,
    #[error("auth.session_ttl must be between 1 minute and 7 days")]
    InvalidSessionTtl,
    #[error("authentication requires at least one login provider")]
    MissingProvider,
    #[error("local usernames must be unique and contain 1–128 bytes")]
    InvalidLocalUsername,
    #[error("invalid local password hash")]
    InvalidPasswordHash(argon2::password_hash::Error),
    #[error("local passwords require salted Argon2id PHC hashes")]
    UnsupportedPasswordHash,
    #[error("OIDC client_id and client_secret are required")]
    MissingOidcCredentials,
    #[error("invalid LDAP URL")]
    InvalidLdapUrl(#[source] url::ParseError),
    #[error(
        "LDAP requires an ldap(s) host URL, search bind credentials, base_dn, attributes, and a filter with one {{username}}"
    )]
    InvalidLdapConfiguration,
    #[error("auth.agent_token must contain at least 32 printable ASCII characters")]
    InvalidAgentToken,
    #[error("password provider IDs must be unique and cannot be oidc")]
    InvalidPasswordProviderIds,
}

/// Authentication for one company/workspace. Secrets are omitted from Debug output.
#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AuthConfig {
    pub enabled: bool,
    pub company_name: String,
    /// Browser-facing origin, including the UI and the /api reverse proxy.
    pub public_url: String,
    #[serde(with = "jiff::fmt::serde::unsigned_duration::required")]
    pub session_ttl: Duration,
    pub local_users: Vec<LocalUser>,
    pub oidc: Option<OidcConfig>,
    pub ldap: Option<LdapConfig>,
    /// Optional fleet credential. Only permits agent operations and published graph reads.
    pub agent_token: Option<String>,
}

impl std::fmt::Debug for AuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthConfig")
            .field("enabled", &self.enabled)
            .field("company_name", &self.company_name)
            .finish_non_exhaustive()
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            company_name: "Rayflow".into(),
            public_url: "http://127.0.0.1:5173".into(),
            session_ttl: Duration::from_secs(8 * 3600),
            local_users: Vec::new(),
            oidc: None,
            ldap: None,
            agent_token: None,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalUser {
    pub username: String,
    pub password_hash: String,
    #[serde(default)]
    pub name: String,
}

#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OidcConfig {
    pub issuer_url: String,
    pub client_id: String,
    pub client_secret: String,
}

#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LdapConfig {
    pub url: String,
    pub bind_dn: String,
    pub bind_password: String,
    pub base_dn: String,
    /// LDAP search filter containing exactly one {username} placeholder.
    pub user_filter: String,
    #[serde(default = "default_name_attribute")]
    pub name_attribute: String,
    #[serde(default = "default_email_attribute")]
    pub email_attribute: String,
}

fn default_name_attribute() -> String {
    "displayName".into()
}
fn default_email_attribute() -> String {
    "mail".into()
}

/// HTTPS is required except for loopback development; never infer it from request headers.
pub(super) fn web_url(value: &str) -> Result<Url, AuthConfigError> {
    let url = Url::parse(value).map_err(AuthConfigError::InvalidUrl)?;
    let local = matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
    if (url.scheme() != "https" && !(url.scheme() == "http" && local))
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(AuthConfigError::UnsafeUrl);
    }
    Ok(url)
}

impl AuthConfig {
    pub fn validate(&self) -> Result<(), AuthConfigError> {
        self.validate_with_providers(false)
    }

    pub(super) fn validate_with_providers(
        &self,
        custom_providers: bool,
    ) -> Result<(), AuthConfigError> {
        if !self.enabled {
            return Ok(());
        }
        if web_url(&self.public_url)?.path() != "/" {
            return Err(AuthConfigError::PublicUrlPath);
        }
        if !(Duration::from_secs(60)..=Duration::from_secs(7 * 86400)).contains(&self.session_ttl) {
            return Err(AuthConfigError::InvalidSessionTtl);
        }
        if !custom_providers
            && self.local_users.is_empty()
            && self.oidc.is_none()
            && self.ldap.is_none()
        {
            return Err(AuthConfigError::MissingProvider);
        }
        let mut names = HashSet::new();
        for user in &self.local_users {
            if user.username.is_empty()
                || user.username.len() > 128
                || !names.insert(&user.username)
            {
                return Err(AuthConfigError::InvalidLocalUsername);
            }
            let hash = PasswordHash::new(&user.password_hash)
                .map_err(AuthConfigError::InvalidPasswordHash)?;
            if hash.algorithm.as_str() != "argon2id" || hash.hash.is_none() || hash.salt.is_none() {
                return Err(AuthConfigError::UnsupportedPasswordHash);
            }
        }
        if let Some(oidc) = &self.oidc {
            web_url(&oidc.issuer_url)?;
            if oidc.client_id.is_empty() || oidc.client_secret.is_empty() {
                return Err(AuthConfigError::MissingOidcCredentials);
            }
        }
        if let Some(ldap) = &self.ldap {
            let url = Url::parse(&ldap.url).map_err(AuthConfigError::InvalidLdapUrl)?;
            if !matches!(url.scheme(), "ldap" | "ldaps")
                || url.host_str().is_none()
                || !url.username().is_empty()
                || url.password().is_some()
                || url.path() != "" && url.path() != "/"
                || url.query().is_some()
                || url.fragment().is_some()
                || ldap.bind_dn.is_empty()
                || ldap.bind_password.is_empty()
                || ldap.base_dn.is_empty()
                || ldap.user_filter.matches("{username}").count() != 1
                || ldap.name_attribute.is_empty()
                || ldap.email_attribute.is_empty()
            {
                return Err(AuthConfigError::InvalidLdapConfiguration);
            }
        }
        if self
            .agent_token
            .as_ref()
            .is_some_and(|s| s.len() < 32 || !s.bytes().all(|b| b.is_ascii_graphic()))
        {
            return Err(AuthConfigError::InvalidAgentToken);
        }
        Ok(())
    }
}
