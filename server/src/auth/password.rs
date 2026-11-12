use std::time::Duration;

use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier, password_hash::SaltString};
use futures_util::future::BoxFuture;
use ldap3::{LdapConnAsync, LdapConnSettings, Scope, SearchEntry, SearchOptions};

use super::{AuthError, LdapConfig, LocalUser, User};

/// Company adapters return a verified identity, never a browser-supplied identity.
/// Returning None means invalid credentials; errors mean the provider is unavailable.
pub trait PasswordProvider: Send + Sync {
    fn id(&self) -> &str;
    fn label(&self) -> &str;
    fn authenticate<'a>(
        &'a self,
        username: &'a str,
        password: &'a str,
    ) -> BoxFuture<'a, Result<Option<User>, AuthError>>;
}

pub(super) struct LocalProvider {
    users: Vec<LocalUser>,
    dummy_hash: String,
    slots: std::sync::Arc<tokio::sync::Semaphore>,
}

impl LocalProvider {
    pub async fn new(users: Vec<LocalUser>) -> Result<Self, AuthError> {
        let dummy_hash = tokio::task::spawn_blocking(|| {
            let salt = SaltString::encode_b64(uuid::Uuid::new_v4().as_bytes()).unwrap();
            Argon2::default()
                .hash_password(uuid::Uuid::new_v4().as_bytes(), &salt)
                .map(|hash| hash.to_string())
                .map_err(|_| AuthError::Unavailable)
        })
        .await
        .map_err(|_| AuthError::Unavailable)??;
        Ok(Self {
            users,
            dummy_hash,
            slots: std::sync::Arc::new(tokio::sync::Semaphore::new(8)),
        })
    }
}

impl PasswordProvider for LocalProvider {
    fn id(&self) -> &str {
        "local"
    }
    fn label(&self) -> &str {
        "Rayflow account"
    }
    fn authenticate<'a>(
        &'a self,
        username: &'a str,
        password: &'a str,
    ) -> BoxFuture<'a, Result<Option<User>, AuthError>> {
        Box::pin(async move {
            let user = self
                .users
                .iter()
                .find(|user| user.username == username)
                .cloned();
            let hash = user
                .as_ref()
                .map(|u| &u.password_hash)
                .unwrap_or(&self.dummy_hash)
                .clone();
            let password = password.to_owned();
            let slot = self
                .slots
                .clone()
                .try_acquire_owned()
                .map_err(|_| AuthError::RateLimited)?;
            let valid = tokio::task::spawn_blocking(move || {
                // Keep the permit until hashing finishes, including cancelled HTTP requests.
                let _slot = slot;
                PasswordHash::new(&hash).is_ok_and(|hash| {
                    Argon2::default()
                        .verify_password(password.as_bytes(), &hash)
                        .is_ok()
                })
            })
            .await
            .map_err(|_| AuthError::Unavailable)?;
            Ok(user.filter(|_| valid).map(|user| {
                let name = if user.name.is_empty() {
                    user.username.clone()
                } else {
                    user.name
                };
                User::new("local", &user.username, name, None)
            }))
        })
    }
}

pub(super) struct LdapProvider(pub LdapConfig);

impl PasswordProvider for LdapProvider {
    fn id(&self) -> &str {
        "ldap"
    }
    fn label(&self) -> &str {
        "Company directory"
    }
    fn authenticate<'a>(
        &'a self,
        username: &'a str,
        password: &'a str,
    ) -> BoxFuture<'a, Result<Option<User>, AuthError>> {
        Box::pin(async move {
            // An empty simple-bind password may mean anonymous authentication.
            if username.is_empty() || password.is_empty() {
                return Ok(None);
            }
            tokio::time::timeout(
                Duration::from_secs(10),
                self.authenticate_ldap(username, password),
            )
            .await
            .map_err(|_| AuthError::Unavailable)?
        })
    }
}

impl LdapProvider {
    async fn authenticate_ldap(
        &self,
        username: &str,
        password: &str,
    ) -> Result<Option<User>, AuthError> {
        let config = &self.0;
        // ldap:// always upgrades to TLS before any credentials are sent.
        let settings = LdapConnSettings::new()
            .set_conn_timeout(Duration::from_secs(5))
            .set_starttls(config.url.starts_with("ldap://"));
        let (connection, mut ldap) = LdapConnAsync::with_settings(settings, &config.url)
            .await
            .map_err(|_| AuthError::Unavailable)?;
        let driver = tokio::spawn(async move {
            let _ = connection.drive().await;
        });
        // Aborts even when the enclosing timeout cancels this future.
        struct Abort(tokio::task::JoinHandle<()>);
        impl Drop for Abort {
            fn drop(&mut self) {
                self.0.abort();
            }
        }
        let _driver = Abort(driver);
        ldap.simple_bind(&config.bind_dn, &config.bind_password)
            .await
            .map_err(|_| AuthError::Unavailable)?
            .success()
            .map_err(|_| AuthError::Unavailable)?;
        let filter = config
            .user_filter
            .replace("{username}", &ldap3::ldap_escape(username));
        let (entries, _) = ldap
            .with_search_options(SearchOptions::new().sizelimit(2))
            .search(
                &config.base_dn,
                Scope::Subtree,
                &filter,
                vec![
                    config.name_attribute.as_str(),
                    config.email_attribute.as_str(),
                ],
            )
            .await
            .map_err(|_| AuthError::Unavailable)?
            .success()
            .map_err(|_| AuthError::Unavailable)?;
        if entries.len() != 1 {
            return Ok(None);
        }
        let entry = SearchEntry::construct(entries.into_iter().next().unwrap());
        if entry.dn.is_empty() {
            return Ok(None);
        }
        let result = ldap
            .simple_bind(&entry.dn, password)
            .await
            .map_err(|_| AuthError::Unavailable)?;
        if result.rc == 49 {
            return Ok(None);
        }
        result.success().map_err(|_| AuthError::Unavailable)?;
        let attribute = |name: &str| {
            entry
                .attrs
                .iter()
                .find(|(key, _)| key.eq_ignore_ascii_case(name))
                .and_then(|(_, values)| values.first())
                .cloned()
        };
        let user = User::new(
            &format!("ldap:{}", config.url),
            &entry.dn,
            attribute(&config.name_attribute).unwrap_or_else(|| username.to_owned()),
            attribute(&config.email_attribute),
        );
        let _ = ldap.unbind().await;
        Ok(Some(user))
    }
}
