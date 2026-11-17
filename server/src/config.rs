//! Server configuration, loaded with the [`config`] crate.
//!
//! [`Config::load`] layers built-in defaults, an optional file, and the
//! environment: `RAYFLOW_ADDR` (listen address, default `0.0.0.0:8080`),
//! `RAYFLOW_POLL_TIMEOUT` (maximum job poll wait, default `30s`),
//! `RAYFLOW_POLL_RETRY_INTERVAL` (idle job poll retry interval, default `250ms`),
//! `DATABASE_URL` (MySQL URL, required), and `RAYFLOW_CONFIG` (path to the
//! config file; by default the `rayflow.yaml` in the working directory, or any
//! other format the `config` crate can find under the `rayflow` name).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use catalog::{
    ObjectStorage,
    path::{PathError, validate_relative_path},
    s3::{Config as S3Config, Credentials},
};
use config::{Config as Builder, Environment, File};
use serde::Deserialize;

/// Error returned while loading [`Config`].
pub use config::ConfigError;

fn default_poll_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_poll_retry_interval() -> Duration {
    Duration::from_millis(250)
}

/// Runtime configuration for the API server.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct Config {
    /// Optional S3-compatible storage for job inputs and outputs.
    #[serde(default)]
    pub storage: Option<StorageConfig>,
    /// User authentication; disabled for backwards-compatible local development.
    #[serde(default)]
    pub auth: crate::auth::AuthConfig,
    /// Address the HTTP server binds and listens on.
    pub addr: SocketAddr,
    /// MySQL connection URL. Required.
    pub database_url: String,
    /// Maximum job poll wait, such as `30s`; must be at least `2ms`.
    #[serde(
        default = "default_poll_timeout",
        with = "jiff::fmt::serde::unsigned_duration::required"
    )]
    pub poll_timeout: Duration,
    /// Delay between idle job poll attempts, such as `250ms`; at least `1ms`.
    #[serde(
        default = "default_poll_retry_interval",
        with = "jiff::fmt::serde::unsigned_duration::required"
    )]
    pub poll_retry_interval: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            storage: None,
            auth: crate::auth::AuthConfig::default(),
            addr: SocketAddr::from(([0, 0, 0, 0], 8080)),
            database_url: String::new(),
            poll_timeout: default_poll_timeout(),
            poll_retry_interval: default_poll_retry_interval(),
        }
    }
}

impl Config {
    /// Loads the configuration from defaults, an optional file, and the environment.
    pub fn load() -> Result<Self, ConfigError> {
        let file = std::env::var("RAYFLOW_CONFIG")
            .ok()
            .filter(|path| !path.is_empty())
            .map(PathBuf::from);

        let builder = Builder::builder().set_default("addr", "0.0.0.0:8080")?;
        let builder = match file {
            Some(path) => builder.add_source(File::from(path).required(true)),
            None => builder.add_source(File::with_name("rayflow").required(false)),
        };

        let config: Self = builder
            .add_source(
                Environment::with_prefix("RAYFLOW")
                    .prefix_separator("_")
                    .separator("__")
                    .try_parsing(true)
                    .ignore_empty(true),
            )
            .add_source(
                Environment::with_prefix("")
                    .prefix_separator("")
                    .try_parsing(true)
                    .ignore_empty(true),
            )
            .build()?
            .try_deserialize()?;

        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if let Some(storage) = &self.storage {
            storage
                .validate()
                .map_err(|error| ConfigError::Foreign(Box::new(error)))?;
        }
        self.auth
            .validate()
            .map_err(|error| ConfigError::Foreign(Box::new(error)))?;
        if self.poll_timeout < Duration::from_millis(2) {
            return Err(ConfigError::Message(
                "poll_timeout must be at least 2ms".to_owned(),
            ));
        }
        if self.poll_retry_interval < Duration::from_millis(1) {
            return Err(ConfigError::Message(
                "poll_retry_interval must be at least 1ms".to_owned(),
            ));
        }
        for (name, duration) in [
            ("poll_timeout", self.poll_timeout),
            ("poll_retry_interval", self.poll_retry_interval),
        ] {
            if Instant::now().checked_add(duration).is_none() {
                return Err(ConfigError::Message(format!("{name} is too large")));
            }
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StorageConfigError {
    #[error(transparent)]
    S3(#[from] catalog::s3::ConfigError),
    #[error("storage.bucket must be an S3 bucket name")]
    InvalidBucket,
    #[error(transparent)]
    Path(#[from] PathError),
    #[error("storage.prefix is too long")]
    PrefixTooLong,
    #[error("storage.presign_ttl must be 1..=604800 whole seconds")]
    InvalidPresignTtl,
    #[error("artifact fullpath must start with /")]
    InvalidFullpath,
    #[error(transparent)]
    Url(#[from] url::ParseError),
    #[error("invalid storage URL")]
    InvalidUrl,
}

#[derive(Clone, PartialEq, Eq, Deserialize)]
pub struct StorageConfig {
    pub endpoint: String,
    pub bucket: String,
    #[serde(default = "default_prefix")]
    pub prefix: String,
    #[serde(default = "default_region")]
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    #[serde(default = "default_multipart")]
    pub multipart_enabled: bool,
    pub session_token: Option<String>,
    pub credential_expiry: Option<jiff::Timestamp>,
    #[serde(
        default = "default_ttl",
        with = "jiff::fmt::serde::unsigned_duration::required"
    )]
    pub presign_ttl: Duration,
}

fn default_prefix() -> String {
    "artifacts".into()
}
fn default_multipart() -> bool {
    true
}
fn default_region() -> String {
    "us-east-1".into()
}
fn default_ttl() -> Duration {
    Duration::from_secs(900)
}

impl std::fmt::Debug for StorageConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageConfig")
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("region", &self.region)
            .field("presign_ttl", &self.presign_ttl)
            .field("multipart_enabled", &self.multipart_enabled)
            .finish_non_exhaustive()
    }
}

impl StorageConfig {
    /// Provider construction is confined to configuration; callers only receive
    /// ObjectStorage, never a concrete signing client.
    pub fn object_storage(
        &self,
        db: toasty::Db,
    ) -> Result<impl ObjectStorage<Err: Send + Sync + 'static> + use<>, StorageConfigError> {
        Ok(self.signer().build(db)?)
    }

    fn signer(&self) -> S3Config {
        let mut config = S3Config::new(
            &self.endpoint,
            Credentials::new(
                &self.access_key,
                &self.secret_key,
                self.session_token.clone(),
                self.credential_expiry.map(Into::into),
                "rayflow-server",
            ),
        );
        config.region = self.region.clone();
        config
    }

    pub fn validate(&self) -> Result<(), StorageConfigError> {
        self.signer().validate()?;
        if self.bucket.is_empty()
            || !self
                .bucket
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'.' || b == b'-')
            || !self.bucket.as_bytes()[0].is_ascii_alphanumeric()
            || !self.bucket.as_bytes()[self.bucket.len() - 1].is_ascii_alphanumeric()
            || self.bucket.contains("..")
        {
            return Err(StorageConfigError::InvalidBucket);
        }
        validate_relative_path(self.prefix.trim_matches('/'))?;
        if self.prefix.chars().count() > 128 {
            return Err(StorageConfigError::PrefixTooLong);
        }
        if !(1..=604800).contains(&self.presign_ttl.as_secs())
            || self.presign_ttl.subsec_nanos() != 0
        {
            return Err(StorageConfigError::InvalidPresignTtl);
        }
        Ok(())
    }

    pub fn object_url(&self, fullpath: &str) -> Result<String, StorageConfigError> {
        self.validate()?;
        let path = fullpath
            .strip_prefix('/')
            .ok_or(StorageConfigError::InvalidFullpath)?;
        validate_relative_path(path)?;
        let mut url = url::Url::parse(&format!("s3://{}/", self.bucket))?;
        url.path_segments_mut()
            .map_err(|_| StorageConfigError::InvalidUrl)?
            .pop_if_empty()
            .extend(path.split('/'));
        Ok(url.into())
    }
}
