//! Server-owned S3 configuration. Agents only receive temporary signed URLs.

use catalog::{
    artifact::{PathError, validate_relative_path},
    s3::{Config, Credentials, S3},
};
use serde::Deserialize;
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
pub enum Error {
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
            .finish_non_exhaustive()
    }
}

impl StorageConfig {
    pub fn signer(&self) -> Config {
        let mut config = Config::new(
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

    pub fn validate(&self) -> Result<(), Error> {
        S3::new((), self.signer())?;
        if self.bucket.is_empty()
            || !self
                .bucket
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'.' || b == b'-')
            || !self.bucket.as_bytes()[0].is_ascii_alphanumeric()
            || !self.bucket.as_bytes()[self.bucket.len() - 1].is_ascii_alphanumeric()
            || self.bucket.contains("..")
        {
            return Err(Error::InvalidBucket);
        }
        validate_relative_path(self.prefix.trim_matches('/'))?;
        if self.prefix.chars().count() > 128 {
            return Err(Error::PrefixTooLong);
        }
        if !(1..=604800).contains(&self.presign_ttl.as_secs())
            || self.presign_ttl.subsec_nanos() != 0
        {
            return Err(Error::InvalidPresignTtl);
        }
        Ok(())
    }

    pub fn object_url(&self, fullpath: &str) -> Result<String, Error> {
        self.validate()?;
        let path = fullpath.strip_prefix('/').ok_or(Error::InvalidFullpath)?;
        validate_relative_path(path)?;
        let mut url = url::Url::parse(&format!("s3://{}/", self.bucket))?;
        url.path_segments_mut()
            .map_err(|_| Error::InvalidUrl)?
            .pop_if_empty()
            .extend(path.split('/'));
        Ok(url.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use config::{Config as Builder, Environment, File, FileFormat};

    #[test]
    fn storage_loads_nested_overrides_and_redacts_credentials() {
        let source = [
            (
                "RAYFLOW_STORAGE__ENDPOINT".into(),
                "https://objects.example.com".into(),
            ),
            (
                "RAYFLOW_STORAGE__SECRET_KEY".into(),
                "deployment-secret".into(),
            ),
            ("RAYFLOW_STORAGE__PRESIGN_TTL".into(), "2m".into()),
        ]
        .into_iter()
        .collect();
        let config: crate::Config = Builder::builder()
            .add_source(File::from_str("addr: 127.0.0.1:8080\ndatabase_url: mysql://localhost/rayflow\nstorage:\n  endpoint: http://localhost:9000\n  bucket: artifacts\n  access_key: deployment-access\n  secret_key: replaced\n", FileFormat::Yaml))
            .add_source(Environment::with_prefix("RAYFLOW").prefix_separator("_").separator("__").try_parsing(true).source(Some(source)))
            .build().unwrap().try_deserialize().unwrap();
        let storage = config.storage.unwrap();
        storage.validate().unwrap();
        assert_eq!(storage.endpoint, "https://objects.example.com");
        assert_eq!(storage.secret_key, "deployment-secret");
        assert_eq!(storage.presign_ttl, Duration::from_secs(120));
        assert!(!format!("{storage:?}").contains("deployment-"));
        assert_eq!(
            storage.object_url("/prefix/a%20b/file #.csv").unwrap(),
            "s3://artifacts/prefix/a%2520b/file%20%23.csv"
        );
        let mut invalid = storage.clone();
        invalid.prefix = "../outside".into();
        assert!(matches!(invalid.validate(), Err(Error::Path(_))));
        invalid = storage.clone();
        invalid.presign_ttl = Duration::from_millis(1001);
        assert!(matches!(invalid.validate(), Err(Error::InvalidPresignTtl)));
        invalid = storage;
        invalid.bucket = "bucket/path".into();
        assert!(matches!(invalid.validate(), Err(Error::InvalidBucket)));
    }
}
