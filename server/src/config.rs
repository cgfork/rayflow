//! Server configuration, loaded with the [`config`] crate.
//!
//! [`Config::load`] layers built-in defaults, an optional file, and the
//! environment: `RAYFLOW_ADDR` (listen address, default `0.0.0.0:8080`),
//! `DATABASE_URL` (MySQL URL, required), and `RAYFLOW_CONFIG` (path to the
//! config file; by default the `rayflow.yaml` in the working directory, or any
//! other format the `config` crate can find under the `rayflow` name).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use config::{Config as Builder, Environment, File};
use serde::Deserialize;

/// Error returned while loading [`Config`].
pub use config::ConfigError;

/// Runtime configuration for the API server.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct Config {
    /// Address the HTTP server binds and listens on.
    pub addr: SocketAddr,
    /// MySQL connection URL. Required.
    pub database_url: String,
    /// Duration of an agent job claim without polling. Match the pilot's timeout.
    #[serde(default = "default_job_lease_ms")]
    pub job_lease_ms: u64,
}

fn default_job_lease_ms() -> u64 {
    scheduler::DEFAULT_JOB_LEASE.as_millis() as u64
}

impl Default for Config {
    fn default() -> Self {
        Self {
            addr: SocketAddr::from(([0, 0, 0, 0], 8080)),
            database_url: String::new(),
            job_lease_ms: default_job_lease_ms(),
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

    /// Validates the job lease duration used by the database queries.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.job_lease_ms == 0 || self.job_lease_ms > (i64::MAX / 1000) as u64 {
            return Err(ConfigError::Message(
                "job_lease_ms must fit in a positive signed microsecond count".into(),
            ));
        }
        Ok(())
    }

    /// Returns the agent job claim duration.
    pub fn job_lease(&self) -> Duration {
        Duration::from_millis(self.job_lease_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_lease_defaults_and_overrides_are_validated() {
        let mut config: Config = Builder::builder()
            .set_override("addr", "127.0.0.1:8080")
            .unwrap()
            .set_override("database_url", "mysql://localhost/rayflow")
            .unwrap()
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap();
        assert_eq!(config.job_lease(), Duration::from_secs(30));
        config.job_lease_ms = 1250;
        assert!(config.validate().is_ok());
        assert_eq!(config.job_lease(), Duration::from_millis(1250));
        for timeout in [0, u64::MAX] {
            config.job_lease_ms = timeout;
            assert!(config.validate().is_err());
        }
    }
}
