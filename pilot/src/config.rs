//! Pilot configuration, loaded with the [`config`] crate.
//!
//! The pilot reads the same `rayflow` configuration file as the server, plus a
//! few pilot-specific keys. `RAYFLOW_PILOT_CONFIG` (or `RAYFLOW_CONFIG`) selects
//! a different file, and `RAYFLOW_PILOT_*` environment variables override it.

use std::env;
use std::path::PathBuf;
use std::time::Duration;

use config::{Config as Builder, ConfigError, Environment, File};
use serde::Deserialize;

fn default_batch() -> usize {
    64
}

fn default_poll_interval_ms() -> u64 {
    250
}

fn default_leader_name() -> String {
    "rayflow".to_string()
}

fn default_leader_lease_ms() -> u64 {
    10_000
}

/// Runtime configuration for the pilot binary.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct Config {
    /// MySQL connection URL. Required unless supplied through `DATABASE_URL`.
    #[serde(default)]
    pub database_url: String,
    /// Maximum commands, tasks, and events consumed per poll.
    #[serde(default = "default_batch")]
    pub batch: usize,
    /// Delay between consumer passes in milliseconds.
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    /// Name of the coordinator role the pilot competes to lead.
    #[serde(default = "default_leader_name")]
    pub leader_name: String,
    /// Leader lease duration in milliseconds.
    #[serde(default = "default_leader_lease_ms")]
    pub leader_lease_ms: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            database_url: String::new(),
            batch: default_batch(),
            poll_interval_ms: default_poll_interval_ms(),
            leader_name: default_leader_name(),
            leader_lease_ms: default_leader_lease_ms(),
        }
    }
}

impl Config {
    /// Loads defaults, the optional `rayflow` file, and environment overrides.
    pub fn load() -> Result<Self, ConfigError> {
        let file = env::var("RAYFLOW_PILOT_CONFIG")
            .or_else(|_| env::var("RAYFLOW_CONFIG"))
            .ok()
            .filter(|path| !path.is_empty())
            .map(PathBuf::from);

        let builder = Builder::builder();
        let builder = match file {
            Some(path) => builder.add_source(File::from(path).required(true)),
            None => builder.add_source(File::with_name("rayflow").required(false)),
        };

        let config: Self = builder
            .add_source(
                Environment::with_prefix("RAYFLOW_PILOT")
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

    /// Validates required values and polling cadence.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.database_url.trim().is_empty() {
            return Err(ConfigError::Message(
                "DATABASE_URL or database_url is required".to_string(),
            ));
        }
        if self.batch == 0 {
            return Err(ConfigError::Message("batch must be at least 1".to_string()));
        }
        if self.poll_interval_ms == 0 {
            return Err(ConfigError::Message(
                "poll_interval_ms must be at least 1".to_string(),
            ));
        }
        if self.leader_name.trim().is_empty() {
            return Err(ConfigError::Message(
                "leader_name must not be empty".to_string(),
            ));
        }
        if self.leader_lease_ms == 0 {
            return Err(ConfigError::Message(
                "leader_lease_ms must be at least 1".to_string(),
            ));
        }
        if self.poll_interval_ms >= self.leader_lease_ms {
            return Err(ConfigError::Message(
                "poll_interval_ms must be less than leader_lease_ms".to_string(),
            ));
        }
        Ok(())
    }

    /// Returns the delay between consumer passes.
    pub fn poll_interval(&self) -> Duration {
        Duration::from_millis(self.poll_interval_ms)
    }

    /// Returns the leader lease duration.
    pub fn leader_lease(&self) -> Duration {
        Duration::from_millis(self.leader_lease_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid_except_for_the_database_url() {
        let config = Config::default();
        assert!(config.validate().is_err());

        let config = Config {
            database_url: "mysql://localhost/rayflow".to_string(),
            ..Config::default()
        };
        assert!(config.validate().is_ok());
        assert_eq!(config.poll_interval(), Duration::from_millis(250));
    }

    #[test]
    fn poll_interval_must_fit_inside_the_leader_lease() {
        let config = Config {
            database_url: "mysql://localhost/rayflow".to_string(),
            poll_interval_ms: 10_000,
            leader_lease_ms: 10_000,
            ..Config::default()
        };
        assert!(config.validate().is_err());
    }
}
