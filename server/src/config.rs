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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_config_deserializes_address_and_database() {
        let config: Config = Builder::builder()
            .set_override("addr", "127.0.0.1:8080")
            .unwrap()
            .set_override("database_url", "mysql://localhost/rayflow")
            .unwrap()
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap();
        assert_eq!(config.addr.to_string(), "127.0.0.1:8080");
        assert_eq!(config.database_url, "mysql://localhost/rayflow");
        assert_eq!(config.poll_timeout, Duration::from_secs(30));
        assert_eq!(config.poll_retry_interval, Duration::from_millis(250));
    }

    #[test]
    fn poll_timeout_accepts_configuration_and_rejects_empty_random_ranges() {
        for poll_timeout_ms in [0, 1, 2, 600, 60_000] {
            let config: Config = Builder::builder()
                .set_override("addr", "127.0.0.1:8080")
                .unwrap()
                .set_override("database_url", "mysql://localhost/rayflow")
                .unwrap()
                .set_override("poll_timeout", format!("{poll_timeout_ms}ms"))
                .unwrap()
                .build()
                .unwrap()
                .try_deserialize()
                .unwrap();
            assert_eq!(config.poll_timeout, Duration::from_millis(poll_timeout_ms));
            assert_eq!(config.validate().is_ok(), poll_timeout_ms >= 2);
        }
    }

    #[test]
    fn poll_retry_interval_accepts_configuration_and_rejects_zero() {
        for poll_retry_interval_ms in [0, 1, 500, 60_000] {
            let config: Config = Builder::builder()
                .set_override("addr", "127.0.0.1:8080")
                .unwrap()
                .set_override("database_url", "mysql://localhost/rayflow")
                .unwrap()
                .set_override("poll_retry_interval", format!("{poll_retry_interval_ms}ms"))
                .unwrap()
                .build()
                .unwrap()
                .try_deserialize()
                .unwrap();
            assert_eq!(
                config.poll_retry_interval,
                Duration::from_millis(poll_retry_interval_ms)
            );
            assert_eq!(config.validate().is_ok(), poll_retry_interval_ms > 0);
        }
    }

    #[test]
    fn duration_strings_load_from_yaml_and_environment() {
        let source = Builder::builder()
            .add_source(File::from_str(
                "addr: 127.0.0.1:8080\ndatabase_url: mysql://localhost/rayflow\npoll_timeout: 10s\npoll_retry_interval: 250ms\n",
                config::FileFormat::Yaml,
            ))
            .build()
            .unwrap();
        let config: Config = source.clone().try_deserialize().unwrap();
        assert_eq!(config.poll_timeout, Duration::from_secs(10));
        assert_eq!(config.poll_retry_interval, Duration::from_millis(250));
        config.validate().unwrap();

        let environment = Environment::with_prefix("RAYFLOW")
            .try_parsing(true)
            .ignore_empty(true)
            .source(Some(
                [
                    ("RAYFLOW_POLL_TIMEOUT".to_owned(), "1m".to_owned()),
                    ("RAYFLOW_POLL_RETRY_INTERVAL".to_owned(), "1.5s".to_owned()),
                ]
                .into_iter()
                .collect(),
            ));
        let config: Config = Builder::builder()
            .add_source(source)
            .add_source(environment)
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap();
        assert_eq!(config.poll_timeout, Duration::from_secs(60));
        assert_eq!(config.poll_retry_interval, Duration::from_millis(1500));
        config.validate().unwrap();
    }

    #[test]
    fn polling_durations_reject_malformed_and_negative_strings() {
        for key in ["poll_timeout", "poll_retry_interval"] {
            for value in ["-10s", "10", "invalid"] {
                let result = Builder::builder()
                    .set_override("addr", "127.0.0.1:8080")
                    .unwrap()
                    .set_override("database_url", "mysql://localhost/rayflow")
                    .unwrap()
                    .set_override(key, value)
                    .unwrap()
                    .build()
                    .unwrap()
                    .try_deserialize::<Config>();
                assert!(result.is_err(), "accepted {key}: {value}");
            }
        }
    }

    #[test]
    fn polling_durations_must_fit_timer_deadlines() {
        for config in [
            Config {
                poll_timeout: Duration::MAX,
                ..Config::default()
            },
            Config {
                poll_retry_interval: Duration::MAX,
                ..Config::default()
            },
        ] {
            assert!(config.validate().is_err());
        }
    }
}
