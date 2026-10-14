//! Server configuration, loaded with the [`config`] crate.
//!
//! [`Config::load`] layers built-in defaults, an optional file, and the
//! environment: `RAYFLOW_ADDR` (listen address, default `0.0.0.0:8080`),
//! `RAYFLOW_SHARDS` (shard count, default `1`), `DATABASE_URL` (MySQL URL,
//! required), and `RAYFLOW_CONFIG` (path to the config file; by default the
//! `rayflow.yaml` in the working directory, or any other format the `config`
//! crate can find under the `rayflow` name).

use std::net::SocketAddr;
use std::path::PathBuf;

use config::{Config as Builder, Environment, File};
use serde::Deserialize;

/// Error returned while loading [`Config`].
pub use config::ConfigError;

/// Runtime configuration for the API server.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct Config {
    /// Address the HTTP server binds and listens on.
    pub addr: SocketAddr,
    /// Number of shards in the shard table.
    pub shards: u32,
    /// MySQL connection URL. Required.
    pub database_url: String,
}

impl Config {
    /// Loads the configuration from defaults, an optional file, and the environment.
    pub fn load() -> Result<Self, ConfigError> {
        let file = std::env::var("RAYFLOW_CONFIG")
            .ok()
            .filter(|path| !path.is_empty())
            .map(PathBuf::from);

        let builder = Builder::builder()
            .set_default("addr", "0.0.0.0:8080")?
            .set_default("shards", 1_i64)?;
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

        if config.shards == 0 {
            return Err(ConfigError::Message(
                "RAYFLOW_SHARDS must be at least 1".to_string(),
            ));
        }
        Ok(config)
    }
}
