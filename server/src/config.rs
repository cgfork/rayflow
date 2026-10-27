//! Server configuration, loaded with the [`config`] crate.
//!
//! [`Config::load`] layers built-in defaults, an optional file, and the
//! environment: `RAYFLOW_ADDR` (listen address, default `0.0.0.0:8080`),
//! `DATABASE_URL` (MySQL URL, required), and `RAYFLOW_CONFIG` (path to the
//! config file; by default the `rayflow.yaml` in the working directory, or any
//! other format the `config` crate can find under the `rayflow` name).

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
    /// MySQL connection URL. Required.
    pub database_url: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            addr: SocketAddr::from(([0, 0, 0, 0], 8080)),
            database_url: String::new(),
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

        Ok(config)
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
    }
}
