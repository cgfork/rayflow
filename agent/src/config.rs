use config::{Config as Builder, ConfigError, Environment, File};
use serde::Deserialize;
use std::{env, path::PathBuf, time::Duration};
use uuid::Uuid;

/// Agent settings from rayflow.yaml and RAYFLOW_AGENT_* overrides.
#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct Config {
    pub api_url: String,
    /// Stable configured identity, or a fresh UUID for this process by default.
    pub agent_id: Uuid,
    /// Optional caps; availability is still sampled from the host on every poll.
    pub cpu_cores: Option<u32>,
    pub memory_bytes: Option<u64>,
    pub batch: usize,
    pub poll_interval_ms: u64,
    /// Deadline for each API request, including connection and response body.
    pub request_timeout_ms: u64,
    pub shutdown_timeout_ms: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            api_url: "http://127.0.0.1:8080".into(),
            agent_id: Uuid::new_v4(),
            cpu_cores: None,
            memory_bytes: None,
            batch: 64,
            poll_interval_ms: 250,
            request_timeout_ms: 30_000,
            shutdown_timeout_ms: 5_000,
        }
    }
}

impl Config {
    pub fn load() -> Result<Self, ConfigError> {
        let path = env::var("RAYFLOW_AGENT_CONFIG")
            .or_else(|_| env::var("RAYFLOW_CONFIG"))
            .ok()
            .filter(|path| !path.is_empty());
        let file = match path {
            Some(path) => File::from(PathBuf::from(path)).required(true),
            None => File::with_name("rayflow").required(false),
        };
        let builder = Builder::builder().add_source(file);
        let config: Self = builder
            .add_source(
                Environment::with_prefix("RAYFLOW_AGENT")
                    .try_parsing(true)
                    .ignore_empty(true),
            )
            .build()?
            .try_deserialize()?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if !(1..=1000).contains(&self.batch)
            || self.poll_interval_ms == 0
            || self.request_timeout_ms == 0
            || self.shutdown_timeout_ms == 0
        {
            return Err(ConfigError::Message(
                "batch must be 1 to 1000; poll_interval_ms, request_timeout_ms and shutdown_timeout_ms must be positive"
                    .into(),
            ));
        }
        let url = self
            .api_url
            .parse::<reqwest::Url>()
            .map_err(|e| ConfigError::Message(e.to_string()))?;
        if url.scheme() != "http"
            || url.cannot_be_a_base()
            || url.path() != "/"
            || url.query().is_some()
            || url.fragment().is_some()
            || !url.username().is_empty()
            || url.password().is_some()
        {
            return Err(ConfigError::Message(
                "api_url must be an HTTP origin such as http://127.0.0.1:8080".into(),
            ));
        }
        Ok(())
    }

    pub fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_identity_is_a_uuid_generated_only_when_omitted() {
        let first: Config = serde_json::from_str("{}").unwrap();
        let second: Config = serde_json::from_str("{}").unwrap();
        assert_ne!(first.agent_id, second.agent_id);
        let id = Uuid::new_v4();
        let config: Config = serde_json::from_value(serde_json::json!({"agent_id": id})).unwrap();
        assert_eq!(config.agent_id, id);
        assert!(serde_json::from_str::<Config>(r#"{"agent_id":"agent-a"}"#).is_err());
    }

    #[test]
    fn timeouts_are_configurable_and_must_be_positive() {
        let config: Config =
            serde_json::from_str(r#"{"request_timeout_ms":250,"poll_interval_ms":500}"#).unwrap();
        assert!(config.validate().is_ok());
        assert_eq!(config.request_timeout(), Duration::from_millis(250));
        for field in [
            "request_timeout_ms",
            "poll_interval_ms",
            "shutdown_timeout_ms",
        ] {
            let config: Config = serde_json::from_value(serde_json::json!({field: 0})).unwrap();
            assert!(config.validate().is_err(), "{field}");
        }
    }
}
