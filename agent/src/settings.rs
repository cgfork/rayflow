use std::{
    env,
    path::PathBuf,
    time::{Duration, Instant},
};

use config::{Config as Builder, Environment, File};
use scheduler::ResourceCapacity;
use serde::Deserialize;
use sysinfo::{MemoryRefreshKind, RefreshKind, System};
use uuid::Uuid;

use crate::errors::Error;

#[derive(Clone, Deserialize)]
pub struct Settings {
    pub id: Uuid,
    pub server_url: String,
    /// Fleet credential when server authentication is enabled.
    #[serde(default)]
    pub auth_token: Option<String>,
    pub workdir: PathBuf,
    /// Root for job input/output bytes, separate from durable job metadata.
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
    pub resources: ResourceCapacity,
    pub batch: usize,
    #[serde(with = "jiff::fmt::serde::unsigned_duration::required")]
    pub poll_interval: Duration,
    #[serde(with = "jiff::fmt::serde::unsigned_duration::required")]
    pub request_timeout: Duration,
}

impl std::fmt::Debug for Settings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Settings")
            .field("id", &self.id)
            .field("server_url", &self.server_url)
            .field("workdir", &self.workdir)
            .field("data_dir", &self.data_dir)
            .field("resources", &self.resources)
            .field("batch", &self.batch)
            .field("poll_interval", &self.poll_interval)
            .field("request_timeout", &self.request_timeout)
            .finish_non_exhaustive()
    }
}

impl Settings {
    /// Loads rayflow configuration and RAYFLOW_AGENT_* overrides. The UUID is required.
    pub fn load() -> Result<Self, Error> {
        let path = env::var("RAYFLOW_AGENT_CONFIG")
            .ok()
            .filter(|path| !path.is_empty())
            .or_else(|| {
                env::var("RAYFLOW_CONFIG")
                    .ok()
                    .filter(|path| !path.is_empty())
            });
        let file = match path {
            Some(path) => File::from(PathBuf::from(path)).required(true),
            None => File::with_name("rayflow").required(false),
        };
        let system = System::new_with_specifics(
            RefreshKind::nothing().with_memory(MemoryRefreshKind::everything()),
        );
        let settings: Self = Builder::builder()
            .set_default("server_url", "http://127.0.0.1:8080")?
            .set_default("workdir", ".rayflow/jobs")?
            .set_default("data_dir", ".rayflow/data")?
            .set_default(
                "resources.cpu_cores",
                std::thread::available_parallelism()
                    .map(|cores| cores.get() as u64)
                    .unwrap_or(1),
            )?
            .set_default("resources.memory_bytes", system.total_memory())?
            .set_default("batch", 64)?
            .set_default("poll_interval", "1s")?
            .set_default("request_timeout", "60s")?
            .add_source(file)
            .add_source(Self::environment())
            .build()?
            .try_deserialize()?;
        settings.validate()?;
        Ok(settings)
    }

    fn environment() -> Environment {
        Environment::with_prefix("RAYFLOW_AGENT")
            .prefix_separator("_")
            .separator("__")
            .try_parsing(true)
            .ignore_empty(true)
    }

    pub fn new(
        id: Uuid,
        server_url: impl Into<String>,
        workdir: impl Into<PathBuf>,
        resources: ResourceCapacity,
    ) -> Self {
        let workdir = workdir.into();
        Self {
            id,
            server_url: server_url.into(),
            auth_token: None,
            data_dir: workdir.join("data"),
            workdir,
            resources,
            batch: 64,
            poll_interval: Duration::from_secs(1),
            request_timeout: Duration::from_secs(60),
        }
    }

    pub fn validate(&self) -> Result<(), Error> {
        if self.id.is_nil() {
            return Err(Error::Settings("id must be a non-nil UUID"));
        }
        if !(1..=1000).contains(&self.batch) {
            return Err(Error::Settings("batch must be between 1 and 1000"));
        }
        if self.resources.cpu_cores == 0 || self.resources.memory_bytes == 0 {
            return Err(Error::Settings("CPU and memory capacity must be positive"));
        }
        if self.workdir.as_os_str().is_empty() {
            return Err(Error::Settings("workdir must not be empty"));
        }
        if self.data_dir.as_os_str().is_empty() {
            return Err(Error::Settings("data_dir must not be empty"));
        }
        for duration in [self.poll_interval, self.request_timeout] {
            if duration.is_zero() || Instant::now().checked_add(duration).is_none() {
                return Err(Error::Settings(
                    "poll interval and request timeout must be positive, finite durations",
                ));
            }
        }
        let url = reqwest::Url::parse(&self.server_url)
            .map_err(|_| Error::Settings("server_url must be an HTTP or HTTPS URL"))?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(Error::Settings(
                "server_url must be an HTTP or HTTPS URL without a query or fragment",
            ));
        }
        Ok(())
    }
}

fn default_data_dir() -> PathBuf {
    ".rayflow/data".into()
}

#[cfg(test)]
mod tests {
    use super::*;

    const YAML: &str = "id: 12345678-1234-4234-8234-123456789abc\nserver_url: http://localhost:8080\nworkdir: jobs\nresources:\n  cpu_cores: 4\n  memory_bytes: 4096\nbatch: 2\npoll_interval: 250ms\nrequest_timeout: 10s\n";

    fn builder() -> config::ConfigBuilder<config::builder::DefaultState> {
        Builder::builder().add_source(File::from_str(YAML, config::FileFormat::Yaml))
    }

    #[test]
    fn settings_accept_duration_strings_and_nested_resource_overrides() {
        let settings: Settings = builder().build().unwrap().try_deserialize().unwrap();
        settings.validate().unwrap();
        assert_eq!(settings.poll_interval, Duration::from_millis(250));
        assert_eq!(settings.request_timeout, Duration::from_secs(10));
        assert_eq!(settings.data_dir, PathBuf::from(".rayflow/data"));

        let source = [
            ("RAYFLOW_AGENT_DATA_DIR".to_owned(), "/srv/data".to_owned()),
            (
                "RAYFLOW_AGENT_SERVER_URL".to_owned(),
                "https://example.com/api".to_owned(),
            ),
            ("RAYFLOW_AGENT_POLL_INTERVAL".to_owned(), "2s".to_owned()),
            (
                "RAYFLOW_AGENT_RESOURCES__CPU_CORES".to_owned(),
                "8".to_owned(),
            ),
        ]
        .into_iter()
        .collect();
        let settings: Settings = builder()
            .add_source(Settings::environment().source(Some(source)))
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap();
        settings.validate().unwrap();
        assert_eq!(settings.server_url, "https://example.com/api");
        assert_eq!(settings.data_dir, PathBuf::from("/srv/data"));
        assert_eq!(settings.poll_interval, Duration::from_secs(2));
        assert_eq!(settings.resources.cpu_cores, 8);
        assert_eq!(settings.resources.memory_bytes, 4096);
    }

    #[test]
    fn settings_require_a_stable_uuid() {
        let without_id = YAML.lines().skip(1).collect::<Vec<_>>().join("\n");
        assert!(
            Builder::builder()
                .add_source(File::from_str(&without_id, config::FileFormat::Yaml))
                .build()
                .unwrap()
                .try_deserialize::<Settings>()
                .is_err()
        );
        for id in ["invalid", "00000000-0000-0000-0000-000000000000"] {
            let settings = builder()
                .set_override("id", id)
                .unwrap()
                .build()
                .unwrap()
                .try_deserialize::<Settings>();
            assert!(settings.is_err() || settings.unwrap().validate().is_err());
        }
    }

    #[test]
    fn settings_reject_invalid_capacity_cadence_and_server_urls() {
        for (key, value) in [
            ("resources.cpu_cores", "0"),
            ("resources.memory_bytes", "0"),
            ("batch", "0"),
            ("batch", "1001"),
            ("poll_interval", "0s"),
            ("request_timeout", "0s"),
            ("workdir", ""),
            ("data_dir", ""),
            ("server_url", "file:///tmp/socket"),
            ("server_url", "http://localhost?query=1"),
            ("server_url", "http://localhost#fragment"),
        ] {
            let settings: Settings = builder()
                .set_override(key, value)
                .unwrap()
                .build()
                .unwrap()
                .try_deserialize()
                .unwrap();
            assert!(settings.validate().is_err(), "{key}: {value}");
        }
    }
}
