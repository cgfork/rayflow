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

/// Files larger than this size use multipart/ranged transfers by default.
pub const DEFAULT_MULTIPART_THRESHOLD: u64 = 64 * 1024 * 1024;

#[derive(Clone, Deserialize)]
pub struct Settings {
    pub id: Uuid,
    /// Server base URL; the client appends `/api/v1` to API requests.
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
    /// Files larger than this size use multipart upload and ranged download.
    #[serde(default = "default_multipart_threshold")]
    pub multipart_threshold: u64,
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
            .field("multipart_threshold", &self.multipart_threshold)
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
            multipart_threshold: DEFAULT_MULTIPART_THRESHOLD,
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
        if self.multipart_threshold == 0 {
            return Err(Error::Settings("multipart threshold must be positive"));
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

fn default_multipart_threshold() -> u64 {
    DEFAULT_MULTIPART_THRESHOLD
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_settings() -> Settings {
        Settings::new(
            Uuid::new_v4(),
            "http://127.0.0.1:8080",
            ".rayflow/test-jobs",
            ResourceCapacity {
                cpu_cores: 2,
                memory_bytes: 1024,
            },
        )
    }

    #[test]
    fn accepts_valid_settings() {
        assert!(valid_settings().validate().is_ok());
    }

    #[test]
    fn rejects_nil_agent_id() {
        let mut settings = valid_settings();
        settings.id = Uuid::nil();
        assert!(matches!(
            settings.validate(),
            Err(Error::Settings("id must be a non-nil UUID"))
        ));
    }

    #[test]
    fn rejects_zero_capacity() {
        let mut settings = valid_settings();
        settings.resources.cpu_cores = 0;
        assert!(matches!(
            settings.validate(),
            Err(Error::Settings("CPU and memory capacity must be positive"))
        ));
    }

    #[test]
    fn rejects_zero_multipart_threshold() {
        let mut settings = valid_settings();
        settings.multipart_threshold = 0;
        assert!(matches!(
            settings.validate(),
            Err(Error::Settings("multipart threshold must be positive"))
        ));
    }
}
