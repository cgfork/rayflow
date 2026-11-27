//! Docker-backed `Containerd` implementation.

use std::pin::Pin;

use bollard::{
    Docker,
    errors::Error as DockerError,
    models::{
        ContainerCreateBody, ContainerStateStatusEnum, ContainerStatsResponse, DeviceRequest,
        HostConfig, Mount, MountType,
    },
    query_parameters::{
        CreateContainerOptionsBuilder, CreateImageOptionsBuilder, RemoveContainerOptionsBuilder,
    },
};
use futures_util::{Stream, StreamExt};
use graph::gpu::is_qualified_cdi_device;
use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Usage, errors::Error};

use super::{Container, ContainerAction, ContainerEvent, ContainerState, Containerd, ExitStatus};

pub use super::ContainerFile as DockerFile;

pub type DockerFiles = Vec<DockerFile>;

/// The Docker container creation parameters derived from a pinned job.
///
/// This type is deliberately defined in the Docker module rather than in the
/// provider-neutral module. Other container providers can expose completely
/// different specifications through [`Containerd::Spec`](super::Containerd::Spec).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DockerSpec {
    #[serde(default)]
    pub files: DockerFiles,
    /// Deterministic Docker name used to reconnect after an agent restart.
    pub name: String,
    /// Image reference that must be available before creation.
    pub image: String,
    #[serde(default)]
    pub command: Option<Vec<String>>,
    #[serde(default)]
    pub env: Option<Vec<String>>,
    #[serde(default)]
    pub working_dir: Option<String>,
    /// CPU limit in whole cores; zero leaves the Docker CPU limit unset.
    pub cpu_cores: u32,
    /// Memory limit in bytes; zero leaves the Docker memory limit unset.
    pub memory_bytes: u64,
    /// GPU memory reserved by the scheduler, in bytes.
    #[serde(default)]
    pub gpu_memory_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assigned_gpu_cdi_device: Option<String>,
}

impl TryFrom<&scheduler::Job> for DockerSpec {
    type Error = Error;

    fn try_from(job: &scheduler::Job) -> Result<Self, Self::Error> {
        let config = job.node_config.get().as_ref().ok_or(Error::MissingConfig)?;
        let image = config
            .config
            .get("image")
            .and_then(serde_json::Value::as_str)
            .filter(|image| !image.trim().is_empty())
            .ok_or(Error::MissingImage)?;
        Ok(Self {
            files: Vec::new(),
            name: container_name(job.uuid),
            image: image.to_owned(),
            command: config
                .config
                .get("command")
                .cloned()
                .map(serde_json::from_value)
                .transpose()?,
            env: config
                .config
                .get("env")
                .cloned()
                .map(serde_json::from_value)
                .transpose()?,
            working_dir: config
                .config
                .get("working_dir")
                .cloned()
                .map(serde_json::from_value)
                .transpose()?,
            cpu_cores: job.required_cpu_cores,
            memory_bytes: job.required_memory_bytes,
            gpu_memory_bytes: job.required_gpu_memory_bytes,
            assigned_gpu_cdi_device: job.assigned_gpu_cdi_device.clone(),
        })
    }
}

impl DockerSpec {
    fn create_body(&self, gpu_cdi_devices: &[String]) -> Result<ContainerCreateBody, Error> {
        let memory = i64::try_from(self.memory_bytes).map_err(|error| {
            Error::Io(std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
        })?;
        let device_requests = if self.gpu_memory_bytes > 0 {
            let device = self.assigned_gpu_cdi_device.as_deref().ok_or_else(|| {
                Error::Provider("GPU job requires a CDI device assignment on this agent".into())
            })?;
            if !gpu_cdi_devices.iter().any(|name| name == device) {
                return Err(Error::Provider(format!(
                    "assigned GPU CDI device {device:?} is unavailable on this agent"
                )));
            }
            Some(vec![DeviceRequest {
                driver: Some("cdi".into()),
                device_ids: Some(vec![device.into()]),
                ..Default::default()
            }])
        } else {
            None
        };
        let mut env = self.env.clone();
        for (key, value) in self.files.iter().flat_map(|file| &file.env) {
            let env = env.get_or_insert_with(Vec::new);
            env.retain(|entry| entry.split('=').next() != Some(key.as_str()));
            env.push(format!("{key}={value}"));
        }
        let mounts = (!self.files.is_empty()).then(|| {
            self.files
                .iter()
                .map(|file| Mount {
                    typ: Some(MountType::BIND),
                    source: Some(file.source.to_string_lossy().into_owned()),
                    target: Some(file.target.clone()),
                    read_only: Some(file.read_only),
                    ..Default::default()
                })
                .collect()
        });
        Ok(ContainerCreateBody {
            image: Some(self.image.clone()),
            cmd: self.command.clone(),
            env,
            working_dir: self.working_dir.clone(),
            host_config: Some(HostConfig {
                mounts,
                nano_cpus: (self.cpu_cores > 0)
                    .then_some(i64::from(self.cpu_cores) * 1_000_000_000),
                memory: (memory > 0).then_some(memory),
                device_requests,
                ..Default::default()
            }),
            ..Default::default()
        })
    }
}

fn container_name(job_id: Uuid) -> String {
    format!("rayflow-{job_id}")
}

/// A Docker-backed implementation of the provider boundary.
#[derive(Clone, Debug)]
pub struct DockerContainerd {
    docker: Docker,
    gpu_cdi_devices: Vec<String>,
}

/// A reconnectable handle for one Docker container.
///
/// Only the Docker client and opaque container identifier are retained. The
/// handle does not own a task or wait future, so it can be reconstructed from
/// `container.json` during agent recovery.
#[derive(Clone, Debug)]
pub struct DockerContainer {
    docker: Docker,
    id: String,
}

impl DockerContainerd {
    /// Creates a provider connection from an existing Bollard client.
    pub fn new(docker: Docker) -> Self {
        Self {
            docker,
            gpu_cdi_devices: Vec::new(),
        }
    }

    /// Configures the qualified CDI names available for GPU assignments.
    pub fn with_gpu_cdi_devices(
        mut self,
        devices: impl IntoIterator<Item = String>,
    ) -> Result<Self, Error> {
        let devices = devices.into_iter().collect::<Vec<_>>();
        if devices
            .iter()
            .any(|device| !is_qualified_cdi_device(device))
        {
            return Err(Error::Settings(
                "gpu_devices must contain qualified CDI device names",
            ));
        }
        self.gpu_cdi_devices = devices;
        Ok(self)
    }

    /// Connects to Docker using Bollard's standard local environment.
    pub fn connect_with_defaults() -> Result<Self, Error> {
        Docker::connect_with_defaults()
            .map(Self::new)
            .map_err(Into::into)
    }

    /// Exposes the underlying client for provider-specific integration code.
    pub fn docker(&self) -> &Docker {
        &self.docker
    }
}

impl DockerContainer {
    /// Creates a handle without contacting Docker or creating a container.
    pub fn new(docker: Docker, id: impl Into<String>) -> Self {
        Self {
            docker,
            id: id.into(),
        }
    }
}

impl Container for DockerContainer {
    fn id(&self) -> &str {
        &self.id
    }

    async fn start(&self) -> Result<(), Error> {
        match self.docker.start_container(&self.id, None).await {
            Ok(()) => Ok(()),
            Err(DockerError::DockerResponseServerError {
                status_code: 304, ..
            }) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    async fn wait(&self) -> Result<ExitStatus, Error> {
        match self.docker.wait_container(&self.id, None).next().await {
            Some(Ok(result)) => Ok(ExitStatus {
                exit_code: result.status_code.try_into().unwrap_or(1),
                finished_at: Some(Timestamp::now()),
            }),
            Some(Err(DockerError::DockerContainerWaitError { code, .. })) => Ok(ExitStatus {
                exit_code: code.try_into().unwrap_or(1),
                finished_at: Some(Timestamp::now()),
            }),
            Some(Err(error)) => Err(error.into()),
            None => Err(Error::Io(std::io::Error::other("container wait ended"))),
        }
    }

    async fn inspect(&self) -> Result<ContainerState, Error> {
        let response = self.docker.inspect_container(&self.id, None).await?;
        let state = response
            .state
            .ok_or_else(|| Error::Io(std::io::Error::other("container state is missing")))?;
        let started_at = state
            .started_at
            .and_then(|value| value.parse::<Timestamp>().ok())
            .filter(|started_at| *started_at >= Timestamp::UNIX_EPOCH);
        if state.running == Some(true)
            || matches!(
                state.status,
                Some(
                    ContainerStateStatusEnum::RUNNING
                        | ContainerStateStatusEnum::PAUSED
                        | ContainerStateStatusEnum::RESTARTING
                )
            )
        {
            return Ok(ContainerState::Running {
                pid: state.pid,
                started_at,
            });
        }
        match state.status {
            Some(ContainerStateStatusEnum::CREATED) => Ok(ContainerState::Created),
            Some(ContainerStateStatusEnum::EXITED | ContainerStateStatusEnum::DEAD) => {
                Ok(ContainerState::Exited {
                    exit_code: state
                        .exit_code
                        .and_then(|value| value.try_into().ok())
                        .unwrap_or(1),
                    started_at,
                    finished_at: state.finished_at.and_then(|value| value.parse().ok()),
                })
            }
            _ => Err(Error::Io(std::io::Error::other(
                "container is changing state",
            ))),
        }
    }

    fn usage(&self) -> Pin<Box<dyn Stream<Item = Result<Usage, Error>> + Send + 'static>> {
        let stats = self.docker.stats(&self.id, Some(Default::default()));
        Box::pin(stats.filter_map(|result| async move {
            match result {
                Ok(stats) => container_usage(stats).map(Ok),
                Err(DockerError::DockerResponseServerError {
                    status_code: 404, ..
                }) => None,
                Err(error) => Some(Err(error.into())),
            }
        }))
    }

    async fn stop(&self) -> Result<(), Error> {
        match self.docker.stop_container(&self.id, None).await {
            Ok(()) => Ok(()),
            Err(DockerError::DockerResponseServerError {
                status_code: 304 | 404,
                ..
            }) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    async fn kill(&self) -> Result<(), Error> {
        match self.docker.kill_container(&self.id, None).await {
            Ok(()) => Ok(()),
            Err(DockerError::DockerResponseServerError {
                status_code: 404 | 409,
                ..
            }) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    async fn remove(&self) -> Result<(), Error> {
        let options = RemoveContainerOptionsBuilder::new().force(true).build();
        match self.docker.remove_container(&self.id, Some(options)).await {
            Ok(()) => Ok(()),
            Err(DockerError::DockerResponseServerError {
                status_code: 404, ..
            }) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

fn container_usage(stats: ContainerStatsResponse) -> Option<Usage> {
    // This adapter uses Linux cgroup accounting and nanosecond CPU counters.
    // Older Windows daemons omit os_type but report num_procs.
    if stats
        .os_type
        .as_deref()
        .is_some_and(|os| !os.is_empty() && os != "linux")
        || stats.num_procs.is_some_and(|count| count > 0)
    {
        return None;
    }
    let sampled_at = stats.read?.parse::<Timestamp>().ok()?;
    // Stopped and restarting containers can return an empty frame with a
    // zero-valued timestamp (year 1).
    if sampled_at < Timestamp::UNIX_EPOCH {
        return None;
    }
    let cpu_usage_ns = stats
        .cpu_stats
        .and_then(|stats| stats.cpu_usage)
        .and_then(|usage| usage.total_usage);
    let memory = stats.memory_stats.as_ref();
    let memory_usage_bytes = memory.and_then(|memory| memory.usage);
    if cpu_usage_ns.is_none() && memory_usage_bytes.is_none() {
        return None;
    }
    let memory_working_set_bytes = memory_usage_bytes.and_then(|usage| {
        let counters = memory?.stats.as_ref()?;
        let inactive_file = counters
            .get("total_inactive_file")
            .or_else(|| counters.get("inactive_file"))?;
        usage.checked_sub(*inactive_file)
    });
    Some(Usage {
        sampled_at,
        cpu_usage_ns,
        memory_usage_bytes,
        memory_working_set_bytes,
        memory_limit_bytes: memory
            .and_then(|memory| memory.limit)
            .filter(|limit| *limit > 0),
    })
}

impl Containerd for DockerContainerd {
    type Container = DockerContainer;
    type Spec<'a> = DockerSpec;

    fn name(&self) -> &'static str {
        "docker"
    }

    fn spec<'a>(&'a self, job: &'a scheduler::Job) -> Result<Self::Spec<'a>, Error> {
        DockerSpec::try_from(job)
    }

    fn spec_with_files<'a, 'files>(
        &'a self,
        job: &'a scheduler::Job,
        files: impl IntoIterator<Item = &'files DockerFile>,
    ) -> Result<Self::Spec<'a>, Error> {
        let mut spec = self.spec(job)?;
        spec.files = files.into_iter().cloned().collect();
        Ok(spec)
    }

    async fn prepare(&self, spec: Self::Spec<'_>) -> Result<(), Error> {
        match self.docker.inspect_image(&spec.image).await {
            Ok(_) => return Ok(()),
            Err(DockerError::DockerResponseServerError {
                status_code: 404, ..
            }) => {}
            Err(error) => return Err(error.into()),
        }
        let options = CreateImageOptionsBuilder::new()
            .from_image(&spec.image)
            .build();
        let mut stream = self.docker.create_image(Some(options), None, None);
        while let Some(result) = stream.next().await {
            result?;
        }
        Ok(())
    }

    async fn create(&self, spec: Self::Spec<'_>) -> Result<Self::Container, Error> {
        let body = spec.create_body(&self.gpu_cdi_devices)?;
        let options = CreateContainerOptionsBuilder::new()
            .name(&spec.name)
            .build();
        match self.docker.create_container(Some(options), body).await {
            Ok(response) => Ok(self.container(&response.id)),
            Err(DockerError::DockerResponseServerError {
                status_code: 409, ..
            }) => Ok(self.container(&spec.name)),
            Err(error) => Err(error.into()),
        }
    }

    fn container(&self, id: &str) -> Self::Container {
        DockerContainer::new(self.docker.clone(), id)
    }

    async fn find_container(&self, job_id: Uuid) -> Result<Option<Self::Container>, Error> {
        let name = container_name(job_id);
        match self.docker.inspect_container(&name, None).await {
            Ok(response) => Ok(Some(
                self.container(response.id.as_deref().unwrap_or(&name)),
            )),
            Err(DockerError::DockerResponseServerError {
                status_code: 404, ..
            }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn events(
        &self,
    ) -> Pin<Box<dyn Stream<Item = Result<ContainerEvent, Error>> + Send + 'static>> {
        let stream = self.docker.events(None).map(|result| {
            result
                .map(|event| ContainerEvent {
                    container_id: event.actor.and_then(|actor| actor.id).unwrap_or_default(),
                    action: match event.action.as_deref() {
                        Some("create") => ContainerAction::Create,
                        Some("start") => ContainerAction::Start,
                        Some("die") => ContainerAction::Die,
                        Some("stop") => ContainerAction::Stop,
                        Some("kill") => ContainerAction::Kill,
                        Some("destroy") => ContainerAction::Destroy,
                        Some(action) => ContainerAction::Other(action.to_owned()),
                        None => ContainerAction::Other(String::new()),
                    },
                    occurred_at: Timestamp::now(),
                })
                .map_err(Into::into)
        });
        Box::pin(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn constructs_docker_mounts_and_environment_from_file_descriptors() {
        let spec: DockerSpec = serde_json::from_value(json!({
            "name": "test",
            "image": "fixture",
            "cpu_cores": 0,
            "memory_bytes": 0,
            "env": ["KEEP=one=two", "ASSET_DIR=/old"],
            "files": [
                {
                    "source": "/host/artifact",
                    "target": "/shared",
                    "read_only": true,
                    "env": [["ASSET_DIR", "/shared"], ["SCRIPT_FILE", "/shared/script.lua"]]
                },
                {
                    "source": "/host/result",
                    "target": "/result",
                    "read_only": false
                }
            ]
        }))
        .unwrap();
        let body = spec
            .create_body(&["nvidia.com/gpu=0".into(), "nvidia.com/gpu=1".into()])
            .unwrap();
        assert_eq!(
            body.env.unwrap(),
            [
                "KEEP=one=two",
                "ASSET_DIR=/shared",
                "SCRIPT_FILE=/shared/script.lua"
            ],
        );
        let host_config = body.host_config.unwrap();
        assert!(host_config.device_requests.is_none());
        let mounts = host_config.mounts.unwrap();
        assert_eq!(mounts.len(), 2);
        assert_eq!(mounts[0].typ, Some(MountType::BIND));
        assert_eq!(mounts[0].source.as_deref(), Some("/host/artifact"));
        assert_eq!(mounts[0].target.as_deref(), Some("/shared"));
        assert_eq!(mounts[0].read_only, Some(true));
        assert_eq!(mounts[1].target.as_deref(), Some("/result"));
        assert_eq!(mounts[1].read_only, Some(false));
    }

    #[test]
    fn requests_only_the_assigned_cdi_gpu_for_each_job() {
        let task = event::task::Task::new(Uuid::nil(), Uuid::nil(), Uuid::nil(), 1);
        let mut job = scheduler::Job::new(&task, Uuid::nil(), Uuid::nil(), Uuid::nil());
        let now = Timestamp::now();
        let gpu_memory_bytes = 8 * 1024 * 1024 * 1024;
        job.required_cpu_cores = 1;
        job.required_memory_bytes = 64 * 1024 * 1024;
        job.required_gpu_memory_bytes = gpu_memory_bytes;
        job.node_config = Some(scheduler::actuator::NodeConfig {
            id: 1,
            uuid: Uuid::nil(),
            node_id: Uuid::nil(),
            version: 1,
            name: "default".into(),
            annotation_match: String::new(),
            config: json!({ "image": "fixture" }),
            lease: Some(30_000),
            required_cpu_cores: job.required_cpu_cores,
            required_memory_bytes: job.required_memory_bytes,
            required_gpu_memory_bytes: gpu_memory_bytes,
            created_at: now,
            updated_at: now,
            created_by: "test".into(),
            updated_by: "test".into(),
        })
        .into();

        let devices = [
            String::from("vendor.example/gpu=GPU-00:01"),
            String::from("vendor.example/gpu=1"),
        ];
        for device in &devices {
            job.assigned_gpu_cdi_device = Some(device.clone());
            let spec = DockerSpec::try_from(&job).unwrap();
            let host_config = spec.create_body(&devices).unwrap().host_config.unwrap();
            assert_eq!(host_config.memory, Some(64 * 1024 * 1024));
            assert!(host_config.devices.is_none());
            assert_eq!(
                serde_json::to_value(host_config).unwrap()["DeviceRequests"],
                json!([{ "Driver": "cdi", "DeviceIDs": [device] }]),
            );
        }

        job.assigned_gpu_cdi_device = Some("vendor.example/gpu=unconfigured".into());
        let spec = DockerSpec::try_from(&job).unwrap();
        assert!(matches!(
            spec.create_body(&devices),
            Err(Error::Provider(_))
        ));

        job.assigned_gpu_cdi_device = None;
        let spec = DockerSpec::try_from(&job).unwrap();
        assert_eq!(spec.gpu_memory_bytes, gpu_memory_bytes);
        assert!(matches!(
            spec.create_body(&devices),
            Err(Error::Provider(_))
        ));
        assert!(matches!(spec.create_body(&[]), Err(Error::Provider(_))));
        assert!(matches!(
            spec.create_body(&devices[..1]),
            Err(Error::Provider(_))
        ));
    }

    #[test]
    fn normalizes_cpu_time_and_memory_for_both_linux_cgroup_versions() {
        let sampled_at = "2026-01-01T00:00:00.123456789Z";
        for cache_key in ["total_inactive_file", "inactive_file"] {
            let stats = serde_json::from_value(json!({
                "os_type": "linux",
                "read": sampled_at,
                "cpu_stats": { "cpu_usage": { "total_usage": 1_500_000_000u64 } },
                "memory_stats": {
                    "usage": 1024,
                    "limit": 4096,
                    "stats": { (cache_key): 256 }
                }
            }))
            .unwrap();
            let usage = container_usage(stats).unwrap();
            assert_eq!(usage.sampled_at, sampled_at.parse().unwrap());
            assert_eq!(usage.cpu_usage_ns, Some(1_500_000_000));
            assert_eq!(usage.memory_usage_bytes, Some(1024));
            assert_eq!(usage.memory_working_set_bytes, Some(768));
            assert_eq!(usage.memory_limit_bytes, Some(4096));
        }
    }

    #[test]
    fn keeps_valid_partial_samples_and_zero_cpu_time() {
        let stats = serde_json::from_value(json!({
            "read": "2026-01-01T00:00:00Z",
            "cpu_stats": { "cpu_usage": { "total_usage": 0 } },
        }))
        .unwrap();
        let usage = container_usage(stats).unwrap();
        assert_eq!(usage.cpu_usage_ns, Some(0));
        assert_eq!(usage.memory_usage_bytes, None);
        assert_eq!(usage.memory_working_set_bytes, None);
        assert_eq!(usage.memory_limit_bytes, None);

        let stats = serde_json::from_value(json!({
            "read": "2026-01-01T00:00:00Z",
            "memory_stats": { "usage": 10, "stats": { "inactive_file": 20 } },
        }))
        .unwrap();
        let usage = container_usage(stats).unwrap();
        assert_eq!(usage.cpu_usage_ns, None);
        assert_eq!(usage.memory_usage_bytes, Some(10));
        assert_eq!(usage.memory_working_set_bytes, None);
    }

    #[test]
    fn skips_empty_stopped_frames_and_windows_cpu_units() {
        assert!(container_usage(ContainerStatsResponse::default()).is_none());
        for (os_type, read, num_procs) in [
            ("linux", "0001-01-01T00:00:00Z", 0),
            ("windows", "2026-01-01T00:00:00Z", 0),
            ("", "2026-01-01T00:00:00Z", 8),
        ] {
            let stats = serde_json::from_value(json!({
                "os_type": os_type,
                "read": read,
                "num_procs": num_procs,
                "cpu_stats": { "cpu_usage": { "total_usage": 0 } },
            }))
            .unwrap();
            assert!(container_usage(stats).is_none());
        }
    }
}
