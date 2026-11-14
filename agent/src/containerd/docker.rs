//! Docker-backed `Containerd` implementation.

use std::{
    path::{Path, PathBuf},
    pin::Pin,
};

use bollard::{
    Docker,
    errors::Error as DockerError,
    models::{ContainerCreateBody, ContainerStateStatusEnum, HostConfig, Mount, MountType},
    query_parameters::{
        CreateContainerOptionsBuilder, CreateImageOptionsBuilder, RemoveContainerOptionsBuilder,
    },
};
use futures_util::{Stream, StreamExt};
use jiff::Timestamp;
use serde::{Deserialize, Serialize};

use crate::errors::Error;

use super::{Container, ContainerAction, ContainerEvent, ContainerState, Containerd, ExitStatus};

/// The Docker container creation parameters derived from a pinned job.
///
/// This type is deliberately defined in the Docker module rather than in the
/// provider-neutral module. Other container providers can expose completely
/// different specifications through [`Containerd::Spec`](super::Containerd::Spec).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DockerSpec {
    #[serde(default)]
    pub files: Option<DockerFiles>,
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
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DockerFiles {
    pub inputs: PathBuf,
    pub outputs: PathBuf,
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
            files: None,
            name: format!("rayflow-{}", job.uuid),
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
        })
    }
}

/// A Docker-backed implementation of the provider boundary.
#[derive(Clone, Debug)]
pub struct DockerContainerd {
    docker: Docker,
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
        Self { docker }
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
                started_at: state.started_at.and_then(|value| value.parse().ok()),
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
                    finished_at: state.finished_at.and_then(|value| value.parse().ok()),
                })
            }
            _ => Err(Error::Io(std::io::Error::other(
                "container is changing state",
            ))),
        }
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

impl Containerd for DockerContainerd {
    type Container = DockerContainer;
    type Spec<'a> = DockerSpec;

    fn name(&self) -> &'static str {
        "docker"
    }

    fn spec<'a>(&'a self, job: &'a scheduler::Job) -> Result<Self::Spec<'a>, Error> {
        DockerSpec::try_from(job)
    }

    fn spec_with_files<'a>(
        &'a self,
        job: &'a scheduler::Job,
        inputs: &Path,
        outputs: &Path,
    ) -> Result<Self::Spec<'a>, Error> {
        let mut spec = DockerSpec::try_from(job)?;
        spec.files = Some(DockerFiles {
            inputs: inputs.to_path_buf(),
            outputs: outputs.to_path_buf(),
        });
        let env = spec.env.get_or_insert_with(Vec::new);
        env.retain(|value| {
            !value.starts_with("RAYFLOW_INPUT_DIR=") && !value.starts_with("RAYFLOW_OUTPUT_DIR=")
        });
        env.extend([
            "RAYFLOW_INPUT_DIR=/data/inputs".into(),
            "RAYFLOW_OUTPUT_DIR=/data/outputs".into(),
        ]);
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
        let memory = i64::try_from(spec.memory_bytes).map_err(|error| {
            Error::Io(std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
        })?;
        let body = ContainerCreateBody {
            image: Some(spec.image.clone()),
            cmd: spec.command.clone(),
            env: spec.env.clone(),
            working_dir: spec.working_dir.clone(),
            host_config: Some(HostConfig {
                mounts: spec.files.as_ref().map(|files| {
                    vec![
                        Mount {
                            typ: Some(MountType::BIND),
                            source: Some(files.inputs.to_string_lossy().into_owned()),
                            target: Some("/data/inputs".into()),
                            read_only: Some(true),
                            ..Default::default()
                        },
                        Mount {
                            typ: Some(MountType::BIND),
                            source: Some(files.outputs.to_string_lossy().into_owned()),
                            target: Some("/data/outputs".into()),
                            read_only: Some(false),
                            ..Default::default()
                        },
                    ]
                }),
                nano_cpus: (spec.cpu_cores > 0)
                    .then_some(i64::from(spec.cpu_cores) * 1_000_000_000),
                memory: (memory > 0).then_some(memory),
                ..Default::default()
            }),
            ..Default::default()
        };
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

pub type DockerConnector = DockerContainerd;
