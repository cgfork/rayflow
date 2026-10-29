use bollard::{
    Docker,
    errors::Error,
    models::{ContainerCreateBody, ContainerStateStatusEnum, EventMessage, HostConfig},
    query_parameters::{
        CreateContainerOptionsBuilder, CreateImageOptionsBuilder, RemoveContainerOptionsBuilder,
    },
};
use futures_util::{Stream, StreamExt};
use serde::Deserialize;

use super::{Connector, Container, State};

pub type Event = EventMessage;

#[derive(Clone, Debug, Default, Deserialize)]
pub struct DockerConfig {
    pub image: String,
}

#[derive(Clone, Debug)]
pub struct DockerContainer {
    docker: Docker,
    id: String,
}

impl DockerContainer {
    pub fn new(docker: Docker, id: impl Into<String>) -> Self {
        Self {
            docker,
            id: id.into(),
        }
    }
}

impl Container for DockerContainer {
    type Err = Error;
    type Config = DockerConfig;

    fn name(&self) -> &str {
        &self.id
    }

    async fn start(&self, _config: Self::Config) -> Result<(), Self::Err> {
        match self.docker.start_container(&self.id, None).await {
            Err(Error::DockerResponseServerError {
                status_code: 304, ..
            }) => Ok(()),
            result => result,
        }
    }

    async fn wait(&self) -> Result<i64, Self::Err> {
        match self.docker.wait_container(&self.id, None).next().await {
            Some(Ok(result)) => Ok(result.status_code),
            Some(Err(Error::DockerContainerWaitError { code, .. })) => Ok(code),
            Some(Err(error)) => Err(error),
            None => Err(Error::IOError {
                err: std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "container wait ended"),
            }),
        }
    }

    async fn stop(&self, _config: Self::Config) -> Result<(), Self::Err> {
        self.docker.stop_container(&self.id, None).await
    }

    async fn kill(&self) -> Result<(), Self::Err> {
        match self.docker.kill_container(&self.id, None).await {
            Err(Error::DockerResponseServerError {
                status_code: 409, ..
            }) => Ok(()),
            result => result,
        }
    }

    async fn remove(&self, _config: Self::Config) -> Result<(), Self::Err> {
        self.docker.remove_container(&self.id, None).await
    }
}

#[derive(Clone, Debug)]
pub struct DockerConnector {
    docker: Docker,
}

impl DockerConnector {
    pub fn new(docker: Docker) -> Self {
        Self { docker }
    }

    pub fn connect_with_defaults() -> Result<Self, Error> {
        Docker::connect_with_defaults().map(Self::new)
    }

    pub fn container(&self, id: &str) -> DockerContainer {
        DockerContainer::new(self.docker.clone(), id)
    }

    pub(crate) async fn prepare(&self, config: DockerConfig) -> Result<(), Error> {
        match self.docker.inspect_image(&config.image).await {
            Ok(_) => return Ok(()),
            Err(Error::DockerResponseServerError {
                status_code: 404, ..
            }) => {}
            Err(error) => return Err(error),
        }
        let options = CreateImageOptionsBuilder::new()
            .from_image(&config.image)
            .build();
        let mut pull = self.docker.create_image(Some(options), None, None);
        while let Some(progress) = pull.next().await {
            progress?;
        }
        Ok(())
    }

    pub(crate) async fn inspect(&self, id: &str) -> Result<Option<State>, Error> {
        let response = match self.docker.inspect_container(id, None).await {
            Ok(response) => response,
            Err(Error::DockerResponseServerError {
                status_code: 404, ..
            }) => return Ok(None),
            Err(error) => return Err(error),
        };
        let state = response.state.ok_or_else(|| Error::IOError {
            err: std::io::Error::other("container state is missing"),
        })?;
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
            return Ok(Some(State::Running {
                pid: state.pid,
                started_at: state.started_at.and_then(|value| value.parse().ok()),
            }));
        }
        match state.status {
            Some(ContainerStateStatusEnum::CREATED) => Ok(Some(State::Created)),
            Some(ContainerStateStatusEnum::EXITED | ContainerStateStatusEnum::DEAD) => {
                Ok(Some(State::Exited(
                    state
                        .exit_code
                        .and_then(|value| value.try_into().ok())
                        .unwrap_or(1),
                )))
            }
            _ => Err(Error::IOError {
                err: std::io::Error::other("container is changing state"),
            }),
        }
    }

    pub(crate) async fn create_job(
        &self,
        name: &str,
        config: DockerConfig,
        cpu: u32,
        memory: u64,
    ) -> Result<DockerContainer, Error> {
        let memory = i64::try_from(memory).map_err(|error| Error::IOError {
            err: std::io::Error::new(std::io::ErrorKind::InvalidInput, error),
        })?;
        let body = ContainerCreateBody {
            image: Some(config.image),
            host_config: Some(HostConfig {
                nano_cpus: (cpu > 0).then_some(i64::from(cpu) * 1_000_000_000),
                memory: (memory > 0).then_some(memory),
                ..Default::default()
            }),
            ..Default::default()
        };
        let options = CreateContainerOptionsBuilder::new().name(name).build();
        match self.docker.create_container(Some(options), body).await {
            Ok(response) => Ok(self.container(&response.id)),
            Err(Error::DockerResponseServerError {
                status_code: 409, ..
            }) => Ok(self.container(name)),
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn remove(&self, id: &str) -> Result<(), Error> {
        let options = RemoveContainerOptionsBuilder::new().force(true).build();
        match self.docker.remove_container(id, Some(options)).await {
            Err(Error::DockerResponseServerError {
                status_code: 404, ..
            }) => Ok(()),
            result => result,
        }
    }
}

impl Connector for DockerConnector {
    type Err = Error;
    type Container = DockerContainer;
    type Config = DockerConfig;
    type Event<'a> = Event;

    async fn create(&self, config: Self::Config) -> Result<Self::Container, Self::Err> {
        let body = ContainerCreateBody {
            image: Some(config.image),
            ..Default::default()
        };
        let response = self.docker.create_container(None, body).await?;
        Ok(DockerContainer::new(self.docker.clone(), response.id))
    }

    fn events(&self) -> impl Stream<Item = Result<Self::Event<'_>, Self::Err>> + Send + '_ {
        self.docker.events(None)
    }
}
