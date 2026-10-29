use futures_util::{Stream, StreamExt};
use scheduler::Job;

use self::docker::{DockerConfig, DockerConnector, DockerContainer};

pub mod docker;

#[derive(Clone, Debug)]
pub(crate) enum State {
    Created,
    Running {
        pid: Option<i64>,
        started_at: Option<jiff::Timestamp>,
    },
    Exited(i32),
}

#[allow(async_fn_in_trait)]
pub trait Container: Send + Sync {
    type Err: std::error::Error + Send + Sync + 'static;
    type Config: Send + 'static;
    fn name(&self) -> &str;
    async fn start(&self, config: Self::Config) -> Result<(), Self::Err>;
    async fn wait(&self) -> Result<i64, Self::Err>;
    async fn stop(&self, config: Self::Config) -> Result<(), Self::Err>;
    async fn kill(&self) -> Result<(), Self::Err>;
    async fn remove(&self, config: Self::Config) -> Result<(), Self::Err>;
}

#[allow(async_fn_in_trait)]
pub trait Connector: Send + Sync {
    type Err: std::error::Error + Send + Sync + 'static;
    type Container: Container;
    type Config: Send + 'static;
    /// Event payload, which may borrow from the connector.
    type Event<'a>: Send
    where
        Self: 'a;
    async fn create(&self, body: Self::Config) -> Result<Self::Container, Self::Err>;
    /// Streams events and errors from the execution provider.
    fn events(&self) -> impl Stream<Item = Result<Self::Event<'_>, Self::Err>> + Send + '_;
}

#[derive(Clone, Debug)]
pub enum ContainerBackend {
    Docker(DockerContainer),
}

impl Container for ContainerBackend {
    type Err = Error;
    type Config = Config;

    fn name(&self) -> &str {
        match self {
            Self::Docker(container) => container.name(),
        }
    }

    async fn start(&self, config: Self::Config) -> Result<(), Self::Err> {
        match (self, config) {
            (Self::Docker(container), Config::Docker(config)) => {
                container.start(config).await.map_err(Error::from)
            }
        }
    }

    async fn wait(&self) -> Result<i64, Self::Err> {
        match self {
            Self::Docker(container) => container.wait().await.map_err(Error::from),
        }
    }

    async fn kill(&self) -> Result<(), Self::Err> {
        match self {
            Self::Docker(container) => container.kill().await.map_err(Error::from),
        }
    }

    async fn stop(&self, config: Self::Config) -> Result<(), Self::Err> {
        match (self, config) {
            (Self::Docker(container), Config::Docker(config)) => {
                container.stop(config).await.map_err(Error::from)
            }
        }
    }

    async fn remove(&self, config: Self::Config) -> Result<(), Self::Err> {
        match (self, config) {
            (Self::Docker(container), Config::Docker(config)) => {
                container.remove(config).await.map_err(Error::from)
            }
        }
    }
}

impl From<DockerContainer> for ContainerBackend {
    fn from(container: DockerContainer) -> Self {
        Self::Docker(container)
    }
}

#[derive(Clone, Debug)]
pub enum Provider {
    Docker(DockerConnector),
}

#[derive(Clone, Debug)]
pub enum Event {
    Docker(docker::Event),
}

impl Provider {
    pub fn container(&self, id: &str) -> ContainerBackend {
        match self {
            Self::Docker(connector) => connector.container(id).into(),
        }
    }

    pub(crate) async fn prepare(&self, config: Config) -> Result<(), Error> {
        match (self, config) {
            (Self::Docker(connector), Config::Docker(config)) => {
                Ok(connector.prepare(config).await?)
            }
        }
    }

    pub(crate) async fn inspect(&self, id: &str) -> Result<Option<State>, Error> {
        match self {
            Self::Docker(connector) => Ok(connector.inspect(id).await?),
        }
    }

    pub(crate) async fn create_job(
        &self,
        job: &Job,
        config: Config,
    ) -> Result<ContainerBackend, Error> {
        match (self, config) {
            (Self::Docker(connector), Config::Docker(config)) => Ok(connector
                .create_job(
                    &format!("rayflow-{}", job.uuid),
                    config,
                    job.required_cpu_cores,
                    job.required_memory_bytes,
                )
                .await?
                .into()),
        }
    }

    pub(crate) async fn remove(&self, id: &str) -> Result<(), Error> {
        match self {
            Self::Docker(connector) => Ok(connector.remove(id).await?),
        }
    }
}

impl Connector for Provider {
    type Err = Error;
    type Container = ContainerBackend;
    type Config = Config;
    type Event<'a> = Event;

    async fn create(&self, config: Self::Config) -> Result<Self::Container, Self::Err> {
        match (self, config) {
            (Self::Docker(connector), Config::Docker(config)) => connector
                .create(config)
                .await
                .map(ContainerBackend::Docker)
                .map_err(Error::from),
        }
    }

    fn events(&self) -> impl Stream<Item = Result<Self::Event<'_>, Self::Err>> + Send + '_ {
        match self {
            Self::Docker(connector) => connector
                .events()
                .map(|event| event.map(Event::Docker).map_err(Error::from)),
        }
    }
}

impl From<DockerConnector> for Provider {
    fn from(connector: DockerConnector) -> Self {
        Self::Docker(connector)
    }
}

#[derive(Clone, Debug)]
pub enum Config {
    Docker(DockerConfig),
}

impl TryFrom<Job> for Config {
    type Error = Error;
    fn try_from(value: Job) -> Result<Self, Self::Error> {
        if value.node_config.is_unloaded() {
            return Err(Error::MissingConfig);
        }
        let config = value
            .node_config
            .get()
            .as_ref()
            .ok_or(Error::MissingConfig)?;
        let config: DockerConfig = serde_json::from_value(config.config.clone())?;
        if config.image.trim().is_empty() {
            return Err(Error::MissingImage);
        }
        Ok(Self::Docker(config))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    DockerError(#[from] bollard::errors::Error),
    #[error(transparent)]
    Config(#[from] serde_json::Error),
    #[error("job node configuration is missing")]
    MissingConfig,
    #[error("job Docker image is empty")]
    MissingImage,
}
