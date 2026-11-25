use std::io;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("artifact: {0}")]
    Artifact(String),
    #[error(transparent)]
    Config(#[from] config::ConfigError),
    #[error(transparent)]
    Docker(#[from] bollard::errors::Error),
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    #[error("unexpected HTTP status {0}")]
    UnexpectedStatus(reqwest::StatusCode),
    #[error("archive commit acknowledged catalog {actual}, expected {expected}")]
    CatalogMismatch {
        expected: uuid::Uuid,
        actual: uuid::Uuid,
    },
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("job claim lost: {0}")]
    ClaimLost(uuid::Uuid),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Task(#[from] tokio::task::JoinError),
    #[error("invalid agent settings: {0}")]
    Settings(&'static str),
    #[error("agent is already running")]
    AlreadyRunning,
    #[error("job is still running: {0}")]
    JobRunning(uuid::Uuid),
    #[error("job manager: {0}")]
    JobManager(String),
    #[error("job node configuration is missing")]
    MissingConfig,
    #[error("job Docker image is empty")]
    MissingImage,
    #[error("container provider error: {0}")]
    Provider(String),
}

impl Error {
    pub fn kind(&self) -> io::ErrorKind {
        match self {
            Self::Io(error) => error.kind(),
            _ => io::ErrorKind::Other,
        }
    }
}
