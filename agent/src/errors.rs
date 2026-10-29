#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    #[error("job claim lost: {0}")]
    ClaimLost(uuid::Uuid),
    #[error(transparent)]
    Container(#[from] crate::container::Error),
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
}
