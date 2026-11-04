//! Command application and execution backend.

pub mod backend;
pub mod command;

pub use command::models;

use uuid::Uuid;

/// Error returned by the MySQL [`backend::Backend`].
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Database error.
    #[error(transparent)]
    Toasty(#[from] toasty::Error),
    /// The graph store returned an error.
    #[error(transparent)]
    Graph(Box<dyn std::error::Error + Send + Sync>),
    /// The run has already been started; it must be rerun instead.
    #[error("run already started; rerun instead")]
    AlreadyStarted,
    #[error("upgrade version {requested} must be newer than current version {current}")]
    InvalidUpgradeVersion { current: u64, requested: u64 },
    #[error("upgrade node must be shared between the current and target graph versions: {0}")]
    InvalidUpgradeNode(Uuid),
    /// A node referenced by a command is not part of the graph.
    #[error("node not found in graph: {0}")]
    NodeNotFound(Uuid),
    /// The node run for the requested node does not exist.
    #[error("node run not found: {0}")]
    NodeRunNotFound(Uuid),
    /// The run for the requested data and graph does not exist.
    #[error("run not found: data {data_id}, graph {graph_id}")]
    RunNotFound { data_id: Uuid, graph_id: Uuid },
    /// The node is not in a state required by the requested transition.
    #[error("node {node} is not {expected} (status: {status})")]
    InvalidTransition {
        node: Uuid,
        expected: String,
        status: String,
    },
    /// The node run changed between the read and the guarded update.
    #[error("node run was modified concurrently: {0}")]
    ConcurrentUpdate(Uuid),
    /// An append-only log operation failed.
    #[error(transparent)]
    Event(#[from] event::Error),
}

impl Error {
    /// Returns the stable Rayflow error code for this error variant.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Toasty(_) => "RAYFLOW_DATABASE_ERROR",
            Self::Graph(_) => "RAYFLOW_GRAPH_ERROR",
            Self::AlreadyStarted => "RAYFLOW_RUN_ALREADY_STARTED",
            Self::InvalidUpgradeVersion { .. } => "RAYFLOW_INVALID_UPGRADE_VERSION",
            Self::InvalidUpgradeNode(_) => "RAYFLOW_INVALID_UPGRADE_NODE",
            Self::NodeNotFound(_) => "RAYFLOW_NODE_NOT_FOUND",
            Self::NodeRunNotFound(_) => "RAYFLOW_NODE_RUN_NOT_FOUND",
            Self::RunNotFound { .. } => "RAYFLOW_RUN_NOT_FOUND",
            Self::InvalidTransition { .. } => "RAYFLOW_INVALID_TRANSITION",
            Self::ConcurrentUpdate(_) => "RAYFLOW_CONCURRENT_UPDATE",
            Self::Event(_) => "RAYFLOW_EVENT_ERROR",
        }
    }

    pub(crate) fn contains_toasty_error(&self) -> bool {
        let mut source = Some(self as &(dyn std::error::Error + 'static));
        while let Some(error) = source {
            if error.downcast_ref::<toasty::Error>().is_some() {
                return true;
            }
            source = error.source();
        }
        false
    }
}

/// Applies a command entry to a backend, driving node state.
#[allow(async_fn_in_trait)]
pub trait CommandApply<B> {
    /// Error that prevented processing and should be retried.
    type Err: std::error::Error;

    /// Applies this command entry to `backend`. Permanent failures are recorded
    /// by the backend and return `Ok(())`.
    async fn apply(&self, backend: B) -> Result<(), Self::Err>;
}
