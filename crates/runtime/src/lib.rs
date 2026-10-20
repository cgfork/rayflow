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

/// Applies a command entry to a backend, driving node state.
#[allow(async_fn_in_trait)]
pub trait CommandApply<B> {
    /// Error returned when applying a command entry.
    type Err: std::error::Error;

    /// Applies this command entry to `backend`.
    async fn apply(&self, backend: B) -> Result<(), Self::Err>;
}
