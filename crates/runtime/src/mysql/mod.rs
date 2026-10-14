//! MySQL-backed task backend.
//!
//! Most operations are expressed with the Toasty [`Model`](toasty::Model)s in
//! [`model`]; register them on the [`Db`](toasty::Db) with [`models`] (or let
//! [`Backend::connect`] do it) before issuing queries. The task, command, and
//! task-event pollers first fetch the relevant per-shard offsets, then query the
//! log with those offsets applied.

use toasty::Db;
use uuid::Uuid;

mod command;
mod entry;
mod event;
mod lock;
pub mod model;
mod scheduled;
mod task;

pub use model::models;

/// Error returned by the MySQL [`Backend`].
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Database error.
    #[error(transparent)]
    Toasty(#[from] toasty::Error),
    /// The task with the given id does not exist.
    #[error("task not found: {0}")]
    TaskNotFound(Uuid),
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
    /// A stored command payload could not be (de)serialized.
    #[error("invalid command payload: {0}")]
    InvalidCommand(#[from] serde_json::Error),
    /// A stored task event name could not be parsed.
    #[error("invalid task event: {0}")]
    InvalidEvent(String),
    /// A scheduled task with the given task id does not exist.
    #[error("scheduled task not found: {0}")]
    ScheduledTaskNotFound(Uuid),
    /// A stored scheduled task status could not be parsed.
    #[error("invalid status: {0}")]
    InvalidStatus(String),
}

/// A MySQL-backed task backend.
#[derive(Clone)]
pub struct Backend<S> {
    db: Db,
    shards: u32,
    graph_store: S,
}

impl<S> Backend<S> {
    /// Creates a backend from an existing database handle, the number of shards in
    /// the shard table, and the graph store used to resolve graph structure.
    pub fn new(db: Db, shards: u32, graph_store: S) -> Self {
        assert!(shards > 0, "shards must be at least 1");
        Self {
            db,
            shards,
            graph_store,
        }
    }

    /// Connects to `url`, registering the runtime's Toasty models.
    ///
    /// This is the recommended constructor: the models must be registered on the
    /// [`Db`] for the query builders the backend uses to work. Combine with
    /// [`models`] when constructing the [`Db`] yourself.
    pub async fn connect(url: &str, shards: u32, graph_store: S) -> Result<Self, Error> {
        let mut builder = toasty::Db::builder();
        builder.models(models());
        let db = builder.connect(url).await?;
        Ok(Self::new(db, shards, graph_store))
    }

    /// Returns the underlying database handle.
    pub fn db(&self) -> &Db {
        &self.db
    }

    /// Returns a mutable reference to the underlying database handle.
    pub fn db_mut(&mut self) -> &mut Db {
        &mut self.db
    }
}
