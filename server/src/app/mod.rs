mod job;
mod offset;
mod operation;
mod state;
mod task_event;

pub use job::PolledJob;
pub use offset::Offsets;
pub use state::{GraphState, NodeState};

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use std::time::Duration;
use toasty::Db;
use uuid::Uuid;

use crate::Config;

/// Application over a MySQL backend and a graph store.
///
/// `App` owns the shared command offset and hands out a [`RunApp`] for a
/// specific `(data_id, graph_id)` run through [`run_with`](Self::run_with). It
/// is the state the HTTP API is served from.
#[derive(Clone)]
pub struct App<S> {
    db: Db,
    offsets: Offsets,
    graph_store: S,
    job_lease: Duration,
}

/// A single run: the commands that drive it and the state it produces.
pub struct RunApp<S> {
    db: Db,
    data_id: Uuid,
    graph_id: Uuid,
    offsets: Offsets,
    graph_store: S,
}

impl<S> App<S> {
    /// Creates an app over `db`, tracking the consumed command offset and
    /// serving graphs from `graph_store`.
    pub fn new(db: Db, config: Config, graph_store: S) -> Self {
        let offsets = Offsets::new();
        Self {
            db,
            offsets,
            graph_store,
            job_lease: config.job_lease(),
        }
    }

    /// Connects to `url`, registering runtime and scheduling models, and builds an
    /// app over `db` (see [`new`](Self::new)).
    pub async fn connect(url: &str, config: Config, graph_store: S) -> Result<Self, toasty::Error> {
        let mut builder = Db::builder();
        builder.models(scheduler::models());
        let db = builder.connect(url).await?;
        Ok(Self::new(db, config, graph_store))
    }

    /// Returns the shared command offset.
    ///
    /// A consumer that advances the offset calls [`Offsets::set`], which wakes
    /// every waiter.
    pub fn offsets(&self) -> &Offsets {
        &self.offsets
    }

    /// Builds the [`RunApp`] for the run identified by `data_id` and `graph_id`.
    pub fn run_with(&self, data_id: Uuid, graph_id: Uuid) -> RunApp<S>
    where
        S: Clone,
    {
        RunApp {
            db: self.db.clone(),
            data_id,
            graph_id,
            offsets: self.offsets.clone(),
            graph_store: self.graph_store.clone(),
        }
    }
}

/// Error returned by an app operation.
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    /// The requested run does not exist.
    #[error("run not found: data {data_id}, graph {graph_id}")]
    RunNotFound {
        /// Data identifier of the run.
        data_id: Uuid,
        /// Graph identifier of the run.
        graph_id: Uuid,
    },
    /// The requested node has no run at the run's current revision.
    #[error("node run not found: {0}")]
    NodeRunNotFound(Uuid),
    /// The requested task does not exist.
    #[error("task not found: {0}")]
    TaskNotFound(Uuid),
    #[error("job not found: {0}")]
    JobNotFound(Uuid),
    #[error("job claim expired, changed owner, or no longer accepts this status: {0}")]
    JobConflict(Uuid),
    #[error("agents may only report Pending, Succeeded, Failed, or Terminated")]
    InvalidJobStatus,
    /// The requested node is not part of the graph.
    #[error("node not found in graph: {0}")]
    NodeNotFound(Uuid),
    /// The graph store returned an error.
    #[error("graph: {0}")]
    Graph(Box<dyn std::error::Error + Send + Sync>),
    /// The backend returned an error.
    #[error(transparent)]
    Backend(#[from] runtime::Error),
    /// Job persistence failed.
    #[error(transparent)]
    Scheduled(scheduler::Error),
}

impl From<event::Error> for AppError {
    fn from(error: event::Error) -> Self {
        Self::Backend(error.into())
    }
}

impl From<scheduler::Error> for AppError {
    fn from(error: scheduler::Error) -> Self {
        match error {
            scheduler::Error::AgentJobNotFound(job_id) => Self::JobNotFound(job_id),
            scheduler::Error::JobConflict(job_id) => Self::JobConflict(job_id),
            error => Self::Scheduled(error),
        }
    }
}

impl From<toasty::Error> for AppError {
    fn from(error: toasty::Error) -> Self {
        Self::Backend(error.into())
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = match self {
            AppError::RunNotFound { .. }
            | AppError::NodeRunNotFound(_)
            | AppError::TaskNotFound(_)
            | AppError::JobNotFound(_)
            | AppError::NodeNotFound(_) => StatusCode::NOT_FOUND,
            AppError::JobConflict(_) => StatusCode::CONFLICT,
            AppError::InvalidJobStatus => StatusCode::BAD_REQUEST,
            AppError::Graph(_) | AppError::Backend(_) | AppError::Scheduled(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };
        (
            status,
            Json(ErrorBody {
                error: self.to_string(),
            }),
        )
            .into_response()
    }
}

/// Error body returned by an app operation.
#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
}

#[cfg(test)]
pub(crate) mod test {
    use std::convert::Infallible;

    use graph::GraphStore;
    use graph::types::DAG;
    use toasty::Db;
    use uuid::Uuid;

    /// An in-memory [`GraphStore`] that serves a single [`DAG`].
    #[derive(Clone)]
    pub(crate) struct TestGraphStore(pub DAG);

    impl Default for TestGraphStore {
        fn default() -> Self {
            Self(DAG {
                id: Uuid::nil(),
                name: String::new(),
                version: 0,
                nodes: Vec::new(),
                edges: Vec::new(),
            })
        }
    }

    impl GraphStore<Uuid> for TestGraphStore {
        type G = DAG;
        type Err = Infallible;

        async fn load(&self, _graph_id: Uuid, _version: Option<u64>) -> Result<DAG, Infallible> {
            Ok(self.0.clone())
        }
    }

    /// Connects and rebuilds the schema from the Toasty models, or returns
    /// `None` when no `DATABASE_URL` is configured.
    pub(crate) async fn setup() -> Option<Db> {
        let url = std::env::var("DATABASE_URL").ok()?;

        let mut builder = Db::builder();
        builder.models(scheduler::models());
        let db = builder
            .connect(&url)
            .await
            .expect("connect to DATABASE_URL");

        db.reset_db().await.expect("reset database");
        db.push_schema().await.expect("push schema");

        Some(db)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_not_found_maps_to_404() {
        let error = AppError::RunNotFound {
            data_id: Uuid::nil(),
            graph_id: Uuid::nil(),
        };
        assert_eq!(error.into_response().status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn node_not_found_maps_to_404() {
        let error = AppError::NodeNotFound(Uuid::nil());
        assert_eq!(error.into_response().status(), StatusCode::NOT_FOUND);
    }
}
