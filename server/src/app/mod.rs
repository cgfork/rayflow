mod agent;
mod catalog;
mod error;
mod graph_draft;
#[path = "graph.rs"]
mod graph_read;
mod job;
mod offset;
mod operation;
mod pagination;
mod state;
mod task_event;

pub use agent::{AgentInstance, AgentView};
pub use error::{AppError, ErrorBody};
pub use offset::Offsets;
pub use pagination::{Page, PageQuery};
pub use state::{GraphState, NodeState, RunView};

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
    pub(crate) config: Config,
    pub(crate) auth: Option<crate::auth::Auth>,
    offsets: Offsets,
    graph_store: S,
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
            config,
            auth: None,
            offsets,
            graph_store,
        }
    }

    /// Installs shared login providers and session storage for the HTTP router.
    pub fn with_auth(mut self, auth: crate::auth::Auth) -> Self {
        self.config.auth = auth.config().clone();
        self.auth = Some(auth);
        self
    }

    /// Connects to `url`, registering runtime and scheduling models, and builds an
    /// app over `db` (see [`new`](Self::new)).
    pub async fn connect(url: &str, config: Config, graph_store: S) -> Result<Self, toasty::Error> {
        let mut builder = Db::builder();
        builder.models(crate::models());
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
                ..DAG::default()
            })
        }
    }

    impl GraphStore<Uuid> for TestGraphStore {
        type G = DAG;
        type Err = Infallible;

        async fn graphs(
            &mut self,
            page: graph::PageRequest,
            graph_id: Option<Uuid>,
        ) -> Result<graph::Page<DAG>, Infallible> {
            Ok(page.select(
                std::iter::once((1, self.0.clone()))
                    .filter(|(_, graph)| graph_id.is_none_or(|id| graph.id == id)),
            ))
        }

        async fn load(
            &mut self,
            _graph_id: Uuid,
            _version: Option<u64>,
        ) -> Result<DAG, Infallible> {
            Ok(self.0.clone())
        }
    }

    /// Connects and creates isolated tables from the Toasty models, or returns
    /// `None` when no `DATABASE_URL` is configured.
    pub(crate) async fn setup() -> Option<Db> {
        let url = std::env::var("DATABASE_URL").ok()?;

        let mut builder = Db::builder();
        builder.models(crate::models());
        builder.table_name_prefix(&format!("server_{}_", Uuid::new_v4().simple()));
        let db = builder
            .connect(&url)
            .await
            .expect("connect to DATABASE_URL");

        db.push_schema().await.expect("push schema");

        Some(db)
    }
}

#[cfg(test)]
mod tests {
    use axum::{http::StatusCode, response::IntoResponse};

    use super::*;

    #[test]
    fn graph_validation_retains_context_and_bad_request_status() {
        use std::error::Error;

        let cause = graph::validator::WorkflowValidationError::InvalidResources {
            node: "build".into(),
        };
        let error = AppError::from(graph::store::Error::InvalidDraft(cause.clone()));
        assert_eq!(
            error
                .source()
                .unwrap()
                .downcast_ref::<graph::validator::WorkflowValidationError>(),
            Some(&cause)
        );
        assert_eq!(
            error.to_string(),
            "invalid graph: build: CPU cores and memory must be positive"
        );
        assert_eq!(error.into_response().status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn storage_validation_preserves_bad_request_status() {
        let error = AppError::from(crate::storage::Error::InvalidBucket);
        assert_eq!(
            error.to_string(),
            "storage.bucket must be an S3 bucket name"
        );
        assert_eq!(error.into_response().status(), StatusCode::BAD_REQUEST);
        let error = AppError::from(::catalog::path::PathError);
        assert_eq!(error.into_response().status(), StatusCode::BAD_REQUEST);
    }

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
