//! Published graph read endpoint.

use ::graph::GraphStore;
use ::graph::types::DAG;
use axum::extract::{Path, Query, State};
use axum::{Json, Router, routing::get};
use serde::Deserialize;
use uuid::Uuid;

use crate::app::{App, AppError};

/// Builds published graph routes.
pub(super) fn routes<S>() -> Router<App<S>>
where
    S: GraphStore<Uuid, G = DAG> + Clone + Send + Sync + 'static,
    S::Err: Send + Sync + 'static,
{
    Router::new().route("/graphs/{graph_id}", get(read_graph::<S>))
}

/// Optional version selector for a published graph read.
#[derive(Debug, Default, Deserialize)]
pub struct ReadGraphQuery {
    /// Loads this exact graph version. When omitted, the store loads its latest
    /// published version.
    pub version: Option<u64>,
}

async fn read_graph<S>(
    Path(graph_id): Path<Uuid>,
    Query(query): Query<ReadGraphQuery>,
    State(state): State<App<S>>,
) -> Result<Json<DAG>, AppError>
where
    S: GraphStore<Uuid, G = DAG> + Clone + Send + Sync + 'static,
    S::Err: Send + Sync + 'static,
{
    state.read_graph(graph_id, query.version).await.map(Json)
}
