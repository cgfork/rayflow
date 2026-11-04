//! Published graph read endpoint.

use ::graph::GraphStore;
use ::graph::types::DAG;
use axum::extract::{Path, Query, State};
use axum::{Json, Router, routing::get};
use serde::Deserialize;
use uuid::Uuid;

use crate::app::{App, AppError, Page, PageQuery};

/// Builds published graph routes.
pub(super) fn routes<S>() -> Router<App<S>>
where
    S: GraphStore<Uuid, G = DAG> + Clone + Send + Sync + 'static,
    S::load(..): Send,
    S::graphs(..): Send,
    S::Err: Send + Sync + 'static,
{
    Router::new()
        .route("/graphs", get(list_graphs::<S>))
        .route("/graphs/{graph_id}", get(read_graph::<S>))
        .route("/graphs/{graph_id}/shared-nodes", get(shared_nodes::<S>))
}

#[derive(Debug, Deserialize)]
pub struct SharedNodesQuery {
    pub from_version: u64,
    pub version: u64,
}

async fn shared_nodes<S>(
    Path(graph_id): Path<Uuid>,
    Query(query): Query<SharedNodesQuery>,
    State(app): State<App<S>>,
) -> Result<Json<Vec<Uuid>>, AppError>
where
    S: GraphStore<Uuid, G = DAG> + Clone,
    S::load(..): Send,
    S::Err: Send + Sync + 'static,
{
    app.shared_graph_nodes(graph_id, query.from_version, query.version)
        .await
        .map(Json)
}

#[derive(Debug, Deserialize)]
pub struct ListGraphsQuery {
    pub graph_id: Option<Uuid>,
}

async fn list_graphs<S>(
    State(app): State<App<S>>,
    Query(page): Query<PageQuery>,
    Query(query): Query<ListGraphsQuery>,
) -> Result<Json<Page<DAG>>, AppError>
where
    S: GraphStore<Uuid, G = DAG> + Clone + Send + Sync + 'static,
    S::graphs(..): Send,
    S::Err: Send + Sync + 'static,
{
    app.list_graphs(page, query.graph_id).await.map(Json)
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
    S::load(..): Send,
    S::Err: Send + Sync + 'static,
{
    state.read_graph(graph_id, query.version).await.map(Json)
}
