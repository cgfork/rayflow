//! Published graphs, editable drafts, and publication endpoints.

use ::graph::GraphStore;
use ::graph::types::DAG;
use axum::extract::{Path, Query, State};
use axum::{
    Extension, Json, Router,
    routing::{get, post},
};
use axum::{http::StatusCode, response::IntoResponse};
use serde::Deserialize;
use uuid::Uuid;

use crate::app::{App, AppError, Page, PageQuery};

/// Builds graph read, draft, and publish routes.
pub(super) fn routes<S>() -> Router<App<S>>
where
    S: GraphStore<Uuid, G = DAG> + Clone + Send + Sync + 'static,
    S::load(..): Send,
    S::graphs(..): Send,
    S::Err: Send + Sync + 'static,
{
    Router::new()
        .route("/graphs", get(list_graphs::<S>))
        .route("/graph_drafts", get(list_drafts::<S>))
        .route(
            "/graphs/{graph_id}/draft",
            get(read_graph_draft::<S>)
                .put(write_graph_draft::<S>)
                .post(write_graph_draft::<S>),
        )
        .route("/graphs/{graph_id}/publish", post(publish_graph_draft::<S>))
        .route("/graphs/{graph_id}", get(read_graph::<S>))
        .route("/graphs/{graph_id}/shared-nodes", get(shared_nodes::<S>))
        .route("/graphs/{graph_id}/diff", get(compare_graphs::<S>))
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
struct GraphDiffQuery {
    from_graph_id: Option<Uuid>,
    from_version: u64,
    version: u64,
}

async fn compare_graphs<S>(
    Path(graph_id): Path<Uuid>,
    Query(query): Query<GraphDiffQuery>,
    State(app): State<App<S>>,
) -> Result<impl IntoResponse, AppError>
where
    S: GraphStore<Uuid, G = DAG> + Clone,
    S::load(..): Send,
    S::Err: Send + Sync + 'static,
{
    app.compare_graphs(
        query.from_graph_id.unwrap_or(graph_id),
        query.from_version,
        graph_id,
        query.version,
    )
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

async fn list_drafts<S>(
    State(app): State<App<S>>,
    Query(page): Query<PageQuery>,
) -> Result<Json<Page<DAG>>, AppError> {
    app.list_drafts(page).await.map(Json)
}

/// Request body for writing a graph draft.
#[derive(Debug, Deserialize)]
pub struct WriteGraphDraftRequest {
    /// Complete graph definition to store as the draft.
    pub graph: DAG,
    /// Actor recorded in `updated_by` or `created_by`.
    #[serde(default)]
    pub actor: String,
}

/// Request body for publishing a graph draft.
#[derive(Debug, Deserialize)]
pub struct PublishGraphDraftRequest {
    /// Actor recorded in the published graph audit fields.
    #[serde(default)]
    pub actor: String,
}

async fn read_graph_draft<S>(
    Path(graph_id): Path<Uuid>,
    State(state): State<App<S>>,
) -> Result<Json<DAG>, AppError> {
    Ok(Json(state.read_graph_draft(graph_id).await?))
}

async fn write_graph_draft<S>(
    Path(graph_id): Path<Uuid>,
    State(state): State<App<S>>,
    user: Option<Extension<crate::auth::User>>,
    Json(request): Json<WriteGraphDraftRequest>,
) -> Result<impl IntoResponse, AppError> {
    let graph = state
        .write_graph_draft(
            graph_id,
            request.graph,
            user.as_ref()
                .map(|user| user.id.as_str())
                .unwrap_or(&request.actor),
        )
        .await?;
    Ok((StatusCode::OK, Json(graph)))
}

async fn publish_graph_draft<S>(
    Path(graph_id): Path<Uuid>,
    State(state): State<App<S>>,
    user: Option<Extension<crate::auth::User>>,
    Json(request): Json<PublishGraphDraftRequest>,
) -> Result<impl IntoResponse, AppError> {
    let graph = state
        .publish_graph_draft(
            graph_id,
            user.as_ref()
                .map(|user| user.id.as_str())
                .unwrap_or(&request.actor),
        )
        .await?;
    Ok((StatusCode::CREATED, Json(graph)))
}
