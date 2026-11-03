//! Graph draft read, write, and publish endpoints.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use graph::GraphStore;
use graph::types::DAG;
use serde::Deserialize;
use uuid::Uuid;

use crate::app::{App, AppError, Page, PageQuery};

/// Builds graph draft routes.
pub(super) fn routes<S>() -> Router<App<S>>
where
    S: GraphStore<Uuid, G = DAG> + Clone + Send + Sync + 'static,
    S::Err: Send + Sync + 'static,
{
    Router::new()
        .route("/graph_drafts", get(list_drafts::<S>))
        .route(
            "/graphs/{graph_id}/draft",
            get(read_graph_draft::<S>)
                .put(write_graph_draft::<S>)
                .post(write_graph_draft::<S>),
        )
        .route("/graphs/{graph_id}/publish", post(publish_graph_draft::<S>))
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
) -> Result<Json<DAG>, AppError>
where
    S: GraphStore<Uuid, G = DAG> + Clone + Send + Sync + 'static,
    S::Err: Send + Sync + 'static,
{
    Ok(Json(state.read_graph_draft(graph_id).await?))
}

async fn write_graph_draft<S>(
    Path(graph_id): Path<Uuid>,
    State(state): State<App<S>>,
    Json(request): Json<WriteGraphDraftRequest>,
) -> Result<impl IntoResponse, AppError>
where
    S: GraphStore<Uuid, G = DAG> + Clone + Send + Sync + 'static,
    S::Err: Send + Sync + 'static,
{
    let graph = state
        .write_graph_draft(graph_id, request.graph, &request.actor)
        .await?;
    Ok((StatusCode::OK, Json(graph)))
}

async fn publish_graph_draft<S>(
    Path(graph_id): Path<Uuid>,
    State(state): State<App<S>>,
    Json(request): Json<PublishGraphDraftRequest>,
) -> Result<impl IntoResponse, AppError>
where
    S: GraphStore<Uuid, G = DAG> + Clone + Send + Sync + 'static,
    S::Err: Send + Sync + 'static,
{
    let graph = state.publish_graph_draft(graph_id, &request.actor).await?;
    Ok((StatusCode::CREATED, Json(graph)))
}
