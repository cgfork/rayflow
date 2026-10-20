//! Run commands and state endpoints.

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use dag::types::DAG;
use runtime::GraphStore;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::app::{App, AppError, GraphState};

/// Builds the run routes.
pub(super) fn routes<S>() -> Router<App<S>>
where
    S: GraphStore<Uuid, G = DAG> + Clone + Send + Sync + 'static,
    <S as GraphStore<Uuid>>::Err: Send + Sync + 'static,
{
    Router::new()
        .route("/runs/start", post(start_run::<S>))
        .route("/runs/rerun", post(rerun_run::<S>))
        .route("/runs/dryrun", post(dryrun_run::<S>))
        .route("/runs/graph_state", get(get_graph_state::<S>))
}

/// Request body for `POST /runs/start`.
#[derive(Debug, Deserialize)]
pub struct StartRunRequest {
    /// Data identifier of the run.
    pub data_id: Uuid,
    /// Graph identifier of the run.
    pub graph_id: Uuid,
    /// Nodes to start; empty means the graph's roots.
    #[serde(default)]
    pub nodes: Vec<Uuid>,
}

/// Response returned by `POST /runs/start`.
#[derive(Debug, Serialize)]
pub struct StartRunResponse {
    /// Identifier of the emitted command entry.
    pub id: Uuid,
}

/// Emits `Command::Start`, beginning a run.
async fn start_run<S>(
    State(state): State<App<S>>,
    Json(request): Json<StartRunRequest>,
) -> Result<impl IntoResponse, AppError>
where
    S: GraphStore<Uuid, G = DAG> + Clone + Send + Sync + 'static,
    <S as GraphStore<Uuid>>::Err: Send + Sync + 'static,
{
    let mut run = state.run_with(request.data_id, request.graph_id);
    let entry = run.start(request.nodes).await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(StartRunResponse { id: entry.id }),
    ))
}

/// Request body for `POST /runs/rerun`.
#[derive(Debug, Deserialize)]
pub struct RerunRunRequest {
    /// Data identifier of the run.
    pub data_id: Uuid,
    /// Graph identifier of the run.
    pub graph_id: Uuid,
    /// Node to rerun.
    pub node: Uuid,
}

/// Response returned by `POST /runs/rerun`.
#[derive(Debug, Serialize)]
pub struct RerunRunResponse {
    /// Identifier of the emitted command entry.
    pub id: Uuid,
}

/// Emits `Command::Rerun`, resetting one node to `Ready`.
async fn rerun_run<S>(
    State(state): State<App<S>>,
    Json(request): Json<RerunRunRequest>,
) -> Result<impl IntoResponse, AppError>
where
    S: GraphStore<Uuid, G = DAG> + Clone + Send + Sync + 'static,
    <S as GraphStore<Uuid>>::Err: Send + Sync + 'static,
{
    let mut run = state.run_with(request.data_id, request.graph_id);
    let entry = run.rerun(request.node).await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(RerunRunResponse { id: entry.id }),
    ))
}

/// Request body for `POST /runs/dryrun`.
#[derive(Debug, Deserialize)]
pub struct DryrunRunRequest {
    /// Data identifier of the run.
    pub data_id: Uuid,
    /// Graph identifier of the run.
    pub graph_id: Uuid,
    /// Nodes to dry-run; empty means the graph's roots.
    #[serde(default)]
    pub nodes: Vec<Uuid>,
}

/// Response returned by `POST /runs/dryrun`.
#[derive(Debug, Serialize)]
pub struct DryrunRunResponse {
    /// Identifier of the emitted command entry.
    pub id: Uuid,
}

/// Emits `Command::Dryrun`, mock-succeeding the nodes' predecessors.
async fn dryrun_run<S>(
    State(state): State<App<S>>,
    Json(request): Json<DryrunRunRequest>,
) -> Result<impl IntoResponse, AppError>
where
    S: GraphStore<Uuid, G = DAG> + Clone + Send + Sync + 'static,
    <S as GraphStore<Uuid>>::Err: Send + Sync + 'static,
{
    let mut run = state.run_with(request.data_id, request.graph_id);
    let entry = run.dryrun(request.nodes).await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(DryrunRunResponse { id: entry.id }),
    ))
}

/// Query string for `GET /runs/graph_state`.
#[derive(Debug, Deserialize)]
pub struct GetGraphStateRequest {
    /// Data identifier of the run.
    pub data_id: Uuid,
    /// Graph identifier of the run.
    pub graph_id: Uuid,
}

/// Response returned by `GET /runs/graph_state`.
pub type GetGraphStateResponse = GraphState;

/// Reads the current state of a run's graph.
async fn get_graph_state<S>(
    State(state): State<App<S>>,
    Query(request): Query<GetGraphStateRequest>,
) -> Result<Json<GetGraphStateResponse>, AppError>
where
    S: GraphStore<Uuid, G = DAG> + Clone + Send + Sync + 'static,
    <S as GraphStore<Uuid>>::Err: Send + Sync + 'static,
{
    let response = state
        .run_with(request.data_id, request.graph_id)
        .graph_state()
        .await?;
    Ok(Json(response))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn start_request_defaults_nodes() {
        let request: StartRunRequest =
            serde_json::from_str(r#"{"data_id":"00000000-0000-0000-0000-000000000001","graph_id":"00000000-0000-0000-0000-000000000002"}"#)
                .unwrap();
        assert!(request.nodes.is_empty());
    }
}
