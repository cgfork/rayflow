//! Run commands and state endpoints.

use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use catalog::{PresignedUrl, model::Catalog};
use graph::GraphStore;
use graph::types::DAG;
use scheduler::JobArtifactName;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::app::{App, AppError, Page, PageQuery, RunView};

/// Builds the run routes.
pub(super) fn routes<S>() -> Router<App<S>>
where
    S: GraphStore<Uuid, G = DAG> + Clone + Send + Sync + 'static,
    S::load(..): Send,
    <S as GraphStore<Uuid>>::Err: Send + Sync + 'static,
{
    Router::new()
        .route("/runs", get(list_runs::<S>))
        .route("/runs/{run_id}", get(inspect_run::<S>))
        .route(
            "/runs/{run_id}/node-runs/{node_run_id}/artifacts/{filename}",
            get(presign_job_artifact::<S>),
        )
        .route("/runs/start", post(start_run::<S>))
        .route("/runs/rerun", post(rerun_run::<S>))
        .route("/runs/upgrade", post(upgrade_run::<S>))
        .route("/runs/resume", post(resume_run::<S>))
        .route("/runs/dryrun", post(dryrun_run::<S>))
        .route("/runs/graph_state", get(get_graph_state::<S>))
        .route("/runs/{run_id}/files/download", post(download_file::<S>))
}

#[derive(Debug, Deserialize)]
pub struct ListRunsQuery {
    pub graph_id: Option<Uuid>,
}

async fn list_runs<S>(
    State(app): State<App<S>>,
    Query(page): Query<PageQuery>,
    Query(query): Query<ListRunsQuery>,
) -> Result<Json<Page<runtime::command::Run>>, AppError> {
    app.list_runs(page, query.graph_id).await.map(Json)
}

#[derive(Debug, Deserialize)]
struct InspectRunQuery {
    revision: Option<u64>,
}

async fn inspect_run<S>(
    State(app): State<App<S>>,
    Path(id): Path<Uuid>,
    Query(query): Query<InspectRunQuery>,
) -> Result<Json<RunView>, AppError>
where
    S: GraphStore<Uuid, G = DAG> + Clone,
    S::load(..): Send,
    S::Err: Send + Sync + 'static,
{
    app.inspect_run_at(id, query.revision).await.map(Json)
}

async fn presign_job_artifact<S>(
    State(app): State<App<S>>,
    Path((run_id, node_run_id, filename)): Path<(Uuid, Uuid, JobArtifactName)>,
) -> Result<impl IntoResponse, AppError> {
    let download = app
        .presign_run_job_artifact(run_id, node_run_id, filename)
        .await?;
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(download)))
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
pub struct CommandResponse {
    /// Identifier of the emitted command entry.
    pub id: Uuid,
}

pub type StartRunResponse = CommandResponse;

/// Emits `Command::Start`, beginning a run.
async fn start_run<S: Clone>(
    State(state): State<App<S>>,
    Json(request): Json<StartRunRequest>,
) -> Result<impl IntoResponse, AppError> {
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
pub type RerunRunResponse = CommandResponse;

/// Emits `Command::Rerun`, resetting one node to `Ready`.
async fn rerun_run<S: Clone>(
    State(state): State<App<S>>,
    Json(request): Json<RerunRunRequest>,
) -> Result<impl IntoResponse, AppError> {
    let mut run = state.run_with(request.data_id, request.graph_id);
    let entry = run.rerun(request.node).await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(RerunRunResponse { id: entry.id }),
    ))
}

#[derive(Debug, Deserialize)]
pub struct UpgradeRunRequest {
    pub data_id: Uuid,
    pub graph_id: Uuid,
    pub version: u64,
    pub node: Uuid,
}

pub type UpgradeRunResponse = CommandResponse;

async fn upgrade_run<S: Clone>(
    State(state): State<App<S>>,
    Json(request): Json<UpgradeRunRequest>,
) -> Result<impl IntoResponse, AppError> {
    let entry = state
        .run_with(request.data_id, request.graph_id)
        .upgrade(request.version, request.node)
        .await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(UpgradeRunResponse { id: entry.id }),
    ))
}

#[derive(Debug, Deserialize)]
pub struct ResumeRunRequest {
    pub data_id: Uuid,
    pub graph_id: Uuid,
    pub node: Uuid,
}

pub type ResumeRunResponse = CommandResponse;

async fn resume_run<S: Clone>(
    State(state): State<App<S>>,
    Json(request): Json<ResumeRunRequest>,
) -> Result<impl IntoResponse, AppError> {
    let mut run = state.run_with(request.data_id, request.graph_id);
    let entry = run.resume(request.node).await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(ResumeRunResponse { id: entry.id }),
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
pub type DryrunRunResponse = CommandResponse;

/// Emits `Command::Dryrun`, mock-succeeding the nodes' predecessors.
async fn dryrun_run<S: Clone>(
    State(state): State<App<S>>,
    Json(request): Json<DryrunRunRequest>,
) -> Result<impl IntoResponse, AppError> {
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
pub type GetGraphStateResponse = Vec<(runtime::command::NodeRun, graph::types::Node)>;

/// Reads the current state of a run's graph.
async fn get_graph_state<S>(
    State(state): State<App<S>>,
    Query(request): Query<GetGraphStateRequest>,
) -> Result<Json<GetGraphStateResponse>, AppError>
where
    S: GraphStore<Uuid, G = DAG> + Clone + Send + Sync + 'static,
    S::load(..): Send,
    <S as GraphStore<Uuid>>::Err: Send + Sync + 'static,
{
    let response = state
        .run_with(request.data_id, request.graph_id)
        .graph_state()
        .await?;
    Ok(Json(response))
}

/// A declared input or output in a revision of a run.
#[derive(Debug, Deserialize)]
pub struct RunFileDownloadRequest {
    pub node_id: Uuid,
    pub file_name: String,
    pub direction: FileDirection,
    pub revision: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct Transfer {
    pub catalog: Catalog,
    pub presigned: PresignedUrl,
    #[serde(default)]
    pub multipart: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileDirection {
    Input,
    Output,
}

async fn download_file<S>(
    State(app): State<App<S>>,
    Path(run_id): Path<Uuid>,
    Json(request): Json<RunFileDownloadRequest>,
) -> Result<Json<Transfer>, AppError>
where
    S: GraphStore<Uuid, G = DAG> + Clone + Send + Sync + 'static,
    S::load(..): Send,
    S::Err: Send + Sync + 'static,
{
    app.download_run_file(run_id, request).await.map(Json)
}
