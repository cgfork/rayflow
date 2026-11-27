//! Job polling, agent reports, and task-event emission.

use crate::app::{App, AppError, Page, PageQuery};
use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use catalog::{PresignedUrl, model::Catalog};
use event::task_event::TaskEvent;
use graph::{GraphStore, types::DAG};
use scheduler::{AgentRef, GpuChoice, GpuDevice, GpuOffers, JobArtifactName, ResourceCapacity};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::{Instant, sleep_until};
use uuid::Uuid;

/// Claim jobs within this agent's CPU, memory, and per-device GPU budgets.
/// Existing claims reserve part of that budget. Poll once at a time per agent ID.
#[derive(Debug, Deserialize)]
pub struct PollJobsRequest {
    pub agent_id: Uuid,
    pub batch: usize,
    pub resources: ResourceCapacity,
    #[serde(default)]
    pub gpu_devices: Vec<GpuDevice>,
    #[serde(default)]
    pub gpu_choices: Vec<GpuChoice>,
    #[serde(default)]
    pub active_jobs: Vec<Uuid>,
    /// Idle wait in milliseconds; defaults to a random half-to-full configured timeout.
    #[serde(default)]
    pub wait_ms: Option<u64>,
}

impl PollJobsRequest {
    fn wait(&self, timeout: Duration) -> Duration {
        self.wait_ms.map(Duration::from_millis).unwrap_or_else(|| {
            let nanos = fastrand::u128(timeout.as_nanos().div_ceil(2)..timeout.as_nanos());
            Duration::new(
                (nanos / 1_000_000_000) as u64,
                (nanos % 1_000_000_000) as u32,
            )
        })
    }
}

#[derive(Debug, Deserialize)]
pub struct JobActionRequest {
    pub job_id: Uuid,
    pub agent_id: Uuid,
}

#[derive(Debug, Deserialize)]
pub struct PrepareJobArtifactRequest {
    pub job_id: Uuid,
    pub agent_id: Uuid,
    #[serde(default)]
    pub filename: JobArtifactName,
    pub size: u64,
}

#[derive(Debug, Deserialize)]
pub struct CommitJobArtifactRequest {
    pub job_id: Uuid,
    pub agent_id: Uuid,
    #[serde(default)]
    pub filename: JobArtifactName,
    pub catalog_uuid: Uuid,
}

#[derive(Debug, Serialize)]
pub struct CommitJobArtifactResponse {
    pub catalog_uuid: Uuid,
    pub catalog: Catalog,
}

#[derive(Debug, Deserialize)]
pub struct PrepareOutputRequest {
    pub job_id: Uuid,
    pub agent_id: Uuid,
    pub filename: String,
    pub size: Option<u64>,
    pub format: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct PrepareOutputResponse {
    pub catalog_uuid: Uuid,
    #[serde(flatten)]
    pub transfer: crate::api::run::Transfer,
}

#[derive(Debug, Deserialize)]
pub struct CommitOutputRequest {
    pub job_id: Uuid,
    pub agent_id: Uuid,
    pub filename: String,
    pub catalog_uuid: Uuid,
}

#[derive(Debug, Serialize)]
pub struct CommitOutputResponse {
    pub file_uuid: Uuid,
    pub catalog: Catalog,
}

#[derive(Debug, Deserialize)]
pub struct PresignInputRequest {
    pub job_id: Uuid,
    pub agent_id: Uuid,
    pub filename: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateMultipartOutputRequest {
    pub job_id: Uuid,
    pub agent_id: Uuid,
    pub filename: String,
    pub size: Option<u64>,
    pub format: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct MultipartOutputContext {
    pub job_id: Uuid,
    pub agent_id: Uuid,
    pub filename: String,
    pub catalog_uuid: Uuid,
}

#[derive(Debug, Deserialize)]
pub struct UploadMultipartOutputRequest {
    #[serde(flatten)]
    pub context: MultipartOutputContext,
    pub upload_id: String,
    pub part_number: u32,
}

#[derive(Debug, Deserialize)]
pub struct SessionMultipartOutputRequest {
    #[serde(flatten)]
    pub context: MultipartOutputContext,
    pub upload_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PollJobsResponse {
    pub jobs: Vec<scheduler::Job>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_offers: Option<GpuOffers>,
}

pub(super) fn routes<S>() -> Router<App<S>>
where
    S: GraphStore<Uuid, G = DAG> + Clone + Send + Sync + 'static,
    S::load(..): Send,
    S::Err: Send + Sync + 'static,
{
    Router::new()
        .route("/jobs", get(list_jobs::<S>))
        .route("/task_events", post(emit_task_event::<S>))
        .route("/jobs/poll", post(poll::<S>))
        .route("/job/cancel", post(cancel_job::<S>))
        .route("/job/yield", post(yield_job::<S>))
        .route("/job/succeed", post(succeed_job::<S>))
        .route("/job/fail", post(fail_job::<S>))
        .route("/job/terminate", post(terminate_job::<S>))
        .route("/job/result/prepare", post(prepare_job_artifact::<S>))
        .route("/job/result/commit", post(commit_job_artifact::<S>))
        .route("/job/output/prepare", post(prepare_output::<S>))
        .route("/job/output/commit", post(commit_output::<S>))
        .route(
            "/job/output/multipart/create",
            post(create_multipart_output::<S>),
        )
        .route(
            "/job/output/multipart/upload",
            post(upload_multipart_output::<S>),
        )
        .route(
            "/job/output/multipart/complete",
            post(complete_multipart_output::<S>),
        )
        .route(
            "/job/output/multipart/abort",
            post(abort_multipart_output::<S>),
        )
        .route("/job/input/presign", post(presign_input::<S>))
}

async fn prepare_job_artifact<S>(
    State(app): State<App<S>>,
    Json(request): Json<PrepareJobArtifactRequest>,
) -> Result<Json<PrepareOutputResponse>, AppError> {
    let transfer = app
        .prepare_job_artifact(
            request.job_id,
            request.agent_id,
            request.filename,
            request.size,
        )
        .await?;
    Ok(Json(PrepareOutputResponse {
        catalog_uuid: transfer.catalog.uuid,
        transfer,
    }))
}

async fn commit_job_artifact<S>(
    State(app): State<App<S>>,
    Json(request): Json<CommitJobArtifactRequest>,
) -> Result<Json<CommitJobArtifactResponse>, AppError> {
    let catalog = app
        .commit_job_artifact(
            request.job_id,
            request.agent_id,
            request.filename,
            request.catalog_uuid,
        )
        .await?;
    Ok(Json(CommitJobArtifactResponse {
        catalog_uuid: catalog.uuid,
        catalog,
    }))
}

async fn prepare_output<S>(
    State(app): State<App<S>>,
    Json(request): Json<PrepareOutputRequest>,
) -> Result<Json<PrepareOutputResponse>, AppError>
where
    S: GraphStore<Uuid, G = DAG> + Clone,
    S::load(..): Send,
    S::Err: Send + Sync + 'static,
{
    app.prepare_output(
        request.job_id,
        request.agent_id,
        request.filename,
        request.size,
        request.format,
    )
    .await
    .map(|transfer| {
        Json(PrepareOutputResponse {
            catalog_uuid: transfer.catalog.uuid,
            transfer,
        })
    })
}

async fn commit_output<S>(
    State(app): State<App<S>>,
    Json(request): Json<CommitOutputRequest>,
) -> Result<Json<CommitOutputResponse>, AppError>
where
    S: GraphStore<Uuid, G = DAG> + Clone,
    S::load(..): Send,
    S::Err: Send + Sync + 'static,
{
    let (file_uuid, catalog) = app
        .commit_output(
            request.job_id,
            request.agent_id,
            request.filename,
            request.catalog_uuid,
        )
        .await?;
    Ok(Json(CommitOutputResponse { file_uuid, catalog }))
}

async fn presign_input<S>(
    State(app): State<App<S>>,
    Json(request): Json<PresignInputRequest>,
) -> Result<Json<crate::api::run::Transfer>, AppError>
where
    S: GraphStore<Uuid, G = DAG> + Clone + Send + Sync + 'static,
    S::load(..): Send,
    S::Err: Send + Sync + 'static,
{
    app.presign_input(request.job_id, request.agent_id, request.filename)
        .await
        .map(Json)
}

async fn create_multipart_output<S>(
    State(app): State<App<S>>,
    Json(request): Json<CreateMultipartOutputRequest>,
) -> Result<Json<PrepareOutputResponse>, AppError>
where
    S: GraphStore<Uuid, G = DAG> + Clone,
    S::load(..): Send,
    S::Err: Send + Sync + 'static,
{
    app.create_multipart_output(
        request.job_id,
        request.agent_id,
        request.filename,
        request.size,
        request.format,
    )
    .await
    .map(|transfer| {
        Json(PrepareOutputResponse {
            catalog_uuid: transfer.catalog.uuid,
            transfer,
        })
    })
}

async fn upload_multipart_output<S>(
    State(app): State<App<S>>,
    Json(request): Json<UploadMultipartOutputRequest>,
) -> Result<Json<PresignedUrl>, AppError>
where
    S: GraphStore<Uuid, G = DAG> + Clone,
    S::load(..): Send,
    S::Err: Send + Sync + 'static,
{
    app.presign_multipart_output(
        request.context.job_id,
        request.context.agent_id,
        request.context.filename,
        request.context.catalog_uuid,
        catalog::PresignOperation::UploadPart {
            upload_id: request.upload_id,
            part_number: request.part_number,
        },
    )
    .await
    .map(Json)
}

async fn complete_multipart_output<S>(
    State(app): State<App<S>>,
    Json(request): Json<SessionMultipartOutputRequest>,
) -> Result<Json<PresignedUrl>, AppError>
where
    S: GraphStore<Uuid, G = DAG> + Clone,
    S::load(..): Send,
    S::Err: Send + Sync + 'static,
{
    app.presign_multipart_output(
        request.context.job_id,
        request.context.agent_id,
        request.context.filename,
        request.context.catalog_uuid,
        catalog::PresignOperation::CompleteMultipart {
            upload_id: request.upload_id,
        },
    )
    .await
    .map(Json)
}

async fn abort_multipart_output<S>(
    State(app): State<App<S>>,
    Json(request): Json<SessionMultipartOutputRequest>,
) -> Result<Json<PresignedUrl>, AppError>
where
    S: GraphStore<Uuid, G = DAG> + Clone,
    S::load(..): Send,
    S::Err: Send + Sync + 'static,
{
    app.presign_multipart_output(
        request.context.job_id,
        request.context.agent_id,
        request.context.filename,
        request.context.catalog_uuid,
        catalog::PresignOperation::AbortMultipart {
            upload_id: request.upload_id,
        },
    )
    .await
    .map(Json)
}

#[derive(Debug, Deserialize)]
pub struct ListJobsQuery {
    pub status: Option<scheduler::actuator::Status>,
    pub agent_id: Option<Uuid>,
    pub run_id: Option<Uuid>,
}

async fn list_jobs<S>(
    State(app): State<App<S>>,
    Query(page): Query<PageQuery>,
    Query(query): Query<ListJobsQuery>,
) -> Result<Json<Page<scheduler::Job>>, AppError> {
    app.list_jobs(page, query.status, query.agent_id, query.run_id)
        .await
        .map(Json)
}

async fn cancel_job<S>(
    State(app): State<App<S>>,
    Json(request): Json<JobActionRequest>,
) -> Result<StatusCode, AppError> {
    app.cancel_job(request.job_id, request.agent_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn yield_job<S>(
    State(app): State<App<S>>,
    Json(request): Json<JobActionRequest>,
) -> Result<StatusCode, AppError> {
    app.yield_job(request.job_id, request.agent_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn succeed_job<S>(
    State(app): State<App<S>>,
    Json(request): Json<JobActionRequest>,
) -> Result<StatusCode, AppError> {
    app.succeed_job(request.job_id, request.agent_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn fail_job<S>(
    State(app): State<App<S>>,
    Json(request): Json<JobActionRequest>,
) -> Result<StatusCode, AppError> {
    app.fail_job(request.job_id, request.agent_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn terminate_job<S>(
    State(app): State<App<S>>,
    Json(request): Json<JobActionRequest>,
) -> Result<StatusCode, AppError> {
    app.terminate_job(request.job_id, request.agent_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn poll<S>(
    State(app): State<App<S>>,
    Json(request): Json<PollJobsRequest>,
) -> Result<Json<PollJobsResponse>, AppError> {
    if !(1..=1000).contains(&request.batch) {
        return Err(AppError::InvalidRequest("batch must be 1 to 1000".into()));
    }
    if request
        .wait_ms
        .is_some_and(|wait_ms| Duration::from_millis(wait_ms) > app.config.poll_timeout)
    {
        return Err(AppError::InvalidRequest(format!(
            "wait_ms must be 0 to {}",
            app.config.poll_timeout.as_millis()
        )));
    }
    let deadline = Instant::now() + request.wait(app.config.poll_timeout);
    let has_gpu_choices = !request.gpu_choices.is_empty();
    let agent = AgentRef::new(request.agent_id, request.resources, request.active_jobs)
        .with_gpu_devices(request.gpu_devices)
        .and_then(|agent| agent.with_gpu_choices(request.gpu_choices, request.batch))
        .map_err(|error| AppError::InvalidRequest(error.into()))?;
    loop {
        let mut polling = agent.clone();
        let jobs = app.poll(&mut polling, request.batch).await?;
        let response = PollJobsResponse {
            jobs,
            gpu_offers: polling.take_gpu_offers(),
        };
        if !response.jobs.is_empty()
            || response.gpu_offers.is_some()
            || has_gpu_choices
            || !agent.active_jobs().is_empty()
            || Instant::now() >= deadline
        {
            return Ok(Json(response));
        }
        sleep_until(deadline.min(Instant::now() + app.config.poll_retry_interval)).await;
        if Instant::now() >= deadline {
            return Ok(Json(response));
        }
    }
}

/// Request body for `POST /task_events`.
#[derive(Debug, Deserialize)]
pub struct EmitTaskEventRequest {
    /// Task whose run produced the event.
    pub task_id: Uuid,
    /// Event to record.
    pub event: TaskEvent,
}

/// Response returned by `POST /task_events`.
#[derive(Debug, Serialize)]
pub struct EmitTaskEventResponse {
    /// Identifier of the emitted task event.
    pub id: Uuid,
}

/// Emits a task event for an existing task.
async fn emit_task_event<S>(
    State(state): State<App<S>>,
    Json(request): Json<EmitTaskEventRequest>,
) -> Result<impl IntoResponse, AppError> {
    let entry = state
        .emit_task_event(request.task_id, request.event)
        .await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(EmitTaskEventResponse { id: entry.id }),
    ))
}
