//! Job polling, agent reports, and task-event emission.

use crate::app::{App, AppError, Page, PageQuery};
use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use event::task_event::TaskEvent;
use scheduler::{AgentRef, ResourceCapacity};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::{Instant, sleep_until};
use uuid::Uuid;

/// Claim jobs with aggregate requirements that fit this agent's total budget.
/// Existing claims reserve part of that budget. Poll once at a time per agent ID.
#[derive(Debug, Deserialize)]
pub struct PollJobsRequest {
    pub agent_id: Uuid,
    pub batch: usize,
    pub resources: ResourceCapacity,
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

#[derive(Debug, Serialize, Deserialize)]
pub struct PollJobsResponse {
    pub jobs: Vec<scheduler::Job>,
}

pub(super) fn routes<S>() -> Router<App<S>>
where
    S: Clone + Send + Sync + 'static,
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
    loop {
        let mut agent = AgentRef::new(
            request.agent_id,
            request.resources,
            request.active_jobs.clone(),
        );
        let jobs = app.poll(&mut agent, request.batch).await?;
        let response = PollJobsResponse { jobs };
        if !response.jobs.is_empty()
            || !request.active_jobs.is_empty()
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
