//! Resource-aware job polling for agents.

use crate::app::{App, AppError};
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
};
use graph::{GraphStore, types::DAG};
use scheduler::{AgentRef, ResourceCapacity};
use serde::{Deserialize, Serialize};
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
}

#[derive(Debug, Deserialize)]
pub struct JobActionRequest {
    pub job_id: Uuid,
    pub agent_id: Uuid,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PollJobsResponse {
    pub jobs: Vec<scheduler::Job>,
    pub renewed_jobs: Vec<Uuid>,
}

pub(super) fn routes<S>() -> Router<App<S>>
where
    S: GraphStore<Uuid, G = DAG> + Clone + Send + Sync + 'static,
    S::Err: Send + Sync + 'static,
{
    Router::new()
        .route("/jobs/poll", post(poll::<S>))
        .route("/job/yield", post(yield_job::<S>))
        .route("/job/succeed", post(succeed_job::<S>))
        .route("/job/fail", post(fail_job::<S>))
        .route("/job/terminate", post(terminate_job::<S>))
}

async fn yield_job<S>(
    State(app): State<App<S>>,
    Json(request): Json<JobActionRequest>,
) -> Result<StatusCode, AppError>
where
    S: GraphStore<Uuid, G = DAG> + Clone + Send + Sync + 'static,
    S::Err: Send + Sync + 'static,
{
    app.yield_job(request.job_id, request.agent_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn succeed_job<S>(
    State(app): State<App<S>>,
    Json(request): Json<JobActionRequest>,
) -> Result<StatusCode, AppError>
where
    S: GraphStore<Uuid, G = DAG> + Clone + Send + Sync + 'static,
    S::Err: Send + Sync + 'static,
{
    app.succeed_job(request.job_id, request.agent_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn fail_job<S>(
    State(app): State<App<S>>,
    Json(request): Json<JobActionRequest>,
) -> Result<StatusCode, AppError>
where
    S: GraphStore<Uuid, G = DAG> + Clone + Send + Sync + 'static,
    S::Err: Send + Sync + 'static,
{
    app.fail_job(request.job_id, request.agent_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn terminate_job<S>(
    State(app): State<App<S>>,
    Json(request): Json<JobActionRequest>,
) -> Result<StatusCode, AppError>
where
    S: GraphStore<Uuid, G = DAG> + Clone + Send + Sync + 'static,
    S::Err: Send + Sync + 'static,
{
    app.terminate_job(request.job_id, request.agent_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn poll<S>(State(app): State<App<S>>, Json(request): Json<PollJobsRequest>) -> Response
where
    S: GraphStore<Uuid, G = DAG> + Clone + Send + Sync + 'static,
    S::Err: Send + Sync + 'static,
{
    if !(1..=1000).contains(&request.batch) {
        return (StatusCode::BAD_REQUEST, "batch must be 1 to 1000").into_response();
    }
    let mut agent = AgentRef::new(request.agent_id, request.resources, request.active_jobs);
    match app.poll(&mut agent, request.batch).await {
        Ok(jobs) => Json(PollJobsResponse {
            jobs,
            renewed_jobs: agent.renewed_jobs().to_vec(),
        })
        .into_response(),
        Err(error) => error.into_response(),
    }
}
