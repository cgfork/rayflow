//! Resource-aware job polling for agents.

use crate::app::App;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
};
use graph::{GraphStore, types::DAG};
use scheduler::ResourceCapacity;
use scheduler::actuator::Status;
use serde::Deserialize;
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
pub struct ReportJobStatusRequest {
    pub agent_id: Uuid,
    pub status: Status,
}

pub(super) fn routes<S>() -> Router<App<S>>
where
    S: GraphStore<Uuid, G = DAG> + Clone + Send + Sync + 'static,
    S::Err: Send + Sync + 'static,
{
    Router::new()
        .route("/jobs/poll", post(poll::<S>))
        .route("/jobs/{job_id}/status", post(report_status::<S>))
}

async fn report_status<S>(
    State(app): State<App<S>>,
    Path(job_id): Path<Uuid>,
    Json(request): Json<ReportJobStatusRequest>,
) -> Response
where
    S: GraphStore<Uuid, G = DAG> + Clone + Send + Sync + 'static,
    S::Err: Send + Sync + 'static,
{
    match app
        .report_job_status(job_id, request.agent_id, request.status)
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => error.into_response(),
    }
}

async fn poll<S>(State(app): State<App<S>>, Json(request): Json<PollJobsRequest>) -> Response
where
    S: GraphStore<Uuid, G = DAG> + Clone + Send + Sync + 'static,
    S::Err: Send + Sync + 'static,
{
    if !(1..=1000).contains(&request.batch) {
        return (StatusCode::BAD_REQUEST, "batch must be 1 to 1000").into_response();
    }
    match app
        .poll_jobs(
            request.agent_id,
            request.resources,
            request.batch,
            &request.active_jobs,
        )
        .await
    {
        Ok(jobs) => Json(jobs).into_response(),
        Err(error) => error.into_response(),
    }
}
