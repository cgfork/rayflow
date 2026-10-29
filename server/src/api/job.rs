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
    S: GraphStore<Uuid, G = DAG> + Clone + Send + Sync + 'static,
    S::Err: Send + Sync + 'static,
{
    Router::new()
        .route("/jobs/poll", post(poll::<S>))
        .route("/job/cancel", post(cancel_job::<S>))
        .route("/job/yield", post(yield_job::<S>))
        .route("/job/succeed", post(succeed_job::<S>))
        .route("/job/fail", post(fail_job::<S>))
        .route("/job/terminate", post(terminate_job::<S>))
}

async fn cancel_job<S>(
    State(app): State<App<S>>,
    Json(request): Json<JobActionRequest>,
) -> Result<StatusCode, AppError>
where
    S: GraphStore<Uuid, G = DAG> + Clone + Send + Sync + 'static,
    S::Err: Send + Sync + 'static,
{
    app.cancel_job(request.job_id, request.agent_id).await?;
    Ok(StatusCode::NO_CONTENT)
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
    if request
        .wait_ms
        .is_some_and(|wait_ms| Duration::from_millis(wait_ms) > app.config.poll_timeout)
    {
        return (
            StatusCode::BAD_REQUEST,
            format!(
                "wait_ms must be 0 to {}",
                app.config.poll_timeout.as_millis()
            ),
        )
            .into_response();
    }
    let deadline = Instant::now() + request.wait(app.config.poll_timeout);
    loop {
        let mut agent = AgentRef::new(
            request.agent_id,
            request.resources,
            request.active_jobs.clone(),
        );
        let jobs = match app.poll(&mut agent, request.batch).await {
            Ok(jobs) => jobs,
            Err(error) => return error.into_response(),
        };
        let response = PollJobsResponse { jobs };
        if !response.jobs.is_empty()
            || !request.active_jobs.is_empty()
            || Instant::now() >= deadline
        {
            return Json(response).into_response();
        }
        sleep_until(deadline.min(Instant::now() + app.config.poll_retry_interval)).await;
        if Instant::now() >= deadline {
            return Json(response).into_response();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_or_null_wait_uses_a_random_configured_timeout() {
        let mut body = serde_json::json!({
            "agent_id": Uuid::new_v4(), "batch": 1,
            "resources": { "cpu_cores": 1, "memory_bytes": 512 },
        });
        let request: PollJobsRequest = serde_json::from_value(body.clone()).unwrap();
        assert_eq!(request.wait_ms, None);
        assert!(request.active_jobs.is_empty());
        for timeout in [
            Duration::from_millis(2),
            Duration::from_micros(2500),
            Duration::from_millis(601),
            Duration::from_secs(30),
            Duration::from_secs(60),
        ] {
            for _ in 0..128 {
                let wait = request.wait(timeout);
                assert!((timeout / 2..timeout).contains(&wait));
            }
        }
        body["wait_ms"] = serde_json::Value::Null;
        let request: PollJobsRequest = serde_json::from_value(body).unwrap();
        assert_eq!(request.wait_ms, None);
        assert!(
            (Duration::from_millis(300)..Duration::from_millis(600))
                .contains(&request.wait(Duration::from_millis(600)))
        );
    }

    #[test]
    fn explicit_wait_is_preserved_including_zero() {
        for wait_ms in [0, 1, 600, 30_000] {
            let request: PollJobsRequest = serde_json::from_value(serde_json::json!({
                "agent_id": Uuid::new_v4(), "batch": 1,
                "resources": { "cpu_cores": 1, "memory_bytes": 512 },
                "wait_ms": wait_ms,
            }))
            .unwrap();
            assert_eq!(request.wait_ms, Some(wait_ms));
            assert_eq!(
                request.wait(Duration::from_secs(30)),
                Duration::from_millis(wait_ms)
            );
        }
    }

    #[test]
    fn wait_requires_nonnegative_integer_milliseconds() {
        for value in [
            serde_json::json!(-1),
            serde_json::json!(0.5),
            serde_json::json!("100"),
        ] {
            let body = serde_json::json!({
                "agent_id": Uuid::new_v4(), "batch": 1,
                "resources": { "cpu_cores": 1, "memory_bytes": 512 },
                "wait_ms": value,
            });
            assert!(serde_json::from_value::<PollJobsRequest>(body).is_err());
        }
    }
}
