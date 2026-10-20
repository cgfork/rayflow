//! Task-event emission endpoint.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router};
use dag::types::DAG;
use runtime::{GraphStore, TaskEvent};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::app::{App, AppError};

/// Builds the task-event routes.
pub(super) fn routes<S>() -> Router<App<S>>
where
    S: GraphStore<Uuid, G = DAG> + Clone + Send + Sync + 'static,
    <S as GraphStore<Uuid>>::Err: Send + Sync + 'static,
{
    Router::new().route("/task_events", post(emit_task_event::<S>))
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
) -> Result<impl IntoResponse, AppError>
where
    S: GraphStore<Uuid, G = DAG> + Clone + Send + Sync + 'static,
    <S as GraphStore<Uuid>>::Err: Send + Sync + 'static,
{
    let entry = state
        .emit_task_event(request.task_id, request.event)
        .await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(EmitTaskEventResponse { id: entry.id }),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_event_request_deserializes() {
        let request: EmitTaskEventRequest = serde_json::from_str(
            r#"{"task_id":"00000000-0000-0000-0000-000000000001","event":"Succeed"}"#,
        )
        .unwrap();
        assert_eq!(request.event, TaskEvent::Succeed);
    }
}
