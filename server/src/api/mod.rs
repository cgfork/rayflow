//! REST API.
//!
//! The server is the command *producer* of the event-sourced runtime: it appends
//! the commands that drive a run to the command log, emits task events, and reads
//! back the state those commands produced once a consumer has applied them.
//!
//! * `POST /runs/start` — emit `Command::Start` for a run.
//! * `POST /runs/rerun` — emit `Command::Rerun` for one node.
//! * `POST /runs/dryrun` — emit `Command::Dryrun` for the given nodes.
//! * `POST /task_events` — emit a `TaskEvent` for a task.
//! * `POST /jobs/poll` — claim jobs matching the agent's CPU and memory capacity.
//! * `POST /jobs/{job_id}/status` — report a job status and emit its task event.
//! * `GET  /runs/graph_state` — read the current state of a run.

pub mod job;
pub mod run;
pub mod task_event;

use axum::Router;
use graph::GraphStore;
use graph::types::DAG;
use uuid::Uuid;

use crate::app::App;

pub use job::{PollJobsRequest, ReportJobStatusRequest};
pub use run::{
    DryrunRunRequest, DryrunRunResponse, GetGraphStateRequest, GetGraphStateResponse,
    RerunRunRequest, RerunRunResponse, StartRunRequest, StartRunResponse,
};
pub use task_event::{EmitTaskEventRequest, EmitTaskEventResponse};

/// Builds the API router, backed by `App`.
pub fn router<S>(state: App<S>) -> Router
where
    S: GraphStore<Uuid, G = DAG> + Clone + Send + Sync + 'static,
    <S as GraphStore<Uuid>>::Err: Send + Sync + 'static,
{
    run::routes::<S>()
        .merge(task_event::routes::<S>())
        .merge(job::routes::<S>())
        .with_state(state)
}
