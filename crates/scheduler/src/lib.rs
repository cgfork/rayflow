//! Graph actuation and destination-independent scheduling contracts.
//!
//! `Actuator` consumes commands, logical tasks, and task events. `Scheduler` assigns
//! materialized jobs through an implementation such as `AgentScheduler`.
pub mod actuator;
pub mod agent;
pub mod scheduler;

pub use actuator::{Actuator, Job, JobRead, JobWrite, Step, models};
pub use agent::{
    AgentScheduler, DEFAULT_JOB_LEASE, ResourceCapacity, cancel_job, fail_job, pend_job,
    poll_polled, poll_scheduled, reclaim_expired_in, succeed_job, terminate_job,
};
pub use event::task_event::{TaskEvent, TaskEventEntry};
pub use scheduler::Scheduler;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Toasty(#[from] toasty::Error),
    #[error(transparent)]
    Event(#[from] event::Error),
    #[error(transparent)]
    Runtime(#[from] runtime::Error),
    #[error("job not found for task: {0}")]
    JobNotFound(uuid::Uuid),
    #[error("job not found: {0}")]
    AgentJobNotFound(uuid::Uuid),
    #[error("job claim expired, changed owner, or no longer accepts this status: {0}")]
    JobConflict(uuid::Uuid),
}
