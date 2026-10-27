//! Graph actuation and destination-independent scheduling contracts.
//!
//! `Actuator` consumes commands, logical tasks, and task events. `Scheduler` assigns
//! materialized jobs through an implementation such as `AgentScheduler`.
pub mod actuator;
pub mod agent;
pub mod scheduler;

pub use actuator::{Actuator, Job, JobRead, JobWrite, Step, models};
pub use agent::{
    AgentRef, AgentScheduler, JobPoll, ResourceCapacity, fail_job, reclaim_expired_in, succeed_job,
    terminate_job, yield_job,
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
    #[error("job lease must be positive milliseconds that fit timestamp arithmetic: {0}")]
    InvalidJobLease(u64),
    #[error("job lease not found for node configuration: {0}")]
    JobLeaseNotFound(u64),
    #[error("job claim expired, changed owner, or no longer accepts this status: {0}")]
    JobConflict(uuid::Uuid),
}
