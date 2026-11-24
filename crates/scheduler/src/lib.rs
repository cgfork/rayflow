//! Graph actuation and destination-independent scheduling contracts.
//!
//! `Actuator` consumes commands, logical tasks, and task events. `Scheduler` assigns
//! materialized jobs through an implementation such as `AgentScheduler`.
pub mod actuator;
pub mod agent;
pub mod agent_registry;
pub mod scheduler;

pub use actuator::{Actuator, Job, JobRead, JobWrite, Step, models};
pub use agent::{
    AgentRef, AgentScheduler, JobPoll, ResourceCapacity, cancel_job, fail_expired_in, fail_job,
    succeed_job, terminate_job, yield_job,
};
pub use agent_registry::{Error as AgentRegistryError, Instance as AgentInstance};
pub use event::task_event::{TaskEvent, TaskEventEntry};
pub use scheduler::Scheduler;

/// Agent-owned job metadata that can be archived separately from graph outputs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum JobArtifactName {
    #[default]
    #[serde(rename = "result.json")]
    Result,
    #[serde(rename = "usage.json")]
    Usage,
}

impl JobArtifactName {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Result => "result.json",
            Self::Usage => "usage.json",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("job timeout gap is outside timestamp range")]
    InvalidTimeoutGap,
    #[error(transparent)]
    Toasty(#[from] toasty::Error),
    #[error(transparent)]
    Event(#[from] event::Error),
    #[error(transparent)]
    Runtime(#[from] runtime::Error),
    #[error("node run not found for task: {0}")]
    NodeRunNotFound(u64),
    #[error("run not found for task: {0}")]
    RunNotFound(uuid::Uuid),
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
