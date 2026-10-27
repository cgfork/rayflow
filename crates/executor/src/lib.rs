//! Task executor.
//!
//! The executor crate provides job primitives for executing a task's work.
//! [`Runnable`] describes a job that runs in a [`JobContext`] and returns a
//! [`TaskState`], whose event drives the task's node. [`script::Script`] is the
//! built-in job that runs source through an interpreter.
//!
//! The job types are feature-agnostic and always available.

pub mod script;

use event::task_event::TaskEvent;
use scheduler::{Job, ResourceCapacity};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Agent {
    pub id: Uuid,
    pub resources: ResourceCapacity,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct JobContext {
    pub job: Job,
    pub agent: Agent,
}

pub trait ErrorCode: std::error::Error {
    fn code(&self) -> u32;
}

pub trait TaskState {
    fn event(&self) -> Option<TaskEvent>;

    fn exit_code(&self) -> Option<i32>;
}

#[allow(async_fn_in_trait)]
pub trait Runnable {
    type Err: ErrorCode;
    type State: TaskState;
    async fn run(&self, ctx: JobContext) -> Result<Self::State, Self::Err>;
}

/// A boxed [`Runnable`] is itself runnable, so a job can be stored behind a
/// pointer without naming its concrete type.
impl<T> Runnable for Box<T>
where
    T: Runnable + ?Sized,
{
    type Err = T::Err;
    type State = T::State;

    async fn run(&self, ctx: JobContext) -> Result<Self::State, Self::Err> {
        (**self).run(ctx).await
    }
}

pub type AnyRunnable = Box<dyn Runnable<Err = Box<dyn ErrorCode>, State = Box<dyn TaskState>>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boxed_runnable_is_runnable() {
        fn assert_runnable<R: Runnable>() {}
        assert_runnable::<Box<script::Script>>();
    }

    #[test]
    fn job_context_round_trips_the_job_and_agent() {
        let task = event::task::Task::new(Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4(), 7);
        let context = JobContext {
            job: Job::new(&task, Uuid::new_v4(), 11, 13),
            agent: Agent {
                id: Uuid::new_v4(),
                resources: ResourceCapacity {
                    cpu_cores: 4,
                    memory_bytes: 1024,
                },
            },
        };
        let encoded = serde_json::to_value(&context).unwrap();
        assert_eq!(encoded.as_object().unwrap().len(), 2);
        assert_eq!(encoded["job"]["uuid"], context.job.uuid.to_string());
        assert_eq!(encoded["agent"]["id"], context.agent.id.to_string());
        let decoded: JobContext = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded.job.uuid, context.job.uuid);
        assert_eq!(decoded.job.task_id, task.id);
        assert_eq!(decoded.job.node_run_id, task.node_run_id);
        assert_eq!(decoded.agent, context.agent);
    }
}
