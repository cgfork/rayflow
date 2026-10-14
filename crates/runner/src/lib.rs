//! Task runner.
//!
//! A runner executes a task's job and reports the event the run produced,
//! implementing the scheduler's `TaskRunner` contract. The job itself is
//! described by [`Job`]:
//!
//! - [`Job::Docker`] — a container image run through the Docker Engine API.
//! - [`Job::Script`] — a script run through an interpreter.
//!
//! A job is deserialized from a node's configuration payload
//! (`node_config.config`), so it uses a tagged wire format: the `type` field
//! selects the variant and the remaining fields are that variant's fields. See
//! [`Job::from_config`].
//!
//! The job types are feature-agnostic and always available. [`local::LocalRunner`]
//! executes jobs on the local machine and persists their events to the
//! `task_event` table; it is gated behind the default `mysql` feature, and the
//! [`docker`] client behind the default `docker` feature.

pub mod script;

use runtime::TaskEvent;
use uuid::Uuid;

pub struct JobContext {
    pub run_id: Uuid,
    pub graph_id: Uuid,
    pub version: u64,
    pub node_id: Uuid,
    pub revision: u64,
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
}
