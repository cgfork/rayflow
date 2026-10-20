//! Task executor.
//!
//! The executor crate provides job primitives for executing a task's work.
//! [`Runnable`] describes a job that runs in a [`JobContext`] and returns a
//! [`TaskState`], whose event drives the task's node. [`script::Script`] is the
//! built-in job that runs source through an interpreter.
//!
//! The job types are feature-agnostic and always available.

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
