//! Script job.
//!
//! A [`Script`] runs its source through an interpreter, e.g. `sh -c <source>`. It
//! is a [`Runnable`] job: [`Runnable::run`] executes the script and reports the
//! resulting [`State`], whose [`TaskState::event`] drives the node. A successful
//! run sleeps for 500ms before returning.

use serde::{Deserialize, Serialize};

use crate::{ErrorCode, JobContext, Runnable, TaskState};
use event::task_event::TaskEvent;

/// A script job: source run through an interpreter.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Script {
    /// Interpreter that runs the script, e.g. `sh`.
    pub interpreter: String,
    /// Script source, passed to the interpreter.
    pub source: String,
}

impl Script {
    /// Creates a script run by `interpreter`.
    pub fn new(interpreter: impl Into<String>, source: impl Into<String>) -> Self {
        Self {
            interpreter: interpreter.into(),
            source: source.into(),
        }
    }

    /// The program and arguments that run this script locally.
    ///
    /// The source is run as `<interpreter> -c <source>`.
    pub fn argv(&self) -> (String, Vec<String>) {
        (
            self.interpreter.clone(),
            vec!["-c".to_string(), self.source.clone()],
        )
    }

    /// Spawns the interpreter and waits for it to finish.
    async fn execute(&self) -> Result<std::process::ExitStatus, std::io::Error> {
        let (program, args) = self.argv();
        tokio::process::Command::new(program)
            .args(args)
            .status()
            .await
    }
}

/// The outcome of running a [`Script`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct State {
    /// The script's exit code, or `None` if it was terminated by a signal.
    pub code: Option<i32>,
}

impl State {
    /// Whether the script exited successfully.
    pub fn succeeded(&self) -> bool {
        self.code == Some(0)
    }
}

impl TaskState for State {
    fn event(&self) -> Option<TaskEvent> {
        Some(if self.succeeded() {
            TaskEvent::Succeed
        } else {
            TaskEvent::Fail
        })
    }

    fn exit_code(&self) -> Option<i32> {
        self.code
    }
}

/// Error returned while running a [`Script`].
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The interpreter could not be spawned or waited on.
    #[error("failed to run script: {0}")]
    Spawn(#[source] std::io::Error),
}

impl ErrorCode for Error {
    fn code(&self) -> u32 {
        match self {
            Error::Spawn(error) => error.raw_os_error().map_or(1, i32::unsigned_abs),
        }
    }
}

impl Runnable for Script {
    type Err = Error;
    type State = State;

    async fn run(&self, _ctx: JobContext) -> Result<Self::State, Self::Err> {
        let status = self.execute().await.map_err(Error::Spawn)?;
        let state = State {
            code: status.code(),
        };
        if state.succeeded() {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
        Ok(state)
    }
}

#[cfg(test)]
mod tests {
    use crate::JobContext;
    use uuid::Uuid;

    use super::*;

    fn context() -> JobContext {
        JobContext {
            run_id: Uuid::new_v4(),
            graph_id: Uuid::new_v4(),
            version: 1,
            node_id: Uuid::new_v4(),
            revision: 0,
        }
    }

    #[test]
    fn builds_interpreter_argv() {
        let script = Script::new("sh", "echo hi");
        assert_eq!(
            script.argv(),
            (
                "sh".to_string(),
                vec!["-c".to_string(), "echo hi".to_string()]
            )
        );
    }

    #[test]
    fn successful_state_maps_to_succeed() {
        assert_eq!(State { code: Some(0) }.event(), Some(TaskEvent::Succeed));
    }

    #[test]
    fn failed_state_maps_to_fail() {
        assert_eq!(State { code: Some(3) }.event(), Some(TaskEvent::Fail));
        assert_eq!(State { code: None }.event(), Some(TaskEvent::Fail));
    }

    #[tokio::test]
    async fn run_reports_success() {
        let started = std::time::Instant::now();
        let state = Script::new("sh", "exit 0").run(context()).await.unwrap();
        assert_eq!(state.event(), Some(TaskEvent::Succeed));
        assert!(
            started.elapsed() >= std::time::Duration::from_millis(500),
            "a successful run sleeps 500ms before returning"
        );
    }

    #[tokio::test]
    async fn run_reports_failure() {
        let started = std::time::Instant::now();
        let state = Script::new("sh", "exit 3").run(context()).await.unwrap();
        assert_eq!(state.code, Some(3));
        assert_eq!(state.event(), Some(TaskEvent::Fail));
        assert!(
            started.elapsed() < std::time::Duration::from_millis(500),
            "a failed run returns without the success sleep"
        );
    }
}
