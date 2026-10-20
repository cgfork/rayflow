//! Task dispatcher.
//!
//! A [`Dispatcher`] sits between the scheduler and the runner that actually
//! executes a task. It implements the scheduler's [`TaskRunner`] contract and,
//! on every [`run`](TaskRunner::run), first materializes a
//! [`ScheduledTask`](runtime::ScheduledTask) row referencing the task, its run,
//! its node run, and the node and run configurations in effect, then forwards
//! the task to the wrapped runner. As a [`TaskEventPoll`], `poll` is
//! forwarded unchanged and `ack` settles the record's status before forwarding,
//! so a dispatcher is a drop-in component that adds dispatch tracking.

use runtime::mysql::model::{
    NodeConfigModel, NodeRunModel, RunConfigModel, RunModel, ScheduledTaskModel,
};
use runtime::{ScheduledTask, ScheduledTaskWrite, Task, TaskEventPoll};
use toasty::Db;
use uuid::Uuid;

use super::{TaskEventEntry, TaskRunner};

/// Error returned by the [`Dispatcher`].
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A database query failed while resolving the dispatch references.
    #[error(transparent)]
    Toasty(#[from] toasty::Error),
    /// The backend returned an error.
    #[error(transparent)]
    Backend(#[from] runtime::mysql::Error),
    /// The node run driving the task does not exist.
    #[error("node run not found: {0}")]
    NodeRunNotFound(u64),
    /// The run the task's node run belongs to does not exist.
    #[error("run not found: {0}")]
    RunNotFound(Uuid),
    /// The wrapped runner returned an error.
    #[error("runner: {0}")]
    Runner(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// A [`TaskRunner`] and [`TaskEventPoll`] that records a [`ScheduledTask`]
/// before running a task.
///
/// The wrapped runner stays responsible for executing the task and reporting its
/// event; the dispatcher only adds the dispatch record.
pub struct Dispatcher<R> {
    db: Db,
    runner: R,
}

impl<R> Dispatcher<R> {
    /// Creates a dispatcher that records scheduled tasks in `db` and forwards
    /// every task to `runner`.
    pub fn new(db: Db, runner: R) -> Self {
        Self { db, runner }
    }

    /// Returns a shared reference to the wrapped runner.
    pub fn runner(&self) -> &R {
        &self.runner
    }

    /// Resolves the references a dispatched task carries and records it.
    ///
    /// The node's configuration in effect is its latest version; the run's is
    /// its latest revision at or before the run's current revision. Either is
    /// recorded as `0` when the node or run has no configuration.
    async fn dispatch(&self, task: &Task) -> Result<(), Error> {
        let mut db = self.db.clone();

        let node_run = NodeRunModel::filter_by_id(task.node_run_id)
            .first()
            .exec(&mut db)
            .await?
            .ok_or(Error::NodeRunNotFound(task.node_run_id))?;

        let run = RunModel::filter_by_uuid(node_run.run_id)
            .first()
            .exec(&mut db)
            .await?
            .ok_or(Error::RunNotFound(node_run.run_id))?;

        let node_config_id = NodeConfigModel::filter_by_node_id(task.node_id)
            .order_by(NodeConfigModel::fields().version().desc())
            .first()
            .exec(&mut db)
            .await?
            .map(|row| row.id)
            .unwrap_or(0);

        let run_config_id = RunConfigModel::filter_by_run_id(run.uuid)
            .filter(RunConfigModel::fields().revision().le(run.revision))
            .order_by(RunConfigModel::fields().revision().desc())
            .first()
            .exec(&mut db)
            .await?
            .map(|row| row.id)
            .unwrap_or(0);

        ScheduledTask::dispatch(task, run.uuid, node_config_id, run_config_id)
            .write(&mut db)
            .await?;
        Ok(())
    }
}

impl<R> TaskRunner for Dispatcher<R>
where
    R: TaskRunner,
{
    type Err = Error;

    async fn run(&self, task: &Task) -> Result<(), Self::Err> {
        self.dispatch(task).await?;
        self.runner.run(task).await.map_err(runner_err)
    }
}

impl<R> TaskEventPoll for Dispatcher<R>
where
    R: TaskEventPoll,
{
    type Err = Error;

    async fn poll(&self, max: usize, shards: &[u32]) -> Result<Vec<TaskEventEntry>, Self::Err> {
        self.runner.poll(max, shards).await.map_err(runner_err)
    }

    async fn ack(&self, event: &TaskEventEntry) -> Result<(), Self::Err> {
        self.settle(event).await?;
        self.runner.ack(event).await.map_err(runner_err)
    }
}

impl<R> Dispatcher<R> {
    /// Advances the scheduled task for `event`'s task to the event's status.
    ///
    /// Events are acknowledged after they have been driven, so settling here
    /// keeps the record's status in step with the run. A task with no scheduled
    /// record (e.g. one dispatched elsewhere) is left untouched.
    async fn settle(&self, event: &TaskEventEntry) -> Result<(), Error> {
        let mut db = self.db.clone();
        let Some(row) = ScheduledTaskModel::filter_by_task_id(event.task_id)
            .first()
            .exec(&mut db)
            .await?
        else {
            return Ok(());
        };
        ScheduledTaskModel::filter_by_id(row.id)
            .update()
            .status(event.event.status().to_string())
            .updated_at(jiff::Timestamp::now())
            .exec(&mut db)
            .await?;
        Ok(())
    }
}

fn runner_err<E>(error: E) -> Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    Error::Runner(Box::new(error))
}
