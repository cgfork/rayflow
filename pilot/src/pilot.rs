//! Continuous leader-elected consumer loop.

use std::future::Future;
use std::time::Duration;

use coordinator::TransactionalLeaderElection;
use event::Emit as CommandEmit;
use event::Poll as CommandPoll;
use event::command::Command;
use event::command::CommandEntry;
use event::task::Task;
use event::task_event::TaskEvent;
use event::task_event::TaskEventEntry;
use runtime::{Error as BackendError, backend::Backend};
use scheduler::Error as SchedulerError;
use server::StaticGraphStore;
use toasty::Db;
use tokio::time::{MissedTickBehavior, interval};

/// Error returned by a pilot consumer pass.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The MySQL backend returned an error.
    #[error(transparent)]
    Backend(#[from] BackendError),
    /// The scheduler returned an error.
    #[error(transparent)]
    Scheduler(#[from] SchedulerError),
    /// Leader election returned an error.
    #[error("leader election: {0}")]
    Leader(#[source] Box<dyn std::error::Error + Send + Sync>),
}

impl From<event::Error> for Error {
    fn from(error: event::Error) -> Self {
        Self::Backend(error.into())
    }
}

/// Work consumed by one pilot pass.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Step {
    /// Command entries applied and acknowledged.
    pub commands: usize,
    /// Tasks scheduled and run; task events are consumed by the same pass.
    pub tasks: usize,
}

/// Consumes commands, tasks, and task events while this pilot holds the leader
/// lease.
pub struct Pilot<E = coordinator::mysql::MySqlLeaderElection> {
    election: E,
    backend: Backend<StaticGraphStore>,
    db: Db,
    batch: usize,
    poll_interval: Duration,
}

impl<E> Pilot<E>
where
    E: for<'tx> TransactionalLeaderElection<toasty::Transaction<'tx>>,
{
    /// Creates a pilot from a leader election and MySQL backend.
    pub fn new(
        election: E,
        backend: Backend<StaticGraphStore>,
        batch: usize,
        poll_interval: Duration,
    ) -> Self {
        assert!(batch > 0, "batch must be at least 1");
        assert!(!poll_interval.is_zero(), "poll interval must be non-zero");

        let db = backend.db().clone();
        Self {
            election,
            backend,
            db,
            batch,
            poll_interval,
        }
    }

    /// Returns the leader-election handle owned by this pilot.
    pub fn election(&self) -> &E {
        &self.election
    }

    /// Returns whether this pilot currently holds the leader lease.
    pub async fn is_leader(&self) -> Result<bool, Error> {
        self.election
            .is_leader()
            .await
            .map_err(|e| Error::Leader(Box::new(e)))
    }

    async fn consume_commands(&mut self, tx: &mut toasty::Transaction<'_>) -> Result<usize, Error> {
        let entries: Vec<CommandEntry> = CommandEntry::poll(&mut *tx, self.batch).await?.collect();
        let count = entries.len();
        for entry in entries {
            self.backend.apply_in(tx, &entry).await?;
            entry.ack(&mut *tx).await?;
        }
        Ok(count)
    }

    /// Runs one consumer pass: renew the lease, apply commands, and run tasks.
    ///
    /// A standby returns [`Step::default`] without touching the logs. The lease
    /// row is locked for the entire database-only pass, so takeover waits even
    /// if the lease expires. Effects and all log acknowledgements commit together;
    /// a failed or interrupted pass rolls them all back.
    pub async fn step(&mut self) -> Result<Step, Error> {
        let mut db = self.db.clone();
        let mut tx = db.transaction().await.map_err(BackendError::from)?;
        if !self
            .election
            .acquire_in(&mut tx)
            .await
            .map_err(|e| Error::Leader(Box::new(e)))?
        {
            tx.commit().await.map_err(BackendError::from)?;
            return Ok(Step::default());
        }

        let mut commands = self.consume_commands(&mut tx).await?;
        let tasks: Vec<Task> = Task::poll(&mut tx, self.batch).await?.collect();
        for task in &tasks {
            CommandEntry::new(
                task.data_id,
                task.graph_id,
                Command::Run { node: task.node_id },
            )
            .emit(&mut tx)
            .await?;
            // The pilot's automatic runner only records success; keep that
            // event and the task acknowledgement in the protected transaction.
            TaskEventEntry::new(task, TaskEvent::Succeed)
                .emit(&mut tx)
                .await?;
            task.ack(&mut tx).await?;
        }
        let events: Vec<_> = TaskEventEntry::poll(&mut tx, self.batch).await?.collect();
        for event in events {
            CommandEntry::new(
                event.data_id,
                event.graph_id,
                event.event.command(event.node_id),
            )
            .emit(&mut tx)
            .await?;
            event.ack(&mut tx).await?;
        }
        commands += self.consume_commands(&mut tx).await?;
        let tasks = tasks.len();
        tx.commit().await.map_err(BackendError::from)?;
        Ok(Step { commands, tasks })
    }

    /// Runs consumer passes until `shutdown` resolves.
    ///
    /// A failed pass is logged and retried on the next interval so transient
    /// database or store errors do not stop the pilot. The lease is released on
    /// shutdown so a standby can take over immediately.
    pub async fn run<F>(&mut self, shutdown: F) -> Result<(), Error>
    where
        F: Future<Output = ()>,
    {
        tokio::pin!(shutdown);
        let mut ticker = interval(self.poll_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    self.election.resign().await.map_err(|e| Error::Leader(Box::new(e)))?;
                    return Ok(());
                }
                _ = ticker.tick() => {
                    if let Err(error) = self.step().await {
                        eprintln!("pilot: consumer pass failed: {error}");
                    }
                }
            }
        }
    }
}
