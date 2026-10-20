//! Continuous leader-elected consumer loop.

use std::future::Future;
use std::time::Duration;

use coordinator::{Error as LeaderError, LeaderElection};
use runtime::mysql::{Backend, Error as BackendError};
use runtime::{CommandApply, CommandEntry, CommandPoll};
use scheduler::{AutoRunner, Error as SchedulerError, Scheduler};
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
    #[error(transparent)]
    Leader(#[from] LeaderError),
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
pub struct Pilot {
    election: LeaderElection,
    scheduler: Scheduler<Backend<StaticGraphStore>, AutoRunner>,
    backend: Backend<StaticGraphStore>,
    db: Db,
    batch: usize,
    poll_interval: Duration,
}

impl Pilot {
    /// Creates a pilot from a leader election and MySQL backend.
    pub fn new(
        election: LeaderElection,
        backend: Backend<StaticGraphStore>,
        batch: usize,
        poll_interval: Duration,
    ) -> Self {
        assert!(batch > 0, "batch must be at least 1");
        assert!(!poll_interval.is_zero(), "poll interval must be non-zero");

        let db = backend.db().clone();
        let runner = AutoRunner::new(db.clone());
        let scheduler = Scheduler::new(backend.clone(), runner, batch);
        Self {
            election,
            scheduler,
            backend,
            db,
            batch,
            poll_interval,
        }
    }

    /// Returns the leader-election handle owned by this pilot.
    pub fn election(&self) -> &LeaderElection {
        &self.election
    }

    /// Returns whether this pilot currently holds the leader lease.
    pub async fn is_leader(&self) -> Result<bool, Error> {
        Ok(self.election.is_leader().await?)
    }

    async fn consume_commands(&mut self) -> Result<usize, Error> {
        let entries: Vec<CommandEntry> = CommandEntry::poll(&mut self.db, self.batch)
            .await?
            .collect();
        let count = entries.len();
        for entry in entries {
            entry.apply(self.backend.clone()).await?;
            entry.ack(&mut self.db).await?;
        }
        Ok(count)
    }

    /// Runs one consumer pass: renew the lease, apply commands, and run tasks.
    ///
    /// A standby returns [`Step::default`] without touching the logs.
    pub async fn step(&mut self) -> Result<Step, Error> {
        if !self.election.try_acquire_or_renew().await? {
            return Ok(Step::default());
        }

        let mut commands = self.consume_commands().await?;
        let tasks = self.scheduler.tick().await?;
        commands += self.consume_commands().await?;
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
                    self.election.resign().await?;
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
