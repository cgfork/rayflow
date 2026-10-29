//! Continuous leader-elected consumer loop.

use std::future::Future;
use std::time::Duration;

use coordinator::TransactionalLeaderElection;
use runtime::{Error as BackendError, backend::Backend};
use scheduler::{Actuator, AgentScheduler, Scheduler};
use server::StaticGraphStore;
use toasty::Db;
use tokio::time::{MissedTickBehavior, interval};

/// Error returned by a pilot consumer pass.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The MySQL backend returned an error.
    #[error(transparent)]
    Backend(#[from] BackendError),
    /// Graph actuation failed.
    #[error(transparent)]
    Actuator(#[from] scheduler::Error),
    /// The scheduling implementation failed.
    #[error("scheduling: {0}")]
    Scheduler(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// Leader election returned an error.
    #[error("leader election: {0}")]
    Leader(#[source] Box<dyn std::error::Error + Send + Sync>),
}

pub use scheduler::Step;

/// Consumes commands, tasks, and task events while this pilot holds the leader
/// lease.
pub struct Pilot<E = coordinator::mysql::MySqlLeaderElection, S = AgentScheduler> {
    election: E,
    actuator: Actuator<StaticGraphStore>,
    db: Db,
    batch: usize,
    poll_interval: Duration,
    scheduler: S,
}

impl<E> Pilot<E, AgentScheduler> {
    /// Waits this long after job expiry before failing unreported timeouts.
    pub fn with_job_timeout_gap(mut self, gap: Duration) -> Self {
        self.scheduler = self.scheduler.with_timeout_gap(gap);
        self
    }
    /// Creates a pilot from a leader election and MySQL backend.
    /// Register `scheduler::models()` and any models required by a custom scheduler.
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
            actuator: Actuator::new(backend, batch),
            db,
            batch,
            poll_interval,
            scheduler: AgentScheduler::new(),
        }
    }
}

impl<E, S> Pilot<E, S> {
    /// Sets the missing-offset timeout for command, task, and task-event logs.
    pub fn with_gap_timeout(mut self, gap_timeout: Duration) -> Self {
        self.actuator = self.actuator.with_gap_timeout(gap_timeout);
        self
    }

    /// Uses another scheduling implementation, such as an external job scheduler.
    pub fn with_scheduler<T>(self, scheduler: T) -> Pilot<E, T> {
        Pilot {
            election: self.election,
            actuator: self.actuator,
            db: self.db,
            batch: self.batch,
            poll_interval: self.poll_interval,
            scheduler,
        }
    }

    /// Returns the leader-election handle owned by this pilot.
    pub fn election(&self) -> &E {
        &self.election
    }
}

impl<E, S> Pilot<E, S>
where
    E: for<'tx> TransactionalLeaderElection<toasty::Transaction<'tx>>,
    S: for<'tx> Scheduler<toasty::Transaction<'tx>>,
{
    /// Returns whether this pilot currently holds the leader lease.
    pub async fn is_leader(&self) -> Result<bool, Error> {
        self.election
            .is_leader()
            .await
            .map_err(|e| Error::Leader(Box::new(e)))
    }

    /// Runs one pass: renew leadership, apply commands, create jobs, and fail timed-out jobs.
    ///
    /// A standby returns [`Step::default`] without touching the logs. The lease
    /// row is locked for the entire pass, so takeover waits even
    /// if the lease expires. Database effects and all log acknowledgements commit
    /// together. External scheduler submissions must follow the durability and
    /// retry requirements in [`Scheduler`].
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

        let step = self.actuator.tick_in(&mut tx).await?;
        self.scheduler
            .schedule(&mut tx)
            .await
            .map_err(|error| Error::Scheduler(Box::new(error)))?;
        tx.commit().await.map_err(BackendError::from)?;
        Ok(step)
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
