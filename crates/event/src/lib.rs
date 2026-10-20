//! Backend-independent access to append-only event logs.
//!
//! Event identity is independent of log position. Emitting appends a new record;
//! reading and polling never change records. Acknowledgement advances consumer
//! state separately from the log. A backend may be a transaction so callers can
//! commit effects and acknowledgements atomically.

pub mod command;
pub mod poll;
pub mod task;
pub mod task_event;

use uuid::Uuid;

/// Errors shared by the standalone event logs and their polling state.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Database operation or stored record decoding failed.
    #[error(transparent)]
    Toasty(#[from] toasty::Error),
    /// No command has the requested UUID.
    #[error("command not found: {0}")]
    CommandNotFound(Uuid),
    /// No task has the requested UUID.
    #[error("task not found: {0}")]
    TaskNotFound(Uuid),
    /// No task event has the requested UUID.
    #[error("task event not found: {0}")]
    TaskEventNotFound(Uuid),
    /// A task event name is unknown.
    #[error("invalid task event: {0}")]
    InvalidTaskEvent(String),
}

/// A record's position in its log and its independent identity.
pub trait Event {
    /// Returns the assigned log offset, or zero before persistence.
    fn offset(&self) -> u64;

    /// Returns this record's UUID, distinct from any referenced record's ID.
    fn id(&self) -> Uuid;
}

/// Appends an immutable record to a log.
#[allow(async_fn_in_trait)]
pub trait Emit<B> {
    /// Backend error.
    type Err: std::error::Error;

    /// Appends this record and returns its assigned monotonic offset.
    /// Existing records must not be overwritten, including on duplicate IDs.
    async fn emit(&self, backend: &mut B) -> Result<u64, Self::Err>;
}

/// Reads an immutable record by its identifier without consuming it.
#[allow(async_fn_in_trait)]
pub trait Read<I, B>: Send + Sized {
    /// Backend error, including a missing identifier.
    type Err: std::error::Error;

    /// Returns the record identified by `id`.
    async fn read(backend: &mut B, id: I) -> Result<Self, Self::Err>;
}

/// Polls an ordered log using backend-managed consumer state.
#[allow(async_fn_in_trait)]
pub trait Poll<B>: Send + Sized {
    /// Backend error.
    type Err: std::error::Error;

    /// Returns up to `maximum` records starting at the first unacknowledged
    /// contiguous offset. Unacknowledged records remain available on retry.
    async fn poll(backend: &mut B, maximum: usize)
    -> Result<impl Iterator<Item = Self>, Self::Err>;

    /// Advances consumer state after processing this record, without modifying
    /// or deleting it. Callers acknowledge returned records in offset order.
    async fn ack(&self, backend: &mut B) -> Result<(), Self::Err>;
}

/// Continuously consumes a log, handling and acknowledging records in order.
///
/// Implement [`handle`](Self::handle) for the event type and backend. Handler
/// effects and acknowledgement are separate operations: handlers must tolerate
/// replay if acknowledgement fails or the process stops after handling.
#[allow(async_fn_in_trait)]
pub trait Reactive<B>: Poll<B> {
    /// Handles one record. An error leaves it unacknowledged and stops the loop.
    async fn handle(&self, backend: &mut B) -> Result<(), Self::Err>;

    /// Polls until `shutdown` resolves or polling, handling, or acknowledgement
    /// fails. Returns the number of records acknowledged by this invocation.
    ///
    /// Full batches are followed immediately by another poll. After a partial
    /// batch, waits `poll_interval` to avoid spinning on an idle log. Shutdown
    /// is checked before polling, between records, and during the idle wait;
    /// an in-flight poll or handle/ack pair is allowed to finish.
    ///
    /// This method does not acquire leadership. The caller must ensure exclusive
    /// consumption of a shared cursor. Use a durable backend rather than a
    /// transaction spanning this unbounded loop.
    ///
    /// # Panics
    /// Panics if `batch` or `poll_interval` is zero.
    async fn run<F>(
        backend: &mut B,
        batch: usize,
        poll_interval: std::time::Duration,
        shutdown: F,
    ) -> Result<usize, Self::Err>
    where
        F: std::future::Future<Output = ()>,
    {
        assert!(batch > 0, "batch must be at least 1");
        assert!(!poll_interval.is_zero(), "poll interval must be non-zero");
        tokio::pin!(shutdown);
        let mut acknowledged = 0;
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => return Ok(acknowledged),
                _ = std::future::ready(()) => {}
            }
            let events: Vec<Self> = Self::poll(backend, batch).await?.collect();
            let count = events.len();
            for event in events {
                tokio::select! {
                    biased;
                    _ = &mut shutdown => return Ok(acknowledged),
                    _ = std::future::ready(()) => {}
                }
                event.handle(backend).await?;
                event.ack(backend).await?;
                acknowledged += 1;
            }
            if count < batch {
                tokio::select! {
                    biased;
                    _ = &mut shutdown => return Ok(acknowledged),
                    _ = tokio::time::sleep(poll_interval) => {}
                }
            } else {
                // Custom backends may return immediately without yielding.
                tokio::task::yield_now().await;
            }
        }
    }
}

/// Registers all standalone logs and their shared consumer-offset table.
/// Use this when commands, tasks, and task events share a database handle.
pub fn models() -> toasty::ModelSet {
    toasty::models!(
        command::CommandEntry,
        task::Task,
        task_event::TaskEventEntry,
        poll::PollOffset
    )
}
