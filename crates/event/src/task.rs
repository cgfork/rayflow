//! Standalone MySQL task log.
//!
//! Register [`models`] on a Toasty database, then use [`crate::Emit`],
//! [`crate::Read`], and [`crate::Poll`] with [`Task`]. Acknowledgements
//! update only the shared `poll_offset` table. Entries remain append-only.
//! Consumers must serialize polling and acknowledgements for the global cursor.
//! The table layout matches `runtime.sql`; do not register another model for
//! these same tables on the same database handle.

use jiff::Timestamp;
use toasty::Executor;
use uuid::Uuid;

use crate::{Emit, Error, Poll, poll};

/// Models needed to use this module without the runtime crate.
pub fn models() -> toasty::ModelSet {
    toasty::models!(Task, poll::PollOffset)
}

/// A task recorded in the append-only log.
///
/// Tasks are consumed in offset order and remain readable after acknowledgement.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, toasty::Model)]
#[table = "task"]
pub struct Task {
    /// Monotonic offset assigned when the entry is appended.
    #[key]
    #[auto]
    #[column("id")]
    pub task_offset: u64,
    /// Unique entry identifier.
    #[unique]
    #[column("uuid")]
    pub id: Uuid,
    /// Data identifier the task applies to.
    pub data_id: Uuid,
    /// Graph identifier that owns the entry.
    pub graph_id: Uuid,
    /// Node that owns this task.
    pub node_id: Uuid,
    /// Owning node-run identifier; no runtime model dependency is required.
    #[index]
    pub node_run_id: u64,
    /// Time at which the entry was created.
    pub created_at: Timestamp,
}

impl crate::Event for Task {
    fn offset(&self) -> u64 {
        self.task_offset
    }

    fn id(&self) -> Uuid {
        self.id
    }
}

impl Task {
    /// Creates a task that has not yet been appended to a log.
    ///
    /// The offset is assigned when the entry is emitted, so it is left at `0`.
    pub fn new(data_id: Uuid, graph_id: Uuid, node_id: Uuid, node_run_id: u64) -> Self {
        Self {
            task_offset: 0,
            id: Uuid::new_v4(),
            data_id,
            graph_id,
            node_id,
            node_run_id,
            created_at: Timestamp::now(),
        }
    }
}

const LOG: &str = "task";

impl<B> Emit<B> for Task
where
    B: Executor,
{
    type Err = Error;

    async fn emit(&self, backend: &mut B) -> Result<u64, Self::Err> {
        let c = Task::create()
            .id(self.id)
            .data_id(self.data_id)
            .graph_id(self.graph_id)
            .node_id(self.node_id)
            .node_run_id(self.node_run_id)
            .created_at(self.created_at)
            .exec(&mut *backend)
            .await?;
        Ok(c.task_offset)
    }
}

impl Task {
    /// Polls using a caller-selected gap timeout. Use the same configuration for
    /// acknowledgement and serialize access to the shared log cursor.
    pub async fn poll_with_config<B: Executor>(
        backend: &mut B,
        maximum: usize,
        config: poll::Config,
    ) -> Result<impl Iterator<Item = Self>, Error> {
        let state = poll::load(&mut *backend, LOG).await?;
        let fields = Task::fields();
        let rows = Task::all()
            .filter(fields.task_offset().gt(state.offset))
            .order_by(fields.task_offset().asc())
            .limit(maximum)
            .exec(&mut *backend)
            .await?;

        let plan = poll::plan(rows, state, Timestamp::now(), |row| row.task_offset, config);
        if plan.changed {
            poll::save(&mut *backend, LOG, plan.state).await?;
        }

        Ok(plan.rows.into_iter())
    }

    /// Acknowledges using the same gap timeout as `poll_with_config`.
    pub async fn ack_with_config<B: Executor>(
        &self,
        backend: &mut B,
        config: poll::Config,
    ) -> Result<(), Error> {
        poll::ack(&mut *backend, LOG, self.task_offset, config).await?;
        Ok(())
    }
}

impl<B: Executor> Poll<B> for Task {
    type Err = Error;

    async fn poll(
        backend: &mut B,
        maximum: usize,
    ) -> Result<impl Iterator<Item = Self>, Self::Err> {
        Self::poll_with_config(backend, maximum, poll::Config::default()).await
    }

    async fn ack(&self, backend: &mut B) -> Result<(), Self::Err> {
        self.ack_with_config(backend, poll::Config::default()).await
    }
}

impl<B: Executor> crate::Read<uuid::Uuid, B> for Task {
    type Err = Error;

    async fn read(backend: &mut B, id: uuid::Uuid) -> Result<Self, Self::Err> {
        Task::filter_by_id(id)
            .first()
            .exec(backend)
            .await?
            .ok_or(Error::TaskNotFound(id))
    }
}
