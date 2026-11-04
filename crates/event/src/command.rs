//! Standalone MySQL command log.
//!
//! Register [`models`] on a Toasty database, then use [`crate::Emit`],
//! [`crate::Read`], and [`crate::Poll`] with [`CommandEntry`]. Acknowledgements
//! update only the shared `poll_offset` table. Entries remain append-only.
//! Consumers must serialize polling and acknowledgements for the global cursor.
//! The table layout matches `rayflow.sql`; do not register another model for
//! these same tables on the same database handle.

use jiff::Timestamp;
use toasty::Executor;
use uuid::Uuid;

use crate::{Emit, Error, Poll, poll};

/// Models needed to use this module without the runtime crate.
pub fn models() -> toasty::ModelSet {
    toasty::models!(CommandEntry, poll::PollOffset)
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Command {
    Start { nodes: Vec<Uuid> },
    Rerun { node: Uuid },
    Upgrade { version: u64, node: Uuid },
    Dryrun { nodes: Vec<Uuid> },
    Run { node: Uuid },
    Pend { node: Uuid },
    Resume { node: Uuid },
    Succeed { node: Uuid },
    Fail { node: Uuid },
    Cancel { node: Uuid },
    Terminate { node: Uuid },
}

/// A command recorded in the event log.
///
/// Commands are stored append-only and consumed in offset order, so a run can be
/// reconstructed from — and driven by — the sequence of entries recorded for it.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, toasty::Model)]
#[table = "command_entry"]
pub struct CommandEntry {
    /// Monotonic offset assigned when the entry is appended.
    #[key]
    #[auto]
    #[column("id")]
    pub command_offset: u64,
    /// Unique entry identifier.
    #[unique]
    #[column("uuid")]
    pub id: Uuid,
    /// Data identifier the command applies to.
    pub data_id: Uuid,
    /// Graph identifier that owns the entry.
    pub graph_id: Uuid,
    /// Command carried by this entry.
    #[column(type = json)]
    pub command: toasty::Json<Command>,
    /// Time at which the entry was created.
    pub created_at: Timestamp,
}

impl crate::Event for CommandEntry {
    fn offset(&self) -> u64 {
        self.command_offset
    }

    fn id(&self) -> Uuid {
        self.id
    }
}

impl CommandEntry {
    /// Creates a command entry that has not yet been appended to a log.
    ///
    /// The offset is assigned when the entry is emitted, so it is left at `0`.
    pub fn new(data_id: Uuid, graph_id: Uuid, command: Command) -> Self {
        Self {
            command_offset: 0,
            id: Uuid::new_v4(),
            data_id,
            graph_id,
            command: toasty::Json(command),
            created_at: Timestamp::now(),
        }
    }
}

const LOG: &str = "command_entry";

impl<B> Emit<B> for CommandEntry
where
    B: Executor,
{
    type Err = Error;

    async fn emit(&self, backend: &mut B) -> Result<u64, Self::Err> {
        let c = CommandEntry::create()
            .id(self.id)
            .data_id(self.data_id)
            .graph_id(self.graph_id)
            .command(self.command.clone())
            .created_at(self.created_at)
            .exec(&mut *backend)
            .await?;
        Ok(c.command_offset)
    }
}

impl CommandEntry {
    /// Polls using a caller-selected gap timeout. Use the same configuration for
    /// acknowledgement and serialize access to the shared log cursor.
    pub async fn poll_with_config<B: Executor>(
        backend: &mut B,
        maximum: usize,
        config: poll::Config,
    ) -> Result<impl Iterator<Item = Self>, Error> {
        let state = poll::load(&mut *backend, LOG).await?;
        let fields = CommandEntry::fields();
        let rows = CommandEntry::all()
            .filter(fields.command_offset().gt(state.offset))
            .order_by(fields.command_offset().asc())
            .limit(maximum)
            .exec(&mut *backend)
            .await?;

        let plan = poll::plan(
            rows,
            state,
            Timestamp::now(),
            |row| row.command_offset,
            config,
        );
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
        poll::ack(&mut *backend, LOG, self.command_offset, config).await?;
        Ok(())
    }
}

impl<B: Executor> Poll<B> for CommandEntry {
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

impl<B: Executor> crate::Read<uuid::Uuid, B> for CommandEntry {
    type Err = Error;

    async fn read(backend: &mut B, id: uuid::Uuid) -> Result<Self, Self::Err> {
        CommandEntry::filter_by_id(id)
            .first()
            .exec(backend)
            .await?
            .ok_or(Error::CommandNotFound(id))
    }
}
