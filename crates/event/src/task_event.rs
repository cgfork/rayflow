//! Standalone append-only MySQL task-event log.
//!
//! Register [`models`] for this log alone or [`crate::models`] for all logs.
//! Event UUIDs are independent of task UUIDs; a task may have multiple events.
//! Acknowledgements update the shared offset table without changing events.
//! Consumers must serialize polling and acknowledgement for the global cursor.

use crate::{Emit, Error, Poll, command::Command, poll, task::Task};
use jiff::Timestamp;
use toasty::Executor;
use uuid::Uuid;

/// Registers this log and its consumer-offset table without runtime models.
pub fn models() -> toasty::ModelSet {
    toasty::models!(TaskEventEntry, poll::PollOffset)
}

/// The event a task run reports, driving its node's status.
///
/// Each event maps to the [`Command`] that applies it, so recording an event and
/// replaying it are enough to drive a run forward.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize, toasty::Embed,
)]
#[column(rename_all = "PascalCase", type = varchar(16))]
pub enum TaskEvent {
    /// Defers the task, driving `Ready`/`Running -> Pending`.
    Pend,
    /// Marks the node `Succeeded`.
    Succeed,
    /// Marks the node `Failed`.
    Fail,
    /// Marks the node `Terminated`.
    Terminate,
    /// Marks the node `Terminated`.
    Cancel,
}

impl TaskEvent {
    /// The command that drives `node` to the status this event names.
    pub fn command(self, node: Uuid) -> Command {
        match self {
            TaskEvent::Pend => Command::Pend { node },
            TaskEvent::Succeed => Command::Succeed { node },
            TaskEvent::Fail => Command::Fail { node },
            TaskEvent::Terminate => Command::Terminate { node },
            TaskEvent::Cancel => Command::Cancel { node },
        }
    }
}

impl std::fmt::Display for TaskEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            TaskEvent::Pend => "Pend",
            TaskEvent::Succeed => "Succeed",
            TaskEvent::Fail => "Fail",
            TaskEvent::Terminate => "Terminate",
            TaskEvent::Cancel => "Cancel",
        })
    }
}

impl std::str::FromStr for TaskEvent {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Pend" => Ok(TaskEvent::Pend),
            "Succeed" => Ok(TaskEvent::Succeed),
            "Fail" => Ok(TaskEvent::Fail),
            "Terminate" => Ok(TaskEvent::Terminate),
            "Cancel" => Ok(TaskEvent::Cancel),
            _ => Err(Error::InvalidTaskEvent(s.to_owned())),
        }
    }
}

/// A task event recorded in the event log.
///
/// Events are stored append-only and consumed in offset order, so the outcome of
/// a run can be reconstructed from — and driven by — the events recorded for it.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, toasty::Model)]
#[table = "task_event"]
pub struct TaskEventEntry {
    /// Monotonic offset assigned when the event is appended.
    #[key]
    #[auto]
    #[column("id")]
    pub event_offset: u64,
    /// Unique event identifier.
    #[unique]
    #[column("uuid")]
    pub id: Uuid,
    /// Identifier of the task whose run produced the event.
    #[index]
    pub task_id: Uuid,
    /// Data identifier of the run that produced the event.
    pub data_id: Uuid,
    /// Graph identifier that owns the event.
    pub graph_id: Uuid,
    /// Node identifier within the graph.
    pub node_id: Uuid,
    /// Identifier of the node run the event drives.
    pub node_run_id: u64,
    /// Event the run reported.
    pub event: TaskEvent,
    /// Time at which the event was created.
    pub created_at: Timestamp,
}

impl crate::Event for TaskEventEntry {
    fn offset(&self) -> u64 {
        self.event_offset
    }

    fn id(&self) -> Uuid {
        self.id
    }
}

impl TaskEventEntry {
    /// Creates an event entry for `task` that has not yet been appended to a log.
    ///
    /// The offset is assigned when the entry is written, so it is left at `0` here.
    pub fn new(task: &Task, event: TaskEvent) -> Self {
        Self {
            event_offset: 0,
            id: Uuid::new_v4(),
            task_id: task.id,
            data_id: task.data_id,
            graph_id: task.graph_id,
            node_id: task.node_id,
            node_run_id: task.node_run_id,
            event,
            created_at: Timestamp::now(),
        }
    }
}

const LOG: &str = "task_event";

impl<B> Emit<B> for TaskEventEntry
where
    B: Executor,
{
    type Err = Error;

    async fn emit(&self, backend: &mut B) -> Result<u64, Self::Err> {
        let c = TaskEventEntry::create()
            .id(self.id)
            .task_id(self.task_id)
            .data_id(self.data_id)
            .graph_id(self.graph_id)
            .node_id(self.node_id)
            .node_run_id(self.node_run_id)
            .event(self.event)
            .created_at(self.created_at)
            .exec(&mut *backend)
            .await?;
        Ok(c.event_offset)
    }
}

impl TaskEventEntry {
    /// Polls using a caller-selected gap timeout. Use the same configuration for
    /// acknowledgement and serialize access to the shared log cursor.
    pub async fn poll_with_config<B: Executor>(
        backend: &mut B,
        maximum: usize,
        config: poll::Config,
    ) -> Result<impl Iterator<Item = Self>, Error> {
        let state = poll::load(&mut *backend, LOG).await?;
        let fields = TaskEventEntry::fields();
        let rows = TaskEventEntry::all()
            .filter(fields.event_offset().gt(state.offset))
            .order_by(fields.event_offset().asc())
            .limit(maximum)
            .exec(&mut *backend)
            .await?;

        let plan = poll::plan(
            rows,
            state,
            Timestamp::now(),
            |row| row.event_offset,
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
        poll::ack(&mut *backend, LOG, self.event_offset, config).await?;
        Ok(())
    }
}

impl<B: Executor> Poll<B> for TaskEventEntry {
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

impl<B: Executor> crate::Read<uuid::Uuid, B> for TaskEventEntry {
    type Err = Error;

    async fn read(backend: &mut B, id: uuid::Uuid) -> Result<Self, Self::Err> {
        TaskEventEntry::filter_by_id(id)
            .first()
            .exec(backend)
            .await?
            .ok_or(Error::TaskEventNotFound(id))
    }
}
