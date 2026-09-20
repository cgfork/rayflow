#[cfg(feature = "mysql")]
pub mod mysql;

use std::collections::HashMap;

use dag::{Graph, Id};
use jiff::Timestamp;
use uuid::Uuid;

/// Stable FNV-1a hash of a data id, used to place keys on the consistent hash ring.
pub fn hash_data_id(data_id: Uuid) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET;
    for byte in data_id.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// Maps a data id to its shard index on the consistent hash ring.
///
/// The result is always in `0..shards`, so it can be stored in the `shard`
/// columns and used to route keys on the ring.
pub fn shard_of(data_id: Uuid, shards: u32) -> u32 {
    (hash_data_id(data_id) % u64::from(shards)) as u32
}

/// The node status.
/// 1. Ready -> Running -> Succeeded/Failed/Terminated
/// 2. Ready -> Running -> Pending -> Succeeded/Failed/Terminated
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    Ready,
    Running,
    Pending,
    Succeeded,
    Failed,
    Terminated,
}

impl std::fmt::Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Status::Ready => "Ready",
            Status::Running => "Running",
            Status::Pending => "Pending",
            Status::Succeeded => "Succeeded",
            Status::Failed => "Failed",
            Status::Terminated => "Terminated",
        })
    }
}

impl std::str::FromStr for Status {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Ready" => Ok(Status::Ready),
            "Running" => Ok(Status::Running),
            "Pending" => Ok(Status::Pending),
            "Succeeded" => Ok(Status::Succeeded),
            "Failed" => Ok(Status::Failed),
            "Terminated" => Ok(Status::Terminated),
            _ => Err(()),
        }
    }
}

/// Applies a command entry to a backend, driving node state.
#[allow(async_fn_in_trait)]
pub trait CommandApply<B> {
    /// Error returned when applying a command entry.
    type Err: std::error::Error;

    /// Applies this command entry to `backend`.
    async fn apply(&self, backend: B) -> Result<(), Self::Err>;
}

/// Appends a command entry to a backend's command log (event sourcing).
#[allow(async_fn_in_trait)]
pub trait CommandEmit<B> {
    /// Error returned when emitting a command entry.
    type Err: std::error::Error;

    /// Appends this command entry to `backend`'s log.
    async fn emit(&self, backend: &mut B) -> Result<(), Self::Err>;
}

/// Polls command entries that are waiting to be applied.
#[allow(async_fn_in_trait)]
pub trait CommandPoll<B>: Send + Sized {
    /// Error returned when polling or acknowledging a command entry.
    type Err: std::error::Error;

    /// Returns up to `maxium` command entries that have not been acknowledged.
    ///
    /// When `shard` is `Some`, only entries routed to that shard are returned;
    /// when `None`, entries are returned across every shard.
    async fn poll(
        backend: &mut B,
        maxium: usize,
        shard: Option<u32>,
    ) -> Result<impl Iterator<Item = Self>, Self::Err>;

    /// Acknowledges this entry, advancing the recorded offset of its shard to
    /// `self.command_offset`.
    async fn ack(&self, backend: &mut B) -> Result<(), Self::Err>;
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Command {
    Start { nodes: Vec<Uuid> },
    Rerun { node: Uuid },
    Dryrun { nodes: Vec<Uuid> },
    Run { node: Uuid },
    Pend { node: Uuid },
    Succeed { node: Uuid },
    Fail { node: Uuid },
    Cancel { node: Uuid },
    Terminate { node: Uuid },
}

/// A command recorded in the event log.
///
/// Commands are stored append-only and consumed in offset order, so a run can be
/// reconstructed from — and driven by — the sequence of entries recorded for it.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CommandEntry {
    /// Monotonic offset assigned when the entry is appended.
    pub command_offset: u64,
    /// Unique entry identifier.
    pub id: Uuid,
    /// Data identifier used to route the entry to a shard.
    pub data_id: Uuid,
    /// Graph identifier that owns the entry.
    pub graph_id: Uuid,
    /// Shard this entry is routed to.
    pub shard: u32,
    /// Command carried by this entry.
    pub command: Command,
    /// Time at which the entry was created.
    pub created_at: Timestamp,
}

impl CommandEntry {
    /// Creates a command entry that has not yet been appended to a log.
    ///
    /// The offset is assigned when the entry is emitted, so it is left at `0`
    /// here. The entry is not routed to a shard; use [`routed`](Self::routed)
    /// before emitting so it lands on the right shard.
    pub fn new(data_id: Uuid, graph_id: Uuid, command: Command) -> Self {
        Self {
            command_offset: 0,
            id: Uuid::new_v4(),
            data_id,
            graph_id,
            shard: 0,
            command,
            created_at: Timestamp::now(),
        }
    }

    /// Creates a command entry routed to the shard of `data_id` over `shards` shards.
    pub fn routed(data_id: Uuid, graph_id: Uuid, command: Command, shards: u32) -> Self {
        let mut entry = Self::new(data_id, graph_id, command);
        entry.shard = shard_of(data_id, shards);
        entry
    }
}

/// Persists a task.
#[allow(async_fn_in_trait)]
pub trait TaskWrite<B> {
    /// Error returned when writing a task.
    type Err: std::error::Error;

    /// Writes this task to `backend`.
    async fn write(&self, backend: &mut B) -> Result<(), Self::Err>;
}

/// Reads a task by its identifier.
#[allow(async_fn_in_trait)]
pub trait TaskRead<I, B>: Send + Sized {
    /// Error returned when reading a task.
    type Err: std::error::Error;

    /// Reads the task identified by `task_id` from `backend`.
    async fn read(backend: &mut B, task_id: I) -> Result<Self, Self::Err>;
}

/// Polls tasks that are waiting to be processed.
#[allow(async_fn_in_trait)]
pub trait TaskPoll<B>: Send + Sized {
    /// Error returned when polling or acknowledging a task.
    type Err: std::error::Error;

    /// Returns up to `maxium` tasks that have not been acknowledged.
    ///
    /// When `shard` is `Some`, only tasks routed to that shard are returned; when
    /// `None`, tasks are returned across every shard.
    async fn poll(
        backend: &mut B,
        maxium: usize,
        shard: Option<u32>,
    ) -> Result<impl Iterator<Item = Self>, Self::Err>;

    /// Acknowledges this task, advancing the recorded offset of its shard to `self.task_offset`.
    async fn ack(&self, backend: &mut B) -> Result<(), Self::Err>;
}

/// A task waiting to be processed.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Task {
    /// Monotonic offset assigned when the task is written.
    pub task_offset: u64,
    /// Unique task identifier.
    pub id: Uuid,
    /// Data identifier used to route the task to a shard.
    pub data_id: Uuid,
    /// Shard this task is routed to.
    pub shard: u32,
    /// Graph identifier that owns the task.
    pub graph_id: Uuid,
    /// Node identifier within the graph.
    pub node_id: Uuid,
    /// Identifier of the node run this task drives.
    pub node_run_id: u64,
    /// Time at which the task was created.
    pub created_at: Timestamp,
}

/// A run's annotation configuration at a given revision.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RunConfig {
    /// Auto-increment primary key.
    pub id: u64,
    /// Unique identifier of the run this configuration belongs to.
    pub run_id: Uuid,
    /// Run revision this configuration applies to.
    pub revision: u64,
    /// Annotation key/value pairs for the run.
    pub annotation: HashMap<String, String>,
    /// Time at which the configuration was created.
    pub created_at: Timestamp,
    /// Time at which the configuration was last updated.
    pub updated_at: Timestamp,
    /// Who created the configuration.
    pub created_by: String,
    /// Who last updated the configuration.
    pub updated_by: String,
}

/// A node's configuration at a given version.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct NodeConfig {
    /// Auto-increment primary key.
    pub id: u64,
    /// Node this configuration applies to.
    pub node_id: Uuid,
    /// Configuration version.
    pub version: u64,
    /// Run annotation match expression that selects this configuration.
    pub annotation_match: String,
    /// Node configuration payload.
    pub config: serde_json::Value,
    /// Time at which the configuration was created.
    pub created_at: Timestamp,
    /// Time at which the configuration was last updated.
    pub updated_at: Timestamp,
    /// Who created the configuration.
    pub created_by: String,
    /// Who last updated the configuration.
    pub updated_by: String,
}

/// The event a task run reports, driving its node's status.
///
/// Each event maps to the [`Command`] that applies it, so recording an event and
/// replaying it are enough to drive a run forward.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
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
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Pend" => Ok(TaskEvent::Pend),
            "Succeed" => Ok(TaskEvent::Succeed),
            "Fail" => Ok(TaskEvent::Fail),
            "Terminate" => Ok(TaskEvent::Terminate),
            "Cancel" => Ok(TaskEvent::Cancel),
            _ => Err(()),
        }
    }
}

/// Appends a task event to a backend's event log (event sourcing).
#[allow(async_fn_in_trait)]
pub trait TaskEventWrite<B> {
    /// Error returned when writing a task event.
    type Err: std::error::Error;

    /// Appends this event to `backend`'s log.
    async fn write(&self, backend: &mut B) -> Result<(), Self::Err>;
}

/// Polls task events that are waiting to be consumed.
#[allow(async_fn_in_trait)]
pub trait TaskEventPoll<B>: Send + Sized {
    /// Error returned when polling or acknowledging a task event.
    type Err: std::error::Error;

    /// Returns up to `maxium` task events that have not been acknowledged.
    ///
    /// When `shard` is `Some`, only events routed to that shard are returned; when
    /// `None`, events are returned across every shard.
    async fn poll(
        backend: &mut B,
        maxium: usize,
        shard: Option<u32>,
    ) -> Result<impl Iterator<Item = Self>, Self::Err>;

    /// Acknowledges this event, advancing the recorded offset of its shard to
    /// `self.event_offset`.
    async fn ack(&self, backend: &mut B) -> Result<(), Self::Err>;
}

/// A task event recorded in the event log.
///
/// Events are stored append-only and consumed in offset order, so the outcome of
/// a run can be reconstructed from — and driven by — the events recorded for it.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TaskEventEntry {
    /// Monotonic offset assigned when the event is appended.
    pub event_offset: u64,
    /// Unique event identifier.
    pub id: Uuid,
    /// Identifier of the task whose run produced the event.
    pub task_id: Uuid,
    /// Data identifier used to route the event to a shard.
    pub data_id: Uuid,
    /// Graph identifier that owns the event.
    pub graph_id: Uuid,
    /// Node identifier within the graph.
    pub node_id: Uuid,
    /// Identifier of the node run the event drives.
    pub node_run_id: u64,
    /// Shard this event is routed to.
    pub shard: u32,
    /// Event the run reported.
    pub event: TaskEvent,
    /// Time at which the event was created.
    pub created_at: Timestamp,
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
            shard: task.shard,
            event,
            created_at: Timestamp::now(),
        }
    }
}

#[allow(async_fn_in_trait)]
pub trait GraphStore<I: Id> {
    type G: Graph<GraphId = I>;
    type Err: std::error::Error;

    async fn load(&self, graph_id: I, version: Option<u64>) -> Result<Self::G, Self::Err>;
}
