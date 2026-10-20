#[cfg(feature = "mysql")]
pub mod mysql;

use std::collections::HashMap;

use dag::{Graph, Id};
use jiff::Timestamp;
use uuid::Uuid;

/// The node status.
/// 1. Ready -> Running -> Succeeded/Failed/Terminated
/// 2. Ready -> Running -> Pending -> Succeeded/Failed/Terminated
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
    async fn emit(&self, backend: &mut B) -> Result<u64, Self::Err>;
}

/// Polls command entries that are waiting to be applied.
#[allow(async_fn_in_trait)]
pub trait CommandPoll<B>: Send + Sized {
    /// Error returned when polling or acknowledging a command entry.
    type Err: std::error::Error;

    /// Returns up to `maximum` command entries from the first unacknowledged
    /// contiguous offset.
    async fn poll(backend: &mut B, maximum: usize)
    -> Result<impl Iterator<Item = Self>, Self::Err>;

    /// Acknowledges this entry, advancing the recorded command offset.
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
    /// Data identifier the command applies to.
    pub data_id: Uuid,
    /// Graph identifier that owns the entry.
    pub graph_id: Uuid,
    /// Command carried by this entry.
    pub command: Command,
    /// Time at which the entry was created.
    pub created_at: Timestamp,
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
            command,
            created_at: Timestamp::now(),
        }
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

    /// Returns up to `maximum` tasks from the first unacknowledged contiguous
    /// offset.
    async fn poll(backend: &mut B, maximum: usize)
    -> Result<impl Iterator<Item = Self>, Self::Err>;

    /// Acknowledges this task, advancing the recorded task offset.
    async fn ack(&self, backend: &mut B) -> Result<(), Self::Err>;
}

/// A task waiting to be processed.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Task {
    /// Monotonic offset assigned when the task is written.
    pub task_offset: u64,
    /// Unique task identifier.
    pub id: Uuid,
    /// Data identifier of the task.
    pub data_id: Uuid,
    /// Graph identifier that owns the task.
    pub graph_id: Uuid,
    /// Node identifier within the graph.
    pub node_id: Uuid,
    /// Identifier of the node run this task drives.
    pub node_run_id: u64,
    /// Time at which the task was created.
    pub created_at: Timestamp,
}

/// A task that has been dispatched to a runner.
///
/// A scheduled task materializes the references a runner needs — the task, its
/// run, node run, and the node and run configurations in effect — together with
/// the status the dispatch reached. It is written when a task is dispatched and
/// is what a runner executes from.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ScheduledTask {
    /// Auto-increment primary key.
    pub id: u64,
    /// Unique scheduled task identifier.
    pub uuid: Uuid,
    /// Identifier of the task being dispatched.
    pub task_id: Uuid,
    /// Identifier of the run the task belongs to.
    pub run_id: Uuid,
    /// Identifier of the node run the task drives.
    pub node_run_id: u64,
    /// Identifier of the node configuration in effect.
    pub node_config_id: u64,
    /// Identifier of the run configuration in effect.
    pub run_config_id: u64,
    /// Status the dispatch reached.
    pub status: Status,
    /// Time at which the scheduled task was created.
    pub created_at: Timestamp,
    /// Time at which the scheduled task was last updated.
    pub updated_at: Timestamp,
}

impl ScheduledTask {
    /// Creates a scheduled task dispatching `task` under `run_id`.
    ///
    /// The task is marked [`Running`](Status::Running) and references the node
    /// and run configurations in effect (`0` when there is none).
    pub fn dispatch(task: &Task, run_id: Uuid, node_config_id: u64, run_config_id: u64) -> Self {
        let now = Timestamp::now();
        Self {
            id: 0,
            uuid: Uuid::new_v4(),
            task_id: task.id,
            run_id,
            node_run_id: task.node_run_id,
            node_config_id,
            run_config_id,
            status: Status::Running,
            created_at: now,
            updated_at: now,
        }
    }
}

/// Persists a scheduled task.
#[allow(async_fn_in_trait)]
pub trait ScheduledTaskWrite<B> {
    /// Error returned when writing a scheduled task.
    type Err: std::error::Error;

    /// Writes this scheduled task to `backend`.
    async fn write(&self, backend: &mut B) -> Result<(), Self::Err>;
}

/// Reads a scheduled task by the task it dispatches.
#[allow(async_fn_in_trait)]
pub trait ScheduledTaskRead<I, B>: Send + Sized {
    /// Error returned when reading a scheduled task.
    type Err: std::error::Error;

    /// Reads the scheduled task dispatching the task identified by `task_id`.
    async fn read(backend: &mut B, task_id: I) -> Result<Self, Self::Err>;
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

    /// The node status this event drives a node to.
    pub fn status(self) -> Status {
        match self {
            TaskEvent::Pend => Status::Pending,
            TaskEvent::Succeed => Status::Succeeded,
            TaskEvent::Fail => Status::Failed,
            TaskEvent::Terminate | TaskEvent::Cancel => Status::Terminated,
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

/// Polls and acknowledges the task events produced by runs.
#[allow(async_fn_in_trait)]
pub trait TaskEventPoll: Send + Sync {
    /// Error returned when polling or acknowledging a task event.
    type Err: std::error::Error + Send + Sync + 'static;

    /// Returns up to `max` task events from the first unacknowledged contiguous
    /// offset.
    async fn poll(&self, max: usize) -> Result<Vec<TaskEventEntry>, Self::Err>;

    /// Acknowledges `event`, so it is not polled again.
    async fn ack(&self, event: &TaskEventEntry) -> Result<(), Self::Err>;
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

/// Loads graphs by id.
///
/// `load` returns a `Send` future so a graph store can be awaited from an async
/// context that must itself be `Send` (a request handler, say).
pub trait GraphStore<I: Id> {
    /// The graph type this store loads.
    type G: Graph<GraphId = I>;
    /// Error returned when loading a graph.
    type Err: std::error::Error;

    /// Loads the graph with the given id, at `version` when one is given.
    fn load(
        &self,
        graph_id: I,
        version: Option<u64>,
    ) -> impl std::future::Future<Output = Result<Self::G, Self::Err>> + Send;
}
