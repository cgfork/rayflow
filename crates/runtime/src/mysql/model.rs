//! Toasty models mirroring the schema in `runtime.sql`.
//!
//! Each struct maps to one table and drives the generated query builders
//! (`create`, `filter_by_*`, `get_by_*`, `update`, …) used by the persistence
//! layer. The models are registered on the [`Db`](toasty::Db) through
//! [`models`] before it is used by a [`Backend`](super::Backend).
//!
//! Two mappings are worth calling out:
//!
//! * `DATETIME(6)` columns are typed as [`jiff::Timestamp`], an instant.
//!   Toasty's `jiff` integration maps it to MySQL's `DATETIME(6)` storage, so
//!   the column stays a real temporal type.
//! * `JSON` columns use [`toasty::Json`] (or [`serde_json::Value`]), which the
//!   MySQL driver round-trips through the column's native JSON type.

use std::collections::HashMap;
use std::str::FromStr;

use jiff::Timestamp;
use toasty::Json;
use uuid::Uuid;

use coordinator::LeaderElectionModel;

use crate::{Command, CommandEntry, ScheduledTask, Status, TaskEvent, TaskEventEntry};

/// The `task` table: a task waiting to be processed.
#[derive(Debug, Clone, toasty::Model)]
#[table = "task"]
pub struct TaskModel {
    /// Auto-increment primary key, doubling as the task offset.
    #[key]
    #[auto]
    pub id: u64,
    /// Unique task identifier.
    #[unique]
    pub uuid: Uuid,
    pub data_id: Uuid,
    pub graph_id: Uuid,
    pub node_id: Uuid,
    #[index]
    pub node_run_id: u64,
    pub created_at: Timestamp,
    #[belongs_to(key = node_run_id, references = id)]
    pub node_run: toasty::Deferred<NodeRunModel>,
}

impl TaskModel {
    /// Converts this model into the domain [`Task`](crate::Task).
    pub fn into_task(self) -> Result<crate::Task, super::Error> {
        Ok(crate::Task {
            task_offset: self.id,
            id: self.uuid,
            data_id: self.data_id,
            graph_id: self.graph_id,
            node_id: self.node_id,
            node_run_id: self.node_run_id,
            created_at: self.created_at,
        })
    }
}

/// The `run` table: one execution of a graph version for a `(data_id, graph_id)`.
#[derive(Debug, Clone, toasty::Model)]
#[table = "run"]
#[unique(data_id, graph_id, version)]
pub struct RunModel {
    #[key]
    #[auto]
    pub id: u64,
    #[unique]
    pub uuid: Uuid,
    pub data_id: Uuid,
    pub graph_id: Uuid,
    pub version: u64,
    pub revision: u64,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

/// The `run_config` table: a run's annotation map at a given revision.
#[derive(Debug, Clone, toasty::Model)]
#[table = "run_config"]
#[unique(run_id, revision)]
pub struct RunConfigModel {
    #[key]
    #[auto]
    pub id: u64,
    #[index]
    pub run_id: Uuid,
    pub revision: u64,
    #[column(type = json)]
    pub annotation: Json<HashMap<String, String>>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    #[column(type = varchar(64))]
    pub created_by: String,
    #[column(type = varchar(64))]
    pub updated_by: String,
    #[belongs_to(key = run_id, references = uuid)]
    pub run: toasty::Deferred<RunModel>,
}

/// The `node_config` table: a node's configuration payload at a given version.
#[derive(Debug, Clone, toasty::Model)]
#[table = "node_config"]
#[unique(node_id, version)]
pub struct NodeConfigModel {
    #[key]
    #[auto]
    pub id: u64,
    #[index]
    pub node_id: Uuid,
    pub version: u64,
    #[column(type = varchar(255))]
    pub annotation_match: String,
    #[column(type = json)]
    pub config: serde_json::Value,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    #[column(type = varchar(64))]
    pub created_by: String,
    #[column(type = varchar(64))]
    pub updated_by: String,
}

/// The `node_run` table: a node's status within a run, one row per revision.
#[derive(Debug, Clone, toasty::Model)]
#[table = "node_run"]
#[unique(run_id, node_id, revision)]
#[index(data_id, graph_id)]
pub struct NodeRunModel {
    #[key]
    #[auto]
    pub id: u64,
    /// Unique identifier of the run this node run belongs to.
    pub run_id: Uuid,
    pub data_id: Uuid,
    pub graph_id: Uuid,
    pub node_id: Uuid,
    pub revision: u64,
    #[column(type = varchar(16))]
    pub status: String,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    #[belongs_to(key = run_id, references = uuid)]
    pub run: toasty::Deferred<RunModel>,
}

/// The `transition_lock` table: transaction-scoped locks serializing command
/// application for a run.
#[derive(Debug, Clone, toasty::Model)]
#[table = "transition_lock"]
#[unique(data_id, graph_id)]
pub struct TransitionLockModel {
    #[key]
    #[auto]
    pub id: u64,
    pub data_id: Uuid,
    pub graph_id: Uuid,
    pub updated_at: Timestamp,
}

/// The `scheduled_task` table: a task materialized with the run, node run, and
/// configurations needed to execute it.
#[derive(Debug, Clone, toasty::Model)]
#[table = "scheduled_task"]
pub struct ScheduledTaskModel {
    #[key]
    #[auto]
    pub id: u64,
    #[unique]
    pub uuid: Uuid,
    #[index]
    pub task_id: Uuid,
    #[index]
    pub run_id: Uuid,
    #[index]
    pub node_run_id: u64,
    #[index]
    pub node_config_id: u64,
    #[index]
    pub run_config_id: u64,
    #[column(type = varchar(16))]
    pub status: String,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    #[belongs_to(key = task_id, references = uuid)]
    pub task: toasty::Deferred<TaskModel>,
    #[belongs_to(key = run_id, references = uuid)]
    pub run: toasty::Deferred<RunModel>,
    #[belongs_to(key = node_run_id, references = id)]
    pub node_run: toasty::Deferred<NodeRunModel>,
    #[belongs_to(key = node_config_id, references = id)]
    pub node_config: toasty::Deferred<NodeConfigModel>,
    #[belongs_to(key = run_config_id, references = id)]
    pub run_config: toasty::Deferred<RunConfigModel>,
}

impl ScheduledTaskModel {
    /// Converts this model into the domain [`ScheduledTask`](crate::ScheduledTask).
    pub fn into_scheduled_task(self) -> Result<ScheduledTask, super::Error> {
        let status = Status::from_str(&self.status)
            .map_err(|_| super::Error::InvalidStatus(self.status.clone()))?;
        Ok(ScheduledTask {
            id: self.id,
            uuid: self.uuid,
            task_id: self.task_id,
            run_id: self.run_id,
            node_run_id: self.node_run_id,
            node_config_id: self.node_config_id,
            run_config_id: self.run_config_id,
            status,
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
    }
}

/// The `command_entry` table: the append-only command event log.
#[derive(Debug, Clone, toasty::Model)]
#[table = "command_entry"]
pub struct CommandEntryModel {
    /// Auto-increment primary key, doubling as the command offset.
    #[key]
    #[auto]
    pub id: u64,
    #[unique]
    pub uuid: Uuid,
    pub data_id: Uuid,
    pub graph_id: Uuid,
    #[column(type = json)]
    pub command: Json<Command>,
    pub created_at: Timestamp,
}

impl CommandEntryModel {
    /// Converts this model into the domain [`CommandEntry`](crate::CommandEntry).
    pub fn into_command_entry(self) -> Result<CommandEntry, super::Error> {
        Ok(CommandEntry {
            command_offset: self.id,
            id: self.uuid,
            data_id: self.data_id,
            graph_id: self.graph_id,
            command: self.command.0,
            created_at: self.created_at,
        })
    }
}

/// The `task_event` table: the append-only task event log.
#[derive(Debug, Clone, toasty::Model)]
#[table = "task_event"]
pub struct TaskEventModel {
    /// Auto-increment primary key, doubling as the event offset.
    #[key]
    #[auto]
    pub id: u64,
    #[unique]
    pub uuid: Uuid,
    #[index]
    pub task_id: Uuid,
    pub data_id: Uuid,
    pub graph_id: Uuid,
    pub node_id: Uuid,
    pub node_run_id: u64,
    #[column(type = varchar(16))]
    pub event: String,
    pub created_at: Timestamp,
    #[belongs_to(key = task_id, references = uuid)]
    pub task: toasty::Deferred<TaskModel>,
    #[belongs_to(key = node_run_id, references = id)]
    pub node_run: toasty::Deferred<NodeRunModel>,
}

impl TaskEventModel {
    /// Converts this model into the domain [`TaskEventEntry`](crate::TaskEventEntry).
    pub fn into_task_event_entry(self) -> Result<TaskEventEntry, super::Error> {
        let event = TaskEvent::from_str(&self.event)
            .map_err(|_| super::Error::InvalidEvent(self.event.clone()))?;
        Ok(TaskEventEntry {
            event_offset: self.id,
            id: self.uuid,
            task_id: self.task_id,
            data_id: self.data_id,
            graph_id: self.graph_id,
            node_id: self.node_id,
            node_run_id: self.node_run_id,
            event,
            created_at: self.created_at,
        })
    }
}

/// The `poll_offset` table: the global acknowledged offset of one append-only
/// log and the time at which its current gap was first observed.
#[derive(Debug, Clone, toasty::Model)]
#[table = "poll_offset"]
pub struct PollOffsetModel {
    /// Log name: `command_entry`, `task`, or `task_event`.
    #[key]
    #[column(type = varchar(64))]
    pub name: String,
    /// Last offset whose row has been acknowledged.
    pub offset: u64,
    /// When the first unacknowledged gap was observed, if one is pending.
    pub gap_since: Option<Timestamp>,
    /// Last time the offset or gap state changed.
    pub updated_at: Timestamp,
}

/// Returns the [`ModelSet`](toasty::ModelSet) describing the MySQL schema.
///
/// Register it on the [`Db`](toasty::Db) before handing it to a
/// [`Backend`](super::Backend):
///
/// ```ignore
/// let db = toasty::Db::builder()
///     .models(runtime::mysql::models())
///     .connect("mysql://root@localhost/rayflow")
///     .await?;
/// ```
pub fn models() -> toasty::ModelSet {
    toasty::models!(
        TaskModel,
        RunModel,
        RunConfigModel,
        NodeConfigModel,
        NodeRunModel,
        TransitionLockModel,
        ScheduledTaskModel,
        CommandEntryModel,
        TaskEventModel,
        PollOffsetModel,
        LeaderElectionModel,
    )
}
