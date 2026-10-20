//! Scheduled tasks and their MySQL persistence contracts.

use event::task::Task;
use jiff::Timestamp;
use runtime::backend::Status;
use runtime::command::{NodeRun, Run};
use std::collections::HashMap;
use std::str::FromStr;
use toasty::{Executor, Json};
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Toasty(#[from] toasty::Error),
    #[error("scheduled task not found: {0}")]
    ScheduledTaskNotFound(Uuid),
    #[error("invalid status: {0}")]
    InvalidStatus(String),
}

/// Registers dispatch storage and its run, event, and configuration models.
pub fn models() -> toasty::ModelSet {
    toasty::models!(
        ScheduledTask,
        Run,
        NodeRun,
        NodeConfig,
        RunConfig,
        event::command::CommandEntry,
        event::task::Task,
        event::task_event::TaskEventEntry,
        event::poll::PollOffset,
        coordinator::mysql::LeaderElectionModel
    )
}

/// A run's annotation configuration at a given revision.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, toasty::Model)]
#[table = "run_config"]
#[unique(run_id, revision)]
pub struct RunConfig {
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
    #[serde(skip)]
    pub run: toasty::Deferred<Run>,
}

/// A node's configuration payload at a given version.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, toasty::Model)]
#[table = "node_config"]
#[unique(node_id, version)]
pub struct NodeConfig {
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

/// A dispatched task and its materialized execution references.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, toasty::Model)]
#[table = "scheduled_task"]
pub struct ScheduledTask {
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
    #[belongs_to(key = task_id, references = id)]
    #[serde(skip)]
    pub task: toasty::Deferred<Task>,
    #[belongs_to(key = run_id, references = uuid)]
    #[serde(skip)]
    pub run: toasty::Deferred<Run>,
    #[belongs_to(key = node_run_id, references = id)]
    #[serde(skip)]
    pub node_run: toasty::Deferred<NodeRun>,
    #[belongs_to(key = node_config_id, references = id)]
    #[serde(skip)]
    pub node_config: toasty::Deferred<NodeConfig>,
    #[belongs_to(key = run_config_id, references = id)]
    #[serde(skip)]
    pub run_config: toasty::Deferred<RunConfig>,
}

impl ScheduledTask {
    /// Creates a scheduled task dispatching `task` under `run_id`.
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
            status: Status::Running.to_string(),
            created_at: now,
            updated_at: now,
            task: toasty::Deferred::default(),
            run: toasty::Deferred::default(),
            node_run: toasty::Deferred::default(),
            node_config: toasty::Deferred::default(),
            run_config: toasty::Deferred::default(),
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

impl<B> ScheduledTaskWrite<B> for ScheduledTask
where
    B: Executor,
{
    type Err = Error;

    async fn write(&self, backend: &mut B) -> Result<(), Self::Err> {
        ScheduledTask::create()
            .uuid(self.uuid)
            .task_id(self.task_id)
            .run_id(self.run_id)
            .node_run_id(self.node_run_id)
            .node_config_id(self.node_config_id)
            .run_config_id(self.run_config_id)
            .status(self.status.clone())
            .created_at(self.created_at)
            .updated_at(self.updated_at)
            .exec(&mut *backend)
            .await?;
        Ok(())
    }
}

impl<B> ScheduledTaskRead<Uuid, B> for ScheduledTask
where
    B: Executor,
{
    type Err = Error;

    async fn read(backend: &mut B, task_id: Uuid) -> Result<Self, Self::Err> {
        let task = ScheduledTask::filter_by_task_id(task_id)
            .first()
            .exec(&mut *backend)
            .await?
            .ok_or(Error::ScheduledTaskNotFound(task_id))?;
        Status::from_str(&task.status).map_err(|_| Error::InvalidStatus(task.status.clone()))?;
        Ok(task)
    }
}
