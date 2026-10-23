//! Consumes runtime logs and materializes jobs from logical tasks.
use crate::Error;
use event::{
    Emit,
    command::{Command, CommandEntry},
    task::Task,
    task_event::TaskEventEntry,
};
use graph::{Graph, GraphStore};
use jiff::Timestamp;
use runtime::{
    backend::Backend,
    command::{NodeRun, Run},
};
use std::{collections::HashMap, time::Duration};
use toasty::{Executor, Json};
use uuid::Uuid;

/// Counts committed log consumption during one actuator pass.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Step {
    pub commands: usize,
    pub tasks: usize,
    pub events: usize,
}

/// Drives graph state and creates jobs from logical tasks. The caller serializes passes
/// through leader election. All effects and log offsets share one transaction.
pub struct Actuator<S> {
    backend: Backend<S>,
    batch: usize,
    poll_config: event::poll::Config,
}

impl<S> Actuator<S> {
    pub fn new(backend: Backend<S>, batch: usize) -> Self {
        assert!(batch > 0, "batch must be positive");
        Self {
            backend,
            batch,
            poll_config: event::poll::Config::default(),
        }
    }

    /// Sets how long missing log offsets must remain absent before being skipped.
    pub fn with_gap_timeout(mut self, gap_timeout: Duration) -> Self {
        assert!(!gap_timeout.is_zero(), "gap timeout must be positive");
        self.poll_config.gap_timeout = gap_timeout;
        self
    }

    /// Settles a job once its logical task has a terminal event.
    pub async fn settle<B: Executor>(
        backend: &mut B,
        event: &event::task_event::TaskEventEntry,
    ) -> Result<(), Error> {
        use event::task_event::TaskEvent;
        let status = match event.event {
            TaskEvent::Pend => return Ok(()),
            TaskEvent::Succeed => Status::Succeeded,
            TaskEvent::Fail => Status::Failed,
            TaskEvent::Terminate | TaskEvent::Cancel => Status::Terminated,
        };
        Job::filter_by_task_id(event.task_id)
            .filter(Job::fields().status().in_list(vec![
                Status::Scheduled,
                Status::Polled,
                Status::Pending,
            ]))
            .update()
            .status(status)
            .agent_id(None)
            .polled_at(None)
            .updated_at(Timestamp::now())
            .exec(backend)
            .await?;
        Ok(())
    }
}

impl<S> Actuator<S>
where
    S: GraphStore<Uuid>,
    S::G: Graph<NodeId = Uuid>,
    S::Err: Send + Sync + 'static,
{
    async fn consume_commands(&mut self, tx: &mut toasty::Transaction<'_>) -> Result<usize, Error> {
        let entries: Vec<_> =
            CommandEntry::poll_with_config(&mut *tx, self.batch, self.poll_config)
                .await?
                .collect();
        let count = entries.len();
        for entry in entries {
            self.backend.apply_in(tx, &entry).await?;
            entry.ack_with_config(&mut *tx, self.poll_config).await?;
        }
        Ok(count)
    }

    /// Drives logs using a caller-owned, leader-protected transaction.
    pub async fn tick_in(&mut self, tx: &mut toasty::Transaction<'_>) -> Result<Step, Error> {
        let mut commands = self.consume_commands(tx).await?;
        let tasks: Vec<Task> = Task::poll_with_config(&mut *tx, self.batch, self.poll_config)
            .await?
            .collect();
        for task in &tasks {
            CommandEntry::new(
                task.data_id,
                task.graph_id,
                Command::Run { node: task.node_id },
            )
            .emit(&mut *tx)
            .await?;
            Job::create_for_task(task, &mut *tx).await?;
            task.ack_with_config(&mut *tx, self.poll_config).await?;
        }
        let events: Vec<_> =
            TaskEventEntry::poll_with_config(&mut *tx, self.batch, self.poll_config)
                .await?
                .collect();
        for event in &events {
            CommandEntry::new(
                event.data_id,
                event.graph_id,
                event.event.command(event.node_id),
            )
            .emit(&mut *tx)
            .await?;
            Self::settle(&mut *tx, event).await?;
            event.ack_with_config(&mut *tx, self.poll_config).await?;
        }
        commands += self.consume_commands(tx).await?;
        Ok(Step {
            commands,
            tasks: tasks.len(),
            events: events.len(),
        })
    }

    /// Runs one atomic pass. The caller must hold exclusive consumer ownership.
    pub async fn tick(&mut self) -> Result<Step, Error> {
        let mut db = self.backend.db().clone();
        let mut tx = db.transaction().await?;
        let step = self.tick_in(&mut tx).await?;
        tx.commit().await?;
        Ok(step)
    }
}

/// Lifecycle of a job, independent of its node's runtime status.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, toasty::Embed,
)]
#[column(rename_all = "PascalCase", type = varchar(16))]
pub enum Status {
    Scheduled,
    Polled,
    /// The agent accepted the claim and is executing it.
    Pending,
    Succeeded,
    Failed,
    Terminated,
}

impl Status {
    /// Whether this job has reached a final outcome.
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Terminated)
    }
}

/// Registers jobs and their run, event, and configuration models.
pub fn models() -> toasty::ModelSet {
    toasty::models!(
        Job,
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
    /// Minimum available CPU cores required by jobs created from this config.
    pub required_cpu_cores: u32,
    /// Minimum available memory required by jobs created from this config.
    pub required_memory_bytes: u64,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    #[column(type = varchar(64))]
    pub created_by: String,
    #[column(type = varchar(64))]
    pub updated_by: String,
}

/// An execution instance created from a logical task and its effective configuration.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, toasty::Model)]
#[table = "job"]
pub struct Job {
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
    #[index]
    pub status: Status,
    /// Minimum CPU capacity required to run this job.
    pub required_cpu_cores: u32,
    /// Minimum memory capacity required to run this job.
    pub required_memory_bytes: u64,
    /// Agent currently holding the polling lease, if any.
    #[index]
    #[column(type = "CHAR(36)")]
    pub agent_id: Option<Uuid>,
    /// Last poll that renewed the agent's lease.
    #[index]
    pub polled_at: Option<Timestamp>,
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

impl Job {
    /// Creates a job for a logical task under `run_id`.
    pub fn new(task: &Task, run_id: Uuid, node_config_id: u64, run_config_id: u64) -> Self {
        let now = Timestamp::now();
        Self {
            id: 0,
            uuid: Uuid::new_v4(),
            task_id: task.id,
            run_id,
            node_run_id: task.node_run_id,
            node_config_id,
            run_config_id,
            status: Status::Scheduled,
            required_cpu_cores: 0,
            required_memory_bytes: 0,
            agent_id: None,
            polled_at: None,
            created_at: now,
            updated_at: now,
            task: toasty::Deferred::default(),
            run: toasty::Deferred::default(),
            node_run: toasty::Deferred::default(),
            node_config: toasty::Deferred::default(),
            run_config: toasty::Deferred::default(),
        }
    }

    /// Persists a job with the node and run configurations in effect.
    pub async fn create_for_task<B: Executor>(task: &Task, backend: &mut B) -> Result<Job, Error> {
        let node_run = NodeRun::filter_by_id(task.node_run_id)
            .get(&mut *backend)
            .await?;
        let run = Run::filter_by_uuid(node_run.run_id)
            .get(&mut *backend)
            .await?;
        let node_config = NodeConfig::filter_by_node_id(task.node_id)
            .order_by(NodeConfig::fields().version().desc())
            .first()
            .exec(&mut *backend)
            .await?;
        let node_config_id = node_config.as_ref().map(|row| row.id).unwrap_or(0);
        let run_config_id = RunConfig::filter_by_run_id(run.uuid)
            .filter(RunConfig::fields().revision().le(run.revision))
            .order_by(RunConfig::fields().revision().desc())
            .first()
            .exec(&mut *backend)
            .await?
            .map(|row| row.id)
            .unwrap_or(0);
        let mut entry = Self::new(task, run.uuid, node_config_id, run_config_id);
        if let Some(node_config) = node_config {
            entry.required_cpu_cores = node_config.required_cpu_cores;
            entry.required_memory_bytes = node_config.required_memory_bytes;
        }
        entry.write(backend).await?;
        Ok(Self::filter_by_uuid(entry.uuid).get(backend).await?)
    }
}

/// Persists a job.
#[allow(async_fn_in_trait)]
pub trait JobWrite<B> {
    /// Error returned when writing a job.
    type Err: std::error::Error;

    /// Writes this job to `backend`.
    async fn write(&self, backend: &mut B) -> Result<(), Self::Err>;
}

/// Reads the job associated with a logical task.
#[allow(async_fn_in_trait)]
pub trait JobRead<I, B>: Send + Sized {
    /// Error returned when reading a job.
    type Err: std::error::Error;

    /// Reads the job for the task identified by `task_id`.
    async fn read(backend: &mut B, task_id: I) -> Result<Self, Self::Err>;
}

impl<B> JobWrite<B> for Job
where
    B: Executor,
{
    type Err = Error;

    async fn write(&self, backend: &mut B) -> Result<(), Self::Err> {
        Job::create()
            .uuid(self.uuid)
            .task_id(self.task_id)
            .run_id(self.run_id)
            .node_run_id(self.node_run_id)
            .node_config_id(self.node_config_id)
            .run_config_id(self.run_config_id)
            .status(self.status)
            .required_cpu_cores(self.required_cpu_cores)
            .required_memory_bytes(self.required_memory_bytes)
            .agent_id(self.agent_id)
            .polled_at(self.polled_at)
            .created_at(self.created_at)
            .updated_at(self.updated_at)
            .exec(&mut *backend)
            .await?;
        Ok(())
    }
}

impl<B> JobRead<Uuid, B> for Job
where
    B: Executor,
{
    type Err = Error;

    async fn read(backend: &mut B, task_id: Uuid) -> Result<Self, Self::Err> {
        let job = Job::filter_by_task_id(task_id)
            .first()
            .exec(&mut *backend)
            .await?
            .ok_or(Error::JobNotFound(task_id))?;
        Ok(job)
    }
}
