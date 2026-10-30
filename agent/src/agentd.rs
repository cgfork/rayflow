//! Durable local job daemon.
//!
//! `Agentd` owns the boundary between remote job definitions, the local job
//! filesystem, and the container provider. It never reports job state over
//! HTTP. `Agentic` or another consumer reads [`AgentResult`] values and owns
//! remote reporting.

use std::{
    collections::HashMap,
    io,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use futures_util::StreamExt;
use graph::{GraphStore, types::DAG};
use jiff::Timestamp;
use scheduler::Job;
use tokio::{
    fs,
    sync::{Mutex, Notify, watch},
    time::sleep,
};
use uuid::Uuid;

use crate::{
    containerd::{Container, ContainerState, Containerd, ExitStatus},
    errors::Error,
    jobfs::{Container as JobContainer, JobFs, JobResult},
};

const WATCH_RETRY_DELAY: Duration = Duration::from_millis(250);
const RECOVERY_INTERVAL: Duration = Duration::from_secs(1);

/// The terminal outcome persisted for a local job.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JobOutcome {
    /// The container exited successfully.
    Succeeded,
    /// The container exited with a failure code or the agent could not run it.
    Failed,
    /// The job was deliberately yielded for later scheduling.
    Yielded,
    /// The job was deliberately terminated.
    Terminated,
}

impl JobOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeed",
            Self::Failed => "fail",
            Self::Yielded => "yield",
            Self::Terminated => "terminate",
        }
    }

    fn parse(value: &str) -> Result<Self, Error> {
        match value {
            "succeed" | "succeeded" => Ok(Self::Succeeded),
            "fail" | "failed" => Ok(Self::Failed),
            "yield" | "yielded" => Ok(Self::Yielded),
            "terminate" | "terminated" => Ok(Self::Terminated),
            outcome => Err(Error::Agentd(format!("unknown job outcome: {outcome}"))),
        }
    }
}

/// A durable result ready for the remote reporting loop.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentResult {
    /// The remote job identifier.
    pub job_id: Uuid,
    /// The terminal outcome selected by the daemon.
    pub outcome: JobOutcome,
    /// The process exit code persisted by the daemon.
    pub exit_code: i32,
    /// The provider-reported or agent-observed start time.
    pub started_at: Option<Timestamp>,
    /// The provider-reported or agent-observed finish time.
    pub finished_at: Option<Timestamp>,
    /// The elapsed runtime, when a start time was available.
    pub duration: Option<Duration>,
}

/// A stop request made by the local orchestration layer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StopReason {
    /// Stop the container and make the result report as yielded.
    Yielded,
    /// Kill the container and make the result report as terminated.
    Terminated,
    /// Kill the container and make the result report as failed.
    Failed,
}

struct Inner<C, R> {
    containerd: C,
    graph_store: R,
    workdir: PathBuf,
    containers: Mutex<HashMap<String, Uuid>>,
    results: Notify,
    shutdown: watch::Sender<bool>,
    watching: AtomicBool,
}

/// The local job daemon.
///
/// The daemon is cloneable so its watcher, polling loop, and request handlers
/// can share one state owner. All mutations of `JobFs` are performed by this
/// type; consumers only read results and acknowledge them after the remote
/// service accepts the corresponding report.
#[derive(Clone)]
pub struct Agentd<C, R> {
    inner: Arc<Inner<C, R>>,
}

impl<C, R> Agentd<C, R>
where
    C: Containerd,
    R: GraphStore<Uuid, G = DAG>,
{
    /// Opens the workdir, discovers persisted jobs, and reconciles containers
    /// that already exited before the current process started.
    pub async fn open(
        containerd: C,
        graph_store: R,
        workdir: impl Into<PathBuf>,
    ) -> Result<Self, Error> {
        let workdir = workdir.into();
        fs::create_dir_all(&workdir).await?;
        let (shutdown, _) = watch::channel(false);
        let agentd = Self {
            inner: Arc::new(Inner {
                containerd,
                graph_store,
                workdir,
                containers: Mutex::new(HashMap::new()),
                results: Notify::new(),
                shutdown,
                watching: AtomicBool::new(false),
            }),
        };
        agentd.reconcile_all().await?;
        Ok(agentd)
    }

    /// Persists a claimed job, loads its pinned graph version, and prepares
    /// the provider resources needed by the job.
    pub async fn prepare(&self, job: Job) -> Result<(), Error> {
        let job_fs = self.job_fs(job.uuid);
        job_fs.lock().await?;
        let result = self.prepare_locked(&job_fs, job).await;
        let unlock = job_fs.unlock().await;
        result.and(unlock)
    }

    async fn prepare_locked(&self, job_fs: &JobFs, job: Job) -> Result<(), Error> {
        if let Ok(existing) = job_fs.job().await {
            if let Ok(container) = job_fs.container().await {
                if container.r#type != self.inner.containerd.name() {
                    return Err(Error::Provider(format!(
                        "container provider mismatch: expected {}, found {}",
                        self.inner.containerd.name(),
                        container.r#type
                    )));
                }
                match self
                    .inner
                    .containerd
                    .container(&container.id)
                    .inspect()
                    .await
                {
                    Ok(state) if state.is_running() => {
                        return Err(Error::JobRunning(existing.uuid));
                    }
                    Ok(_) => {}
                    Err(error) if is_not_found(&error) => {}
                    Err(error) => return Err(error),
                }
            }
            if job_fs.job_result().await.is_ok() {
                return Err(Error::Agentd(format!(
                    "job result is waiting for acknowledgement: {}",
                    existing.uuid
                )));
            }
        }

        if job.run.is_unloaded() {
            return Err(Error::Agentd(format!(
                "job run relation is not loaded: {}",
                job.uuid
            )));
        }
        if job.node_config.is_unloaded() || job.node_config.get().is_none() {
            return Err(Error::MissingConfig);
        }
        if job.node_run.is_unloaded() {
            return Err(Error::Agentd(format!(
                "job node run relation is not loaded: {}",
                job.uuid
            )));
        }

        job_fs.write_job(job.clone()).await?;

        let run = job.run.get();
        let graph = self
            .inner
            .graph_store
            .load(run.graph_id, Some(run.version))
            .await
            .map_err(|error| Error::Agentd(format!("load graph: {error}")))?;
        let node_id = job.node_run.get().node_id;
        let node = graph
            .nodes
            .iter()
            .find(|node| node.id == node_id)
            .ok_or_else(|| Error::Agentd(format!("node is missing from graph: {node_id}")))?;
        job_fs.write_node(node).await?;

        let spec = self.inner.containerd.spec(&job)?;
        self.inner.containerd.prepare(spec).await
    }

    /// Creates or reconnects to a container, persists its identity, and starts
    /// it without blocking the caller on process completion.
    pub async fn start(&self, job_id: Uuid) -> Result<(), Error> {
        let job_fs = self.job_fs(job_id);
        job_fs.lock().await?;
        let result = self.start_locked(&job_fs).await;
        let unlock = job_fs.unlock().await;
        result.and(unlock)
    }

    async fn start_locked(&self, job_fs: &JobFs) -> Result<(), Error> {
        let job = job_fs.job().await?;
        if job_fs.job_result().await.is_ok() {
            return Ok(());
        }
        let expired = job
            .expired_at
            .is_some_and(|expired_at| expired_at <= Timestamp::now());

        let container = match job_fs.container().await {
            Ok(metadata) => {
                if metadata.r#type != self.inner.containerd.name() {
                    return Err(Error::Provider(format!(
                        "container provider mismatch: expected {}, found {}",
                        self.inner.containerd.name(),
                        metadata.r#type
                    )));
                }
                let persisted_id = metadata.id;
                let container = self.inner.containerd.container(&persisted_id);
                self.register_container(persisted_id.clone(), job.uuid)
                    .await;
                match container.inspect().await {
                    Ok(_) => container,
                    Err(error) if is_not_found(&error) => {
                        self.inner.containers.lock().await.remove(&persisted_id);
                        self.create_container(job_fs, &job).await?
                    }
                    Err(error) => return Err(error),
                }
            }
            Err(error) if is_not_found(&error) => {
                if job.node_config.is_unloaded() || job.node_config.get().is_none() {
                    return Err(Error::MissingConfig);
                }
                self.create_container(job_fs, &job).await?
            }
            Err(error) => return Err(error),
        };

        match container.inspect().await? {
            ContainerState::Running { pid, started_at } => {
                job_fs.write_runtime(pid, started_at).await?;
                if expired {
                    container.kill().await?;
                    self.finish_locked(
                        job_fs,
                        &job,
                        ExitStatus {
                            exit_code: 124,
                            finished_at: Some(Timestamp::now()),
                        },
                        Some(JobOutcome::Failed),
                    )
                    .await
                } else {
                    Ok(())
                }
            }
            ContainerState::Exited {
                exit_code,
                finished_at,
            } => {
                self.finish_locked(
                    job_fs,
                    &job,
                    ExitStatus {
                        exit_code,
                        finished_at,
                    },
                    None,
                )
                .await
            }
            ContainerState::Created => {
                if expired {
                    container.kill().await?;
                    return self
                        .finish_locked(
                            job_fs,
                            &job,
                            ExitStatus {
                                exit_code: 124,
                                finished_at: Some(Timestamp::now()),
                            },
                            Some(JobOutcome::Failed),
                        )
                        .await;
                }
                container.start().await?;
                match container.inspect().await? {
                    ContainerState::Running { pid, started_at } => {
                        job_fs.write_runtime(pid, started_at).await?;
                        if job
                            .expired_at
                            .is_some_and(|expired_at| expired_at <= Timestamp::now())
                        {
                            container.kill().await?;
                            self.finish_locked(
                                job_fs,
                                &job,
                                ExitStatus {
                                    exit_code: 124,
                                    finished_at: Some(Timestamp::now()),
                                },
                                Some(JobOutcome::Failed),
                            )
                            .await
                        } else {
                            Ok(())
                        }
                    }
                    ContainerState::Exited {
                        exit_code,
                        finished_at,
                    } => {
                        self.finish_locked(
                            job_fs,
                            &job,
                            ExitStatus {
                                exit_code,
                                finished_at,
                            },
                            None,
                        )
                        .await
                    }
                    ContainerState::Created => Ok(()),
                }
            }
        }
    }

    async fn create_container(&self, job_fs: &JobFs, job: &Job) -> Result<C::Container, Error> {
        let spec = self.inner.containerd.spec(job)?;
        let container = self.inner.containerd.create(spec).await?;
        job_fs
            .write_container(JobContainer {
                r#type: self.inner.containerd.name().to_owned(),
                id: container.id().to_owned(),
            })
            .await?;
        self.register_container(container.id().to_owned(), job.uuid)
            .await;
        Ok(container)
    }

    /// Runs the global provider event watcher and periodic recovery scan.
    ///
    /// Provider event streams are reconnectable hints. A stream error or EOF
    /// causes a delayed reconnect; periodic scans remain the crash-recovery
    /// mechanism when no event was delivered.
    pub async fn watch(&self) -> Result<(), Error> {
        if self
            .inner
            .watching
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(Error::AlreadyRunning);
        }
        let _guard = WatchGuard(&self.inner.watching);
        let mut shutdown = self.inner.shutdown.subscribe();

        loop {
            let events = self.inner.containerd.events();
            tokio::pin!(events);
            let mut recovery = tokio::time::interval(RECOVERY_INTERVAL);
            recovery.tick().await;
            loop {
                tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            return Ok(());
                        }
                    }
                    event = events.next() => match event {
                        Some(Ok(event)) => {
                            if let Some(job_id) = self.job_for_container(&event.container_id).await {
                                self.reconcile_job(job_id).await?;
                            }
                        }
                        Some(Err(_)) | None => break,
                    },
                    _ = recovery.tick() => {
                        self.reconcile_all().await?;
                    }
                }
            }

            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                _ = sleep(WATCH_RETRY_DELAY) => {}
            }
        }
    }

    /// Requests the watcher to stop after its current reconciliation step.
    pub fn shutdown(&self) {
        let _ = self.inner.shutdown.send(true);
    }

    /// Returns persisted jobs without terminal results for crash recovery.
    pub async fn jobs(&self) -> Result<Vec<Job>, Error> {
        let mut jobs = Vec::new();
        for job_id in JobFs::job_ids(&self.inner.workdir).await? {
            let job_fs = self.job_fs(job_id);
            job_fs.lock().await?;
            let read = async {
                if job_fs.job_result().await.is_ok() {
                    return Ok(None);
                }
                match job_fs.job().await {
                    Ok(job) => Ok(Some(job)),
                    Err(error) if is_not_found(&error) => Ok(None),
                    Err(error) => Err(error),
                }
            }
            .await;
            let unlock = job_fs.unlock().await;
            if let Some(job) = read.and_then(|job| unlock.map(|_| job))? {
                jobs.push(job);
            }
        }
        Ok(jobs)
    }

    /// Reads one durable result, if present, for a job that failed to start.
    pub async fn result(&self, job_id: Uuid) -> Result<Option<AgentResult>, Error> {
        let job_fs = self.job_fs(job_id);
        job_fs.lock().await?;
        let read = async {
            match job_fs.job_result().await {
                Ok(result) => {
                    let job = job_fs.job().await?;
                    Ok(Some(self.agent_result(job_id, &job_fs, result, job).await?))
                }
                Err(error) if is_not_found(&error) => Ok(None),
                Err(error) => Err(error),
            }
        }
        .await;
        let unlock = job_fs.unlock().await;
        read.and_then(|result| unlock.map(|_| result))
    }

    /// Persists a failed preparation or start so it can be reported after a restart.
    pub async fn fail(&self, job: Job) -> Result<(), Error> {
        let job_fs = self.job_fs(job.uuid);
        job_fs.lock().await?;
        let result = async {
            if job_fs.job_result().await.is_ok() {
                return Ok(());
            }
            if let Ok(container) = job_fs.container().await {
                if let Err(error) = self.inner.containerd.container(&container.id).kill().await
                    && !is_not_found(&error)
                {
                    return Err(error);
                }
            }
            if job_fs.job().await.is_err() {
                job_fs.write_job(job).await?;
            }
            job_fs
                .write_job_result(JobResult {
                    exit_code: 1,
                    outcome: Some(JobOutcome::Failed.as_str().to_owned()),
                    finished_at: Some(Timestamp::now()),
                    duration_ms: None,
                })
                .await?;
            self.inner.results.notify_waiters();
            Ok(())
        }
        .await;
        let unlock = job_fs.unlock().await;
        result.and(unlock)
    }

    /// Returns all durable results currently waiting for remote acknowledgement.
    pub async fn poll_results(&self) -> Result<Vec<AgentResult>, Error> {
        let mut results = Vec::new();
        for job_id in JobFs::job_ids(&self.inner.workdir).await? {
            let job_fs = self.job_fs(job_id);
            job_fs.lock().await?;
            let read_result = async {
                match job_fs.job_result().await {
                    Ok(result) => {
                        let job = job_fs.job().await?;
                        Ok(Some(self.agent_result(job_id, &job_fs, result, job).await?))
                    }
                    Err(error) if is_not_found(&error) => Ok(None),
                    Err(error) => Err(error),
                }
            }
            .await;
            let unlock_result = job_fs.unlock().await;
            let result = read_result.and_then(|result| unlock_result.map(|_| result))?;
            if let Some(result) = result {
                results.push(result);
            }
        }
        Ok(results)
    }

    /// Waits until at least one durable result is available.
    pub async fn wait_results(&self) -> Result<Vec<AgentResult>, Error> {
        loop {
            let notified = self.inner.results.notified();
            let results = self.poll_results().await?;
            if !results.is_empty() {
                return Ok(results);
            }
            notified.await;
        }
    }

    /// Removes the provider container and local files after a remote report is
    /// accepted. Results remain durable until this method succeeds.
    pub async fn acknowledge_result(&self, job_id: Uuid) -> Result<(), Error> {
        let job_fs = self.job_fs(job_id);
        job_fs.lock().await?;
        let result = self.acknowledge_locked(&job_fs, job_id).await;
        let unlock = job_fs.unlock().await;
        let result = result.and(unlock);
        if result.is_ok() {
            let _ = fs::remove_dir(job_fs.dir()).await;
        }
        result
    }

    /// Removes a stopped container and all local files for a job.
    pub async fn remove(&self, job_id: Uuid) -> Result<(), Error> {
        let job_fs = self.job_fs(job_id);
        job_fs.lock().await?;
        let result = self.remove_locked(&job_fs, job_id).await;
        let unlock = job_fs.unlock().await;
        let result = result.and(unlock);
        if result.is_ok() {
            let _ = fs::remove_dir(job_fs.dir()).await;
        }
        result
    }

    async fn acknowledge_locked(&self, job_fs: &JobFs, job_id: Uuid) -> Result<(), Error> {
        if job_fs.job_result().await.is_err() {
            return Err(Error::Agentd(format!(
                "cannot acknowledge a job without a result: {job_id}"
            )));
        }
        self.remove_locked(job_fs, job_id).await
    }

    async fn remove_locked(&self, job_fs: &JobFs, job_id: Uuid) -> Result<(), Error> {
        if let Ok(container) = job_fs.container().await {
            let handle = self.inner.containerd.container(&container.id);
            match handle.inspect().await {
                Ok(state) if state.is_running() => return Err(Error::JobRunning(job_id)),
                Ok(_) => handle.remove().await?,
                Err(error) if is_not_found(&error) => {}
                Err(error) => return Err(error),
            }
            self.inner.containers.lock().await.remove(&container.id);
        }
        job_fs.clear().await?;
        Ok(())
    }

    /// Stops a job and persists the requested terminal outcome immediately.
    pub async fn stop(&self, job_id: Uuid, reason: StopReason) -> Result<(), Error> {
        let job_fs = self.job_fs(job_id);
        job_fs.lock().await?;
        let result = self.stop_locked(&job_fs, reason).await;
        let unlock = job_fs.unlock().await;
        result.and(unlock)
    }

    async fn stop_locked(&self, job_fs: &JobFs, reason: StopReason) -> Result<(), Error> {
        let job = job_fs.job().await?;
        let container = job_fs.container().await?;
        let handle = self.inner.containerd.container(&container.id);
        match reason {
            StopReason::Yielded => handle.stop().await?,
            StopReason::Terminated | StopReason::Failed => handle.kill().await?,
        }
        let exit_code = match reason {
            StopReason::Yielded => 0,
            StopReason::Terminated => 143,
            StopReason::Failed => 1,
        };
        self.finish_locked(
            job_fs,
            &job,
            ExitStatus {
                exit_code,
                finished_at: Some(Timestamp::now()),
            },
            Some(match reason {
                StopReason::Yielded => JobOutcome::Yielded,
                StopReason::Terminated => JobOutcome::Terminated,
                StopReason::Failed => JobOutcome::Failed,
            }),
        )
        .await
    }

    async fn reconcile_all(&self) -> Result<(), Error> {
        let containers = JobFs::discover(&self.inner.workdir).await?;
        self.inner.containers.lock().await.extend(containers);
        for job_id in JobFs::job_ids(&self.inner.workdir).await? {
            self.reconcile_job(job_id).await?;
        }
        Ok(())
    }

    async fn reconcile_job(&self, job_id: Uuid) -> Result<(), Error> {
        let job_fs = self.job_fs(job_id);
        job_fs.lock().await?;
        let result = self.reconcile_locked(&job_fs).await;
        let unlock = job_fs.unlock().await;
        result.and(unlock)
    }

    async fn reconcile_locked(&self, job_fs: &JobFs) -> Result<(), Error> {
        if job_fs.job_result().await.is_ok() {
            return Ok(());
        }
        let job = match job_fs.job().await {
            Ok(job) => job,
            Err(error) if is_not_found(&error) => return Ok(()),
            Err(error) => return Err(error),
        };
        let container = match job_fs.container().await {
            Ok(container) => container,
            Err(error) if is_not_found(&error) => return Ok(()),
            Err(error) => return Err(error),
        };
        if container.r#type != self.inner.containerd.name() {
            return Err(Error::Provider(format!(
                "container provider mismatch: expected {}, found {}",
                self.inner.containerd.name(),
                container.r#type
            )));
        }
        let handle = self.inner.containerd.container(&container.id);
        let state = match handle.inspect().await {
            Ok(state) => state,
            Err(error) if is_not_found(&error) => {
                self.finish_locked(
                    job_fs,
                    &job,
                    ExitStatus {
                        exit_code: 1,
                        finished_at: Some(Timestamp::now()),
                    },
                    Some(JobOutcome::Failed),
                )
                .await?;
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        match state {
            ContainerState::Running { pid, started_at } => {
                job_fs.write_runtime(pid, started_at).await?;
                if job
                    .expired_at
                    .is_some_and(|expired_at| expired_at <= Timestamp::now())
                {
                    handle.kill().await?;
                    self.finish_locked(
                        job_fs,
                        &job,
                        ExitStatus {
                            exit_code: 124,
                            finished_at: Some(Timestamp::now()),
                        },
                        Some(JobOutcome::Failed),
                    )
                    .await?;
                }
            }
            ContainerState::Exited {
                exit_code,
                finished_at,
            } => {
                self.finish_locked(
                    job_fs,
                    &job,
                    ExitStatus {
                        exit_code,
                        finished_at,
                    },
                    None,
                )
                .await?;
            }
            ContainerState::Created => {
                if job
                    .expired_at
                    .is_some_and(|expired_at| expired_at <= Timestamp::now())
                {
                    handle.kill().await?;
                    self.finish_locked(
                        job_fs,
                        &job,
                        ExitStatus {
                            exit_code: 124,
                            finished_at: Some(Timestamp::now()),
                        },
                        Some(JobOutcome::Failed),
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    async fn finish_locked(
        &self,
        job_fs: &JobFs,
        _job: &Job,
        status: ExitStatus,
        forced_outcome: Option<JobOutcome>,
    ) -> Result<(), Error> {
        if job_fs.job_result().await.is_ok() {
            return Ok(());
        }
        let finished_at = status.finished_at.unwrap_or_else(Timestamp::now);
        let started_at = match job_fs.started_at().await {
            Ok(started_at) => Some(started_at),
            Err(error) if is_not_found(&error) => None,
            Err(error) => return Err(error),
        };
        let outcome = forced_outcome.unwrap_or(if status.exit_code == 0 {
            JobOutcome::Succeeded
        } else {
            JobOutcome::Failed
        });
        let duration_ms = started_at.and_then(|started_at| {
            (finished_at - started_at)
                .try_into()
                .ok()
                .and_then(|duration: Duration| u64::try_from(duration.as_millis()).ok())
        });
        job_fs
            .write_job_result(JobResult {
                exit_code: status.exit_code,
                outcome: Some(outcome.as_str().to_owned()),
                finished_at: Some(finished_at),
                duration_ms,
            })
            .await?;
        self.inner.results.notify_waiters();
        Ok(())
    }

    async fn agent_result(
        &self,
        job_id: Uuid,
        job_fs: &JobFs,
        result: JobResult,
        _job: Job,
    ) -> Result<AgentResult, Error> {
        let outcome = match result.outcome.as_deref() {
            Some(outcome) => JobOutcome::parse(outcome)?,
            None if result.exit_code == 0 => JobOutcome::Succeeded,
            None => JobOutcome::Failed,
        };
        let started_at = match job_fs.started_at().await {
            Ok(started_at) => Some(started_at),
            Err(error) if is_not_found(&error) => None,
            Err(error) => return Err(error),
        };
        Ok(AgentResult {
            job_id,
            outcome,
            exit_code: result.exit_code,
            started_at,
            finished_at: result.finished_at,
            duration: result.duration_ms.map(Duration::from_millis),
        })
    }

    fn job_fs(&self, job_id: Uuid) -> JobFs {
        JobFs::new(&self.inner.workdir, job_id)
    }

    async fn register_container(&self, container_id: String, job_id: Uuid) {
        self.inner
            .containers
            .lock()
            .await
            .insert(container_id, job_id);
    }

    async fn job_for_container(&self, container_id: &str) -> Option<Uuid> {
        if let Some(job_id) = self
            .inner
            .containers
            .lock()
            .await
            .get(container_id)
            .copied()
        {
            return Some(job_id);
        }
        let discovered = JobFs::discover(&self.inner.workdir).await.ok()?;
        let mut containers = self.inner.containers.lock().await;
        containers.extend(discovered);
        containers.get(container_id).copied()
    }
}

struct WatchGuard<'a>(&'a AtomicBool);

impl Drop for WatchGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

fn is_not_found(error: &Error) -> bool {
    match error {
        Error::Io(error) => error.kind() == io::ErrorKind::NotFound,
        Error::Docker(bollard::errors::Error::DockerResponseServerError {
            status_code: 404,
            ..
        }) => true,
        _ => false,
    }
}
