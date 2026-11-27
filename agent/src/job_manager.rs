//! Per-agent job execution, reporting, and recovery.

use std::{
    collections::{HashMap, HashSet},
    fs::{File, TryLockError},
    future::Future,
    io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use futures_util::{StreamExt, stream::FuturesUnordered};
use graph::{GraphStore, types::DAG};
use jiff::Timestamp;
use scheduler::{Job, JobArtifactName};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::{
    fs,
    sync::{Mutex as AsyncMutex, Notify, Semaphore},
    time::{self, Instant, MissedTickBehavior},
};
use uuid::Uuid;

use crate::{
    client::Client,
    containerd::{Container, ContainerFile, ContainerState, Containerd},
    errors::Error,
    read_json,
    settings::Settings,
    transfer::{ASSET_DIR, JobTransfer},
    write_json,
};

mod completion;
mod owner;
mod usage;

use completion::CLEANUP_FILE;
pub use usage::{JobUsage, Usage, UsagePercentiles, UsageSummary};

const RECOVERY_INTERVAL: Duration = Duration::from_secs(1);
const EVENT_RETRY_DELAY: Duration = Duration::from_millis(250);
const INPUT_MOUNT: &str = "/data/inputs";
const OUTPUT_MOUNT: &str = "/data/outputs";
const ASSET_MOUNT: &str = "/data/assets";
const MAX_ARCHIVE_UPLOADS: usize = 4;
const CONTAINER_FILE: &str = "container.json";

/// The terminal action reported to the server.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobOutcome {
    Succeeded,
    Failed,
    Yielded,
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
            outcome => Err(Error::JobManager(format!("unknown job outcome: {outcome}"))),
        }
    }
}

/// The result recorded when a container exits or a job cannot start.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct JobResult {
    pub exit_code: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
}

impl JobResult {
    pub fn outcome(&self) -> Result<JobOutcome, Error> {
        match self.outcome.as_deref() {
            Some(value) => JobOutcome::parse(value),
            None if self.exit_code == 0 => Ok(JobOutcome::Succeeded),
            None => Ok(JobOutcome::Failed),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JobContainer {
    r#type: String,
    id: String,
    #[serde(default)]
    removed: bool,
}

/// The outcome to record when explicitly stopping a job.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StopReason {
    Yielded,
    Terminated,
    Failed,
}

struct Inner<C, R> {
    containerd: C,
    graph_store: R,
    client: Client,
    transfer: JobTransfer,
    archive_slots: Semaphore,
    archive_retry_after: Mutex<Option<Instant>>,
    _owner: File,
    active: Mutex<HashSet<Uuid>>,
    containers: Mutex<HashMap<String, Uuid>>,
    lifecycle_changed: Notify,
    usage_changed: Notify,
    watching_lifecycle: AsyncMutex<()>,
    watching_usage: AsyncMutex<()>,
}

/// Owns the jobs in a single workdir. Clones share the same manager and lock.
#[derive(Clone)]
pub struct JobManager<C, R> {
    inner: Arc<Inner<C, R>>,
}

impl<C, R> JobManager<C, R>
where
    C: Containerd,
    R: GraphStore<Uuid, G = DAG> + Clone + Send + Sync,
{
    pub async fn open(containerd: C, graph_store: R, client: Client) -> Result<Self, Error> {
        let settings = client.settings();
        settings.validate()?;
        let owner = owner::acquire(settings)?;
        fs::create_dir_all(&settings.data_dir).await?;
        let transfer = JobTransfer::new(client.clone())?;
        Ok(Self {
            inner: Arc::new(Inner {
                containerd,
                graph_store,
                client,
                transfer,
                archive_slots: Semaphore::new(MAX_ARCHIVE_UPLOADS),
                archive_retry_after: Mutex::default(),
                _owner: owner,
                active: Mutex::default(),
                containers: Mutex::default(),
                lifecycle_changed: Notify::new(),
                usage_changed: Notify::new(),
                watching_lifecycle: AsyncMutex::default(),
                watching_usage: AsyncMutex::default(),
            }),
        })
    }

    pub fn settings(&self) -> &Settings {
        self.client().settings()
    }

    pub(crate) fn client(&self) -> &Client {
        &self.inner.client
    }

    pub fn active_job_ids(&self) -> Vec<Uuid> {
        self.inner.active.lock().unwrap().iter().copied().collect()
    }

    /// Loads persisted jobs before polling. The caller passes returned jobs
    /// to `start` before sending its first poll.
    pub async fn recover(&self) -> Result<Vec<Job>, Error> {
        let mut pending = Vec::new();
        let mut reporting = HashSet::new();
        for job_id in self.job_ids().await? {
            if self.is_completed(job_id).await? {
                continue;
            }
            if self.result(job_id).await?.is_some() {
                reporting.insert(job_id);
            } else {
                pending.push(self.read_job(job_id).await?);
            }
        }
        *self.inner.active.lock().unwrap() = reporting;
        Ok(pending)
    }

    /// Reserves the claim immediately and returns the future that starts its job.
    /// Driving several of these futures lets independent image and input transfers overlap.
    pub fn start(&self, job: Job) -> impl Future<Output = ()> + '_ {
        let job_id = job.uuid;
        let admitted = self.inner.active.lock().unwrap().insert(job_id);
        async move {
            if !admitted {
                return;
            }
            if let Err(error) = self.launch(&job).await {
                tracing::warn!(%job_id, %error, "job could not start");
                loop {
                    match self.record_start_failure(&job).await {
                        Ok(()) => break,
                        Err(error) => {
                            tracing::warn!(%job_id, %error, "could not persist job failure");
                            time::sleep(self.settings().poll_interval).await;
                        }
                    }
                }
            }
        }
    }

    async fn launch(&self, claim: &Job) -> Result<(), Error> {
        let job_id = claim.uuid;
        let directory = self.job_dir(job_id);
        ensure_dir(&directory).await?;
        let job;
        {
            let _lock = self.lock_job(job_id).await?;
            if self.is_completed(job_id).await? {
                self.release(job_id);
                return Ok(());
            }
            job = self.persist_claim(claim).await?;
            if self.result(job_id).await?.is_some() {
                return Ok(());
            }
            if let Some(container) = self.read_container(job_id).await? {
                return self.reconcile_container(&job, &container, true).await;
            }
            if let Some(container) = self.find_container(job_id).await? {
                return self.reconcile_container(&job, &container, true).await;
            }
            if lease_expired(&job) {
                return self.record_expired(job_id, None).await;
            }
        }

        // The claim reservation excludes another startup while preparation runs
        // without the lifecycle lock.
        if job.run.is_unloaded() {
            return Err(Error::JobManager(format!(
                "job {} is missing its pinned run",
                job.uuid
            )));
        }
        if job.node_config.is_unloaded() || job.node_config.get().is_none() {
            return Err(Error::MissingConfig);
        }
        let graph = self.load_graph(&job).await?;
        let node_run = job.node_run.get();
        let node = graph
            .nodes
            .iter()
            .find(|node| node.id == node_run.node_id)
            .ok_or_else(|| Error::JobManager("job node is missing from graph".into()))?;
        write_json(&directory.join("graph.json"), &graph).await?;
        let data = self.data_dir(job_id);
        ensure_dir(&data).await?;
        let inputs = data.join("inputs");
        let outputs = data.join("outputs");
        let assets = data.join(ASSET_DIR);
        ensure_dir(&inputs).await?;
        ensure_dir(&outputs).await?;
        ensure_dir(&assets).await?;

        let files = [
            ContainerFile {
                source: fs::canonicalize(&directory).await?,
                target: "/rayflow".into(),
                read_only: true,
                env: vec![
                    ("RAYFLOW_JOB_FILE".into(), "/rayflow/job.json".into()),
                    ("RAYFLOW_GRAPH_FILE".into(), "/rayflow/graph.json".into()),
                ],
            },
            ContainerFile {
                source: fs::canonicalize(&inputs).await?,
                target: INPUT_MOUNT.into(),
                read_only: true,
                env: vec![("RAYFLOW_INPUT_DIR".into(), INPUT_MOUNT.into())],
            },
            ContainerFile {
                source: fs::canonicalize(&outputs).await?,
                target: OUTPUT_MOUNT.into(),
                read_only: false,
                env: vec![("RAYFLOW_OUTPUT_DIR".into(), OUTPUT_MOUNT.into())],
            },
            ContainerFile {
                source: fs::canonicalize(&assets).await?,
                target: ASSET_MOUNT.into(),
                read_only: true,
                env: vec![("RAYFLOW_ASSET_DIR".into(), ASSET_MOUNT.into())],
            },
        ];
        let provider = &self.inner.containerd;
        let spec = || provider.spec_with_files(&job, &files);
        provider.prepare(spec()?).await?;
        while !lease_expired(&job) {
            let prepared = async {
                self.inner.transfer.prepare_input(&job, &data, node).await?;
                self.inner.transfer.prepare_assets(&job, &data, node).await
            }
            .await;
            match prepared {
                Ok(()) => break,
                Err(Error::Http(error)) if retryable_input(&error) => {
                    tracing::warn!(%job_id, %error, "artifact download failed, retrying");
                    time::sleep(self.settings().poll_interval).await;
                }
                Err(error) => return Err(error),
            }
        }

        let _lock = self.lock_job(job_id).await?;
        if self.result(job_id).await?.is_some() {
            return Ok(());
        }
        if lease_expired(&job) {
            return self.record_expired(job_id, None).await;
        }
        let container = provider.create(spec()?).await?;
        let metadata = self.persist_container(job_id, &container).await?;
        self.reconcile_container(&job, &metadata, true).await
    }

    async fn load_graph(&self, job: &Job) -> Result<DAG, Error> {
        if job.node_run.is_unloaded() {
            return Err(Error::JobManager(format!(
                "job {} is missing its pinned node run",
                job.uuid
            )));
        }
        let node_run = job.node_run.get();
        let path = self.job_dir(job.uuid).join("graph.json");
        let graph: DAG = match read_optional_json(&path).await? {
            Some(graph) => graph,
            None => self
                .inner
                .graph_store
                .clone()
                .load(node_run.graph_id, Some(node_run.version))
                .await
                .map_err(|error| Error::JobManager(format!("load graph: {error}")))?,
        };
        if graph.id != node_run.graph_id || graph.version != node_run.version {
            return Err(Error::JobManager(
                "job graph identity does not match its pinned version".into(),
            ));
        }
        Ok(graph)
    }

    async fn persist_claim(&self, claim: &Job) -> Result<Job, Error> {
        let path = self.job_dir(claim.uuid).join("job.json");
        match read_optional_json(&path).await? {
            Some(job) => validate_job_id(job, claim.uuid),
            None => {
                write_json(&path, claim).await?;
                Ok(claim.clone())
            }
        }
    }

    async fn record_start_failure(&self, claim: &Job) -> Result<(), Error> {
        let job_id = claim.uuid;
        ensure_dir(&self.job_dir(job_id)).await?;
        let _lock = self.lock_job(job_id).await?;
        if self.is_completed(job_id).await? {
            self.release(job_id);
            return Ok(());
        }
        let job = self.persist_claim(claim).await?;
        if self.result(job_id).await?.is_some() {
            return Ok(());
        }
        if let Some(metadata) = self.read_container(job_id).await? {
            let container = self.container(&metadata)?;
            // Inspect again before failing a start. The provider may have
            // started the container before the failed response arrived.
            match container.inspect().await {
                Ok(ContainerState::Running { .. } | ContainerState::Exited { .. }) => {
                    return self.reconcile_container(&job, &metadata, false).await;
                }
                Ok(ContainerState::Created) if lease_expired(&job) => {
                    container.kill().await?;
                }
                Ok(ContainerState::Created) => {}
                Err(error) if is_not_found(&error) => {}
                Err(error) => return Err(error),
            }
        } else if let Some(container) = self.find_container(job_id).await? {
            return self.reconcile_container(&job, &container, true).await;
        }
        if lease_expired(&job) {
            return self.record_expired(job_id, None).await;
        }
        self.record_result(job_id, 1, JobOutcome::Failed, None, None)
            .await
    }

    /// Drives container events, periodic lease checks, and durable job delivery.
    /// Dropping this future leaves containers and files available for recovery.
    pub async fn start_lifecycle(&self) -> Result<(), Error> {
        let _watching = self
            .inner
            .watching_lifecycle
            .try_lock()
            .map_err(|_| Error::AlreadyRunning)?;
        let mut sweep = time::interval(RECOVERY_INTERVAL);
        sweep.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut events = Some(self.inner.containerd.events());
        let reconnect = time::sleep(EVENT_RETRY_DELAY);
        tokio::pin!(reconnect);

        let mut pending = FuturesUnordered::new();
        let mut busy = HashSet::new();
        loop {
            let mut reconcile_all = false;
            let mut event_job = None;
            tokio::select! {
                _ = sweep.tick() => reconcile_all = true,
                _ = self.inner.lifecycle_changed.notified() => reconcile_all = true,
                _ = &mut reconnect, if events.is_none() => {
                    events = Some(self.inner.containerd.events());
                }
                event = async { events.as_mut().unwrap().next().await }, if events.is_some() => {
                    match event {
                        Some(Ok(event)) => {
                            event_job = self.inner.containers.lock().unwrap()
                                .get(&event.container_id).copied();
                        }
                        Some(Err(_)) | None => {
                            events = None;
                            reconnect.as_mut().reset(Instant::now() + EVENT_RETRY_DELAY);
                        }
                    }
                }
                Some(job_id) = pending.next(), if !pending.is_empty() => {
                    busy.remove(&job_id);
                }
            }

            if reconcile_all {
                match self.job_ids().await {
                    Ok(job_ids) => {
                        for job_id in job_ids {
                            if busy.insert(job_id) {
                                pending.push(self.advance_job(job_id));
                            }
                        }
                    }
                    Err(error) => tracing::warn!(%error, "could not scan job workdir"),
                }
            }
            if let Some(job_id) = event_job
                && busy.insert(job_id)
            {
                pending.push(self.advance_job(job_id));
            }
        }
    }

    async fn advance_job(&self, job_id: Uuid) -> Uuid {
        let result = async {
            if !fs::try_exists(self.job_dir(job_id).join(CLEANUP_FILE)).await? {
                self.reconcile(job_id).await?;
            }
            self.advance_completion(job_id).await
        }
        .await;
        if let Err(error) = result
            && !is_not_found(&error)
        {
            tracing::warn!(%job_id, %error, "could not advance job");
        }
        job_id
    }

    async fn reconcile(&self, job_id: Uuid) -> Result<(), Error> {
        // Reported or already exited jobs need no provider round trip.
        if self.result(job_id).await?.is_some() || self.read_container(job_id).await?.is_none() {
            return Ok(());
        }
        let _lock = self.lock_job(job_id).await?;
        if self.result(job_id).await?.is_some() {
            return Ok(());
        }
        if let Some(metadata) = self.read_container(job_id).await? {
            let job = self.read_job(job_id).await?;
            self.reconcile_container(&job, &metadata, false).await?;
        }
        Ok(())
    }

    async fn reconcile_container(
        &self,
        job: &Job,
        metadata: &JobContainer,
        start_created: bool,
    ) -> Result<(), Error> {
        let container = self.container(metadata)?;
        let job_id = job.uuid;
        self.inner
            .containers
            .lock()
            .unwrap()
            .insert(metadata.id.clone(), job_id);
        let mut state = match container.inspect().await {
            Ok(state) => state,
            Err(error) if is_not_found(&error) => {
                return self
                    .record_result(job_id, 1, JobOutcome::Failed, None, None)
                    .await;
            }
            Err(error) => return Err(error),
        };

        if start_created && state == ContainerState::Created && !lease_expired(job) {
            container.start().await?;
            state = container.inspect().await?;
        }
        match state {
            ContainerState::Exited {
                exit_code,
                started_at,
                finished_at,
            } => {
                let outcome = if exit_code == 0 {
                    JobOutcome::Succeeded
                } else {
                    JobOutcome::Failed
                };
                self.record_result(job_id, exit_code, outcome, started_at, finished_at)
                    .await
            }
            _ if lease_expired(job) => {
                container.kill().await?;
                self.record_expired(job_id, state.started_at()).await
            }
            ContainerState::Running { pid, started_at } => {
                self.record_runtime(job_id, pid, started_at).await
            }
            ContainerState::Created => Ok(()),
        }
    }

    pub async fn result(&self, job_id: Uuid) -> Result<Option<JobResult>, Error> {
        read_optional_json(&self.job_dir(job_id).join(JobArtifactName::Result.as_str())).await
    }

    /// Stops an existing container and records the selected terminal outcome.
    pub async fn stop(&self, job_id: Uuid, reason: StopReason) -> Result<(), Error> {
        let _lock = self.lock_job(job_id).await?;
        if self.result(job_id).await?.is_some() {
            return Ok(());
        }
        let metadata = self
            .read_container(job_id)
            .await?
            .ok_or_else(|| Error::JobManager("container metadata is missing".into()))?;
        let container = self.container(&metadata)?;
        let (exit_code, outcome) = match reason {
            StopReason::Yielded => {
                container.stop().await?;
                (0, JobOutcome::Yielded)
            }
            StopReason::Terminated => {
                container.kill().await?;
                (143, JobOutcome::Terminated)
            }
            StopReason::Failed => {
                container.kill().await?;
                (1, JobOutcome::Failed)
            }
        };
        let started_at = container
            .inspect()
            .await
            .ok()
            .and_then(|state| state.started_at());
        self.record_result(job_id, exit_code, outcome, started_at, None)
            .await
    }

    async fn record_expired(
        &self,
        job_id: Uuid,
        started_at: Option<Timestamp>,
    ) -> Result<(), Error> {
        self.record_result(job_id, 124, JobOutcome::Failed, started_at, None)
            .await
    }

    async fn record_result(
        &self,
        job_id: Uuid,
        exit_code: i32,
        outcome: JobOutcome,
        started_at: Option<Timestamp>,
        finished_at: Option<Timestamp>,
    ) -> Result<(), Error> {
        let directory = self.job_dir(job_id);
        let finished_at = finished_at.unwrap_or_else(Timestamp::now);
        let started_at = match started_at {
            Some(started_at) => Some(started_at),
            None => match fs::read_to_string(directory.join("runtime/started_at")).await {
                Ok(value) => value.trim().parse().ok(),
                Err(error) if error.kind() == io::ErrorKind::NotFound => None,
                Err(error) => return Err(error.into()),
            },
        };
        let duration_ms = started_at
            .and_then(|start| Duration::try_from(finished_at - start).ok())
            .and_then(|duration| u64::try_from(duration.as_millis()).ok());
        let result = JobResult {
            exit_code,
            outcome: Some(outcome.as_str().into()),
            finished_at: Some(finished_at),
            duration_ms,
        };
        write_json(&directory.join(JobArtifactName::Result.as_str()), &result).await?;
        self.inner.lifecycle_changed.notify_one();
        self.inner.usage_changed.notify_one();
        Ok(())
    }

    async fn record_runtime(
        &self,
        job_id: Uuid,
        pid: Option<i64>,
        started_at: Option<Timestamp>,
    ) -> Result<(), Error> {
        let directory = self.job_dir(job_id);
        let mut changed = false;
        if let Some(pid) = pid {
            changed |= write_changed(&directory.join("pid"), pid.to_string()).await?;
        }
        if let Some(started_at) = started_at {
            let runtime = directory.join("runtime");
            ensure_dir(&runtime).await?;
            changed |= write_changed(&runtime.join("started_at"), started_at.to_string()).await?;
        }
        if changed {
            self.inner.usage_changed.notify_one();
        }
        Ok(())
    }

    fn container(&self, metadata: &JobContainer) -> Result<C::Container, Error> {
        let provider = &self.inner.containerd;
        if metadata.r#type != provider.name() {
            return Err(Error::Provider(format!(
                "container provider mismatch: expected {}, found {}",
                provider.name(),
                metadata.r#type,
            )));
        }
        Ok(provider.container(&metadata.id))
    }

    async fn read_container(&self, job_id: Uuid) -> Result<Option<JobContainer>, Error> {
        read_optional_json(&self.job_dir(job_id).join(CONTAINER_FILE)).await
    }

    async fn find_container(&self, job_id: Uuid) -> Result<Option<JobContainer>, Error> {
        let Some(container) = self.inner.containerd.find_container(job_id).await? else {
            return Ok(None);
        };
        Ok(Some(self.persist_container(job_id, &container).await?))
    }

    async fn persist_container(
        &self,
        job_id: Uuid,
        container: &C::Container,
    ) -> Result<JobContainer, Error> {
        let metadata = JobContainer {
            r#type: self.inner.containerd.name().into(),
            id: container.id().into(),
            removed: false,
        };
        write_json(&self.job_dir(job_id).join(CONTAINER_FILE), &metadata).await?;
        self.inner.usage_changed.notify_one();
        Ok(metadata)
    }

    async fn read_job(&self, job_id: Uuid) -> Result<Job, Error> {
        let path = self.job_dir(job_id).join("job.json");
        validate_job_id(read_json(&path).await?, job_id)
    }

    fn release(&self, job_id: Uuid) {
        self.inner.active.lock().unwrap().remove(&job_id);
    }

    fn forget_container(&self, job_id: Uuid) {
        self.inner
            .containers
            .lock()
            .unwrap()
            .retain(|_, registered| *registered != job_id);
    }

    fn job_dir(&self, job_id: Uuid) -> PathBuf {
        self.settings().workdir.join(job_id.to_string())
    }

    fn data_dir(&self, job_id: Uuid) -> PathBuf {
        self.settings().data_dir.join(job_id.to_string())
    }

    async fn job_ids(&self) -> Result<Vec<Uuid>, Error> {
        let mut entries = fs::read_dir(&self.settings().workdir).await?;
        let mut jobs = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            if !entry.file_type().await?.is_dir() {
                continue;
            }
            if let Ok(job_id) = entry.file_name().to_string_lossy().parse()
                && (fs::try_exists(entry.path().join("job.json")).await?
                    || fs::try_exists(entry.path().join(CLEANUP_FILE)).await?)
            {
                jobs.push(job_id);
            }
        }
        Ok(jobs)
    }

    async fn lock_job(&self, job_id: Uuid) -> Result<File, Error> {
        let directory = self.job_dir(job_id);
        if !fs::symlink_metadata(&directory).await?.is_dir() {
            return Err(invalid_directory(&directory));
        }
        let lock = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(directory.join("lock"))
            .await?
            .into_std()
            .await;
        loop {
            match lock.try_lock() {
                Ok(()) => return Ok(lock),
                Err(TryLockError::WouldBlock) => time::sleep(Duration::from_millis(10)).await,
                Err(TryLockError::Error(error)) => return Err(error.into()),
            }
        }
    }
}

async fn ensure_dir(path: &Path) -> Result<(), Error> {
    match fs::create_dir(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            if fs::symlink_metadata(path).await?.is_dir() {
                Ok(())
            } else {
                Err(invalid_directory(path))
            }
        }
        Err(error) => Err(error.into()),
    }
}

fn invalid_directory(path: &Path) -> Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("job path is not a directory: {}", path.display()),
    )
    .into()
}

async fn read_optional_json<T: DeserializeOwned>(path: &Path) -> Result<Option<T>, Error> {
    match read_json(path).await {
        Ok(value) => Ok(Some(value)),
        Err(error) if is_not_found(&error) => Ok(None),
        Err(error) => Err(error),
    }
}

async fn write_changed(path: &Path, value: String) -> Result<bool, Error> {
    match fs::read_to_string(path).await {
        Ok(previous) if previous == value => return Ok(false),
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    fs::write(path, value).await?;
    Ok(true)
}

fn validate_job_id(job: Job, job_id: Uuid) -> Result<Job, Error> {
    if job.uuid != job_id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "job UUID does not match its workdir",
        )
        .into());
    }
    Ok(job)
}

fn is_not_found(error: &Error) -> bool {
    matches!(error,
        Error::Io(error) if error.kind() == io::ErrorKind::NotFound
    ) || matches!(
        error,
        Error::Docker(bollard::errors::Error::DockerResponseServerError {
            status_code: 404,
            ..
        })
    )
}

fn lease_expired(job: &Job) -> bool {
    job.expired_at
        .is_some_and(|expires| expires <= Timestamp::now())
}

fn retryable_input(error: &reqwest::Error) -> bool {
    error.is_timeout()
        || error.is_connect()
        || error.is_body()
        || error.status().is_some_and(|status| {
            status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS
        })
}
