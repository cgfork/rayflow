//! Agent-side job execution and durable per-job filesystem support.
//!
//! An [`Agent`] owns its execution provider and job workdir. [`Agentic`] uses
//! an independent [`client::Client`] to poll and report while running jobs asynchronously.

pub mod client;
pub mod container;
pub mod errors;
pub mod jobfs;
pub mod settings;

use std::{
    collections::HashSet,
    future::pending,
    io,
    path::PathBuf,
    sync::atomic::{AtomicBool, Ordering},
};

use futures_util::{FutureExt, StreamExt, stream::FuturesUnordered};
use jiff::Timestamp;
use scheduler::{Job, actuator::Status};
use tokio::{
    fs,
    sync::{Notify, watch},
    time::sleep,
};
use uuid::Uuid;

use crate::{
    client::{Client, PollRequest},
    container::{
        Config, Connector, Container, ContainerBackend, Event, Provider, State,
        docker::DockerConnector,
    },
    errors::Error,
    jobfs::{JobFs, JobResult},
    settings::Settings,
};

/// Agent connection to the execution backend and the local job workdir.
pub struct Agent {
    connector: Provider,
    workdir: PathBuf,
}

impl Agent {
    pub fn new(workdir: impl Into<PathBuf>, connector: impl Into<Provider>) -> Self {
        Self {
            connector: connector.into(),
            workdir: workdir.into(),
        }
    }

    pub fn jobfs(&self, job_id: Uuid) -> JobFs {
        JobFs::new(&self.workdir, job_id)
    }

    /// Discovers UUID directories under the workdir without loading their job files.
    pub async fn jobs(&self) -> Result<Vec<JobFs>, Error> {
        fs::create_dir_all(&self.workdir).await?;
        let mut entries = fs::read_dir(&self.workdir).await?;
        let mut jobs = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_dir()
                && let Some(id) = entry
                    .file_name()
                    .to_str()
                    .and_then(|name| name.parse().ok())
            {
                jobs.push(self.jobfs(id));
            }
        }
        Ok(jobs)
    }

    async fn container(&self, job_fs: &JobFs) -> Result<ContainerBackend, Error> {
        let id = match job_fs.container().await {
            Ok(container) if container.r#type == "docker" => container.id,
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unknown container provider",
                )
                .into());
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                format!("rayflow-{}", job_fs.job_id)
            }
            Err(error) => return Err(error.into()),
        };
        Ok(self.connector.container(&id))
    }

    async fn save_state(&self, job_fs: &JobFs, state: State) -> Result<(), Error> {
        match state {
            State::Running { pid, started_at } => job_fs.set_runtime(pid, started_at).await?,
            State::Exited(exit_code) => job_fs.set_job_result(JobResult { exit_code }).await?,
            State::Created => {}
        }
        Ok(())
    }

    /// Persists the pinned configuration and fetches its image, reusing existing execution.
    pub async fn prepare(&self, job: Job) -> Result<(), Error> {
        let job_fs = self.jobfs(job.uuid);
        job_fs.lock().await?;
        if optional(job_fs.job_result().await)?.is_some() {
            return Ok(());
        }
        let container = self.container(&job_fs).await?;
        if let Some(state) = self.connector.inspect(container.name()).await? {
            if optional(job_fs.job().await)?.is_none() {
                job_fs.set_job(job).await?;
            }
            job_fs
                .set_container(jobfs::Container {
                    r#type: "docker".into(),
                    id: container.name().into(),
                })
                .await?;
            self.save_state(&job_fs, state).await?;
            return Ok(());
        }
        if optional(job_fs.container().await)?.is_some() {
            job_fs.set_job_result(JobResult { exit_code: 1 }).await?;
            return Ok(());
        }
        let config = Config::try_from(job.clone())?;
        job_fs.clear().await?;
        job_fs.set_job(job).await?;
        self.connector.prepare(config).await?;
        Ok(())
    }

    /// Starts a container and returns without waiting for its exit.
    pub async fn run(&self, job: Job) -> Result<(), Error> {
        self.prepare(job.clone()).await?;
        let job_fs = self.jobfs(job.uuid);
        job_fs.lock().await?;
        if optional(job_fs.job_result().await)?.is_some() {
            return Ok(());
        }
        let config = Config::try_from(job.clone())?;
        let mut container = self.container(&job_fs).await?;
        let state = self.connector.inspect(container.name()).await?;
        if let Some(state @ (State::Running { .. } | State::Exited(_))) = state {
            self.save_state(&job_fs, state).await?;
            return Ok(());
        }
        if state.is_none() {
            container = self.connector.create_job(&job, config.clone()).await?;
            job_fs
                .set_container(jobfs::Container {
                    r#type: "docker".into(),
                    id: container.name().into(),
                })
                .await?;
        }
        container.start(config).await?;
        if let Some(state) = self.connector.inspect(container.name()).await? {
            self.save_state(&job_fs, state).await?;
        }
        Ok(())
    }

    /// Removes a stopped container and its job files, retaining the lock inode.
    pub async fn destroy(&self, job: Job) -> Result<(), Error> {
        let job_fs = self.jobfs(job.uuid);
        job_fs.lock().await?;
        let container = self.container(&job_fs).await?;
        if matches!(
            self.connector.inspect(container.name()).await?,
            Some(State::Running { .. })
        ) {
            return Err(Error::JobRunning(job.uuid));
        }
        self.connector.remove(container.name()).await?;
        job_fs.clear().await?;
        job_fs.unlock().await?;
        Ok(())
    }

    async fn stop(&self, job: &Job, exit_code: i32) -> Result<i32, Error> {
        let job_fs = self.jobfs(job.uuid);
        job_fs.lock().await?;
        if let Some(result) = optional(job_fs.job_result().await)? {
            return Ok(result.exit_code);
        }
        if optional(job_fs.job().await)?.is_none() {
            job_fs.set_job(job.clone()).await?;
        }
        let container = self.container(&job_fs).await?;
        if self.connector.inspect(container.name()).await?.is_some() {
            container.kill().await?;
        }
        self.connector.remove(container.name()).await?;
        job_fs.set_job_result(JobResult { exit_code }).await?;
        Ok(exit_code)
    }
}

/// Coordinates polling, provider events, recovery, and result delivery.
pub struct Agentic {
    agent: Agent,
    client: Client,
    settings: Settings,
    stop: watch::Sender<bool>,
    events: Notify,
    running: AtomicBool,
    stopped: Notify,
}

impl Agentic {
    pub fn new(settings: Settings) -> Result<Self, Error> {
        let connector = DockerConnector::connect_with_defaults().map_err(container::Error::from)?;
        let agent = Agent::new(&settings.workdir, connector);
        let client = Client::new(&settings)?;
        Self::with_agent(agent, client, settings)
    }

    pub fn with_agent(agent: Agent, client: Client, settings: Settings) -> Result<Self, Error> {
        settings.validate()?;
        Ok(Self {
            agent,
            client,
            settings,
            stop: watch::channel(false).0,
            events: Notify::new(),
            running: AtomicBool::new(false),
            stopped: Notify::new(),
        })
    }

    async fn cancelled(&self) {
        let mut stop = self.stop.subscribe();
        stop.wait_for(|stopped| *stopped).await.ok();
    }

    async fn execute(&self, job: &Job) -> Result<i32, Error> {
        let job_fs = self.agent.jobfs(job.uuid);
        if let Some(result) = optional(job_fs.job_result().await)? {
            return Ok(result.exit_code);
        }
        self.agent.run(job.clone()).await?;
        let container = self.agent.container(&job_fs).await?;
        loop {
            if let Some(result) = optional(job_fs.job_result().await)? {
                return Ok(result.exit_code);
            }
            let changed = self.events.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            match self.agent.connector.inspect(container.name()).await? {
                Some(State::Exited(exit_code)) => return Ok(exit_code),
                None => return Ok(1),
                _ => {}
            }
            tokio::select! {
                result = container.wait() => match result {
                    Ok(code) => return Ok(code.try_into().unwrap_or(1)),
                    Err(error) => {
                        eprintln!("job {} wait: {error}", job.uuid);
                        sleep(self.settings.poll_interval).await;
                    }
                },
                _ = &mut changed => {},
            }
        }
    }

    async fn run(&self, job: Job) -> Result<(), Error> {
        let job_fs = self.agent.jobfs(job.uuid);
        let job = optional(job_fs.job().await)?.unwrap_or(job);
        if job.status.is_terminal() {
            return Ok(());
        }
        let result = tokio::select! {
            biased;
            _ = self.cancelled() => return Ok(()),
            _ = deadline(&job) => self.stop_job(&job, 124).await?,
            result = self.execute(&job) => match result {
                Ok(exit_code) => exit_code,
                Err(error) => {
                    eprintln!("job {} execution: {error}", job.uuid);
                    self.stop_job(&job, 1).await?
                }
            },
        };
        job_fs
            .set_job_result(JobResult { exit_code: result })
            .await?;
        loop {
            let report = async {
                if result == 0 {
                    self.client.succeed(self.settings.id, job.uuid).await
                } else {
                    self.client.fail(self.settings.id, job.uuid).await
                }
            };
            let reported = tokio::select! {
                _ = self.cancelled() => return Ok(()),
                reported = report => reported,
            };
            match reported {
                Ok(()) | Err(Error::ClaimLost(_)) => {
                    job_fs.lock().await?;
                    let mut job = job;
                    job.status = if result == 0 {
                        Status::Succeeded
                    } else {
                        Status::Failed
                    };
                    job.agent_id = None;
                    job_fs.set_job(job).await?;
                    return Ok(());
                }
                Err(error) => eprintln!("job {} report: {error}", job.uuid),
            }
            tokio::select! {
                _ = self.cancelled() => return Ok(()),
                _ = sleep(self.settings.poll_interval) => {},
            }
        }
    }

    async fn stop_job(&self, job: &Job, exit_code: i32) -> Result<i32, Error> {
        loop {
            tokio::select! {
                _ = self.cancelled() => return Err(io::Error::new(io::ErrorKind::Interrupted, "agent stopped").into()),
                result = self.agent.stop(job, exit_code) => match result {
                    Ok(code) => return Ok(code),
                    Err(error) => eprintln!("job {} cleanup: {error}", job.uuid),
                },
            }
            tokio::select! {
                _ = self.cancelled() => return Err(io::Error::new(io::ErrorKind::Interrupted, "agent stopped").into()),
                _ = sleep(self.settings.poll_interval) => {},
            }
        }
    }

    async fn poll_loop(&self) -> Result<(), Error> {
        let mut active = HashSet::new();
        let mut jobs = FuturesUnordered::new();
        for job_fs in self.agent.jobs().await? {
            match job_fs.job().await {
                Ok(job)
                    if job.status == Status::Polled
                        && job.agent_id.is_none_or(|id| id == self.settings.id) =>
                {
                    active.insert(job.uuid);
                    jobs.push(
                        async move {
                            let id = job.uuid;
                            (id, self.run(job).await)
                        }
                        .boxed(),
                    );
                }
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => eprintln!("job {} recovery: {error}", job_fs.job_id),
            }
        }
        loop {
            let request = PollRequest {
                agent_id: self.settings.id,
                batch: self.settings.batch,
                resources: self.settings.resources,
                active_jobs: active.iter().copied().collect(),
            };
            let poll = self.client.poll(&request);
            tokio::pin!(poll);
            let response = loop {
                tokio::select! {
                    _ = self.cancelled() => return Ok(()),
                    Some((id, result)) = jobs.next(), if !jobs.is_empty() => {
                        active.remove(&id);
                        if let Err(error) = result { eprintln!("job {id}: {error}"); }
                    },
                    response = &mut poll => break response,
                }
            };
            match response {
                Ok(response) => {
                    for job in response.jobs {
                        if active.insert(job.uuid) {
                            jobs.push(
                                async move {
                                    let id = job.uuid;
                                    (id, self.run(job).await)
                                }
                                .boxed(),
                            );
                        }
                    }
                }
                Err(error) => eprintln!("agent poll: {error}"),
            }
            let retry = sleep(self.settings.poll_interval);
            tokio::pin!(retry);
            loop {
                tokio::select! {
                    _ = self.cancelled() => return Ok(()),
                    Some((id, result)) = jobs.next(), if !jobs.is_empty() => {
                        active.remove(&id);
                        if let Err(error) = result { eprintln!("job {id}: {error}"); }
                    },
                    _ = &mut retry => break,
                }
            }
        }
    }

    async fn event_loop(&self) -> Result<(), Error> {
        loop {
            let events = self.agent.connector.events();
            tokio::pin!(events);
            loop {
                tokio::select! {
                    _ = self.cancelled() => return Ok(()),
                    event = events.next() => match event {
                        Some(Ok(Event::Docker(event))) => {
                            if matches!(event.action.as_deref(), Some("die" | "destroy")) {
                                self.events.notify_waiters();
                            }
                        },
                        Some(Err(error)) => {
                            eprintln!("agent events: {error}");
                            break;
                        },
                        None => break,
                    },
                }
            }
            self.events.notify_waiters();
            tokio::select! {
                _ = self.cancelled() => return Ok(()),
                _ = sleep(self.settings.poll_interval) => {},
            }
        }
    }

    pub async fn start(&self) -> Result<(), Error> {
        if self
            .running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(Error::AlreadyRunning);
        }
        let _running = Running(self);
        tokio::try_join!(self.poll_loop(), self.event_loop())?;
        Ok(())
    }

    /// Stops orchestration, leaving isolated containers and durable results for recovery.
    pub async fn shutdown(&self) -> Result<(), Error> {
        self.stop.send_replace(true);
        loop {
            let stopped = self.stopped.notified();
            tokio::pin!(stopped);
            stopped.as_mut().enable();
            if !self.running.load(Ordering::Acquire) {
                return Ok(());
            }
            stopped.await;
        }
    }
}

struct Running<'a>(&'a Agentic);

impl Drop for Running<'_> {
    fn drop(&mut self) {
        self.0.running.store(false, Ordering::Release);
        self.0.stopped.notify_waiters();
    }
}

fn optional<T>(result: io::Result<T>) -> io::Result<Option<T>> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

async fn deadline(job: &Job) {
    match job.expired_at {
        Some(expired_at) => {
            let remaining = (expired_at - Timestamp::now())
                .try_into()
                .unwrap_or_default();
            sleep(remaining).await;
        }
        None => pending().await,
    }
}
