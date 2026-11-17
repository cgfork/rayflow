//! Remote orchestration for the local job daemon.
//!
//! Job polling, preparation, and result reporting progress independently.
//! Agentd owns execution and durable state; this module owns remote delivery
//! and acknowledges local results only after the server accepts them.

use std::{
    collections::HashSet,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use futures_util::{StreamExt, stream::FuturesUnordered};
use graph::{GraphStore, types::DAG};
use scheduler::Job;
use tokio::{
    sync::{Mutex, Notify, watch},
    time::sleep,
};
use uuid::Uuid;

use crate::{
    AgentResult, Agentd, JobOutcome,
    client::{Client, PollRequest, PollResponse},
    containerd::{Containerd, docker::DockerContainerd},
    errors::Error,
    filed::Filed,
    settings::Settings,
};

/// Connects the remote scheduling service to a local [`Agentd`].
///
/// One loop polls jobs sequentially while preparing and starting them
/// concurrently. A second loop delivers durable results, including results
/// recovered after a crash. Pending preparations and unacknowledged results
/// remain active so the server does not redeliver their jobs.
pub struct Agentic<C = DockerContainerd, R = Client> {
    agentd: Agentd<C, R>,
    client: Client,
    filed: Filed,
    settings: Settings,
    active: Mutex<HashSet<Uuid>>,
    reporting: Mutex<()>,
    stop: watch::Sender<bool>,
    running: AtomicBool,
    stopped: Notify,
}

impl Agentic {
    /// Opens the configured workdir with Docker and HTTP connections.
    pub async fn new(settings: Settings) -> Result<Self, Error> {
        settings.validate()?;
        let client = Client::new(&settings)?;
        let agentd = Agentd::open_with_data_dir(
            DockerContainerd::connect_with_defaults()?,
            client.clone(),
            &settings.workdir,
            &settings.data_dir,
        )
        .await?;
        Self::with_agent(agentd, client, settings)
    }
}

impl<C, R> Agentic<C, R>
where
    C: Containerd,
    R: GraphStore<Uuid, G = DAG> + Clone + Send + Sync,
{
    /// Uses an existing daemon and an independent remote client.
    pub fn with_agent(
        agentd: Agentd<C, R>,
        client: Client,
        settings: Settings,
    ) -> Result<Self, Error> {
        settings.validate()?;
        if agentd.data_dir() != settings.data_dir {
            return Err(Error::Settings(
                "agent daemon and filed must use the same data_dir",
            ));
        }
        Ok(Self {
            filed: Filed::new(client.clone(), &settings)?,
            agentd,
            client,
            settings,
            active: Mutex::new(HashSet::new()),
            reporting: Mutex::new(()),
            stop: watch::channel(false).0,
            running: AtomicBool::new(false),
            stopped: Notify::new(),
        })
    }

    /// Runs both delivery flows and the daemon watcher until shutdown.
    /// Dropping this future cancels orchestration without stopping containers.
    pub async fn start(&self) -> Result<(), Error> {
        if self.running.swap(true, Ordering::AcqRel) {
            return Err(Error::AlreadyRunning);
        }
        let _guard = RunningGuard {
            running: &self.running,
            stopped: &self.stopped,
        };
        tokio::select! {
            biased;
            _ = self.cancelled() => Ok(()),
            result = self.run() => result,
        }
    }

    async fn run(&self) -> Result<(), Error> {
        let recovered = self.agentd.jobs().await?;
        let results = self.agentd.poll_results().await?;
        self.active.lock().await.extend(
            recovered
                .iter()
                .map(|job| job.uuid)
                .chain(results.iter().map(|result| result.job_id)),
        );
        tokio::select! {
            result = self.poll_jobs(recovered) => result,
            result = self.poll_results() => result,
            result = self.watch() => result,
        }
    }

    /// Stops polling and waits for orchestration to release its local state.
    /// Containers and unreported results remain available for the next agent.
    pub async fn shutdown(&self) -> Result<(), Error> {
        self.stop.send_replace(true);
        loop {
            let stopped = self.stopped.notified();
            if !self.running.load(Ordering::Acquire) {
                return Ok(());
            }
            stopped.await;
        }
    }

    async fn cancelled(&self) {
        let mut stop = self.stop.subscribe();
        let _ = stop.wait_for(|stopped| *stopped).await;
    }

    async fn poll_after(&self, delay: Duration) -> Result<PollResponse, Error> {
        sleep(delay).await;
        let active_jobs = self.active.lock().await.iter().copied().collect();
        self.client
            .poll(&PollRequest {
                agent_id: self.settings.id,
                batch: self.settings.batch,
                resources: self.settings.resources,
                active_jobs,
            })
            .await
    }

    async fn poll_jobs(&self, recovered: Vec<Job>) -> Result<(), Error> {
        let mut starting = FuturesUnordered::new();
        for job in recovered {
            starting.push(self.execute(job));
        }
        let mut polling = Box::pin(self.poll_after(Duration::ZERO));
        loop {
            tokio::select! {
                _ = starting.next(), if !starting.is_empty() => {},
                response = &mut polling => {
                    match response {
                        Ok(response) => {
                            for job in response.jobs {
                                if self.active.lock().await.insert(job.uuid) {
                                    starting.push(self.execute(job));
                                }
                            }
                        }
                        Err(error) => tracing::warn!(%error, "job poll failed"),
                    }
                    polling = Box::pin(self.poll_after(self.settings.poll_interval));
                }
            }
        }
    }

    async fn execute(&self, job: Job) {
        let job_id = job.uuid;
        if !self.active.lock().await.contains(&job_id) {
            return;
        }
        let result = match self.agentd.prepare(job.clone()).await {
            Ok(()) => match self.download_inputs(job_id).await {
                Ok(()) => self.agentd.start(job_id).await,
                Err(error) => Err(error),
            },
            Err(error) => Err(error),
        };
        match result {
            Ok(()) | Err(Error::JobRunning(_)) => return,
            Err(error) => tracing::error!(%job_id, %error, "job could not start"),
        }

        loop {
            if !self.active.lock().await.contains(&job_id) {
                return;
            }
            match self.agentd.fail(job.clone()).await {
                Ok(()) => return,
                Err(error) => {
                    tracing::error!(%job_id, %error, "job failure could not be persisted");
                }
            }
            sleep(self.settings.poll_interval).await;
        }
    }

    async fn download_inputs(&self, job_id: Uuid) -> Result<(), Error> {
        let job_fs = self.agentd.job_fs(job_id);
        let node = job_fs.node().await?;
        let job = job_fs.job().await?;
        loop {
            match self.filed.download_inputs(&job, &node).await {
                Err(Error::Http(error))
                    if job.expired_at.is_some_and(|at| at > jiff::Timestamp::now())
                        && (error.is_timeout()
                            || error.is_connect()
                            || error.is_body()
                            || error.status().is_some_and(|s| {
                                s.is_server_error() || s == reqwest::StatusCode::TOO_MANY_REQUESTS
                            })) =>
                {
                    tracing::warn!(%job_id, %error, "input download failed; retrying");
                    sleep(self.settings.poll_interval).await;
                }
                result => return result,
            }
        }
    }

    async fn poll_results(&self) -> Result<(), Error> {
        loop {
            {
                let _reporting = self.reporting.lock().await;
                match self.agentd.poll_results().await {
                    Ok(results) => {
                        for result in results {
                            if let Err(error) = self.report_result(&result).await {
                                tracing::warn!(job_id = %result.job_id, %error, "job result report failed");
                            }
                        }
                    }
                    Err(error) => tracing::warn!(%error, "job result poll failed"),
                }
            }
            sleep(self.settings.poll_interval).await;
        }
    }

    async fn report_result(&self, result: &AgentResult) -> Result<(), Error> {
        let mut outcome = result.outcome;
        let mut lost = false;
        if outcome == JobOutcome::Succeeded {
            match self
                .filed
                .upload_outputs(
                    &self.agentd.job_fs(result.job_id).job().await?,
                    &self.agentd.job_fs(result.job_id).node().await?,
                )
                .await
            {
                Ok(()) => {}
                Err(Error::ClaimLost(_)) => lost = true,
                Err(Error::Artifact(message)) => {
                    tracing::error!(job_id = %result.job_id, %message, "job outputs are invalid");
                    self.agentd.fail_artifacts(result.job_id).await?;
                    outcome = JobOutcome::Failed;
                }
                Err(error) => return Err(error),
            }
        }
        match if lost {
            Err(Error::ClaimLost(result.job_id))
        } else {
            self.report(result.job_id, outcome).await
        } {
            Ok(()) | Err(Error::ClaimLost(_)) => {}
            Err(error) => return Err(error),
        }
        self.agentd.acknowledge_result(result.job_id).await?;
        self.active.lock().await.remove(&result.job_id);
        Ok(())
    }

    async fn report(&self, job_id: Uuid, outcome: JobOutcome) -> Result<(), Error> {
        match outcome {
            JobOutcome::Succeeded => self.client.succeed(self.settings.id, job_id).await,
            JobOutcome::Failed => self.client.fail(self.settings.id, job_id).await,
            JobOutcome::Yielded => self.client.yield_job(self.settings.id, job_id).await,
            JobOutcome::Terminated => self.client.terminate(self.settings.id, job_id).await,
        }
    }

    async fn watch(&self) -> Result<(), Error> {
        loop {
            match self.agentd.watch().await {
                Ok(()) => return Ok(()),
                Err(error) => tracing::warn!(%error, "container watcher failed"),
            }
            sleep(self.settings.poll_interval).await;
        }
    }
}

struct RunningGuard<'a> {
    running: &'a AtomicBool,
    stopped: &'a Notify,
}

impl Drop for RunningGuard<'_> {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Release);
        self.stopped.notify_waiters();
    }
}
