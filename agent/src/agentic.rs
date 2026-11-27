//! Polls the server for jobs and runs the local job manager.

use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use futures_util::{StreamExt, stream::FuturesUnordered};
use graph::{GraphStore, types::DAG};
use scheduler::Job;
use tokio::{
    sync::{Notify, watch},
    time::sleep,
};
use uuid::Uuid;

use crate::{
    JobManager,
    client::{Client, PollRequest, PollResponse},
    containerd::{Containerd, docker::DockerContainerd},
    errors::Error,
    settings::Settings,
};

/// Polls sequentially and lets one manager run all claimed jobs.
pub struct Agentic<C = DockerContainerd, R = Client> {
    manager: JobManager<C, R>,
    stop: watch::Sender<bool>,
    running: AtomicBool,
    stopped: Notify,
}

impl Agentic {
    /// Opens the configured workdir with Docker and HTTP connections.
    pub async fn new(settings: Settings) -> Result<Self, Error> {
        let client = Client::new(settings)?;
        let docker = DockerContainerd::connect_with_defaults()?.with_gpu_cdi_devices(
            client
                .settings()
                .gpu_devices
                .iter()
                .map(|device| device.cdi_device.clone()),
        )?;
        let manager = JobManager::open(docker, client.clone(), client).await?;
        Ok(Self::with_manager(manager))
    }
}

impl<C, R> Agentic<C, R>
where
    C: Containerd,
    R: GraphStore<Uuid, G = DAG> + Clone + Send + Sync,
{
    pub fn with_manager(manager: JobManager<C, R>) -> Self {
        Self {
            manager,
            stop: watch::channel(false).0,
            running: AtomicBool::new(false),
            stopped: Notify::new(),
        }
    }

    /// Runs polling and both monitors until shutdown. Dropping this future
    /// leaves running containers and their workdirs available for recovery.
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
        let recovered = self.manager.recover().await?;
        tokio::select! {
            result = self.poll_jobs(recovered) => result,
            result = self.manager.start_lifecycle() => result,
            result = self.manager.start_usage() => result,
        }
    }

    /// Stops polling and waits for the monitors and local start futures.
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
        let settings = self.manager.settings();
        self.manager
            .client()
            .poll(&PollRequest::from_settings(
                settings,
                self.manager.active_job_ids(),
            ))
            .await
    }

    async fn poll_jobs(&self, recovered: Vec<Job>) -> Result<(), Error> {
        let mut starting = FuturesUnordered::new();
        for job in recovered {
            starting.push(self.manager.start(job));
        }
        let mut polling = Box::pin(self.poll_after(Duration::ZERO));
        loop {
            tokio::select! {
                _ = starting.next(), if !starting.is_empty() => {}
                response = &mut polling => {
                    match response {
                        Ok(response) => {
                            for job in response.jobs {
                                starting.push(self.manager.start(job));
                            }
                        }
                        Err(error) => tracing::warn!(%error, "job poll failed"),
                    }
                    polling = Box::pin(self.poll_after(self.manager.settings().poll_interval));
                }
            }
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
