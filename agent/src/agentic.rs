//! Polls the server for jobs and runs the local job manager.

use std::{
    collections::HashSet,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use futures_util::{StreamExt, stream::FuturesUnordered};
use graph::{GraphStore, types::DAG};
use scheduler::{GpuChoice, GpuDevice, GpuOffers, Job};
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

    async fn poll_after(
        &self,
        delay: Duration,
        gpu_choices: Vec<GpuChoice>,
    ) -> Result<PollResponse, Error> {
        if !delay.is_zero() {
            sleep(delay).await;
        }
        let mut request =
            PollRequest::from_settings(self.manager.settings(), self.manager.active_job_ids());
        if !gpu_choices.is_empty() {
            request.wait_ms = Some(0);
            request.gpu_choices = gpu_choices;
        }
        self.manager.client().poll(&request).await
    }

    async fn poll_jobs(&self, recovered: Vec<Job>) -> Result<(), Error> {
        let mut starting = FuturesUnordered::new();
        for job in recovered {
            starting.push(self.manager.start(job));
        }
        let settings = self.manager.settings();
        let mut confirming_gpu = false;
        let mut polling = Box::pin(self.poll_after(Duration::ZERO, Vec::new()));
        loop {
            tokio::select! {
                _ = starting.next(), if !starting.is_empty() => {}
                response = &mut polling => {
                    let mut choices = Vec::new();
                    match response {
                        Ok(response) => {
                            // Retry at the ordinary poll interval if none of the choices
                            // came back as committed GPU jobs.
                            let gpu_confirmed = response
                                .jobs
                                .iter()
                                .any(|job| job.required_gpu_memory_bytes > 0);
                            if (!confirming_gpu || gpu_confirmed) && let Some(offers) = response.gpu_offers {
                                choices = choose_gpu_devices(offers, &settings.gpu_devices, settings.batch);
                            }
                            for job in response.jobs {
                                starting.push(self.manager.start(job));
                            }
                        }
                        Err(error) => tracing::warn!(%error, "job poll failed"),
                    }
                    confirming_gpu = !choices.is_empty();
                    let delay = if confirming_gpu {
                        Duration::ZERO
                    } else {
                        settings.poll_interval
                    };
                    polling = Box::pin(self.poll_after(delay, choices));
                }
            }
        }
    }
}

fn choose_gpu_devices(
    offers: GpuOffers,
    configured: &[GpuDevice],
    maximum: usize,
) -> Vec<GpuChoice> {
    let mut available: Vec<_> = configured
        .iter()
        .filter_map(|device| {
            let offered = offers
                .devices
                .iter()
                .find(|offered| offered.cdi_device == device.cdi_device)?;
            Some((
                device.cdi_device.as_str(),
                device.memory_bytes.min(offered.memory_bytes),
            ))
        })
        .collect();
    let mut seen = HashSet::new();
    offers
        .jobs
        .into_iter()
        .filter(|job| job.required_gpu_memory_bytes > 0 && seen.insert(job.job_id))
        .filter_map(|job| {
            let (name, remaining) = available
                .iter_mut()
                .filter(|(_, remaining)| *remaining >= job.required_gpu_memory_bytes)
                .min_by_key(|(name, remaining)| (*remaining, *name))?;
            *remaining -= job.required_gpu_memory_bytes;
            Some(GpuChoice {
                job_id: job.job_id,
                cdi_device: (*name).to_owned(),
            })
        })
        .take(maximum)
        .collect()
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

#[cfg(test)]
mod tests {
    use scheduler::GpuOffer;

    use super::*;

    fn device(index: u8, memory_bytes: u64) -> GpuDevice {
        GpuDevice {
            cdi_device: format!("vendor.example/gpu={index}"),
            memory_bytes,
        }
    }

    fn job(index: u128, required_gpu_memory_bytes: u64) -> GpuOffer {
        GpuOffer {
            job_id: Uuid::from_u128(index),
            required_gpu_memory_bytes,
        }
    }

    #[test]
    fn shares_the_best_fit_then_chooses_another_device_with_enough_memory() {
        let configured = [device(1, 8), device(0, 8), device(2, 16)];
        let offers = GpuOffers {
            jobs: vec![job(1, 5), job(2, 3), job(3, 6), job(4, 9), job(5, 8)],
            devices: configured.to_vec(),
        };
        let choices: Vec<_> = choose_gpu_devices(offers, &configured, 5)
            .into_iter()
            .map(|choice| (choice.job_id.as_u128(), choice.cdi_device))
            .collect();

        assert_eq!(
            choices,
            vec![
                (1, device(0, 8).cdi_device),
                (2, device(0, 8).cdi_device),
                (3, device(1, 8).cdi_device),
                (4, device(2, 16).cdi_device),
            ]
        );
    }

    #[test]
    fn only_uses_remaining_memory_within_local_device_limits() {
        let configured = [device(0, 8), device(1, 16)];
        let offers = GpuOffers {
            jobs: vec![job(1, 9), job(2, 4), job(3, 8), job(4, 1)],
            devices: vec![device(2, 32), device(0, 32), device(1, 4)],
        };
        let choices: Vec<_> = choose_gpu_devices(offers, &configured, 4)
            .into_iter()
            .map(|choice| (choice.job_id.as_u128(), choice.cdi_device))
            .collect();

        assert_eq!(
            choices,
            vec![(2, device(1, 16).cdi_device), (3, device(0, 8).cdi_device)]
        );
    }
}
