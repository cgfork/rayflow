//! Samples resources, polls matching jobs, executes them and reports their status.
mod client;
mod config;
mod job;
mod resources;

use scheduler::ResourceCapacity;
use std::{collections::HashMap, error::Error, time::Duration};
use tokio::{
    sync::watch,
    task::JoinSet,
    time::{Instant, MissedTickBehavior, interval, timeout},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let config = config::Config::load()?;
    let client = client::Client::new(&config)?;
    let mut resources = resources::Resources::new(&config);
    let mut ticker = interval(Duration::from_millis(config.poll_interval_ms));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut running = JoinSet::new();
    let mut active = HashMap::new();
    let (shutdown, _) = watch::channel(false);

    'polling: loop {
        let reserved = active.values().fold(
            ResourceCapacity::default(),
            |mut sum, value: &ResourceCapacity| {
                sum.cpu_cores = sum.cpu_cores.saturating_add(value.cpu_cores);
                sum.memory_bytes = sum.memory_bytes.saturating_add(value.memory_bytes);
                sum
            },
        );
        let active_jobs = active.keys().copied().collect::<Vec<_>>();
        let poll = async {
            ticker.tick().await;
            let capacity = resources.sample(reserved).await;
            let started = Instant::now();
            (started, client.poll(&config, capacity, &active_jobs).await)
        };
        tokio::pin!(poll);
        // Preserve an in-flight poll when a job finishes. Cancelling it could
        // overlap the next request while the server is still committing claims.
        loop {
            tokio::select! {
                result = tokio::signal::ctrl_c() => {
                    result?;
                    shutdown.send_replace(true);
                    if timeout(Duration::from_millis(config.shutdown_timeout_ms), async {
                        while running.join_next().await.is_some() {}
                    }).await.is_err() {
                        running.abort_all();
                        while running.join_next().await.is_some() {}
                    }
                    break 'polling;
                }
                polled = &mut poll => {
                    let (started, result) = polled;
                    let jobs = match result {
                        Ok(jobs) => jobs,
                        Err(error) => {
                            eprintln!("agent: poll failed: {error}");
                            continue 'polling;
                        }
                    };
                    for job in jobs {
                        let id = job.job.uuid;
                        if active.contains_key(&id) {
                            continue;
                        }
                        active.insert(id, ResourceCapacity {
                            cpu_cores: job.job.required_cpu_cores,
                            memory_bytes: job.job.required_memory_bytes,
                        });
                        let client = client.clone();
                        let config = config.clone();
                        let shutdown = shutdown.subscribe();
                        running.spawn(async move {
                            job::execute(client, config, job, started, shutdown).await;
                            id
                        });
                    }
                    continue 'polling;
                }
                result = running.join_next(), if !running.is_empty() => {
                    match result {
                        Some(Ok(id)) => { active.remove(&id); }
                        Some(Err(error)) => return Err(error.into()),
                        None => {}
                    }
                }
            }
        }
    }
    Ok(())
}
