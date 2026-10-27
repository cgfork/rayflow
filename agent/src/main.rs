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
    time::{Instant, sleep_until, timeout},
};

struct ActiveJob {
    resources: ResourceCapacity,
    lease: Duration,
    deadline: watch::Sender<Option<Instant>>,
}

fn renew_active(
    active: &HashMap<uuid::Uuid, ActiveJob>,
    requested: &[uuid::Uuid],
    renewed: &[uuid::Uuid],
    started: Instant,
) {
    for id in requested {
        if let Some(job) = active.get(id) {
            let valid = job
                .deadline
                .borrow()
                .is_some_and(|deadline| deadline > Instant::now());
            let deadline = (valid && renewed.contains(id)).then_some(started + job.lease);
            job.deadline.send_replace(deadline);
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let config = config::Config::load()?;
    let client = client::Client::new(&config)?;
    let mut resources = resources::Resources::new(&config);
    let mut next_poll = Instant::now();
    let mut running = JoinSet::new();
    let mut active = HashMap::new();
    let (shutdown, _) = watch::channel(false);

    'polling: loop {
        let reserved =
            active
                .values()
                .fold(ResourceCapacity::default(), |mut sum, value: &ActiveJob| {
                    sum.cpu_cores = sum.cpu_cores.saturating_add(value.resources.cpu_cores);
                    sum.memory_bytes = sum
                        .memory_bytes
                        .saturating_add(value.resources.memory_bytes);
                    sum
                });
        let active_jobs = active.keys().copied().collect::<Vec<_>>();
        let scheduled = next_poll;
        let poll = async {
            sleep_until(scheduled).await;
            let capacity = resources.sample(reserved).await;
            let started = Instant::now();
            (
                started,
                capacity,
                client.poll(&config, capacity, &active_jobs).await,
            )
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
                    let (started, capacity, result) = polled;
                    next_poll = Instant::now() + active.values().fold(
                        Duration::from_millis(config.poll_interval_ms),
                        |interval, job| interval.min((job.lease / 3).max(Duration::from_millis(1))),
                    );
                    let polled = match result {
                        Ok(polled) => polled,
                        Err(error) => {
                            eprintln!("agent: poll failed: {error}");
                            continue 'polling;
                        }
                    };
                    renew_active(&active, &active_jobs, &polled.renewed_jobs, started);
                    for job in polled.jobs {
                        let id = job.uuid;
                        if active.contains_key(&id) {
                            continue;
                        }
                        let lease = match job.job_lease() {
                            Ok(lease) => lease,
                            Err(error) => {
                                eprintln!("agent: invalid lease for job {id}: {error}");
                                continue;
                            }
                        };
                        let (deadline, receiver) = watch::channel(Some(started + lease));
                        next_poll = next_poll.min(Instant::now() + (lease / 3).max(Duration::from_millis(1)));
                        active.insert(id, ActiveJob {
                            resources: ResourceCapacity {
                                cpu_cores: job.required_cpu_cores,
                                memory_bytes: job.required_memory_bytes,
                            },
                            lease,
                            deadline,
                        });
                        let client = client.clone();
                        let config = config.clone();
                        let shutdown = shutdown.subscribe();
                        let context = executor::JobContext {
                            job,
                            agent: executor::Agent {
                                id: config.agent_id,
                                resources: capacity,
                            },
                        };
                        running.spawn(async move {
                            job::execute(client, config, context, receiver, shutdown).await;
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

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn only_confirmed_unexpired_requested_claims_are_renewed() {
        let started = Instant::now();
        let lease = Duration::from_secs(30);
        let confirmed = Uuid::new_v4();
        let missing = Uuid::new_v4();
        let expired = Uuid::new_v4();
        let unrequested = Uuid::new_v4();
        let mut active = HashMap::new();
        for id in [confirmed, missing, expired, unrequested] {
            let expires = if id == expired {
                started - Duration::from_secs(1)
            } else {
                started + Duration::from_secs(10)
            };
            let (deadline, _) = watch::channel(Some(expires));
            active.insert(
                id,
                ActiveJob {
                    resources: ResourceCapacity::default(),
                    lease,
                    deadline,
                },
            );
        }
        renew_active(
            &active,
            &[confirmed, missing, expired],
            &[confirmed, expired, unrequested],
            started,
        );
        assert_eq!(*active[&confirmed].deadline.borrow(), Some(started + lease));
        assert_eq!(*active[&missing].deadline.borrow(), None);
        assert_eq!(*active[&expired].deadline.borrow(), None);
        assert_eq!(
            *active[&unrequested].deadline.borrow(),
            Some(started + Duration::from_secs(10))
        );
        renew_active(&active, &[missing], &[missing], started);
        assert_eq!(*active[&missing].deadline.borrow(), None);
    }
}
