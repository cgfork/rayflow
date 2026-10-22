//! Direct job polling for agents with resource-aware claims.

use crate::{Error, Job, Scheduler, actuator::Status};
use event::{
    Emit, Read,
    task::Task,
    task_event::{TaskEvent, TaskEventEntry},
};
use jiff::Timestamp;
use std::time::Duration;
use toasty::{Executor, stmt::IntoStatement};
use uuid::Uuid;

/// Default duration before an abandoned agent claim becomes available again.
pub const DEFAULT_JOB_LEASE: Duration = Duration::from_secs(30);
const CANDIDATE_PAGE_SIZE: usize = 128;

/// Total resource budget for this agent's jobs, including existing claims.
/// Each agent identity must poll sequentially to avoid overlapping budgets.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ResourceCapacity {
    pub cpu_cores: u32,
    pub memory_bytes: u64,
}

/// Polls jobs directly instead of assigning them to per-agent queues.
pub struct AgentScheduler {
    lease: Duration,
}

impl AgentScheduler {
    pub fn new() -> Self {
        Self {
            lease: DEFAULT_JOB_LEASE,
        }
    }

    /// Sets how long a job remains claimed without a poll from its agent.
    pub fn with_lease(mut self, lease: Duration) -> Self {
        assert!(
            lease.as_micros() > 0 && lease.as_micros() <= i64::MAX as u128,
            "job lease must fit in a positive signed microsecond count"
        );
        self.lease = lease;
        self
    }
}

/// Renews eligible owned claims, returns unstarted claims, and reports reserved resources.
pub async fn poll_polled<B: Executor>(
    backend: &mut B,
    agent_id: Uuid,
    active_jobs: &[Uuid],
    maximum: usize,
    lease: Duration,
) -> Result<(Vec<Job>, ResourceCapacity), Error> {
    assert!(
        lease.as_micros() > 0 && lease.as_micros() <= i64::MAX as u128,
        "job lease must fit in a positive signed microsecond count"
    );

    let mut tx = backend.transaction().await?;
    let now = Timestamp::now();
    let cutoff = now - lease;
    reclaim_expired_in(&mut tx, cutoff).await?;
    let active = Job::all()
        .filter(Job::fields().agent_id().eq(Some(agent_id)))
        .filter(
            Job::fields()
                .status()
                .in_list(vec![Status::Polled, Status::Pending]),
        )
        .order_by(Job::fields().id().asc())
        .exec(&mut tx)
        .await?;
    let mut reserved = ResourceCapacity::default();
    let mut claimed = Vec::new();
    for mut job in active {
        if job.status == Status::Pending && !active_jobs.contains(&job.uuid) {
            reserved.cpu_cores = reserved.cpu_cores.saturating_add(job.required_cpu_cores);
            reserved.memory_bytes = reserved
                .memory_bytes
                .saturating_add(job.required_memory_bytes);
            continue;
        }
        // Renew by primary key: a bulk update for a new agent would lock
        // the empty secondary-index range and conflict with other claimers.
        if update_if(
            &mut tx,
            Job::filter_by_id(job.id)
                .update()
                .polled_at(Some(now))
                .updated_at(now),
            Job::fields()
                .status()
                .in_list(vec![Status::Polled, Status::Pending])
                .and(Job::fields().agent_id().eq(Some(agent_id)))
                .and(Job::fields().polled_at().gt(Some(cutoff))),
        )
        .await?
        {
            reserved.cpu_cores = reserved.cpu_cores.saturating_add(job.required_cpu_cores);
            reserved.memory_bytes = reserved
                .memory_bytes
                .saturating_add(job.required_memory_bytes);
            if job.status == Status::Polled && claimed.len() < maximum {
                job.polled_at = Some(now);
                job.updated_at = now;
                claimed.push(job);
            }
        }
    }
    tx.commit().await?;
    Ok((claimed, reserved))
}

/// Claims scheduled jobs that fit within `capacity` for this agent.
pub async fn poll_scheduled<B: Executor>(
    backend: &mut B,
    agent_id: Uuid,
    capacity: ResourceCapacity,
    maximum: usize,
) -> Result<Vec<Job>, Error> {
    let mut tx = backend.transaction().await?;
    let now = Timestamp::now();
    let mut remaining = capacity;
    let mut claimed = Vec::new();
    let mut after_id = 0;

    while claimed.len() < maximum {
        let candidates = Job::all()
            .filter(Job::fields().status().eq(Status::Scheduled))
            .filter(Job::fields().required_cpu_cores().le(remaining.cpu_cores))
            .filter(
                Job::fields()
                    .required_memory_bytes()
                    .le(remaining.memory_bytes),
            )
            .filter(Job::fields().id().gt(after_id))
            .order_by(Job::fields().id().asc())
            .limit(CANDIDATE_PAGE_SIZE)
            .exec(&mut tx)
            .await?;
        if candidates.is_empty() {
            break;
        }

        let page_ended = candidates.len() < CANDIDATE_PAGE_SIZE;
        for mut job in candidates {
            after_id = job.id;
            if job.required_cpu_cores > remaining.cpu_cores
                || job.required_memory_bytes > remaining.memory_bytes
            {
                continue;
            }
            let updated = update_if(
                &mut tx,
                Job::filter_by_id(job.id)
                    .update()
                    .status(Status::Polled)
                    .agent_id(Some(agent_id))
                    .polled_at(Some(now))
                    .updated_at(now),
                Job::fields()
                    .status()
                    .eq(Status::Scheduled)
                    .and(Job::fields().required_cpu_cores().le(remaining.cpu_cores))
                    .and(
                        Job::fields()
                            .required_memory_bytes()
                            .le(remaining.memory_bytes),
                    ),
            )
            .await?;
            if updated {
                job.status = Status::Polled;
                job.agent_id = Some(agent_id);
                job.polled_at = Some(now);
                job.updated_at = now;
                remaining.cpu_cores -= job.required_cpu_cores;
                remaining.memory_bytes -= job.required_memory_bytes;
                claimed.push(job);
                if claimed.len() == maximum {
                    break;
                }
            }
        }
        if page_ended {
            break;
        }
    }

    tx.commit().await?;
    Ok(claimed)
}

/// Releases agent claims whose lease expired at or before `cutoff`.
pub async fn reclaim_expired_in<B: Executor>(
    backend: &mut B,
    cutoff: Timestamp,
) -> Result<usize, Error> {
    let stale = Job::all()
        .filter(
            Job::fields()
                .status()
                .in_list(vec![Status::Polled, Status::Pending]),
        )
        .filter(Job::fields().agent_id().is_some())
        .filter(Job::fields().polled_at().le(Some(cutoff)))
        .order_by(Job::fields().id().asc())
        .exec(&mut *backend)
        .await?;
    let mut reclaimed = 0;
    let now = Timestamp::now();
    for job in stale {
        let updated = update_if(
            &mut *backend,
            Job::filter_by_id(job.id)
                .update()
                .status(Status::Scheduled)
                .agent_id(None)
                .polled_at(None)
                .updated_at(now),
            Job::fields()
                .status()
                .in_list(vec![Status::Polled, Status::Pending])
                .and(Job::fields().agent_id().is_some())
                .and(Job::fields().polled_at().le(Some(cutoff))),
        )
        .await?;
        reclaimed += usize::from(updated);
    }
    Ok(reclaimed)
}

impl Default for AgentScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl<B: Executor> Scheduler<B> for AgentScheduler {
    type Err = Error;

    /// Releases expired agent claims. Agents claim matching jobs directly by polling.
    async fn schedule(&self, backend: &mut B) -> Result<usize, Self::Err> {
        let cutoff = Timestamp::now() - self.lease;
        reclaim_expired_in(backend, cutoff).await
    }
}

/// Records that an agent has started executing its claimed job.
pub async fn pend_job<B: Executor>(
    backend: &mut B,
    job_id: Uuid,
    agent_id: Uuid,
    lease: Duration,
) -> Result<(), Error> {
    update_agent_job(
        backend,
        job_id,
        agent_id,
        lease,
        Status::Pending,
        TaskEvent::Pend,
    )
    .await
}

/// Records successful completion of an agent's claimed job.
pub async fn succeed_job<B: Executor>(
    backend: &mut B,
    job_id: Uuid,
    agent_id: Uuid,
    lease: Duration,
) -> Result<(), Error> {
    update_agent_job(
        backend,
        job_id,
        agent_id,
        lease,
        Status::Succeeded,
        TaskEvent::Succeed,
    )
    .await
}

/// Records failure of an agent's claimed job.
pub async fn fail_job<B: Executor>(
    backend: &mut B,
    job_id: Uuid,
    agent_id: Uuid,
    lease: Duration,
) -> Result<(), Error> {
    update_agent_job(
        backend,
        job_id,
        agent_id,
        lease,
        Status::Failed,
        TaskEvent::Fail,
    )
    .await
}

/// Records termination of an agent's claimed job.
pub async fn terminate_job<B: Executor>(
    backend: &mut B,
    job_id: Uuid,
    agent_id: Uuid,
    lease: Duration,
) -> Result<(), Error> {
    update_agent_job(
        backend,
        job_id,
        agent_id,
        lease,
        Status::Terminated,
        TaskEvent::Terminate,
    )
    .await
}

/// Cancels a live job and records the matching task event atomically.
pub async fn cancel_job<B: Executor>(backend: &mut B, job_id: Uuid) -> Result<(), Error> {
    let mut tx = backend.transaction().await?;
    let job = Job::filter_by_uuid(job_id)
        .first()
        .exec(&mut tx)
        .await?
        .ok_or(Error::AgentJobNotFound(job_id))?;
    if job.status == Status::Terminated {
        let already_cancelled = TaskEventEntry::filter_by_task_id(job.task_id)
            .filter(TaskEventEntry::fields().event().eq(TaskEvent::Cancel))
            .first()
            .exec(&mut tx)
            .await?
            .is_some();
        if already_cancelled {
            tx.commit().await?;
            return Ok(());
        }
    }
    if job.status.is_terminal() {
        return Err(Error::JobConflict(job_id));
    }
    let now = Timestamp::now();
    let updated = update_if(
        &mut tx,
        Job::filter_by_id(job.id)
            .update()
            .status(Status::Terminated)
            .agent_id(None)
            .polled_at(None)
            .updated_at(now),
        Job::fields()
            .status()
            .in_list(vec![Status::Scheduled, Status::Polled, Status::Pending]),
    )
    .await?;
    if !updated {
        return Err(Error::JobConflict(job_id));
    }
    let task = Task::read(&mut tx, job.task_id).await?;
    TaskEventEntry::new(&task, TaskEvent::Cancel)
        .emit(&mut tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

async fn update_agent_job<B: Executor>(
    backend: &mut B,
    job_id: Uuid,
    agent_id: Uuid,
    lease: Duration,
    status: Status,
    event: TaskEvent,
) -> Result<(), Error> {
    assert!(
        lease.as_micros() > 0 && lease.as_micros() <= i64::MAX as u128,
        "job lease must fit in a positive signed microsecond count"
    );
    let mut tx = backend.transaction().await?;
    let job = Job::filter_by_uuid(job_id)
        .first()
        .exec(&mut tx)
        .await?
        .ok_or(Error::AgentJobNotFound(job_id))?;
    if job.status == status && status.is_terminal() {
        tx.commit().await?;
        return Ok(());
    }
    if !matches!(job.status, Status::Polled | Status::Pending) || job.agent_id != Some(agent_id) {
        return Err(Error::JobConflict(job_id));
    }
    let now = Timestamp::now();
    let cutoff = now - lease;
    if job.polled_at.is_none_or(|at| at <= cutoff) {
        return Err(Error::JobConflict(job_id));
    }
    let terminal = status.is_terminal();
    let expected_status = if terminal {
        Job::fields()
            .status()
            .in_list(vec![Status::Polled, Status::Pending])
    } else {
        Job::fields().status().eq(job.status)
    };
    let mut update = Job::filter_by_id(job.id)
        .update()
        .status(status)
        .agent_id(if terminal { None } else { Some(agent_id) })
        .polled_at(if terminal { None } else { Some(now) })
        .updated_at(now)
        .into_statement()
        .into_untyped();
    update.condition_mut_unwrap().expr = Some(
        expected_status
            .and(Job::fields().agent_id().eq(Some(agent_id)))
            .and(Job::fields().polled_at().gt(Some(cutoff)))
            .into(),
    );
    toasty::Statement::<()>::from_untyped_stmt(update)
        .exec(&mut tx)
        .await
        .map_err(|error| {
            if error.is_condition_failed() || error.is_record_not_found() {
                Error::JobConflict(job_id)
            } else {
                error.into()
            }
        })?;

    if job.status != status {
        let pending_already_emitted = status == Status::Pending
            && TaskEventEntry::filter_by_task_id(job.task_id)
                .filter(TaskEventEntry::fields().event().eq(TaskEvent::Pend))
                .first()
                .exec(&mut tx)
                .await?
                .is_some();
        if !pending_already_emitted {
            let task = Task::read(&mut tx, job.task_id).await?;
            TaskEventEntry::new(&task, event).emit(&mut tx).await?;
        }
    }
    tx.commit().await?;
    Ok(())
}

/// Executes a guarded ORM update and reports whether it changed the row.
async fn update_if<B: Executor>(
    backend: &mut B,
    update: impl IntoStatement<Returning = ()>,
    condition: toasty::stmt::Expr<bool>,
) -> Result<bool, Error> {
    let mut statement = update.into_statement().into_untyped();
    statement.condition_mut_unwrap().expr = Some(condition.into());
    match toasty::Statement::<()>::from_untyped_stmt(statement)
        .exec(backend)
        .await
    {
        Ok(()) => Ok(true),
        Err(error) if error.is_condition_failed() || error.is_record_not_found() => Ok(false),
        Err(error) => Err(error.into()),
    }
}
