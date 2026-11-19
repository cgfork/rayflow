//! Direct job polling for agents with resource-aware claims.

use crate::{Error, Job, Scheduler, actuator::Status};
use event::{
    Emit, Read,
    task::Task,
    task_event::{TaskEvent, TaskEventEntry},
};
use jiff::Timestamp;
use std::time::Duration;
use toasty::{Executor, schema::Model, stmt::IntoStatement};
use uuid::Uuid;

const JOB_PAGE_SIZE: usize = 128;

/// Total resource budget for this agent's jobs, including existing claims.
/// Each agent identity must poll sequentially to avoid overlapping budgets.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ResourceCapacity {
    pub cpu_cores: u32,
    pub memory_bytes: u64,
}

impl ResourceCapacity {
    fn exhausted(self) -> bool {
        self.cpu_cores == 0 || self.memory_bytes == 0
    }
}

/// Polls jobs from an agent-facing scheduling backend.
#[allow(async_fn_in_trait)]
pub trait JobPoll<B> {
    type Err: std::error::Error + Send + Sync + 'static;

    async fn poll(
        &mut self,
        backend: &mut B,
        maximum: usize,
    ) -> Result<impl Iterator<Item = Job>, Self::Err>;
}

/// Reports terminal job results back to the scheduling backend.
#[allow(async_fn_in_trait)]
pub trait Report<B> {
    type Err: std::error::Error + Send + Sync + 'static;

    async fn apply(&self, backend: &mut B) -> Result<(), Self::Err>;
}

/// Agent identity, active executions, and remaining resource budget for one poll.
/// Construct a new reference with a fresh total budget for each poll request.
#[derive(Clone, Debug)]
pub struct AgentRef {
    id: Uuid,
    active_jobs: Vec<Uuid>,
    resource: ResourceCapacity,
}

impl AgentRef {
    pub fn new(id: Uuid, resource: ResourceCapacity, active_jobs: Vec<Uuid>) -> Self {
        Self {
            id,
            active_jobs,
            resource,
        }
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn resource(&self) -> ResourceCapacity {
        self.resource
    }

    pub fn active_jobs(&self) -> &[Uuid] {
        &self.active_jobs
    }

    /// Redelivers inactive Polled jobs and resets their lifetime on delivery.
    async fn polled<B: Executor>(
        &mut self,
        backend: &mut B,
        maximum: usize,
    ) -> Result<Vec<Job>, Error> {
        let now = Timestamp::now();
        let mut claimed = Vec::new();
        let mut after_id = 0;
        while claimed.len() < maximum && !self.resource.exhausted() {
            let page_size = JOB_PAGE_SIZE.min(maximum - claimed.len());
            let candidates = Job::all()
                .filter(Job::fields().agent_id().eq(Some(self.id)))
                .filter(Job::fields().status().eq(Status::Polled))
                .filter(Job::fields().expired_at().gt(Some(now)))
                .filter(Job::fields().uuid().in_list(self.active_jobs.clone()).not())
                .filter(
                    Job::fields()
                        .required_cpu_cores()
                        .le(self.resource.cpu_cores),
                )
                .filter(
                    Job::fields()
                        .required_memory_bytes()
                        .le(self.resource.memory_bytes),
                )
                .filter(Job::fields().id().gt(after_id))
                .include(Job::fields().task())
                .include(Job::fields().run())
                .include(Job::fields().node_run())
                .include(Job::fields().node_config())
                .include(Job::fields().run_config())
                .order_by(Job::fields().id().asc())
                .limit(page_size)
                .exec(&mut *backend)
                .await?;
            let page_ended = candidates.len() < page_size;
            for mut job in candidates {
                after_id = job.id;
                if job.required_cpu_cores > self.resource.cpu_cores
                    || job.required_memory_bytes > self.resource.memory_bytes
                {
                    continue;
                }
                let lifetime = job.job_lease()?;
                let expired_at = now
                    .checked_add(lifetime)
                    .map_err(|_| Error::InvalidJobLease(lifetime.as_millis() as u64))?;
                let updated = update_if(
                    &mut *backend,
                    Job::filter_by_id(job.id)
                        .update()
                        .polled_at(Some(now))
                        .expired_at(Some(expired_at))
                        .updated_at(now),
                    Job::fields()
                        .status()
                        .eq(Status::Polled)
                        .and(Job::fields().agent_id().eq(Some(self.id)))
                        .and(Job::fields().expired_at().eq(job.expired_at)),
                )
                .await?;
                if !updated {
                    continue;
                }
                job.polled_at = Some(now);
                job.expired_at = Some(expired_at);
                job.updated_at = now;
                self.resource.cpu_cores -= job.required_cpu_cores;
                self.resource.memory_bytes -= job.required_memory_bytes;
                claimed.push(job);
                if claimed.len() == maximum || self.resource.exhausted() {
                    break;
                }
            }
            if page_ended {
                break;
            }
        }
        Ok(claimed)
    }

    /// Deducts owned claims selected by `condition`, in bounded pages.
    async fn reserve_claims<B: Executor>(
        &mut self,
        backend: &mut B,
        condition: toasty::stmt::Expr<bool>,
    ) -> Result<(), Error> {
        let mut after_id = 0;
        while !self.resource.exhausted() {
            let active = Job::all()
                .filter(Job::fields().agent_id().eq(Some(self.id)))
                .filter(condition.clone())
                .filter(Job::fields().id().gt(after_id))
                .order_by(Job::fields().id().asc())
                .limit(JOB_PAGE_SIZE)
                .exec(&mut *backend)
                .await?;
            let page_ended = active.len() < JOB_PAGE_SIZE;
            for job in active {
                after_id = job.id;
                self.resource.cpu_cores = self
                    .resource
                    .cpu_cores
                    .saturating_sub(job.required_cpu_cores);
                self.resource.memory_bytes = self
                    .resource
                    .memory_bytes
                    .saturating_sub(job.required_memory_bytes);
            }
            if page_ended {
                break;
            }
        }
        Ok(())
    }

    /// Claims fitting Scheduled jobs from the remaining budget, excluding active jobs.
    async fn scheduled<B: Executor>(
        &mut self,
        backend: &mut B,
        maximum: usize,
    ) -> Result<Vec<Job>, Error> {
        if maximum == 0 {
            return Ok(Vec::new());
        }
        let now = Timestamp::now();
        let mut claimed = Vec::new();
        let mut after_id = 0;
        let table = backend
            .schema()
            .table_for(Job::id())
            .name
            .replace('`', "``");

        while claimed.len() < maximum && !self.resource.exhausted() {
            let page_size = JOB_PAGE_SIZE.min(maximum - claimed.len());
            // Toasty cannot express SKIP LOCKED. Lock only the queue rows we may
            // claim, so other agents can move past an in-flight claim.
            let ids: Vec<u64> = toasty::sql::query(format!(
                "SELECT id FROM `{table}` WHERE status = 'Scheduled' AND id > ? \
                 AND required_cpu_cores <= ? AND required_memory_bytes <= ? \
                 ORDER BY id ASC LIMIT ? FOR UPDATE SKIP LOCKED",
            ))
            .bind(after_id)
            .bind(self.resource.cpu_cores)
            .bind(self.resource.memory_bytes)
            .bind(page_size as u64)
            .column_types([toasty::stmt::Type::U64])
            .exec(&mut *backend)
            .await?
            .into_iter()
            .map(|row| row.into_record()[0].clone().try_into())
            .collect::<Result<_, toasty::Error>>()?;
            if ids.is_empty() {
                break;
            }
            let page_ended = ids.len() < page_size;
            after_id = *ids.last().expect("nonempty locked page");
            let candidates = Job::all()
                .filter(Job::fields().id().in_list(ids))
                .filter(Job::fields().uuid().in_list(self.active_jobs.clone()).not())
                .include(Job::fields().task())
                .include(Job::fields().run())
                .include(Job::fields().node_run())
                .include(Job::fields().node_config())
                .include(Job::fields().run_config())
                .order_by(Job::fields().id().asc())
                .exec(&mut *backend)
                .await?;
            for mut job in candidates {
                if job.required_cpu_cores > self.resource.cpu_cores
                    || job.required_memory_bytes > self.resource.memory_bytes
                {
                    continue;
                }
                let lifetime = job.job_lease()?;
                let expired_at = now
                    .checked_add(lifetime)
                    .map_err(|_| Error::InvalidJobLease(lifetime.as_millis() as u64))?;
                let updated = update_if(
                    &mut *backend,
                    Job::filter_by_id(job.id)
                        .update()
                        .status(Status::Polled)
                        .agent_id(Some(self.id))
                        .polled_at(Some(now))
                        .expired_at(Some(expired_at))
                        .updated_at(now),
                    Job::fields()
                        .status()
                        .eq(Status::Scheduled)
                        .and(
                            Job::fields()
                                .required_cpu_cores()
                                .le(self.resource.cpu_cores),
                        )
                        .and(
                            Job::fields()
                                .required_memory_bytes()
                                .le(self.resource.memory_bytes),
                        ),
                )
                .await?;
                if updated {
                    job.status = Status::Polled;
                    job.agent_id = Some(self.id);
                    job.polled_at = Some(now);
                    job.expired_at = Some(expired_at);
                    job.updated_at = now;
                    self.resource.cpu_cores -= job.required_cpu_cores;
                    self.resource.memory_bytes -= job.required_memory_bytes;
                    claimed.push(job);
                    if claimed.len() == maximum || self.resource.exhausted() {
                        break;
                    }
                }
            }
            if page_ended {
                break;
            }
        }

        Ok(claimed)
    }
}

impl<B: Executor> JobPoll<B> for AgentRef {
    type Err = Error;

    /// Claims jobs atomically, then stores the remaining resource budget.
    /// Poll sequentially per agent ID with a fresh resource budget per request.
    async fn poll(
        &mut self,
        backend: &mut B,
        maximum: usize,
    ) -> Result<impl Iterator<Item = Job>, Self::Err> {
        // Stage the budget so an error or cancelled poll leaves the caller unchanged.
        let mut agent = self.clone();
        let mut tx = backend.transaction().await?;
        if maximum > 0 {
            // Active Polled jobs consume resources without redelivery.
            agent
                .reserve_claims(
                    &mut tx,
                    Job::fields()
                        .status()
                        .eq(Status::Polled)
                        .and(Job::fields().uuid().in_list(agent.active_jobs.clone())),
                )
                .await?;
        }
        let mut claimed = agent.polled(&mut tx, maximum).await?;
        if claimed.len() < maximum && !agent.resource.exhausted() {
            // Redelivered jobs were already deducted. Reserve other unstarted claims
            // before claiming new work, including jobs outside the delivery limit.
            let accounted: Vec<_> = agent
                .active_jobs
                .iter()
                .copied()
                .chain(claimed.iter().map(|job| job.uuid))
                .collect();
            agent
                .reserve_claims(
                    &mut tx,
                    Job::fields()
                        .status()
                        .eq(Status::Polled)
                        .and(Job::fields().uuid().in_list(accounted).not()),
                )
                .await?;
            claimed.extend(agent.scheduled(&mut tx, maximum - claimed.len()).await?);
        }
        tx.commit().await?;
        self.resource = agent.resource;
        Ok(claimed.into_iter())
    }
}

/// Fails timed-out jobs after allowing agents time to stop and report them.
pub struct AgentScheduler {
    timeout_gap: Duration,
}

impl AgentScheduler {
    pub fn new() -> Self {
        Self {
            timeout_gap: Duration::from_secs(10),
        }
    }

    pub fn with_timeout_gap(mut self, gap: Duration) -> Self {
        self.timeout_gap = gap;
        self
    }
}

/// Fails expired jobs and emits their task events in the same transaction.
pub async fn fail_expired_in<B: Executor>(backend: &mut B, gap: Duration) -> Result<usize, Error> {
    let now = Timestamp::now();
    let cutoff = now.checked_sub(gap).map_err(|_| Error::InvalidTimeoutGap)?;
    let mut tx = backend.transaction().await?;
    let expired = Job::all()
        .filter(Job::fields().status().eq(Status::Polled))
        .filter(Job::fields().expired_at().lt(Some(cutoff)))
        .order_by(Job::fields().id().asc())
        .exec(&mut tx)
        .await?;
    let mut failed = 0;
    for job in expired {
        let updated = update_if(
            &mut tx,
            Job::filter_by_id(job.id)
                .update()
                .status(Status::Failed)
                .agent_id(None)
                .polled_at(None)
                .updated_at(now),
            Job::fields()
                .status()
                .eq(Status::Polled)
                .and(Job::fields().expired_at().eq(job.expired_at)),
        )
        .await?;
        if updated {
            let task = Task::read(&mut tx, job.task_id).await?;
            TaskEventEntry::new(&task, TaskEvent::Fail)
                .emit(&mut tx)
                .await?;
            failed += 1;
        }
    }
    tx.commit().await?;
    Ok(failed)
}

impl Default for AgentScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl<B: Executor> Scheduler<B> for AgentScheduler {
    type Err = Error;

    /// Fails expired jobs after the configured grace period.
    async fn schedule(&self, backend: &mut B) -> Result<usize, Self::Err> {
        fail_expired_in(backend, self.timeout_gap).await
    }
}

/// Records cancellation of an agent's claimed job.
pub async fn cancel_job<B: Executor>(
    backend: &mut B,
    job_id: Uuid,
    agent_id: Uuid,
) -> Result<(), Error> {
    update_agent_job(
        backend,
        job_id,
        agent_id,
        Status::Terminated,
        TaskEvent::Cancel,
    )
    .await
}

pub async fn yield_job<B: Executor>(
    backend: &mut B,
    job_id: Uuid,
    agent_id: Uuid,
) -> Result<(), Error> {
    update_agent_job(backend, job_id, agent_id, Status::Yielded, TaskEvent::Yield).await
}

/// Records successful completion of an agent's claimed job.
pub async fn succeed_job<B: Executor>(
    backend: &mut B,
    job_id: Uuid,
    agent_id: Uuid,
) -> Result<(), Error> {
    update_agent_job(
        backend,
        job_id,
        agent_id,
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
) -> Result<(), Error> {
    update_agent_job(backend, job_id, agent_id, Status::Failed, TaskEvent::Fail).await
}

/// Records termination of an agent's claimed job.
pub async fn terminate_job<B: Executor>(
    backend: &mut B,
    job_id: Uuid,
    agent_id: Uuid,
) -> Result<(), Error> {
    update_agent_job(
        backend,
        job_id,
        agent_id,
        Status::Terminated,
        TaskEvent::Terminate,
    )
    .await
}

async fn update_agent_job<B: Executor>(
    backend: &mut B,
    job_id: Uuid,
    agent_id: Uuid,
    status: Status,
    event: TaskEvent,
) -> Result<(), Error> {
    let mut tx = backend.transaction().await?;
    let job = Job::filter_by_uuid(job_id)
        .first()
        .exec(&mut tx)
        .await?
        .ok_or(Error::AgentJobNotFound(job_id))?;
    if job.status == status {
        tx.commit().await?;
        return Ok(());
    }
    if job.status != Status::Polled || job.agent_id != Some(agent_id) {
        return Err(Error::JobConflict(job_id));
    }
    let now = Timestamp::now();
    if status != Status::Failed && job.expired_at.is_none_or(|at| at <= now) {
        return Err(Error::JobConflict(job_id));
    }
    let updated = update_if(
        &mut tx,
        Job::filter_by_id(job.id)
            .update()
            .status(status)
            .agent_id(None)
            .polled_at(None)
            .updated_at(now),
        Job::fields()
            .status()
            .eq(Status::Polled)
            .and(Job::fields().agent_id().eq(Some(agent_id))),
    )
    .await?;
    if !updated {
        return Err(Error::JobConflict(job_id));
    }

    let task = Task::read(&mut tx, job.task_id).await?;
    TaskEventEntry::new(&task, event).emit(&mut tx).await?;
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
