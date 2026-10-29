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

#[allow(async_fn_in_trait)]
pub trait JobPoll<B> {
    type Err: std::error::Error + Send + Sync + 'static;

    async fn poll(
        &mut self,
        backend: &mut B,
        maximum: usize,
    ) -> Result<impl Iterator<Item = Job>, Self::Err>;
}

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

        while claimed.len() < maximum && !self.resource.exhausted() {
            let page_size = JOB_PAGE_SIZE.min(maximum - claimed.len());
            let candidates = Job::all()
                .filter(Job::fields().status().eq(Status::Scheduled))
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
            if candidates.is_empty() {
                break;
            }

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        JobWrite,
        actuator::{NodeConfig, RunConfig},
    };
    use runtime::command::{NodeRun, Run};
    use std::time::Duration;

    async fn configured_task() -> Option<(toasty::Db, Run, NodeRun, Task, NodeConfig, RunConfig)> {
        let url = std::env::var("DATABASE_URL").ok()?;
        let mut db = toasty::Db::builder()
            .models(crate::models())
            .connect(&url)
            .await
            .unwrap();
        db.reset_db().await.unwrap();
        db.push_schema().await.unwrap();
        let now = Timestamp::now();
        let run = Run::create()
            .uuid(Uuid::new_v4())
            .data_id(Uuid::new_v4())
            .graph_id(Uuid::new_v4())
            .version(1)
            .revision(1)
            .created_at(now)
            .updated_at(now)
            .exec(&mut db)
            .await
            .unwrap();
        let node_run = NodeRun::create()
            .run_id(run.uuid)
            .data_id(run.data_id)
            .graph_id(run.graph_id)
            .node_id(Uuid::new_v4())
            .revision(1)
            .status("Running".to_owned())
            .created_at(now)
            .updated_at(now)
            .exec(&mut db)
            .await
            .unwrap();
        let task = Task::new(run.data_id, run.graph_id, node_run.node_id, node_run.id);
        task.emit(&mut db).await.unwrap();
        let config = NodeConfig::create()
            .node_id(task.node_id)
            .version(1)
            .annotation_match(String::new())
            .config(serde_json::json!({}))
            .lease(Some(30_000))
            .required_cpu_cores(0)
            .required_memory_bytes(0)
            .created_at(now)
            .updated_at(now)
            .created_by(String::new())
            .updated_by(String::new())
            .exec(&mut db)
            .await
            .unwrap();
        let run_config = RunConfig::create()
            .run_id(run.uuid)
            .revision(1)
            .annotation(toasty::stmt::Json(std::collections::HashMap::new()))
            .created_at(now)
            .updated_at(now)
            .created_by(String::new())
            .updated_by(String::new())
            .exec(&mut db)
            .await
            .unwrap();
        Some((db, run, node_run, task, config, run_config))
    }

    #[tokio::test]
    async fn timeout_uses_stored_expiration_and_gap_without_loading_configs() {
        let Some((mut db, run, _, task, config, run_config)) = configured_task().await else {
            return;
        };
        NodeConfig::filter_by_id(config.id)
            .update()
            .lease(None)
            .exec(&mut db)
            .await
            .unwrap();
        let now = Timestamp::now();
        let past = now - Duration::from_secs(60);
        let future = now + Duration::from_secs(3600);
        let owner = Some(Uuid::new_v4());
        let mut expected = Vec::new();
        for (status, agent_id, polled_at, expired_at, config_id, reclaimed) in [
            (
                Status::Polled,
                owner,
                Some(now),
                Some(past),
                config.id,
                true,
            ),
            (Status::Yielded, owner, Some(now), Some(past), 0, false),
            (Status::Polled, owner, None, Some(past), 0, true),
            (Status::Polled, owner, Some(now), Some(now), 0, false),
            (Status::Polled, owner, Some(past), Some(future), 0, false),
            (Status::Yielded, owner, Some(past), None, config.id, false),
            (Status::Polled, None, Some(past), Some(past), 0, true),
            (Status::Polled, owner, Some(past), None, 0, false),
            (Status::Scheduled, owner, Some(past), Some(past), 0, false),
            (Status::Succeeded, owner, Some(past), Some(past), 0, false),
            (Status::Failed, owner, Some(past), Some(past), 0, false),
            (Status::Terminated, owner, Some(past), Some(past), 0, false),
        ] {
            let mut job = Job::new(&task, run.uuid, config_id, run_config.id);
            job.status = status;
            job.agent_id = agent_id;
            job.polled_at = polled_at;
            job.expired_at = expired_at;
            job.write(&mut db).await.unwrap();
            let stored = Job::filter_by_uuid(job.uuid).get(&mut db).await.unwrap();
            expected.push((stored, reclaimed));
        }
        let gap = Duration::from_secs(10);
        assert_eq!(fail_expired_in(&mut db, gap).await.unwrap(), 3);
        for (before, reclaimed) in expected {
            let after = Job::filter_by_id(before.id).get(&mut db).await.unwrap();
            if reclaimed {
                assert_eq!(after.status, Status::Failed);
                assert!(after.agent_id.is_none());
                assert!(after.polled_at.is_none());
                assert_eq!(after.expired_at, before.expired_at);
            } else {
                assert_eq!(after.status, before.status);
                assert_eq!(after.agent_id, before.agent_id);
                assert_eq!(after.polled_at, before.polled_at);
                assert_eq!(after.expired_at, before.expired_at);
                assert_eq!(after.updated_at, before.updated_at);
            }
        }
        assert_eq!(fail_expired_in(&mut db, gap).await.unwrap(), 0);
        let events = TaskEventEntry::all().exec(&mut db).await.unwrap();
        assert_eq!(events.len(), 3);
        assert!(events.iter().all(|event| event.event == TaskEvent::Fail));
    }

    #[tokio::test]
    async fn timeout_event_failure_rolls_back_the_job_update() {
        let Some((mut db, run, _, task, config, run_config)) = configured_task().await else {
            return;
        };
        let mut orphan = Job::new(&task, run.uuid, config.id, run_config.id);
        orphan.task_id = Uuid::new_v4();
        orphan.status = Status::Polled;
        orphan.agent_id = Some(Uuid::new_v4());
        orphan.expired_at = Some(Timestamp::now() - Duration::from_secs(60));
        orphan.write(&mut db).await.unwrap();
        let before = Job::filter_by_uuid(orphan.uuid).get(&mut db).await.unwrap();
        assert!(fail_expired_in(&mut db, Duration::ZERO).await.is_err());
        let after = Job::filter_by_uuid(orphan.uuid).get(&mut db).await.unwrap();
        assert_eq!(after.status, Status::Polled);
        assert_eq!(after.agent_id, before.agent_id);
        assert_eq!(after.expired_at, before.expired_at);
        assert!(
            TaskEventEntry::all()
                .exec(&mut db)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn active_job_poll_does_not_change_any_lifetimes() {
        let Some((mut db, run, _, task, config, run_config)) = configured_task().await else {
            return;
        };
        let id = Uuid::new_v4();
        let other_id = Uuid::new_v4();
        let before = Timestamp::now() - Duration::from_secs(5);
        let mut active_jobs = Vec::new();
        let mut expected = Vec::new();
        for (status, owner, active) in [
            (Status::Polled, Some(id), true),
            (Status::Yielded, Some(id), true),
            (Status::Polled, Some(id), false),
            (Status::Yielded, Some(id), false),
            (Status::Polled, Some(other_id), true),
            (Status::Yielded, Some(other_id), true),
            (Status::Polled, None, true),
            (Status::Scheduled, Some(id), true),
            (Status::Succeeded, Some(id), true),
            (Status::Failed, Some(id), true),
            (Status::Terminated, Some(id), true),
        ] {
            let mut job = Job::new(&task, run.uuid, config.id, run_config.id);
            job.status = status;
            job.agent_id = owner;
            job.polled_at = Some(before);
            job.expired_at = Some(before + Duration::from_secs(30));
            job.updated_at = before;
            job.write(&mut db).await.unwrap();
            if active {
                active_jobs.push(job.uuid);
            }
            let stored = Job::filter_by_uuid(job.uuid).get(&mut db).await.unwrap();
            expected.push(stored);
        }
        assert_eq!(
            AgentRef::new(id, ResourceCapacity::default(), Vec::new())
                .poll(&mut db, 0)
                .await
                .unwrap()
                .count(),
            0
        );
        for before in &expected {
            let after = Job::filter_by_id(before.id).get(&mut db).await.unwrap();
            assert_eq!(after.polled_at, before.polled_at);
            assert_eq!(after.expired_at, before.expired_at);
            assert_eq!(after.updated_at, before.updated_at);
        }
        assert_eq!(
            AgentRef::new(id, ResourceCapacity::default(), active_jobs)
                .poll(&mut db, 0)
                .await
                .unwrap()
                .count(),
            0
        );
        for before in expected {
            let after = Job::filter_by_id(before.id).get(&mut db).await.unwrap();
            assert_eq!(after.status, before.status);
            assert_eq!(after.agent_id, before.agent_id);
            assert_eq!(after.polled_at, before.polled_at);
            assert_eq!(after.expired_at, before.expired_at);
            assert_eq!(after.updated_at, before.updated_at);
        }
    }

    #[tokio::test]
    async fn poll_resets_only_redelivered_polled_jobs() {
        let Some((mut db, run, _, task, config, run_config)) = configured_task().await else {
            return;
        };
        let id = Uuid::new_v4();
        let mut owned = Vec::new();
        let mut untouched = Vec::new();
        for (status, owner, required) in [
            (Status::Polled, id, 1),
            (Status::Polled, id, 1),
            (Status::Polled, id, 4),
            (Status::Polled, id, 1),
            (Status::Yielded, id, 0),
            (Status::Polled, Uuid::new_v4(), 0),
        ] {
            let mut job = Job::new(&task, run.uuid, config.id, run_config.id);
            job.status = status;
            job.agent_id = Some(owner);
            job.polled_at = Some(Timestamp::now() - Duration::from_secs(5));
            job.required_cpu_cores = required;
            job.required_memory_bytes = u64::from(required);
            job.write(&mut db).await.unwrap();
            let stored = Job::filter_by_uuid(job.uuid).get(&mut db).await.unwrap();
            if status == Status::Polled && owner == id {
                owned.push(stored);
            } else {
                untouched.push(stored);
            }
        }
        for (maximum, cpu_cores, memory_bytes, delivered) in [
            (0, 4, 4, 0),
            (1, 4, 4, 1),
            (10, 2, 2, 1),
            (10, 0, 4, 0),
            (10, 4, 0, 0),
        ] {
            let before = Timestamp::now() - Duration::from_secs(5);
            for job in &owned {
                Job::filter_by_id(job.id)
                    .update()
                    .polled_at(Some(before))
                    .expired_at(Some(before + Duration::from_secs(30)))
                    .updated_at(before)
                    .exec(&mut db)
                    .await
                    .unwrap();
            }
            let mut agent = AgentRef::new(
                id,
                ResourceCapacity {
                    cpu_cores,
                    memory_bytes,
                },
                vec![owned[3].uuid],
            );
            let jobs: Vec<_> = agent.poll(&mut db, maximum).await.unwrap().collect();
            assert_eq!(jobs.len(), delivered);
            if delivered > 0 {
                assert_eq!(jobs[0].uuid, owned[0].uuid);
            }
            for job in &owned {
                let renewed = Job::filter_by_id(job.id).get(&mut db).await.unwrap();
                assert_eq!(renewed.status, Status::Polled);
                assert_eq!(renewed.agent_id, Some(id));
                if jobs.iter().any(|entry| entry.uuid == job.uuid) {
                    assert!(renewed.polled_at.unwrap() > before);
                    assert!(renewed.updated_at > before);
                } else {
                    assert!(renewed.polled_at.unwrap() <= before);
                    assert!(renewed.updated_at <= before);
                }
                assert_eq!(
                    renewed.expired_at,
                    Some(renewed.polled_at.unwrap() + Duration::from_secs(30))
                );
            }
            for job in &untouched {
                let unchanged = Job::filter_by_id(job.id).get(&mut db).await.unwrap();
                assert_eq!(unchanged.polled_at, job.polled_at);
                assert_eq!(unchanged.expired_at, job.expired_at);
                assert_eq!(unchanged.updated_at, job.updated_at);
            }
        }
    }

    #[tokio::test]
    async fn polled_limits_loading_and_leaves_yielded_jobs_untouched() {
        let Some((mut db, run, node_run, task, config, run_config)) = configured_task().await
        else {
            return;
        };
        let now = Timestamp::now();
        let id = Uuid::new_v4();
        let mut polled = Job::new(&task, run.uuid, config.id, run_config.id);
        polled.status = Status::Polled;
        polled.agent_id = Some(id);
        polled.polled_at = Some(now - Duration::from_secs(5));
        polled.expired_at = Some(now + Duration::from_secs(25));
        polled.write(&mut db).await.unwrap();

        // A Yielded job with no configuration must not be read for lease renewal.
        let mut yielded = Job::new(&task, Uuid::new_v4(), 0, 0);
        yielded.status = Status::Yielded;
        yielded.agent_id = Some(id);
        yielded.polled_at = polled.polled_at;
        yielded.write(&mut db).await.unwrap();
        let before = Job::filter_by_uuid(yielded.uuid)
            .get(&mut db)
            .await
            .unwrap();
        // The next Polled row must not be fetched: its included task is missing.
        let mut excluded = Job::new(&task, run.uuid, config.id, run_config.id);
        excluded.task_id = Uuid::new_v4();
        excluded.status = Status::Polled;
        excluded.agent_id = Some(id);
        excluded.polled_at = polled.polled_at;
        excluded.expired_at = polled.expired_at;
        excluded.write(&mut db).await.unwrap();
        let excluded_before = Job::filter_by_uuid(excluded.uuid)
            .get(&mut db)
            .await
            .unwrap();
        let mut agent = AgentRef::new(
            id,
            ResourceCapacity {
                cpu_cores: 1,
                memory_bytes: 1,
            },
            vec![yielded.uuid],
        );
        let polled_before = Job::filter_by_uuid(polled.uuid).get(&mut db).await.unwrap();
        assert!(agent.polled(&mut db, 0).await.unwrap().is_empty());
        let unchanged = Job::filter_by_uuid(polled.uuid).get(&mut db).await.unwrap();
        assert_eq!(unchanged.polled_at, polled_before.polled_at);
        assert_eq!(unchanged.updated_at, polled_before.updated_at);
        let jobs = agent.polled(&mut db, 1).await.unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].uuid, polled.uuid);
        assert_eq!(jobs[0].status, Status::Polled);
        assert!(jobs[0].polled_at > polled.polled_at);
        assert_eq!(jobs[0].task.get().id, task.id);
        assert_eq!(jobs[0].run.get().uuid, run.uuid);
        assert_eq!(jobs[0].node_run.get().id, node_run.id);
        assert_eq!(jobs[0].node_config.get().as_ref().unwrap().id, config.id);
        assert_eq!(jobs[0].run_config.get().as_ref().unwrap().id, run_config.id);
        assert_eq!(
            jobs[0].expired_at,
            Some(jobs[0].polled_at.unwrap() + Duration::from_secs(30))
        );
        let after = Job::filter_by_uuid(yielded.uuid)
            .get(&mut db)
            .await
            .unwrap();
        assert_eq!(after.status, Status::Yielded);
        assert_eq!(after.agent_id, before.agent_id);
        assert_eq!(after.polled_at, before.polled_at);
        assert_eq!(after.updated_at, before.updated_at);
        let excluded_after = Job::filter_by_uuid(excluded.uuid)
            .get(&mut db)
            .await
            .unwrap();
        assert_eq!(excluded_after.status, excluded_before.status);
        assert_eq!(excluded_after.polled_at, excluded_before.polled_at);
        assert_eq!(excluded_after.updated_at, excluded_before.updated_at);
    }

    #[tokio::test]
    async fn scheduled_limits_loading_and_leaves_other_statuses_untouched() {
        let Some((mut db, run, node_run, task, config, run_config)) = configured_task().await
        else {
            return;
        };
        let id = Uuid::new_v4();
        let mut other_jobs = Vec::new();
        for status in [
            Status::Polled,
            Status::Yielded,
            Status::Succeeded,
            Status::Failed,
            Status::Terminated,
        ] {
            // Non-Scheduled rows must not load relations or consume the supplied budget.
            let mut job = Job::new(&task, Uuid::new_v4(), 0, 0);
            job.task_id = Uuid::new_v4();
            job.status = status;
            job.required_cpu_cores = 1;
            job.required_memory_bytes = 1;
            if !status.is_terminal() {
                job.agent_id = Some(id);
                job.polled_at = Some(Timestamp::now() - Duration::from_secs(5));
            }
            job.write(&mut db).await.unwrap();
            other_jobs.push(Job::filter_by_uuid(job.uuid).get(&mut db).await.unwrap());
        }
        let mut scheduled = Job::new(&task, run.uuid, config.id, run_config.id);
        scheduled.required_cpu_cores = 1;
        scheduled.required_memory_bytes = 1;
        scheduled.write(&mut db).await.unwrap();
        let scheduled = Job::filter_by_uuid(scheduled.uuid)
            .get(&mut db)
            .await
            .unwrap();
        // The next Scheduled row must not be fetched: its included task is missing.
        let mut excluded = Job::new(&task, run.uuid, config.id, run_config.id);
        excluded.task_id = Uuid::new_v4();
        excluded.write(&mut db).await.unwrap();
        other_jobs.push(
            Job::filter_by_uuid(excluded.uuid)
                .get(&mut db)
                .await
                .unwrap(),
        );
        let capacity = ResourceCapacity {
            cpu_cores: 1,
            memory_bytes: 1,
        };
        let mut agent = AgentRef::new(id, capacity, Vec::new());
        assert!(agent.scheduled(&mut db, 0).await.unwrap().is_empty());
        let unchanged = Job::filter_by_id(scheduled.id).get(&mut db).await.unwrap();
        assert_eq!(unchanged.status, Status::Scheduled);
        assert_eq!(unchanged.agent_id, scheduled.agent_id);
        assert_eq!(unchanged.polled_at, scheduled.polled_at);
        assert_eq!(unchanged.updated_at, scheduled.updated_at);
        let jobs = agent.scheduled(&mut db, 1).await.unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].uuid, scheduled.uuid);
        assert_eq!(jobs[0].status, Status::Polled);
        assert_eq!(jobs[0].agent_id, Some(id));
        assert_eq!(jobs[0].task.get().id, task.id);
        assert_eq!(jobs[0].run.get().uuid, run.uuid);
        assert_eq!(jobs[0].node_run.get().id, node_run.id);
        assert_eq!(jobs[0].node_config.get().as_ref().unwrap().id, config.id);
        assert_eq!(jobs[0].run_config.get().as_ref().unwrap().id, run_config.id);
        assert_eq!(
            jobs[0].expired_at,
            Some(jobs[0].polled_at.unwrap() + Duration::from_secs(30))
        );
        let claimed = Job::filter_by_id(scheduled.id).get(&mut db).await.unwrap();
        assert_eq!(claimed.status, Status::Polled);
        assert_eq!(claimed.agent_id, Some(id));
        assert!(claimed.polled_at.is_some());
        assert_eq!(
            jobs[0].expired_at,
            Some(jobs[0].polled_at.unwrap() + Duration::from_secs(30))
        );
        assert_eq!(
            claimed.expired_at,
            Some(claimed.polled_at.unwrap() + Duration::from_secs(30))
        );
        for before in other_jobs {
            let after = Job::filter_by_id(before.id).get(&mut db).await.unwrap();
            assert_eq!(after.status, before.status);
            assert_eq!(after.agent_id, before.agent_id);
            assert_eq!(after.polled_at, before.polled_at);
            assert_eq!(after.updated_at, before.updated_at);
        }
    }

    #[tokio::test]
    async fn reserve_claims_reserves_all_matching_claims_across_pages() {
        let Some((mut db, run, _, task, config, run_config)) = configured_task().await else {
            return;
        };
        let id = Uuid::new_v4();
        for _ in 0..=JOB_PAGE_SIZE {
            let mut job = Job::new(&task, run.uuid, config.id, run_config.id);
            job.status = Status::Polled;
            job.agent_id = Some(id);
            job.polled_at = Some(Timestamp::now());
            job.required_cpu_cores = 1;
            job.required_memory_bytes = 2;
            job.write(&mut db).await.unwrap();
        }
        for (status, owner) in [
            (Status::Scheduled, None),
            (Status::Succeeded, Some(id)),
            (Status::Polled, Some(Uuid::new_v4())),
            (Status::Yielded, Some(id)),
        ] {
            let mut job = Job::new(&task, run.uuid, config.id, run_config.id);
            job.status = status;
            job.agent_id = owner;
            job.required_cpu_cores = 1000;
            job.required_memory_bytes = 1000;
            job.write(&mut db).await.unwrap();
        }
        let capacity = ResourceCapacity {
            cpu_cores: JOB_PAGE_SIZE as u32 + 2,
            memory_bytes: (JOB_PAGE_SIZE as u64 + 2) * 2,
        };
        let mut agent = AgentRef::new(id, capacity, Vec::new());
        agent
            .reserve_claims(&mut db, Job::fields().status().eq(Status::Polled))
            .await
            .unwrap();
        assert_eq!(
            agent.resource,
            ResourceCapacity {
                cpu_cores: 1,
                memory_bytes: 2
            }
        );
    }

    #[tokio::test]
    async fn both_pollers_skip_active_jobs_and_deduct_only_fitting_jobs() {
        for status in [Status::Polled, Status::Scheduled] {
            let Some((mut db, run, _, task, config, run_config)) = configured_task().await else {
                return;
            };
            let id = Uuid::new_v4();
            let before = Timestamp::now() - Duration::from_secs(5);
            let mut expected = Vec::new();
            let mut unchanged = Vec::new();
            let mut active_jobs = Vec::new();
            // The active and oversized rows have missing tasks, so they must be filtered
            // before includes are loaded. Later rows exercise the shrinking page budget.
            for (index, (cpu, memory)) in [(0, 0), (6, 1), (1, 7), (3, 3), (3, 3), (2, 3), (0, 0)]
                .into_iter()
                .enumerate()
            {
                let mut job = Job::new(&task, run.uuid, config.id, run_config.id);
                job.status = status;
                if status == Status::Polled {
                    job.agent_id = Some(id);
                    job.polled_at = Some(before);
                    job.expired_at = Some(before + Duration::from_secs(30));
                }
                job.required_cpu_cores = cpu;
                job.required_memory_bytes = memory;
                if index < 3 {
                    job.task_id = Uuid::new_v4();
                }
                if index == 0 {
                    active_jobs.push(job.uuid);
                }
                job.write(&mut db).await.unwrap();
                if index == 3 || index == 5 {
                    expected.push(job.uuid);
                } else {
                    unchanged.push(Job::filter_by_uuid(job.uuid).get(&mut db).await.unwrap());
                }
            }
            let mut agent = AgentRef::new(
                id,
                ResourceCapacity {
                    cpu_cores: 5,
                    memory_bytes: 6,
                },
                active_jobs,
            );
            let jobs = if status == Status::Polled {
                agent.polled(&mut db, 2).await.unwrap()
            } else {
                agent.scheduled(&mut db, 2).await.unwrap()
            };
            assert_eq!(
                jobs.iter().map(|job| job.uuid).collect::<Vec<_>>(),
                expected
            );
            assert_eq!(agent.resource, ResourceCapacity::default());
            for before in unchanged {
                let after = Job::filter_by_id(before.id).get(&mut db).await.unwrap();
                assert_eq!(after.status, before.status);
                assert_eq!(after.polled_at, before.polled_at);
                assert_eq!(after.agent_id, before.agent_id);
            }
        }
    }

    #[tokio::test]
    async fn both_pollers_stop_when_either_resource_is_exhausted() {
        for status in [Status::Polled, Status::Scheduled] {
            for (cpu, memory, required_cpu, required_memory) in [(2, 5, 2, 1), (5, 2, 1, 2)] {
                let Some((mut db, run, _, task, config, run_config)) = configured_task().await
                else {
                    return;
                };
                let id = Uuid::new_v4();
                let mut job = Job::new(&task, run.uuid, config.id, run_config.id);
                job.status = status;
                if status == Status::Polled {
                    job.agent_id = Some(id);
                    job.polled_at = Some(Timestamp::now() - Duration::from_secs(5));
                    job.expired_at = Some(Timestamp::now() + Duration::from_secs(25));
                }
                job.required_cpu_cores = required_cpu;
                job.required_memory_bytes = required_memory;
                job.write(&mut db).await.unwrap();
                let mut free = Job::new(&task, run.uuid, config.id, run_config.id);
                free.status = status;
                free.agent_id = job.agent_id;
                free.polled_at = job.polled_at;
                free.expired_at = job.expired_at;
                free.write(&mut db).await.unwrap();
                let free_before = Job::filter_by_uuid(free.uuid).get(&mut db).await.unwrap();
                let mut agent = AgentRef::new(
                    id,
                    ResourceCapacity {
                        cpu_cores: cpu,
                        memory_bytes: memory,
                    },
                    Vec::new(),
                );
                let jobs = if status == Status::Polled {
                    agent.polled(&mut db, 10).await.unwrap()
                } else {
                    agent.scheduled(&mut db, 10).await.unwrap()
                };
                assert_eq!(jobs.len(), 1);
                assert_eq!(jobs[0].uuid, job.uuid);
                assert_eq!(
                    agent.resource,
                    ResourceCapacity {
                        cpu_cores: cpu - required_cpu,
                        memory_bytes: memory - required_memory
                    }
                );
                // Subsequent calls must stop before loading even a zero-requirement job.
                let jobs = if status == Status::Polled {
                    agent.polled(&mut db, 10).await.unwrap()
                } else {
                    agent.scheduled(&mut db, 10).await.unwrap()
                };
                assert!(jobs.is_empty());
                let free_after = Job::filter_by_id(free_before.id)
                    .get(&mut db)
                    .await
                    .unwrap();
                assert_eq!(free_after.status, free_before.status);
                assert_eq!(free_after.agent_id, free_before.agent_id);
                assert_eq!(free_after.polled_at, free_before.polled_at);
            }
        }
    }

    #[tokio::test]
    async fn poll_shares_budget_between_redelivery_and_new_claims_without_double_counting() {
        let Some((mut db, run, _, task, config, run_config)) = configured_task().await else {
            return;
        };
        let id = Uuid::new_v4();
        let mut expected = Vec::new();
        let mut active_jobs = Vec::new();
        for (index, status) in [
            Status::Yielded,
            Status::Polled,
            Status::Polled,
            Status::Scheduled,
            Status::Scheduled,
        ]
        .into_iter()
        .enumerate()
        {
            let mut job = Job::new(&task, run.uuid, config.id, run_config.id);
            job.status = status;
            job.required_cpu_cores = 1;
            job.required_memory_bytes = 1;
            if status != Status::Scheduled {
                job.agent_id = Some(id);
                job.polled_at = Some(Timestamp::now() - Duration::from_secs(5));
                job.expired_at = Some(Timestamp::now() + Duration::from_secs(25));
            }
            if index == 1 {
                active_jobs.push(job.uuid);
            }
            if index >= 2 {
                expected.push(job.uuid);
            }
            job.write(&mut db).await.unwrap();
        }
        let mut agent = AgentRef::new(
            id,
            ResourceCapacity {
                cpu_cores: 4,
                memory_bytes: 4,
            },
            active_jobs,
        );
        let jobs: Vec<_> = agent.poll(&mut db, 10).await.unwrap().collect();
        assert_eq!(
            jobs.iter().map(|job| job.uuid).collect::<Vec<_>>(),
            expected
        );
        assert_eq!(agent.resource, ResourceCapacity::default());
    }

    #[tokio::test]
    async fn failed_poll_preserves_resources_and_rolls_back_claims() {
        let Some((mut db, run, _, task, config, run_config)) = configured_task().await else {
            return;
        };
        let mut valid = Job::new(&task, run.uuid, config.id, run_config.id);
        valid.required_cpu_cores = 1;
        valid.required_memory_bytes = 1;
        valid.write(&mut db).await.unwrap();
        let invalid = Job::new(&task, run.uuid, 0, run_config.id);
        invalid.write(&mut db).await.unwrap();
        let capacity = ResourceCapacity {
            cpu_cores: 2,
            memory_bytes: 2,
        };
        let mut agent = AgentRef::new(Uuid::new_v4(), capacity, Vec::new());
        assert!(matches!(
            agent.poll(&mut db, 2).await,
            Err(Error::JobLeaseNotFound(0))
        ));
        assert_eq!(agent.resource, capacity);
        let unchanged = Job::filter_by_uuid(valid.uuid).get(&mut db).await.unwrap();
        assert_eq!(unchanged.status, Status::Scheduled);
        assert!(unchanged.agent_id.is_none());
    }
}
