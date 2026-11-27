//! Direct job polling for agents with resource-aware claims.

use crate::{
    Error, Job, Scheduler,
    actuator::{NodeConfig, Status},
};
use event::{
    Emit, Read,
    task::Task,
    task_event::{TaskEvent, TaskEventEntry},
};
use jiff::Timestamp;
use runtime::command::{NodeRun, Run, RunConfig};
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};
use toasty::{Executor, schema::Model, stmt::IntoStatement};
use uuid::Uuid;

const JOB_PAGE_SIZE: usize = 128;

/// A single CDI device and its usable GPU memory budget in bytes.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GpuDevice {
    pub cdi_device: String,
    pub memory_bytes: u64,
}

/// A scheduled GPU job that fits the remaining resources on this agent.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GpuOffer {
    pub job_id: Uuid,
    pub required_gpu_memory_bytes: u64,
}

/// An agent's choice of CDI device for a scheduled GPU job.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GpuChoice {
    pub job_id: Uuid,
    pub cdi_device: String,
}

/// Unclaimed candidate jobs and the budget remaining after existing claims.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GpuOffers {
    pub jobs: Vec<GpuOffer>,
    pub devices: Vec<GpuDevice>,
}

/// Validates the advertised inventory and returns its total GPU memory budget.
pub fn validate_gpu_devices(devices: &[GpuDevice]) -> Result<u64, &'static str> {
    let mut names = HashSet::with_capacity(devices.len());
    let mut total = 0_u64;
    for device in devices {
        if !graph::gpu::is_qualified_cdi_device(&device.cdi_device) {
            return Err("gpu_devices must contain qualified CDI device names");
        }
        if !names.insert(device.cdi_device.as_str()) {
            return Err("gpu_devices must have unique CDI device names");
        }
        if device.memory_bytes == 0 {
            return Err("gpu_devices must have positive memory budgets");
        }
        total = total
            .checked_add(device.memory_bytes)
            .ok_or("total GPU memory exceeds the supported limit")?;
    }
    Ok(total)
}

/// Total CPU and RAM budget for this agent's jobs, including existing claims.
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

    fn fits(self, job: &Job) -> bool {
        job.required_cpu_cores <= self.cpu_cores && job.required_memory_bytes <= self.memory_bytes
    }

    fn reserve(&mut self, job: &Job) {
        self.cpu_cores = self.cpu_cores.saturating_sub(job.required_cpu_cores);
        self.memory_bytes = self.memory_bytes.saturating_sub(job.required_memory_bytes);
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
    gpu_devices: Vec<GpuDevice>,
    gpu_choices: HashMap<Uuid, String>,
    gpu_offers: Option<GpuOffers>,
}

impl AgentRef {
    pub fn new(id: Uuid, resource: ResourceCapacity, active_jobs: Vec<Uuid>) -> Self {
        Self {
            id,
            active_jobs,
            resource,
            gpu_devices: Vec::new(),
            gpu_choices: HashMap::new(),
            gpu_offers: None,
        }
    }

    /// Configures the named device budgets reported by this agent.
    pub fn with_gpu_devices(mut self, gpu_devices: Vec<GpuDevice>) -> Result<Self, &'static str> {
        validate_gpu_devices(&gpu_devices)?;
        self.gpu_devices = gpu_devices;
        Ok(self)
    }

    /// Supplies at most one device choice per job up to the requested batch size.
    pub fn with_gpu_choices(
        mut self,
        choices: Vec<GpuChoice>,
        maximum: usize,
    ) -> Result<Self, &'static str> {
        if choices.len() > maximum {
            return Err("gpu_choices must fit within batch");
        }
        let mut by_job = HashMap::with_capacity(choices.len());
        for choice in choices {
            if !graph::gpu::is_qualified_cdi_device(&choice.cdi_device) {
                return Err("gpu_choices must contain qualified CDI device names");
            }
            if by_job.insert(choice.job_id, choice.cdi_device).is_some() {
                return Err("gpu_choices must have unique job IDs");
            }
        }
        self.gpu_choices = by_job;
        Ok(self)
    }

    /// Returns the GPU offers from the most recent successful poll.
    pub fn take_gpu_offers(&mut self) -> Option<GpuOffers> {
        self.gpu_offers.take()
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

    pub fn gpu_devices(&self) -> &[GpuDevice] {
        &self.gpu_devices
    }

    /// Total remaining GPU budget across the named devices.
    pub fn gpu_memory_bytes(&self) -> u64 {
        self.gpu_devices
            .iter()
            .map(|device| device.memory_bytes)
            .sum()
    }

    fn max_gpu_memory_bytes(&self) -> u64 {
        self.gpu_devices
            .iter()
            .map(|device| device.memory_bytes)
            .max()
            .unwrap_or_default()
    }

    /// SQL prefilters by the largest available single device.
    fn fitting_jobs(&self) -> toasty::stmt::Expr<bool> {
        Job::fields()
            .required_cpu_cores()
            .le(self.resource.cpu_cores)
            .and(
                Job::fields()
                    .required_memory_bytes()
                    .le(self.resource.memory_bytes),
            )
            .and(
                Job::fields()
                    .required_gpu_memory_bytes()
                    .le(self.max_gpu_memory_bytes()),
            )
    }

    fn device_fits(&self, job: &Job, name: &str) -> bool {
        self.gpu_devices.iter().any(|device| {
            name == device.cdi_device && device.memory_bytes >= job.required_gpu_memory_bytes
        })
    }

    fn chosen_device(&self, job: &Job) -> Option<String> {
        let name = self.gpu_choices.get(&job.uuid)?;
        (job.required_gpu_memory_bytes > 0
            && self.resource.fits(job)
            && self.device_fits(job, name))
        .then(|| name.clone())
    }

    fn can_redeliver(&self, job: &Job) -> bool {
        self.resource.fits(job)
            && match (job.required_gpu_memory_bytes, &job.assigned_gpu_cdi_device) {
                (0, None) => true,
                (1.., Some(assigned)) => self.device_fits(job, assigned),
                _ => false,
            }
    }

    fn reserve(&mut self, job: &Job) {
        self.resource.reserve(job);
        if let Some(name) = &job.assigned_gpu_cdi_device
            && let Some(device) = self
                .gpu_devices
                .iter_mut()
                .find(|device| name == &device.cdi_device)
        {
            device.memory_bytes = device
                .memory_bytes
                .saturating_sub(job.required_gpu_memory_bytes);
        }
    }

    async fn load_gpu_delivery<B: Executor>(backend: &mut B, mut job: Job) -> Result<Job, Error> {
        job.node_config = NodeConfig::filter_by_uuid(job.node_config_id)
            .first()
            .exec(&mut *backend)
            .await?
            .into();
        job.task = Task::read(&mut *backend, job.task_id).await?.into();
        job.run = Run::filter_by_uuid(job.run_id)
            .get(&mut *backend)
            .await?
            .into();
        job.node_run = NodeRun::filter_by_uuid(job.node_run_id)
            .get(&mut *backend)
            .await?
            .into();
        job.run_config = RunConfig::filter_by_uuid(job.run_config_id)
            .first()
            .exec(backend)
            .await?
            .into();
        Ok(job)
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
                .filter(self.fitting_jobs())
                .filter(Job::fields().id().gt(after_id))
                .order_by(Job::fields().id().asc())
                .limit(page_size)
                .exec(&mut *backend)
                .await?;
            let page_ended = candidates.len() < page_size;
            let cpu_ids: Vec<_> = candidates
                .iter()
                .filter(|job| job.required_gpu_memory_bytes == 0 && self.can_redeliver(job))
                .map(|job| job.id)
                .collect();
            let mut cpu_jobs = if cpu_ids.is_empty() {
                HashMap::new()
            } else {
                Job::all()
                    .filter(Job::fields().id().in_list(cpu_ids))
                    .include(Job::fields().task())
                    .include(Job::fields().run())
                    .include(Job::fields().node_run())
                    .include(Job::fields().node_config())
                    .include(Job::fields().run_config())
                    .exec(&mut *backend)
                    .await?
                    .into_iter()
                    .map(|job| (job.id, job))
                    .collect()
            };
            for job in candidates {
                after_id = job.id;
                if !self.can_redeliver(&job) {
                    continue;
                }
                let mut job = if job.required_gpu_memory_bytes > 0 {
                    Self::load_gpu_delivery(&mut *backend, job).await?
                } else if let Some(job) = cpu_jobs.remove(&job.id) {
                    job
                } else {
                    continue;
                };
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
                        .and(Job::fields().expired_at().eq(job.expired_at))
                        .and(
                            Job::fields()
                                .assigned_gpu_cdi_device()
                                .eq(job.assigned_gpu_cdi_device.clone()),
                        )
                        .and(self.fitting_jobs()),
                )
                .await?;
                if !updated {
                    continue;
                }
                job.polled_at = Some(now);
                job.expired_at = Some(expired_at);
                job.updated_at = now;
                self.reserve(&job);
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
                self.reserve(&job);
            }
            if page_ended {
                break;
            }
        }
        Ok(())
    }

    /// Claims a specific Scheduled row without waiting on a competing poll.
    async fn claim_gpu_job<B: Executor>(
        &mut self,
        backend: &mut B,
        job: Job,
        assigned_device: String,
        now: Timestamp,
    ) -> Result<Option<Job>, Error> {
        let table = backend
            .schema()
            .table_for(Job::id())
            .name
            .replace('`', "``");
        // Toasty cannot express SKIP LOCKED. Lock the chosen row so the
        // guarded update and budget reservation apply to the same claim.
        let locked = toasty::sql::query(format!(
            "SELECT id FROM `{table}` WHERE id = ? AND status = 'Scheduled' FOR UPDATE SKIP LOCKED",
        ))
        .bind(job.id)
        .column_types([toasty::stmt::Type::U64])
        .exec(&mut *backend)
        .await?;
        if locked.is_empty() {
            return Ok(None);
        }
        let job = Self::load_gpu_delivery(&mut *backend, job).await?;
        self.claim_locked_job(backend, job, Some(assigned_device), now)
            .await
    }

    async fn claim_locked_job<B: Executor>(
        &mut self,
        backend: &mut B,
        mut job: Job,
        assigned_device: Option<String>,
        now: Timestamp,
    ) -> Result<Option<Job>, Error> {
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
                .assigned_gpu_cdi_device(assigned_device.clone())
                .updated_at(now),
            Job::fields()
                .status()
                .eq(Status::Scheduled)
                .and(
                    Job::fields()
                        .required_cpu_cores()
                        .eq(job.required_cpu_cores),
                )
                .and(
                    Job::fields()
                        .required_memory_bytes()
                        .eq(job.required_memory_bytes),
                )
                .and(
                    Job::fields()
                        .required_gpu_memory_bytes()
                        .eq(job.required_gpu_memory_bytes),
                ),
        )
        .await?;
        if !updated {
            return Ok(None);
        }
        job.status = Status::Polled;
        job.agent_id = Some(self.id);
        job.polled_at = Some(now);
        job.expired_at = Some(expired_at);
        job.updated_at = now;
        job.assigned_gpu_cdi_device = assigned_device;
        self.reserve(&job);
        Ok(Some(job))
    }

    /// Confirms only the agent's explicit choices, using the current claim budget.
    async fn confirm_gpu_choices<B: Executor>(
        &mut self,
        backend: &mut B,
        maximum: usize,
        now: Timestamp,
    ) -> Result<Vec<Job>, Error> {
        if self.gpu_choices.is_empty() || self.max_gpu_memory_bytes() == 0 {
            return Ok(Vec::new());
        }
        let candidates = Job::all()
            .filter(
                Job::fields()
                    .uuid()
                    .in_list(self.gpu_choices.keys().copied().collect::<Vec<_>>()),
            )
            .filter(Job::fields().uuid().in_list(self.active_jobs.clone()).not())
            .filter(Job::fields().status().eq(Status::Scheduled))
            .filter(Job::fields().required_gpu_memory_bytes().gt(0))
            .filter(self.fitting_jobs())
            .order_by(Job::fields().id().asc())
            .exec(&mut *backend)
            .await?;
        let mut claimed = Vec::new();
        for job in candidates {
            if claimed.len() == maximum || self.resource.exhausted() {
                break;
            }
            if let Some(assigned) = self.chosen_device(&job)
                && let Some(job) = self
                    .claim_gpu_job(&mut *backend, job, assigned, now)
                    .await?
            {
                claimed.push(job);
            }
        }
        Ok(claimed)
    }

    /// Claims CPU jobs up to the earliest fitting GPU offer in the queue.
    async fn claim_cpu_jobs<B: Executor>(
        &mut self,
        backend: &mut B,
        maximum: usize,
        last_id: u64,
        now: Timestamp,
    ) -> Result<Vec<Job>, Error> {
        if maximum == 0 {
            return Ok(Vec::new());
        }
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
                 AND id <= ? AND required_cpu_cores <= ? AND required_memory_bytes <= ? \
                 AND required_gpu_memory_bytes = 0 \
                 ORDER BY id ASC LIMIT ? FOR UPDATE SKIP LOCKED",
            ))
            .bind(after_id)
            .bind(last_id)
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
            for job in candidates {
                if job.required_gpu_memory_bytes != 0 || !self.resource.fits(&job) {
                    continue;
                }
                if let Some(job) = self.claim_locked_job(&mut *backend, job, None, now).await? {
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
    async fn fitting_gpu_jobs<B: Executor>(
        &self,
        backend: &mut B,
        maximum: usize,
    ) -> Result<Vec<Job>, Error> {
        if maximum == 0 || self.resource.exhausted() || self.max_gpu_memory_bytes() == 0 {
            return Ok(Vec::new());
        }
        Ok(Job::all()
            .filter(Job::fields().status().eq(Status::Scheduled))
            .filter(Job::fields().uuid().in_list(self.active_jobs.clone()).not())
            .filter(Job::fields().required_gpu_memory_bytes().gt(0))
            .filter(self.fitting_jobs())
            .order_by(Job::fields().id().asc())
            .limit(maximum)
            .exec(backend)
            .await?)
    }

    /// Confirms choices, then claims CPU work and offers GPU work in queue order.
    async fn scheduled<B: Executor>(
        &mut self,
        backend: &mut B,
        maximum: usize,
    ) -> Result<Vec<Job>, Error> {
        let now = Timestamp::now();
        let mut claimed = self
            .confirm_gpu_choices(&mut *backend, maximum, now)
            .await?;
        let remaining = maximum - claimed.len();
        let mut offers = self.fitting_gpu_jobs(&mut *backend, remaining).await?;
        let last_cpu_id = offers.first().map_or(u64::MAX, |job| job.id - 1);
        let cpu_jobs = self
            .claim_cpu_jobs(&mut *backend, remaining, last_cpu_id, now)
            .await?;

        if !offers.is_empty() && !cpu_jobs.is_empty() {
            offers = self
                .fitting_gpu_jobs(&mut *backend, remaining - cpu_jobs.len())
                .await?;
        }
        claimed.extend(cpu_jobs);
        if !offers.is_empty() {
            self.gpu_offers = Some(GpuOffers {
                jobs: offers
                    .into_iter()
                    .map(|job| GpuOffer {
                        job_id: job.uuid,
                        required_gpu_memory_bytes: job.required_gpu_memory_bytes,
                    })
                    .collect(),
                devices: self
                    .gpu_devices
                    .iter()
                    .filter(|device| device.memory_bytes > 0)
                    .cloned()
                    .collect(),
            });
        } else if last_cpu_id != u64::MAX {
            // Older CPU claims may leave no fitting device job for this batch.
            claimed.extend(
                self.claim_cpu_jobs(&mut *backend, maximum - claimed.len(), u64::MAX, now)
                    .await?,
            );
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
        agent.gpu_offers = None;
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
        self.gpu_devices = agent.gpu_devices;
        self.gpu_offers = agent.gpu_offers;
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

    const GPU_0: &str = "nvidia.com/gpu=0";
    const GPU_1: &str = "nvidia.com/gpu=1";

    fn gpu(cdi_device: &str, memory_bytes: u64) -> GpuDevice {
        GpuDevice {
            cdi_device: cdi_device.into(),
            memory_bytes,
        }
    }

    fn gpu_agent(devices: Vec<GpuDevice>) -> AgentRef {
        AgentRef::new(
            Uuid::nil(),
            ResourceCapacity {
                cpu_cores: 8,
                memory_bytes: 8192,
            },
            Vec::new(),
        )
        .with_gpu_devices(devices)
        .unwrap()
    }

    fn job(gpu_memory: u64) -> Job {
        let task = Task::new(Uuid::nil(), Uuid::nil(), Uuid::nil(), 0);
        let mut job = Job::new(&task, Uuid::nil(), Uuid::nil(), Uuid::nil());
        job.required_cpu_cores = 1;
        job.required_memory_bytes = 1024;
        job.required_gpu_memory_bytes = gpu_memory;
        job
    }

    #[test]
    fn inventory_requires_qualified_unique_devices_and_positive_budgets() {
        assert_eq!(
            validate_gpu_devices(&[gpu(GPU_0, 4), gpu(GPU_1, 8)]),
            Ok(12)
        );
        assert!(validate_gpu_devices(&[gpu("/dev/nvidia0", 4)]).is_err());
        assert!(validate_gpu_devices(&[gpu(GPU_0, 0)]).is_err());
        assert!(validate_gpu_devices(&[gpu(GPU_0, 4), gpu(GPU_0, 8)]).is_err());
        assert!(validate_gpu_devices(&[gpu(GPU_0, u64::MAX), gpu(GPU_1, 1)]).is_err());
    }

    fn choice(job: &Job, device: &str) -> GpuChoice {
        GpuChoice {
            job_id: job.uuid,
            cdi_device: device.into(),
        }
    }

    #[test]
    fn choices_are_bounded_and_unique_per_job() {
        let agent = gpu_agent(vec![gpu(GPU_0, 8192)]);
        let job = job(4096);
        assert!(
            agent
                .clone()
                .with_gpu_choices(vec![choice(&job, GPU_0)], 0)
                .is_err()
        );
        assert!(
            agent
                .clone()
                .with_gpu_choices(vec![choice(&job, GPU_0), choice(&job, GPU_1)], 2)
                .is_err()
        );
        assert!(
            agent
                .with_gpu_choices(vec![choice(&job, "/dev/nvidia0")], 1)
                .is_err()
        );
    }

    #[test]
    fn uses_the_chosen_device_only_when_its_remaining_budget_fits() {
        let mut placed = job(4096);
        let next = job(4097);
        let mut agent = gpu_agent(vec![gpu(GPU_1, 8192), gpu(GPU_0, 4096)])
            .with_gpu_choices(vec![choice(&placed, GPU_1), choice(&next, GPU_1)], 2)
            .unwrap();
        assert_eq!(agent.chosen_device(&job(4096)), None);
        assert_eq!(agent.chosen_device(&next).as_deref(), Some(GPU_1));
        placed.assigned_gpu_cdi_device = agent.chosen_device(&placed);
        assert_eq!(placed.assigned_gpu_cdi_device.as_deref(), Some(GPU_1));
        agent.reserve(&placed);
        assert_eq!(agent.chosen_device(&next), None);
    }

    #[test]
    fn gpu_redelivery_requires_an_existing_assignment_and_capacity_on_that_device() {
        let mut claim = job(4096);
        claim.status = Status::Polled;
        let agent = gpu_agent(vec![gpu(GPU_0, 8192), gpu(GPU_1, 4095)]);
        assert!(!agent.can_redeliver(&claim));

        claim.assigned_gpu_cdi_device = Some(GPU_0.into());
        assert!(agent.can_redeliver(&claim));
        claim.assigned_gpu_cdi_device = Some(GPU_1.into());
        assert!(!agent.can_redeliver(&claim));
    }

    #[test]
    fn reserves_gpu_memory_without_exhausting_capacity_for_cpu_jobs() {
        let cpu_job = job(0);
        let mut gpu_job = job(4096);

        let cpu_agent = gpu_agent(Vec::new());
        assert_eq!(cpu_agent.max_gpu_memory_bytes(), 0);
        assert!(cpu_agent.can_redeliver(&cpu_job));
        assert!(!cpu_agent.can_redeliver(&gpu_job));

        let mut agent = gpu_agent(vec![gpu(GPU_0, 4096)])
            .with_gpu_choices(vec![choice(&gpu_job, GPU_0), choice(&cpu_job, GPU_0)], 2)
            .unwrap();
        gpu_job.assigned_gpu_cdi_device = agent.chosen_device(&gpu_job);
        agent.reserve(&gpu_job);
        assert_eq!(agent.gpu_memory_bytes(), 0);
        assert_eq!(agent.chosen_device(&gpu_job), None);
        assert_eq!(agent.chosen_device(&cpu_job), None);
        assert!(agent.can_redeliver(&cpu_job));
    }
}
