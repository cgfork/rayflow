//! Job delivery and conversion of agent status reports into task events.

use executor::JobContext;
use runtime::command::{NodeRun, Run};
use scheduler::{
    Job, ResourceCapacity,
    actuator::{NodeConfig, Status},
    fail_job, pend_job, poll_polled, poll_scheduled, succeed_job, terminate_job,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{App, AppError};

/// A claim together with the pinned configuration needed to execute it.
#[derive(Debug, Serialize, Deserialize)]
pub struct PolledJob {
    pub job: Job,
    pub context: JobContext,
    /// The configured script (`interpreter` and `source`). Invalid configs fail on the agent.
    pub config: serde_json::Value,
    pub lease_ms: u64,
}

impl<S> App<S> {
    /// Claims fitting jobs and includes their configuration and original node revision.
    pub async fn poll_jobs(
        &self,
        agent_id: Uuid,
        capacity: ResourceCapacity,
        maximum: usize,
        active_jobs: &[Uuid],
    ) -> Result<Vec<PolledJob>, AppError> {
        let mut db = self.db.clone();
        let mut tx = db.transaction().await?;
        let (mut polled_jobs, reserved) =
            poll_polled(&mut tx, agent_id, active_jobs, maximum, self.job_lease).await?;
        let remaining = ResourceCapacity {
            cpu_cores: capacity.cpu_cores.saturating_sub(reserved.cpu_cores),
            memory_bytes: capacity.memory_bytes.saturating_sub(reserved.memory_bytes),
        };
        let scheduled_jobs = poll_scheduled(
            &mut tx,
            agent_id,
            remaining,
            maximum.saturating_sub(polled_jobs.len()),
        )
        .await?;
        polled_jobs.extend(scheduled_jobs);
        tx.commit().await?;

        let jobs = polled_jobs;
        let mut polled = Vec::with_capacity(jobs.len());
        for job in jobs {
            let run = Run::filter_by_uuid(job.run_id).get(&mut db).await?;
            let node_run = NodeRun::filter_by_id(job.node_run_id).get(&mut db).await?;
            let config = NodeConfig::filter_by_id(job.node_config_id)
                .first()
                .exec(&mut db)
                .await?
                .map(|config| config.config)
                .unwrap_or(serde_json::Value::Null);
            polled.push(PolledJob {
                context: JobContext {
                    run_id: run.uuid,
                    graph_id: run.graph_id,
                    version: run.version,
                    node_id: node_run.node_id,
                    revision: node_run.revision,
                },
                job,
                config,
                lease_ms: self.job_lease.as_millis() as u64,
            });
        }
        Ok(polled)
    }

    /// Changes an owned job and appends its task event in the same transaction.
    /// Repeated reports of the same status do not append another event.
    pub async fn report_job_status(
        &self,
        job_id: Uuid,
        agent_id: Uuid,
        status: Status,
    ) -> Result<(), AppError> {
        let mut db = self.db.clone();
        match status {
            Status::Pending => pend_job(&mut db, job_id, agent_id, self.job_lease).await?,
            Status::Succeeded => succeed_job(&mut db, job_id, agent_id, self.job_lease).await?,
            Status::Failed => fail_job(&mut db, job_id, agent_id, self.job_lease).await?,
            Status::Terminated => terminate_job(&mut db, job_id, agent_id, self.job_lease).await?,
            Status::Scheduled | Status::Polled => return Err(AppError::InvalidJobStatus),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Config,
        app::test::{TestGraphStore, setup},
    };
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use event::{Emit, task::Task, task_event::TaskEventEntry};
    use jiff::Timestamp;
    use scheduler::{AgentScheduler, JobWrite, Scheduler, TaskEvent, cancel_job};
    use std::time::Duration;
    use tower::ServiceExt;

    async fn configured_job(db: &mut toasty::Db) -> (Task, Job) {
        let now = Timestamp::now();
        let data_id = Uuid::new_v4();
        let graph_id = Uuid::new_v4();
        let node_id = Uuid::new_v4();
        let run = Run::create()
            .uuid(Uuid::new_v4())
            .data_id(data_id)
            .graph_id(graph_id)
            .version(5)
            .revision(7)
            .created_at(now)
            .updated_at(now)
            .exec(&mut *db)
            .await
            .unwrap();
        let node_run = NodeRun::create()
            .run_id(run.uuid)
            .data_id(data_id)
            .graph_id(graph_id)
            .node_id(node_id)
            .revision(3)
            .status("Running".to_owned())
            .created_at(now)
            .updated_at(now)
            .exec(&mut *db)
            .await
            .unwrap();
        NodeConfig::create()
            .node_id(node_id)
            .version(1)
            .annotation_match(String::new())
            .config(serde_json::json!({"interpreter": "sh", "source": "exit 0"}))
            .required_cpu_cores(1)
            .required_memory_bytes(512)
            .created_at(now)
            .updated_at(now)
            .created_by(String::new())
            .updated_by(String::new())
            .exec(&mut *db)
            .await
            .unwrap();
        let task = Task::new(data_id, graph_id, node_id, node_run.id);
        task.emit(&mut *db).await.unwrap();
        let job = Job::create_for_task(&task, db).await.unwrap();
        (task, job)
    }

    async fn report(
        app: &App<TestGraphStore>,
        job: Uuid,
        agent: Uuid,
        status: Status,
    ) -> StatusCode {
        crate::api::router(app.clone())
            .oneshot(
                Request::post(format!("/jobs/{job}/status"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "agent_id": agent, "status": status,
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    }

    #[tokio::test]
    async fn status_reports_are_atomic_idempotent_and_reject_expired_claims() {
        let Some(mut db) = setup().await else {
            eprintln!("skipping: set DATABASE_URL to run MySQL job status tests");
            return;
        };
        let app = App::new(db.clone(), Config::default(), TestGraphStore::default());
        let agent = Uuid::new_v4();
        let capacity = ResourceCapacity {
            cpu_cores: 1,
            memory_bytes: 512,
        };
        let (task, job) = configured_job(&mut db).await;
        let polled = app.poll_jobs(agent, capacity, 1, &[]).await.unwrap();
        assert_eq!(polled.len(), 1);
        assert_eq!(polled[0].context.version, 5);
        assert_eq!(
            polled[0].context.revision, 3,
            "use the node revision, not the current run revision"
        );
        assert_eq!(polled[0].context.node_id, task.node_id);
        assert_eq!(
            polled[0].config,
            serde_json::json!({"interpreter": "sh", "source": "exit 0"})
        );
        assert_eq!(
            report(&app, job.uuid, Uuid::new_v4(), Status::Pending).await,
            StatusCode::CONFLICT
        );
        assert_eq!(
            report(&app, job.uuid, agent, Status::Scheduled).await,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            report(&app, Uuid::new_v4(), agent, Status::Pending).await,
            StatusCode::NOT_FOUND
        );
        for _ in 0..2 {
            assert_eq!(
                report(&app, job.uuid, agent, Status::Pending).await,
                StatusCode::NO_CONTENT
            );
        }
        let pending = Job::filter_by_uuid(job.uuid).get(&mut db).await.unwrap();
        assert_eq!(pending.status, Status::Pending);
        assert_eq!(pending.agent_id, Some(agent));
        assert!(
            app.poll_jobs(agent, capacity, 1, &[job.uuid])
                .await
                .unwrap()
                .is_empty()
        );

        // Reclaim a running job and ensure its retried start does not append
        // another Pend event to an already-pending logical task.
        let expired = Timestamp::now() - Duration::from_secs(60);
        Job::filter_by_id(job.id)
            .update()
            .polled_at(Some(expired))
            .exec(&mut db)
            .await
            .unwrap();
        assert_eq!(
            report(&app, job.uuid, agent, Status::Succeeded).await,
            StatusCode::CONFLICT
        );
        let new_agent = Uuid::new_v4();
        assert_eq!(
            app.poll_jobs(new_agent, capacity, 1, &[])
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            report(&app, job.uuid, agent, Status::Failed).await,
            StatusCode::CONFLICT
        );
        assert_eq!(
            report(&app, job.uuid, new_agent, Status::Pending).await,
            StatusCode::NO_CONTENT
        );
        for _ in 0..2 {
            assert_eq!(
                report(&app, job.uuid, new_agent, Status::Succeeded).await,
                StatusCode::NO_CONTENT
            );
        }
        assert_eq!(
            report(&app, job.uuid, new_agent, Status::Pending).await,
            StatusCode::CONFLICT
        );
        assert_eq!(
            report(&app, job.uuid, new_agent, Status::Failed).await,
            StatusCode::CONFLICT
        );
        let events = TaskEventEntry::filter_by_task_id(task.id)
            .order_by(TaskEventEntry::fields().event_offset().asc())
            .exec(&mut db)
            .await
            .unwrap();
        assert_eq!(
            events.iter().map(|entry| entry.event).collect::<Vec<_>>(),
            vec![TaskEvent::Pend, TaskEvent::Succeed]
        );
        assert!(
            events
                .iter()
                .all(|entry| entry.id != task.id && entry.node_run_id == task.node_run_id)
        );
        assert_ne!(events[0].id, events[1].id);
        let settled = Job::filter_by_id(job.id).get(&mut db).await.unwrap();
        assert_eq!(settled.status, Status::Succeeded);
        assert!(settled.agent_id.is_none() && settled.polled_at.is_none());

        for (status, event) in [
            (Status::Failed, TaskEvent::Fail),
            (Status::Terminated, TaskEvent::Terminate),
        ] {
            let (task, job) = configured_job(&mut db).await;
            app.poll_jobs(agent, capacity, 1, &[]).await.unwrap();
            assert_eq!(
                report(&app, job.uuid, agent, status).await,
                StatusCode::NO_CONTENT
            );
            let events = TaskEventEntry::filter_by_task_id(task.id)
                .exec(&mut db)
                .await
                .unwrap();
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].event, event);
        }

        let (task, job) = configured_job(&mut db).await;
        cancel_job(&mut db, job.uuid).await.unwrap();
        cancel_job(&mut db, job.uuid).await.unwrap();
        assert_eq!(
            Job::filter_by_uuid(job.uuid)
                .get(&mut db)
                .await
                .unwrap()
                .status,
            Status::Terminated
        );
        let events = TaskEventEntry::filter_by_task_id(task.id)
            .exec(&mut db)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event, TaskEvent::Cancel);

        // Shutdown can cancel an in-flight start report and send a terminal
        // report before the first request has committed.
        let (task, job) = configured_job(&mut db).await;
        app.poll_jobs(agent, capacity, 1, &[]).await.unwrap();
        let (pending, terminal) = tokio::join!(
            report(&app, job.uuid, agent, Status::Pending),
            report(&app, job.uuid, agent, Status::Terminated),
        );
        assert!(matches!(
            pending,
            StatusCode::NO_CONTENT | StatusCode::CONFLICT
        ));
        assert_eq!(terminal, StatusCode::NO_CONTENT);
        let events = TaskEventEntry::filter_by_task_id(task.id)
            .order_by(TaskEventEntry::fields().event_offset().asc())
            .exec(&mut db)
            .await
            .unwrap();
        let events = events.iter().map(|entry| entry.event).collect::<Vec<_>>();
        assert!(
            events == vec![TaskEvent::Terminate]
                || events == vec![TaskEvent::Pend, TaskEvent::Terminate]
        );

        // A failed event append must roll back the job status update too.
        let missing_task = Task::new(Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4(), 0);
        let mut orphan = Job::new(&missing_task, Uuid::new_v4(), 0, 0);
        orphan.agent_id = Some(agent);
        orphan.polled_at = Some(Timestamp::now());
        orphan.status = Status::Polled;
        orphan.write(&mut db).await.unwrap();
        assert!(
            app.report_job_status(orphan.uuid, agent, Status::Succeeded)
                .await
                .is_err()
        );
        assert_eq!(
            Job::filter_by_uuid(orphan.uuid)
                .get(&mut db)
                .await
                .unwrap()
                .status,
            Status::Polled
        );
        Job::filter_by_uuid(orphan.uuid)
            .update()
            .status(Status::Terminated)
            .agent_id(None)
            .polled_at(None)
            .exec(&mut db)
            .await
            .unwrap();
        // Polling without an active execution must not renew abandoned Pending claims.
        let (_, job) = configured_job(&mut db).await;
        app.poll_jobs(agent, capacity, 1, &[]).await.unwrap();
        app.report_job_status(job.uuid, agent, Status::Pending)
            .await
            .unwrap();
        let before = Job::filter_by_id(job.id)
            .get(&mut db)
            .await
            .unwrap()
            .polled_at;
        app.poll_jobs(agent, capacity, 1, &[]).await.unwrap();
        assert_eq!(
            Job::filter_by_id(job.id)
                .get(&mut db)
                .await
                .unwrap()
                .polled_at,
            before
        );
        Job::filter_by_id(job.id)
            .update()
            .polled_at(Some(expired))
            .exec(&mut db)
            .await
            .unwrap();
        assert_eq!(AgentScheduler::new().schedule(&mut db).await.unwrap(), 1);
    }
}
