//! Job delivery and conversion of agent status reports into task events.

use runtime::command::{NodeRun, Run};
use scheduler::{
    AgentRef, Job, JobPoll, actuator::NodeConfig, fail_job, succeed_job, terminate_job, yield_job,
};
use uuid::Uuid;

use super::{App, AppError};

impl<S> App<S> {
    /// Claims fitting jobs and includes their configuration and original node revision.
    pub async fn poll(&self, agent: &mut AgentRef, maximum: usize) -> Result<Vec<Job>, AppError> {
        let mut db = self.db.clone();
        let mut tx = db.transaction().await?;
        let mut polling = agent.clone();
        let mut jobs: Vec<_> = polling.poll(&mut tx, maximum).await?.collect();
        for job in &mut jobs {
            let run = Run::filter_by_uuid(job.run_id).get(&mut tx).await?;
            let node_run = NodeRun::filter_by_id(job.node_run_id).get(&mut tx).await?;
            let config = NodeConfig::filter_by_id(job.node_config_id)
                .first()
                .exec(&mut tx)
                .await?
                .ok_or(scheduler::Error::JobLeaseNotFound(job.node_config_id))?;
            config.job_lease()?;
            job.run = run.into();
            job.node_run = node_run.into();
            job.node_config = Some(config).into();
        }
        tx.commit().await?;
        *agent = polling;
        Ok(jobs)
    }

    pub async fn yield_job(&self, job_id: Uuid, agent_id: Uuid) -> Result<(), AppError> {
        let mut db = self.db.clone();
        yield_job(&mut db, job_id, agent_id).await?;
        Ok(())
    }

    pub async fn succeed_job(&self, job_id: Uuid, agent_id: Uuid) -> Result<(), AppError> {
        let mut db = self.db.clone();
        succeed_job(&mut db, job_id, agent_id).await?;
        Ok(())
    }

    pub async fn fail_job(&self, job_id: Uuid, agent_id: Uuid) -> Result<(), AppError> {
        let mut db = self.db.clone();
        fail_job(&mut db, job_id, agent_id).await?;
        Ok(())
    }

    pub async fn terminate_job(&self, job_id: Uuid, agent_id: Uuid) -> Result<(), AppError> {
        let mut db = self.db.clone();
        terminate_job(&mut db, job_id, agent_id).await?;
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
    use scheduler::{
        AgentScheduler, JobWrite, ResourceCapacity, Scheduler, TaskEvent, actuator::Status,
    };
    use std::time::Duration;
    use tower::ServiceExt;

    async fn configured_job(db: &mut toasty::Db) -> (Task, Job) {
        configured_job_with_lease(db, Some(30_000)).await
    }

    async fn configured_job_with_lease(db: &mut toasty::Db, lease: Option<u64>) -> (Task, Job) {
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
            .lease(lease)
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
        let action = match status {
            Status::Yielded => "yield",
            Status::Succeeded => "succeed",
            Status::Failed => "fail",
            Status::Terminated => "terminate",
            _ => panic!("not a terminal action: {status:?}"),
        };
        crate::api::router(app.clone())
            .oneshot(
                Request::post(format!("/job/{action}"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "job_id": job, "agent_id": agent,
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    }

    async fn poll_active(
        app: &App<TestGraphStore>,
        agent: Uuid,
        jobs: Vec<Uuid>,
    ) -> Result<crate::api::PollJobsResponse, StatusCode> {
        let response = crate::api::router(app.clone())
            .oneshot(
                Request::post("/jobs/poll")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "agent_id": agent, "batch": 1,
                            "resources": { "cpu_cores": 0, "memory_bytes": 0 },
                            "active_jobs": jobs,
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        if response.status() != StatusCode::OK {
            return Err(response.status());
        }
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        Ok(serde_json::from_slice(&body).unwrap())
    }

    #[tokio::test]
    async fn job_actions_validate_bodies_and_replace_the_status_endpoint() {
        let Some(db) = setup().await else {
            return;
        };
        let app = App::new(db, Config::default(), TestGraphStore::default());
        for action in ["yield", "succeed", "fail", "terminate"] {
            for body in [
                serde_json::json!({}),
                serde_json::json!({"job_id": Uuid::new_v4()}),
                serde_json::json!({"job_id": "invalid", "agent_id": Uuid::new_v4()}),
            ] {
                let response = crate::api::router(app.clone())
                    .oneshot(
                        Request::post(format!("/job/{action}"))
                            .header("content-type", "application/json")
                            .body(Body::from(serde_json::to_vec(&body).unwrap()))
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
            }
        }
        let response = crate::api::router(app)
            .oneshot(
                Request::post(format!("/jobs/{}/status", Uuid::new_v4()))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({"agent_id": Uuid::new_v4(), "status": "Polled"})
                            .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn polling_returns_jobs_with_loaded_execution_relations() {
        let Some(mut db) = setup().await else {
            return;
        };
        let app = App::new(db.clone(), Config::default(), TestGraphStore::default());
        let agent = Uuid::new_v4();
        let (task, job) = configured_job(&mut db).await;
        for _ in 0..2 {
            let response = crate::api::router(app.clone())
                .oneshot(
                    Request::post("/jobs/poll")
                        .header("content-type", "application/json")
                        .body(Body::from(
                            serde_json::json!({
                                "agent_id": agent, "batch": 1,
                                "resources": { "cpu_cores": 1, "memory_bytes": 512 },
                            })
                            .to_string(),
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
            let entry = &json["jobs"][0];
            assert_eq!(entry["uuid"], job.uuid.to_string());
            for removed in ["job", "context", "config", "lease_ms"] {
                assert!(entry.get(removed).is_none());
            }
            let response: crate::api::PollJobsResponse = serde_json::from_value(json).unwrap();
            assert_eq!(response.jobs.len(), 1);
            assert!(response.renewed_jobs.is_empty());
            let delivered = &response.jobs[0];
            assert_eq!(delivered.run.get().uuid, job.run_id);
            assert_eq!(delivered.run.get().graph_id, task.graph_id);
            assert_eq!(delivered.run.get().version, 5);
            assert_eq!(delivered.node_run.get().id, task.node_run_id);
            assert_eq!(delivered.node_run.get().node_id, task.node_id);
            assert_eq!(delivered.node_run.get().revision, 3);
            assert_eq!(
                delivered.node_config.get().as_ref().unwrap().id,
                job.node_config_id
            );
            assert_eq!(
                delivered.node_config.get().as_ref().unwrap().config,
                serde_json::json!({"interpreter": "sh", "source": "exit 0"})
            );
            assert_eq!(delivered.job_lease().unwrap(), Duration::from_secs(30));
        }
    }

    #[tokio::test]
    async fn polling_acknowledges_only_owned_live_active_claims() {
        let Some(mut db) = setup().await else {
            return;
        };
        let app = App::new(db.clone(), Config::default(), TestGraphStore::default());
        let agent = Uuid::new_v4();
        let (_, owned) = configured_job(&mut db).await;
        let (_, expired) = configured_job(&mut db).await;
        let (_, foreign) = configured_job(&mut db).await;
        let (_, terminal) = configured_job(&mut db).await;
        let now = Timestamp::now();
        for job in [&owned, &expired, &foreign, &terminal] {
            Job::filter_by_id(job.id)
                .update()
                .status(Status::Polled)
                .agent_id(Some(if job.uuid == foreign.uuid {
                    Uuid::new_v4()
                } else {
                    agent
                }))
                .polled_at(Some(now))
                .expired_at(Some(if job.uuid == expired.uuid {
                    now - Duration::from_secs(1)
                } else {
                    now + Duration::from_secs(30)
                }))
                .exec(&mut db)
                .await
                .unwrap();
        }
        app.yield_job(terminal.uuid, agent).await.unwrap();
        let response = poll_active(
            &app,
            agent,
            vec![
                owned.uuid,
                owned.uuid,
                expired.uuid,
                foreign.uuid,
                terminal.uuid,
                Uuid::new_v4(),
            ],
        )
        .await
        .unwrap();
        assert!(response.jobs.is_empty());
        assert_eq!(response.renewed_jobs, vec![owned.uuid]);
        assert_eq!(
            Job::filter_by_id(expired.id)
                .get(&mut db)
                .await
                .unwrap()
                .status,
            Status::Scheduled
        );
        assert_eq!(TaskEventEntry::all().exec(&mut db).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn yielding_releases_resources_and_only_emits_one_yield_event() {
        let Some(mut db) = setup().await else {
            return;
        };
        let app = App::new(db.clone(), Config::default(), TestGraphStore::default());
        let agent = Uuid::new_v4();
        let capacity = ResourceCapacity {
            cpu_cores: 1,
            memory_bytes: 512,
        };
        let (task, job) = configured_job(&mut db).await;
        app.poll(&mut AgentRef::new(agent, capacity, vec![]), 1)
            .await
            .unwrap();
        assert_eq!(
            report(&app, job.uuid, Uuid::new_v4(), Status::Yielded).await,
            StatusCode::CONFLICT
        );
        for _ in 0..2 {
            assert_eq!(
                report(&app, job.uuid, agent, Status::Yielded).await,
                StatusCode::NO_CONTENT
            );
        }
        let yielded = Job::filter_by_id(job.id).get(&mut db).await.unwrap();
        assert_eq!(yielded.status, Status::Yielded);
        assert!(
            yielded.agent_id.is_none()
                && yielded.polled_at.is_none()
                && yielded.expired_at.is_none()
        );
        let events = TaskEventEntry::filter_by_task_id(task.id)
            .exec(&mut db)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event, TaskEvent::Yield);
        for status in [Status::Succeeded, Status::Failed, Status::Terminated] {
            assert_eq!(
                report(&app, job.uuid, agent, status).await,
                StatusCode::CONFLICT
            );
        }
        assert_eq!(AgentScheduler::new().schedule(&mut db).await.unwrap(), 0);
        let (_, next) = configured_job(&mut db).await;
        let jobs = app
            .poll(&mut AgentRef::new(agent, capacity, vec![job.uuid]), 1)
            .await
            .unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].uuid, next.uuid);
    }

    #[tokio::test]
    async fn missing_leases_return_http_errors_without_committing_claims() {
        let Some(mut db) = setup().await else {
            return;
        };
        let app = App::new(db.clone(), Config::default(), TestGraphStore::default());
        let (_, valid) = configured_job(&mut db).await;
        let (_, invalid) = configured_job(&mut db).await;
        NodeConfig::filter_by_id(invalid.node_config_id)
            .update()
            .lease(None)
            .exec(&mut db)
            .await
            .unwrap();
        let agent = Uuid::new_v4();
        let response = crate::api::router(app.clone())
            .oneshot(
                Request::post("/jobs/poll")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "agent_id": agent, "batch": 2,
                            "resources": { "cpu_cores": 2, "memory_bytes": 1024 },
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            body["error"],
            format!(
                "job lease not found for node configuration: {}",
                invalid.node_config_id
            )
        );
        for job in [&valid, &invalid] {
            let unchanged = Job::filter_by_id(job.id).get(&mut db).await.unwrap();
            assert_eq!(unchanged.status, Status::Scheduled);
            assert!(unchanged.agent_id.is_none() && unchanged.polled_at.is_none());
        }
        Job::filter_by_id(invalid.id)
            .update()
            .status(Status::Polled)
            .agent_id(Some(agent))
            .polled_at(Some(Timestamp::now()))
            .exec(&mut db)
            .await
            .unwrap();
        assert_eq!(
            poll_active(&app, agent, vec![invalid.uuid])
                .await
                .unwrap_err(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert!(
            TaskEventEntry::all()
                .exec(&mut db)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn delivery_context_failure_rolls_back_claims() {
        let Some(mut db) = setup().await else {
            return;
        };
        let app = App::new(db.clone(), Config::default(), TestGraphStore::default());
        let (_, job) = configured_job(&mut db).await;
        Job::filter_by_id(job.id)
            .update()
            .run_id(Uuid::new_v4())
            .exec(&mut db)
            .await
            .unwrap();
        let mut agent = AgentRef::new(
            Uuid::new_v4(),
            ResourceCapacity {
                cpu_cores: 1,
                memory_bytes: 512,
            },
            Vec::new(),
        );
        assert!(app.poll(&mut agent, 1).await.is_err());
        let unchanged = Job::filter_by_id(job.id).get(&mut db).await.unwrap();
        assert_eq!(unchanged.status, Status::Scheduled);
        assert!(unchanged.agent_id.is_none() && unchanged.polled_at.is_none());
    }

    #[tokio::test]
    async fn node_leases_control_delivery_renewal_reports_and_recovery() {
        let Some(mut db) = setup().await else {
            eprintln!("skipping: set DATABASE_URL to run MySQL node lease tests");
            return;
        };
        let app = App::new(db.clone(), Config::default(), TestGraphStore::default());
        let agent = Uuid::new_v4();
        let capacity = ResourceCapacity {
            cpu_cores: 3,
            memory_bytes: 1536,
        };
        let (_, short) = configured_job_with_lease(&mut db, Some(5_000)).await;
        let (task, long) = configured_job_with_lease(&mut db, Some(120_000)).await;
        let (_, medium) = configured_job(&mut db).await;

        // A new node version must not change an existing job's pinned lease.
        let now = Timestamp::now();
        NodeConfig::create()
            .node_id(task.node_id)
            .version(2)
            .annotation_match(String::new())
            .config(serde_json::json!({}))
            .lease(Some(1_000))
            .required_cpu_cores(1)
            .required_memory_bytes(512)
            .created_at(now)
            .updated_at(now)
            .created_by(String::new())
            .updated_by(String::new())
            .exec(&mut db)
            .await
            .unwrap();

        let polled = app
            .poll(&mut AgentRef::new(agent, capacity, vec![]), 3)
            .await
            .unwrap();
        assert_eq!(polled.len(), 3);
        for (job, expected) in [(&short, 5_000), (&long, 120_000), (&medium, 30_000)] {
            let delivered = polled.iter().find(|entry| entry.uuid == job.uuid).unwrap();
            assert_eq!(
                delivered.job_lease().unwrap(),
                Duration::from_millis(expected)
            );
            assert_eq!(
                delivered.expired_at,
                Some(delivered.polled_at.unwrap() + Duration::from_millis(expected))
            );
        }
        for (job, age, lease) in [(&short, 10, 5), (&long, 60, 120), (&medium, 10, 30)] {
            let polled_at = Timestamp::now() - Duration::from_secs(age);
            Job::filter_by_id(job.id)
                .update()
                .polled_at(Some(polled_at))
                .expired_at(Some(polled_at + Duration::from_secs(lease)))
                .exec(&mut db)
                .await
                .unwrap();
        }
        assert_eq!(
            report(&app, short.uuid, agent, Status::Succeeded).await,
            StatusCode::CONFLICT
        );
        // A long lease remains valid while shorter leases expire.
        let redelivered = app
            .poll(&mut AgentRef::new(agent, capacity, vec![]), 3)
            .await
            .unwrap();
        assert_eq!(redelivered.len(), 3);
        assert!(redelivered.iter().any(|entry| entry.uuid == long.uuid));
        assert_eq!(
            poll_active(&app, agent, vec![short.uuid, long.uuid, medium.uuid])
                .await
                .unwrap()
                .renewed_jobs
                .len(),
            3
        );
        for (job, age, lease) in [(&short, 10, 5), (&long, 60, 120), (&medium, 10, 30)] {
            let polled_at = Timestamp::now() - Duration::from_secs(age);
            Job::filter_by_id(job.id)
                .update()
                .polled_at(Some(polled_at))
                .expired_at(Some(polled_at + Duration::from_secs(lease)))
                .exec(&mut db)
                .await
                .unwrap();
        }
        assert_eq!(AgentScheduler::new().schedule(&mut db).await.unwrap(), 1);
        assert_eq!(
            Job::filter_by_id(short.id)
                .get(&mut db)
                .await
                .unwrap()
                .status,
            Status::Scheduled
        );
        assert_eq!(
            Job::filter_by_id(long.id)
                .get(&mut db)
                .await
                .unwrap()
                .status,
            Status::Polled
        );
        assert_eq!(
            Job::filter_by_id(medium.id)
                .get(&mut db)
                .await
                .unwrap()
                .status,
            Status::Polled
        );

        // Active polls renew the long claim without redelivering it.
        let redelivered: Vec<_> = AgentRef::new(agent, capacity, vec![long.uuid])
            .poll(&mut db, 0)
            .await
            .unwrap()
            .collect();
        assert!(redelivered.is_empty());
        let renewed = Job::filter_by_id(long.id).get(&mut db).await.unwrap();
        assert!(renewed.polled_at.unwrap() > Timestamp::now() - Duration::from_secs(30));
        assert_eq!(
            renewed.expired_at,
            Some(renewed.polled_at.unwrap() + Duration::from_secs(120))
        );
        Job::filter_by_id(long.id)
            .update()
            .polled_at(Some(Timestamp::now() - Duration::from_secs(60)))
            .expired_at(Some(Timestamp::now() + Duration::from_secs(60)))
            .exec(&mut db)
            .await
            .unwrap();
        assert_eq!(
            report(&app, long.uuid, agent, Status::Succeeded).await,
            StatusCode::NO_CONTENT
        );

        // The polling endpoint reclaims short leases independently of other leases.
        let next_agent = Uuid::new_v4();
        app.poll(&mut AgentRef::new(next_agent, capacity, vec![]), 3)
            .await
            .unwrap();
        assert_eq!(
            poll_active(&app, next_agent, vec![short.uuid])
                .await
                .unwrap()
                .renewed_jobs,
            vec![short.uuid]
        );
        Job::filter_by_id(short.id)
            .update()
            .polled_at(Some(Timestamp::now() - Duration::from_secs(10)))
            .expired_at(Some(Timestamp::now() - Duration::from_secs(5)))
            .exec(&mut db)
            .await
            .unwrap();
        assert_eq!(
            report(&app, short.uuid, next_agent, Status::Failed).await,
            StatusCode::CONFLICT
        );
        let recovered = app
            .poll(&mut AgentRef::new(Uuid::new_v4(), capacity, vec![]), 3)
            .await
            .unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].uuid, short.uuid);
        assert_eq!(
            recovered[0].job_lease().unwrap(),
            Duration::from_millis(5_000)
        );
    }

    #[tokio::test]
    async fn concurrent_job_actions_emit_only_the_winning_status_event() {
        let Some(mut db) = setup().await else {
            return;
        };
        let app = App::new(db.clone(), Config::default(), TestGraphStore::default());
        let agent = Uuid::new_v4();
        let (task, job) = configured_job(&mut db).await;
        app.poll(
            &mut AgentRef::new(
                agent,
                ResourceCapacity {
                    cpu_cores: 1,
                    memory_bytes: 512,
                },
                vec![],
            ),
            1,
        )
        .await
        .unwrap();
        let (yielded, succeeded, failed, terminated) = tokio::join!(
            report(&app, job.uuid, agent, Status::Yielded),
            report(&app, job.uuid, agent, Status::Succeeded),
            report(&app, job.uuid, agent, Status::Failed),
            report(&app, job.uuid, agent, Status::Terminated),
        );
        let actions = [
            (yielded, Status::Yielded, TaskEvent::Yield),
            (succeeded, Status::Succeeded, TaskEvent::Succeed),
            (failed, Status::Failed, TaskEvent::Fail),
            (terminated, Status::Terminated, TaskEvent::Terminate),
        ];
        assert_eq!(
            actions
                .iter()
                .filter(|(response, _, _)| *response == StatusCode::NO_CONTENT)
                .count(),
            1
        );
        let settled = Job::filter_by_id(job.id).get(&mut db).await.unwrap();
        let events = TaskEventEntry::filter_by_task_id(task.id)
            .exec(&mut db)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        for (response, status, event) in actions {
            if response == StatusCode::NO_CONTENT {
                assert_eq!(settled.status, status);
                assert_eq!(events[0].event, event);
                assert_eq!(
                    report(&app, job.uuid, agent, status).await,
                    StatusCode::NO_CONTENT
                );
            } else {
                assert_eq!(response, StatusCode::CONFLICT);
            }
        }
        assert_eq!(
            TaskEventEntry::filter_by_task_id(task.id)
                .exec(&mut db)
                .await
                .unwrap()
                .len(),
            1
        );
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
        let polled = app
            .poll(&mut AgentRef::new(agent, capacity, vec![]), 1)
            .await
            .unwrap();
        assert_eq!(polled.len(), 1);
        assert_eq!(polled[0].run.get().version, 5);
        assert_eq!(
            polled[0].node_run.get().revision,
            3,
            "use the node revision, not the current run revision"
        );
        assert_eq!(polled[0].node_run.get().node_id, task.node_id);
        assert_eq!(
            polled[0].node_config.get().as_ref().unwrap().config,
            serde_json::json!({"interpreter": "sh", "source": "exit 0"})
        );
        assert_eq!(
            report(&app, job.uuid, Uuid::new_v4(), Status::Succeeded).await,
            StatusCode::CONFLICT
        );
        assert_eq!(
            report(&app, Uuid::new_v4(), agent, Status::Succeeded).await,
            StatusCode::NOT_FOUND
        );
        for _ in 0..2 {
            assert_eq!(
                poll_active(&app, agent, vec![job.uuid])
                    .await
                    .unwrap()
                    .renewed_jobs,
                vec![job.uuid]
            );
        }
        let renewed = Job::filter_by_uuid(job.uuid).get(&mut db).await.unwrap();
        assert_eq!(renewed.status, Status::Polled);
        assert_eq!(renewed.agent_id, Some(agent));
        assert_eq!(
            renewed.expired_at,
            Some(renewed.polled_at.unwrap() + Duration::from_secs(30))
        );
        assert!(
            app.poll(&mut AgentRef::new(agent, capacity, vec![job.uuid]), 1)
                .await
                .unwrap()
                .is_empty()
        );

        // Reclaim a running job; confirming its new claim emits no task event.
        let expired = Timestamp::now() - Duration::from_secs(60);
        Job::filter_by_id(job.id)
            .update()
            .polled_at(Some(expired))
            .expired_at(Some(expired + Duration::from_secs(30)))
            .exec(&mut db)
            .await
            .unwrap();
        assert_eq!(
            report(&app, job.uuid, agent, Status::Succeeded).await,
            StatusCode::CONFLICT
        );
        let new_agent = Uuid::new_v4();
        assert_eq!(
            app.poll(&mut AgentRef::new(new_agent, capacity, vec![]), 1)
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
            poll_active(&app, new_agent, vec![job.uuid])
                .await
                .unwrap()
                .renewed_jobs,
            vec![job.uuid]
        );
        for _ in 0..2 {
            assert_eq!(
                report(&app, job.uuid, new_agent, Status::Succeeded).await,
                StatusCode::NO_CONTENT
            );
        }
        assert!(
            poll_active(&app, new_agent, vec![job.uuid])
                .await
                .unwrap()
                .renewed_jobs
                .is_empty()
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
            vec![TaskEvent::Succeed]
        );
        assert!(
            events
                .iter()
                .all(|entry| entry.id != task.id && entry.node_run_id == task.node_run_id)
        );
        assert_eq!(events.len(), 1);
        let settled = Job::filter_by_id(job.id).get(&mut db).await.unwrap();
        assert_eq!(settled.status, Status::Succeeded);
        assert!(settled.agent_id.is_none() && settled.polled_at.is_none());
        assert!(settled.expired_at.is_none());

        for (status, event) in [
            (Status::Failed, TaskEvent::Fail),
            (Status::Terminated, TaskEvent::Terminate),
        ] {
            let (task, job) = configured_job(&mut db).await;
            app.poll(&mut AgentRef::new(agent, capacity, vec![]), 1)
                .await
                .unwrap();
            assert_eq!(
                report(&app, job.uuid, Uuid::new_v4(), status).await,
                StatusCode::CONFLICT
            );
            for _ in 0..2 {
                assert_eq!(
                    report(&app, job.uuid, agent, status).await,
                    StatusCode::NO_CONTENT
                );
            }
            let events = TaskEventEntry::filter_by_task_id(task.id)
                .exec(&mut db)
                .await
                .unwrap();
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].event, event);
        }

        let (task, job) = configured_job(&mut db).await;
        app.poll(&mut AgentRef::new(agent, capacity, vec![]), 1)
            .await
            .unwrap();
        let (renewed, terminal) = tokio::join!(
            poll_active(&app, agent, vec![job.uuid]),
            report(&app, job.uuid, agent, Status::Terminated),
        );
        let renewed = renewed.unwrap();
        assert!(renewed.renewed_jobs.is_empty() || renewed.renewed_jobs == vec![job.uuid]);
        assert_eq!(terminal, StatusCode::NO_CONTENT);
        let events = TaskEventEntry::filter_by_task_id(task.id)
            .order_by(TaskEventEntry::fields().event_offset().asc())
            .exec(&mut db)
            .await
            .unwrap();
        let events = events.iter().map(|entry| entry.event).collect::<Vec<_>>();
        assert!(events == vec![TaskEvent::Terminate]);

        // A failed event append must roll back the job status update too.
        let missing_task = Task::new(Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4(), 0);
        let mut orphan = Job::new(&missing_task, Uuid::new_v4(), job.node_config_id, 0);
        orphan.agent_id = Some(agent);
        orphan.polled_at = Some(Timestamp::now());
        orphan.expired_at = Some(Timestamp::now() + Duration::from_secs(30));
        orphan.status = Status::Polled;
        orphan.write(&mut db).await.unwrap();
        assert!(app.succeed_job(orphan.uuid, agent).await.is_err());
        assert!(
            TaskEventEntry::filter_by_task_id(missing_task.id)
                .exec(&mut db)
                .await
                .unwrap()
                .is_empty()
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
            .expired_at(None)
            .exec(&mut db)
            .await
            .unwrap();
        // Polling without active executions or redelivery must not renew claims.
        let (_, job) = configured_job(&mut db).await;
        app.poll(&mut AgentRef::new(agent, capacity, vec![]), 1)
            .await
            .unwrap();
        assert_eq!(
            poll_active(&app, agent, vec![job.uuid])
                .await
                .unwrap()
                .renewed_jobs,
            vec![job.uuid]
        );
        let before = Job::filter_by_id(job.id)
            .get(&mut db)
            .await
            .unwrap()
            .polled_at;
        app.poll(&mut AgentRef::new(agent, capacity, vec![]), 0)
            .await
            .unwrap();
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
            .expired_at(Some(expired + Duration::from_secs(30)))
            .exec(&mut db)
            .await
            .unwrap();
        assert_eq!(AgentScheduler::new().schedule(&mut db).await.unwrap(), 1);
    }
}
