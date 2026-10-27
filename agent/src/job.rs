//! Execute a claim while reporting status and checking that it remains owned.

use crate::{
    client::{Client, JobAction},
    config::Config,
};
use executor::{JobContext, Runnable, TaskState, script::Script};
use std::time::Duration;
use tokio::{
    sync::watch,
    time::{Instant, sleep, sleep_until},
};
use uuid::Uuid;

pub async fn execute(
    client: Client,
    config: Config,
    context: JobContext,
    deadline: watch::Receiver<Option<Instant>>,
    mut shutdown: watch::Receiver<bool>,
) {
    let id = context.job.uuid;
    let agent_id = context.agent.id;
    let lease = match context.job.job_lease() {
        Ok(lease) => lease,
        Err(error) => {
            eprintln!("agent: invalid lease for job {id}: {error}");
            return;
        }
    };
    let retry = Duration::from_millis(config.poll_interval_ms)
        .min((lease / 3).max(Duration::from_millis(1)));
    if deadline
        .borrow()
        .is_none_or(|deadline| deadline <= Instant::now())
    {
        return;
    }
    println!("executing job {id} (task {})", context.job.task_id);
    let action = {
        let work = async {
            let job = &context.job;
            if job.run.is_unloaded() || job.node_run.is_unloaded() {
                eprintln!("agent: missing execution context for job {id}");
                return JobAction::Fail;
            }
            let config = &job.node_config.get().as_ref().unwrap().config;
            let script: Script = match serde_json::from_value(config.clone()) {
                Ok(script) => script,
                Err(error) => {
                    eprintln!("agent: invalid config for job {id}: {error}");
                    return JobAction::Fail;
                }
            };
            match script.run(context).await {
                Ok(state) => match state.event() {
                    Some(scheduler::TaskEvent::Yield) => JobAction::Yield,
                    _ if state.exit_code() == Some(0) => JobAction::Succeed,
                    _ => JobAction::Fail,
                },
                Err(error) => {
                    eprintln!("agent: job {id} failed: {error}");
                    JobAction::Fail
                }
            }
        };
        let stopped = async {
            if !*shutdown.borrow_and_update() {
                let _ = shutdown.changed().await;
            }
        };
        tokio::select! {
            biased;
            _ = lease_expired(deadline.clone()) => None,
            _ = stopped => Some(JobAction::Terminate),
            action = work => Some(action),
        }
        // Dropping work cancels the interpreter before reporting termination.
    };
    if let Some(action) = action {
        tokio::select! {
            biased;
            _ = lease_expired(deadline) => {},
            _ = report(&client, id, agent_id, action, retry) => {},
        }
    }
}

async fn lease_expired(mut deadline: watch::Receiver<Option<Instant>>) {
    loop {
        let Some(expires) = *deadline.borrow_and_update() else {
            return;
        };
        tokio::select! {
            biased;
            _ = sleep_until(expires) => return,
            changed = deadline.changed() => {
                if changed.is_err() {
                    return;
                }
            }
        }
    }
}

async fn report(client: &Client, job_id: Uuid, agent_id: Uuid, action: JobAction, retry: Duration) {
    loop {
        match client.finish_job(job_id, agent_id, action).await {
            Ok(()) => return,
            Err(error)
                if error.status().is_some_and(|status| {
                    status.is_client_error() && status != reqwest::StatusCode::TOO_MANY_REQUESTS
                }) =>
            {
                eprintln!("agent: action {action:?} for job {job_id} rejected: {error}");
                return;
            }
            Err(error) => {
                eprintln!("agent: action {action:?} for job {job_id} failed: {error}");
                sleep(retry).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Json, Router,
        extract::{Path, State},
        http::StatusCode,
        routing::post,
    };
    use executor::Agent;
    use scheduler::{Job, ResourceCapacity, actuator::Status};
    use std::sync::{Arc, Mutex};
    use tokio::{net::TcpListener, task::JoinHandle, time::timeout};

    #[derive(Default)]
    struct Reports {
        statuses: Vec<Status>,
        agent_ids: Vec<Uuid>,
        fail_terminal_once: bool,
    }

    struct Api {
        reports: Arc<Mutex<Reports>>,
        config: Config,
        task: JoinHandle<()>,
    }

    impl Drop for Api {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn api(reports: Reports) -> Api {
        async fn report(
            State(reports): State<Arc<Mutex<Reports>>>,
            Path(action): Path<String>,
            Json(request): Json<server::api::JobActionRequest>,
        ) -> StatusCode {
            let mut reports = reports.lock().unwrap();
            assert!(!request.job_id.is_nil());
            assert!(!request.agent_id.is_nil());
            let status = match action.as_str() {
                "yield" => Status::Yielded,
                "succeed" => Status::Succeeded,
                "fail" => Status::Failed,
                "terminate" => Status::Terminated,
                _ => return StatusCode::NOT_FOUND,
            };
            reports.statuses.push(status);
            reports.agent_ids.push(request.agent_id);
            if reports.fail_terminal_once {
                reports.fail_terminal_once = false;
                return StatusCode::SERVICE_UNAVAILABLE;
            }
            StatusCode::NO_CONTENT
        }
        let reports = Arc::new(Mutex::new(reports));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let router = Router::new()
            .route("/job/{action}", post(report))
            .with_state(reports.clone());
        let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        Api {
            reports,
            config: Config {
                api_url: format!("http://{addr}"),
                poll_interval_ms: 20,
                request_timeout_ms: 100,
                ..Config::default()
            },
            task,
        }
    }

    fn job(config: serde_json::Value) -> Job {
        let task = event::task::Task::new(Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4(), 1);
        let mut job = Job::new(&task, Uuid::new_v4(), 1, 1);
        job.run = serde_json::from_value(serde_json::json!({
            "id": 1, "uuid": job.run_id, "data_id": task.data_id,
            "graph_id": task.graph_id, "version": 1, "revision": 7,
            "created_at": job.created_at, "updated_at": job.updated_at,
        }))
        .unwrap();
        job.node_run = serde_json::from_value(serde_json::json!({
            "id": task.node_run_id, "run_id": job.run_id, "data_id": task.data_id,
            "graph_id": task.graph_id, "node_id": task.node_id,
            "revision": 2, "status": "Running",
            "created_at": job.created_at, "updated_at": job.updated_at,
        }))
        .unwrap();
        job.node_config = Some(scheduler::actuator::NodeConfig {
            id: job.node_config_id,
            node_id: task.node_id,
            version: 1,
            annotation_match: String::new(),
            config,
            lease: Some(2000),
            required_cpu_cores: 1,
            required_memory_bytes: 512,
            created_at: job.created_at,
            updated_at: job.updated_at,
            created_by: String::new(),
            updated_by: String::new(),
        })
        .into();
        let response = server::api::PollJobsResponse {
            jobs: vec![job],
            renewed_jobs: vec![],
        };
        let mut decoded: crate::client::PollJobsResponse =
            serde_json::from_value(serde_json::to_value(response).unwrap()).unwrap();
        decoded.jobs.pop().unwrap()
    }

    fn context(api: &Api, job: Job) -> JobContext {
        JobContext {
            job,
            agent: Agent {
                id: api.config.agent_id,
                resources: ResourceCapacity {
                    cpu_cores: 2,
                    memory_bytes: 2048,
                },
            },
        }
    }

    async fn run(api: &Api, job: Job) {
        let (_shutdown, receiver) = watch::channel(false);
        let (_deadline, deadline) = watch::channel(Some(Instant::now() + job.job_lease().unwrap()));
        timeout(
            Duration::from_secs(3),
            execute(
                Client::new(&api.config).unwrap(),
                api.config.clone(),
                context(api, job),
                deadline,
                receiver,
            ),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn executes_scripts_and_retries_terminal_reports() {
        let api = api(Reports {
            fail_terminal_once: true,
            ..Reports::default()
        })
        .await;
        run(
            &api,
            job(serde_json::json!({"interpreter": "sh", "source": "exit 0"})),
        )
        .await;
        let statuses = api.reports.lock().unwrap().statuses.clone();
        assert_eq!(statuses, vec![Status::Succeeded, Status::Succeeded]);
        for payload in [
            serde_json::json!({"interpreter": "sh", "source": "exit 7"}),
            serde_json::json!({"interpreter": "/no-such-rayflow-interpreter", "source": "exit 0"}),
            serde_json::json!({}),
        ] {
            api.reports.lock().unwrap().statuses.clear();
            run(&api, job(payload)).await;
            let reports = api.reports.lock().unwrap();
            assert_eq!(reports.statuses, vec![Status::Failed]);
        }
    }

    #[tokio::test]
    async fn execution_reports_the_agent_from_the_job_context() {
        let api = api(Reports::default()).await;
        let mut context = context(
            &api,
            job(serde_json::json!({"interpreter": "sh", "source": "exit 0"})),
        );
        let agent_id = Uuid::new_v4();
        context.agent.id = agent_id;
        let (_shutdown, shutdown) = watch::channel(false);
        let (_deadline, deadline) =
            watch::channel(Some(Instant::now() + context.job.job_lease().unwrap()));
        timeout(
            Duration::from_secs(3),
            execute(
                Client::new(&api.config).unwrap(),
                api.config.clone(),
                context,
                deadline,
                shutdown,
            ),
        )
        .await
        .unwrap();
        let reports = api.reports.lock().unwrap();
        assert_eq!(reports.statuses, vec![Status::Succeeded]);
        assert_eq!(reports.agent_ids, vec![agent_id]);
    }

    #[tokio::test]
    async fn missing_job_relations_fail_without_executing_work() {
        let api = api(Reports::default()).await;
        for missing_run in [true, false] {
            let mut job = job(serde_json::json!({"interpreter": "sh", "source": "exec sleep 30"}));
            if missing_run {
                job.run.unload();
            } else {
                job.node_run.unload();
            }
            run(&api, job).await;
        }
        assert_eq!(
            api.reports.lock().unwrap().statuses,
            vec![Status::Failed, Status::Failed]
        );
    }

    #[tokio::test]
    async fn expired_claim_does_not_start_a_script() {
        let api = api(Reports::default()).await;
        let marker = std::env::temp_dir().join(format!("rayflow-job-{}", Uuid::new_v4()));
        let source = format!("touch {}", marker.display());
        let (_shutdown, shutdown) = watch::channel(false);
        let (_deadline, deadline) = watch::channel(Some(Instant::now() - Duration::from_millis(1)));
        execute(
            Client::new(&api.config).unwrap(),
            api.config.clone(),
            context(
                &api,
                job(serde_json::json!({"interpreter": "sh", "source": source})),
            ),
            deadline,
            shutdown,
        )
        .await;
        assert!(!marker.exists());
        assert!(api.reports.lock().unwrap().statuses.is_empty());
    }

    #[tokio::test]
    async fn shutdown_reports_terminated_and_stops_running_work() {
        let api = api(Reports::default()).await;
        let job = job(serde_json::json!({"interpreter": "sh", "source": "exec sleep 30"}));
        let (shutdown, receiver) = watch::channel(false);
        let (_deadline, deadline) = watch::channel(Some(Instant::now() + Duration::from_secs(2)));
        let execution = tokio::spawn(execute(
            Client::new(&api.config).unwrap(),
            api.config.clone(),
            context(&api, job),
            deadline,
            receiver,
        ));
        sleep(Duration::from_millis(50)).await;
        shutdown.send_replace(true);
        timeout(Duration::from_secs(2), execution)
            .await
            .unwrap()
            .unwrap();
        let reports = api.reports.lock().unwrap();
        assert_eq!(reports.statuses.last(), Some(&Status::Terminated));
        assert!(!reports.statuses.contains(&Status::Succeeded));
    }

    #[tokio::test]
    async fn lost_claim_stops_execution_without_a_terminal_report() {
        let api = api(Reports::default()).await;
        let (_shutdown, shutdown) = watch::channel(false);
        let (sender, deadline) = watch::channel(Some(Instant::now() + Duration::from_secs(2)));
        let execution = tokio::spawn(execute(
            Client::new(&api.config).unwrap(),
            api.config.clone(),
            context(
                &api,
                job(serde_json::json!({"interpreter": "sh", "source": "exec sleep 30"})),
            ),
            deadline,
            shutdown,
        ));
        sleep(Duration::from_millis(50)).await;
        sender.send_replace(None);
        timeout(Duration::from_secs(2), execution)
            .await
            .unwrap()
            .unwrap();
        assert!(api.reports.lock().unwrap().statuses.is_empty());
    }

    #[tokio::test]
    async fn polling_renewals_extend_the_execution_deadline() {
        let api = api(Reports::default()).await;
        let (_shutdown, shutdown) = watch::channel(false);
        let (sender, deadline) = watch::channel(Some(Instant::now() + Duration::from_secs(1)));
        let execution = tokio::spawn(execute(
            Client::new(&api.config).unwrap(),
            api.config.clone(),
            context(
                &api,
                job(serde_json::json!({"interpreter": "sh", "source": "sleep 1.2"})),
            ),
            deadline,
            shutdown,
        ));
        sleep(Duration::from_millis(100)).await;
        sender.send_replace(Some(Instant::now() + Duration::from_secs(2)));
        timeout(Duration::from_secs(3), execution)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            api.reports.lock().unwrap().statuses,
            vec![Status::Succeeded]
        );
    }

    #[tokio::test]
    async fn missing_poll_renewals_stop_execution_at_the_deadline() {
        let api = api(Reports::default()).await;
        let (_shutdown, shutdown) = watch::channel(false);
        let (_sender, deadline) = watch::channel(Some(Instant::now() + Duration::from_millis(100)));
        timeout(
            Duration::from_secs(2),
            execute(
                Client::new(&api.config).unwrap(),
                api.config.clone(),
                context(
                    &api,
                    job(serde_json::json!({"interpreter": "sh", "source": "exec sleep 30"})),
                ),
                deadline,
                shutdown,
            ),
        )
        .await
        .unwrap();
        assert!(api.reports.lock().unwrap().statuses.is_empty());
    }

    #[tokio::test]
    async fn client_posts_each_terminal_action() {
        let api = api(Reports::default()).await;
        let client = Client::new(&api.config).unwrap();
        for action in [
            JobAction::Yield,
            JobAction::Succeed,
            JobAction::Fail,
            JobAction::Terminate,
        ] {
            client
                .finish_job(Uuid::new_v4(), api.config.agent_id, action)
                .await
                .unwrap();
        }
        assert_eq!(
            api.reports.lock().unwrap().statuses,
            vec![
                Status::Yielded,
                Status::Succeeded,
                Status::Failed,
                Status::Terminated
            ]
        );
    }
}
