//! Execute a claim while reporting status and checking that it remains owned.

use crate::{
    client::{Client, PolledJob},
    config::Config,
};
use executor::{Runnable, TaskState, script::Script};
use scheduler::actuator::Status;
use std::time::Duration;
use tokio::{
    sync::watch,
    time::{Instant, sleep, timeout_at},
};
use uuid::Uuid;

pub async fn execute(
    client: Client,
    config: Config,
    job: PolledJob,
    polled_at: Instant,
    mut shutdown: watch::Receiver<bool>,
) {
    let id = job.job.uuid;
    let lease = Duration::from_millis(job.lease_ms);
    let retry = Duration::from_millis(config.poll_interval_ms)
        .min((lease / 3).max(Duration::from_millis(1)));
    let mut deadline = polled_at + lease;
    let pending = tokio::select! {
        accepted = report(&client, id, config.agent_id, Status::Pending, lease, retry, &mut deadline) => accepted,
        _ = shutdown.changed() => {
            report(&client, id, config.agent_id, Status::Terminated, lease, retry, &mut deadline).await;
            return;
        }
    };
    if !pending {
        return;
    }
    println!("executing job {id} (task {})", job.job.task_id);
    let status = {
        let work = async {
            let script: Script = match serde_json::from_value(job.config) {
                Ok(script) => script,
                Err(error) => {
                    eprintln!("agent: invalid config for job {id}: {error}");
                    return Status::Failed;
                }
            };
            match script.run(job.context).await {
                Ok(state) if state.exit_code() == Some(0) => Status::Succeeded,
                Ok(_) => Status::Failed,
                Err(error) => {
                    eprintln!("agent: job {id} failed: {error}");
                    Status::Failed
                }
            }
        };
        let keep_alive = async {
            loop {
                sleep(retry).await;
                if !report(
                    &client,
                    id,
                    config.agent_id,
                    Status::Pending,
                    lease,
                    retry,
                    &mut deadline,
                )
                .await
                {
                    break;
                }
            }
        };
        tokio::select! {
            status = work => Some(status),
            _ = keep_alive => None,
            _ = shutdown.changed() => Some(Status::Terminated),
        }
        // Dropping work cancels the interpreter before reporting termination.
    };
    if let Some(status) = status {
        report(
            &client,
            id,
            config.agent_id,
            status,
            lease,
            retry,
            &mut deadline,
        )
        .await;
    }
}

/// Retry transient failures only while this execution still has a confirmed lease.
async fn report(
    client: &Client,
    job_id: Uuid,
    agent_id: Uuid,
    status: Status,
    lease: Duration,
    retry: Duration,
    deadline: &mut Instant,
) -> bool {
    loop {
        let started = Instant::now();
        if started >= *deadline {
            return false;
        }
        match timeout_at(*deadline, client.report_status(job_id, agent_id, status)).await {
            Ok(Ok(())) => {
                *deadline = started + lease;
                return true;
            }
            Ok(Err(error))
                if error.status().is_some_and(|status| {
                    status.is_client_error() && status != reqwest::StatusCode::TOO_MANY_REQUESTS
                }) =>
            {
                eprintln!("agent: status {status:?} for job {job_id} rejected: {error}");
                return false;
            }
            Ok(Err(error)) => {
                eprintln!("agent: status {status:?} for job {job_id} failed: {error}");
                tokio::select! {
                    _ = sleep(retry) => {},
                    _ = tokio::time::sleep_until(*deadline) => return false,
                }
            }
            Err(_) => {
                eprintln!("agent: lease expired for job {job_id}");
                return false;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, Router, extract::State, http::StatusCode, routing::post};
    use std::sync::{Arc, Mutex};
    use tokio::{net::TcpListener, task::JoinHandle, time::timeout};

    #[derive(Default)]
    struct Reports {
        statuses: Vec<Status>,
        reject_pending: bool,
        lose_claim: bool,
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
            Json(request): Json<server::api::ReportJobStatusRequest>,
        ) -> StatusCode {
            let mut reports = reports.lock().unwrap();
            reports.statuses.push(request.status);
            if request.status == Status::Pending
                && (reports.reject_pending || (reports.lose_claim && reports.statuses.len() > 1))
            {
                return StatusCode::CONFLICT;
            }
            if request.status.is_terminal() && reports.fail_terminal_once {
                reports.fail_terminal_once = false;
                return StatusCode::SERVICE_UNAVAILABLE;
            }
            StatusCode::NO_CONTENT
        }
        let reports = Arc::new(Mutex::new(reports));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let router = Router::new()
            .route("/jobs/{job_id}/status", post(report))
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

    fn job(config: serde_json::Value) -> PolledJob {
        let task = event::task::Task::new(Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4(), 1);
        let job = scheduler::Job::new(&task, Uuid::new_v4(), 1, 1);
        // Verify that the agent can decode the actual server response type.
        serde_json::from_value(
            serde_json::to_value(server::app::PolledJob {
                context: executor::JobContext {
                    run_id: job.run_id,
                    graph_id: task.graph_id,
                    version: 1,
                    node_id: task.node_id,
                    revision: 2,
                },
                job,
                config,
                lease_ms: 2000,
            })
            .unwrap(),
        )
        .unwrap()
    }

    async fn run(api: &Api, job: PolledJob) {
        let (_shutdown, receiver) = watch::channel(false);
        timeout(
            Duration::from_secs(3),
            execute(
                Client::new(&api.config).unwrap(),
                api.config.clone(),
                job,
                Instant::now(),
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
        assert_eq!(statuses.first(), Some(&Status::Pending));
        assert_eq!(
            &statuses[statuses.len() - 2..],
            &[Status::Succeeded, Status::Succeeded]
        );
        assert!(
            statuses[..statuses.len() - 2]
                .iter()
                .all(|status| *status == Status::Pending)
        );
        for payload in [
            serde_json::json!({"interpreter": "sh", "source": "exit 7"}),
            serde_json::json!({"interpreter": "/no-such-rayflow-interpreter", "source": "exit 0"}),
            serde_json::json!({}),
        ] {
            api.reports.lock().unwrap().statuses.clear();
            run(&api, job(payload)).await;
            let reports = api.reports.lock().unwrap();
            assert_eq!(reports.statuses, vec![Status::Pending, Status::Failed]);
        }
    }

    #[tokio::test]
    async fn rejected_claim_does_not_start_a_script() {
        let api = api(Reports {
            reject_pending: true,
            ..Reports::default()
        })
        .await;
        let marker = std::env::temp_dir().join(format!("rayflow-job-{}", Uuid::new_v4()));
        let source = format!("touch {}", marker.display());
        run(
            &api,
            job(serde_json::json!({"interpreter": "sh", "source": source})),
        )
        .await;
        assert!(!marker.exists());
        assert_eq!(api.reports.lock().unwrap().statuses, vec![Status::Pending]);
    }

    #[tokio::test]
    async fn shutdown_reports_terminated_and_stops_running_work() {
        let api = api(Reports::default()).await;
        let job = job(serde_json::json!({"interpreter": "sh", "source": "exec sleep 30"}));
        let (shutdown, receiver) = watch::channel(false);
        let execution = tokio::spawn(execute(
            Client::new(&api.config).unwrap(),
            api.config.clone(),
            job,
            Instant::now(),
            receiver,
        ));
        timeout(Duration::from_secs(2), async {
            while api.reports.lock().unwrap().statuses.len() < 2 {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
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
        let api = api(Reports {
            lose_claim: true,
            ..Reports::default()
        })
        .await;
        run(
            &api,
            job(serde_json::json!({"interpreter": "sh", "source": "exec sleep 30"})),
        )
        .await;
        assert_eq!(
            api.reports.lock().unwrap().statuses,
            vec![Status::Pending, Status::Pending]
        );
    }
}
