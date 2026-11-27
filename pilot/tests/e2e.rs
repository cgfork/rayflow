#[path = "../../server/tests/support/mod.rs"]
mod support;

use std::{env, future::Future, time::Duration};

use agent::{
    Agentic, JobManager, StopReason,
    client::Client,
    containerd::{Container, ContainerState, Containerd, docker::DockerContainerd},
    settings::Settings,
};
use bollard::exec::{CreateExecOptions, StartExecOptions};
use coordinator::mysql::MySqlLeaderElection;
use graph::types::{DAG, Node, NodeSettings, Resource};
use pilot::Pilot;
use reqwest::{Method, StatusCode};
use runtime::{backend::Backend, command::NodeRun};
use scheduler::{Job, ResourceCapacity, actuator::Status};
use serde_json::{Value, json};
use toasty::Db;
use tokio::{net::TcpListener, task::JoinSet, time};
use uuid::Uuid;

const WAIT: Duration = Duration::from_secs(45);
const POLL: Duration = Duration::from_millis(150);
const MEMORY: u64 = 64 * 1024 * 1024;

#[tokio::test]
#[ignore = "requires MySQL and a local Docker image with sh and sleep"]
async fn docker_workflow_yields_resumes_and_propagates_results() {
    support::with_test_database(run_workflow).await;
}

async fn run_workflow(mut db: Db) {
    let image = env::var("RAYFLOW_TEST_DOCKER_IMAGE").expect("set RAYFLOW_TEST_DOCKER_IMAGE");
    let docker = DockerContainerd::connect_with_defaults().unwrap();
    docker
        .docker()
        .inspect_image(&image)
        .await
        .expect("the test Docker image must already be available");
    let workdir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let address = listener.local_addr().unwrap();
    let server_url = format!("http://{address}");
    let config = server::Config {
        addr: address,
        poll_timeout: Duration::from_millis(300),
        poll_retry_interval: Duration::from_millis(50),
        ..server::Config::default()
    };
    let router = server::router(server::App::new(db.clone(), config, db.clone()));
    let election = MySqlLeaderElection::new(
        db.clone(),
        "docker-e2e",
        "test-pilot",
        Duration::from_secs(20),
    );
    let mut pilot = Pilot::new(election, Backend::new(db.clone(), db.clone()), 16, POLL);
    let mut settings = Settings::new(
        Uuid::new_v4(),
        &server_url,
        workdir.path().join("jobs"),
        ResourceCapacity {
            cpu_cores: 2,
            memory_bytes: MEMORY * 2,
        },
    );
    settings.poll_interval = POLL;
    settings.request_timeout = Duration::from_secs(5);
    let client = Client::new(settings).unwrap();
    let manager = JobManager::open(docker.clone(), client.clone(), client)
        .await
        .unwrap();
    let agent = Agentic::with_manager(manager.clone());

    let mut scenario = JoinSet::new();
    scenario.spawn(exercise_workflow(
        db.clone(),
        docker.clone(),
        manager,
        format!("{server_url}/api/v1"),
        image,
    ));
    let outcome = tokio::select! {
        result = scenario.join_next() => result.unwrap().map_err(|error| error.to_string()),
        result = axum::serve(listener, router) => Err(format!("HTTP server exited: {result:?}")),
        result = pilot.run(std::future::pending()) => Err(format!("pilot exited: {result:?}")),
        result = agent.start() => Err(format!("agent exited: {result:?}")),
    };
    scenario.shutdown().await;

    // Only containers named for jobs in this test's isolated schema can match.
    for job in Job::all().exec(&mut db).await.unwrap() {
        docker
            .container(&container_name(job.uuid))
            .remove()
            .await
            .unwrap();
    }
    outcome.expect("Docker workflow scenario");
}

async fn exercise_workflow(
    db: Db,
    docker: DockerContainerd,
    manager: JobManager<DockerContainerd, Client>,
    api: String,
    image: String,
) {
    let http = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let flow = workflow(&image);
    let graph_id = flow.id;
    let [first, second, third, last] =
        <[Uuid; 4]>::try_from(flow.nodes.iter().map(|node| node.id).collect::<Vec<_>>()).unwrap();
    send(
        &http,
        &api,
        Method::PUT,
        &format!("/graphs/{graph_id}/draft"),
        json!({"graph": flow, "actor": "docker-e2e"}),
        StatusCode::OK,
    )
    .await;
    send(
        &http,
        &api,
        Method::POST,
        &format!("/graphs/{graph_id}/publish"),
        json!({"actor": "docker-e2e"}),
        StatusCode::CREATED,
    )
    .await;
    let data_id = Uuid::new_v4();
    send(
        &http,
        &api,
        Method::POST,
        "/runs/start",
        json!({"data_id": data_id, "graph_id": graph_id}),
        StatusCode::ACCEPTED,
    )
    .await;
    let initial = wait_for_running_job(&db, &docker, None).await;
    let run_id = initial.run_id;
    manager
        .stop(initial.uuid, StopReason::Yielded)
        .await
        .unwrap();
    let pending = wait_for_nodes(&http, &api, run_id, &[(first, "Pending")]).await;
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].0.uuid, initial.node_run_id);
    let yielded = Job::filter_by_uuid(initial.uuid)
        .get(&mut db.clone())
        .await
        .unwrap();
    assert_eq!(yielded.status, Status::Yielded);
    assert_eq!(yielded.agent_id, None);

    send(
        &http,
        &api,
        Method::POST,
        "/runs/resume",
        json!({"data_id": data_id, "graph_id": graph_id, "node": first}),
        StatusCode::ACCEPTED,
    )
    .await;
    let resumed = wait_for_running_job(&db, &docker, Some(yielded.uuid)).await;
    assert_eq!(resumed.node_run_id, initial.node_run_id);
    assert_ne!(resumed.task_id, yielded.task_id);
    let exec = docker
        .docker()
        .create_exec(
            &container_name(resumed.uuid),
            CreateExecOptions {
                cmd: Some(vec!["sh", "-c", ": > /tmp/rayflow-e2e-continue"]),
                ..CreateExecOptions::default()
            },
        )
        .await
        .unwrap();
    docker
        .docker()
        .start_exec(
            &exec.id,
            Some(StartExecOptions {
                detach: true,
                ..StartExecOptions::default()
            }),
        )
        .await
        .unwrap();
    let nodes = wait_for_nodes(
        &http,
        &api,
        run_id,
        &[
            (first, "Succeeded"),
            (second, "Succeeded"),
            (third, "Failed"),
        ],
    )
    .await;
    assert!(!nodes.iter().any(|(node_run, _)| node_run.node_id == last));
    let jobs = Job::filter_by_run_id(run_id)
        .order_by(Job::fields().id().asc())
        .exec(&mut db.clone())
        .await
        .unwrap();
    assert_eq!(
        jobs.iter().map(|job| job.status).collect::<Vec<_>>(),
        [
            Status::Yielded,
            Status::Succeeded,
            Status::Succeeded,
            Status::Failed
        ],
    );
}

fn workflow(image: &str) -> DAG {
    let steps = [
        (
            "yieldable",
            "trap 'exit 0' TERM; while [ ! -e /tmp/rayflow-e2e-continue ]; do sleep 1; done",
        ),
        ("successor", "exit 0"),
        ("fails", "exit 7"),
        ("blocked", "exit 0"),
    ];
    let nodes: Vec<_> = steps
        .into_iter()
        .map(|(name, command)| Node {
            id: Uuid::new_v4(),
            name: name.into(),
            node_type: "docker".into(),
            assets: vec![],
            settings: NodeSettings {
                config: json!({"image": image, "command": ["sh", "-c", command]}),
                resource: Resource {
                    cpu_cores: 1,
                    memory_bytes: MEMORY,
                    gpu_memory_bytes: None,
                },
                timeout: Duration::from_secs(120),
                overrides: Default::default(),
            },
            inputs: vec![],
            outputs: vec![],
        })
        .collect();
    DAG {
        id: Uuid::new_v4(),
        name: "docker-workflow".into(),
        edges: nodes
            .windows(2)
            .map(|pair| (pair[0].id, pair[1].id))
            .collect(),
        nodes,
        ..DAG::default()
    }
}

fn container_name(job_id: Uuid) -> String {
    format!("rayflow-{job_id}")
}

async fn send(
    http: &reqwest::Client,
    api: &str,
    method: Method,
    path: &str,
    body: Value,
    expected: StatusCode,
) -> Value {
    let mut request = http.request(method.clone(), format!("{api}{path}"));
    if body != Value::Null {
        request = request.json(&body);
    }
    let response = request.send().await.unwrap();
    let status = response.status();
    let bytes = response.bytes().await.unwrap();
    assert_eq!(
        status,
        expected,
        "{method} {path}: {}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).unwrap()
}

async fn wait_for<T, F>(description: &str, mut check: impl FnMut() -> F) -> T
where
    F: Future<Output = Option<T>>,
{
    time::timeout(WAIT, async move {
        loop {
            if let Some(value) = check().await {
                return value;
            }
            time::sleep(POLL).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {description}"))
}

async fn wait_for_running_job(db: &Db, docker: &DockerContainerd, previous: Option<Uuid>) -> Job {
    wait_for("a running Docker container", || async {
        let mut query = Job::all().filter(Job::fields().status().eq(Status::Polled));
        if let Some(previous) = previous {
            query = query.filter(Job::fields().uuid().ne(previous));
        }
        let job = query.first().exec(&mut db.clone()).await.unwrap()?;
        let container = docker.container(&container_name(job.uuid));
        match container.inspect().await {
            Ok(ContainerState::Running { .. }) => Some(job),
            Ok(_) => None,
            Err(agent::errors::Error::Docker(
                bollard::errors::Error::DockerResponseServerError {
                    status_code: 404, ..
                },
            )) => None,
            Err(error) => panic!("cannot inspect test container: {error}"),
        }
    })
    .await
}

async fn wait_for_nodes(
    http: &reqwest::Client,
    api: &str,
    run_id: Uuid,
    statuses: &[(Uuid, &str)],
) -> Vec<(NodeRun, Node)> {
    let path = format!("/runs/{run_id}");
    wait_for(&format!("node states {statuses:?}"), || async {
        let mut view = send(http, api, Method::GET, &path, Value::Null, StatusCode::OK).await;
        let rows: Vec<(NodeRun, Node)> = serde_json::from_value(view["node_runs"].take()).unwrap();
        statuses
            .iter()
            .all(|(node_id, status)| {
                rows.iter()
                    .any(|(run, _)| run.node_id == *node_id && run.status == *status)
            })
            .then_some(rows)
    })
    .await
}
