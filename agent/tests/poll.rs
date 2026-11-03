use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use agent::{
    Agentd, Agentic,
    client::{Client, PollRequest},
    containerd::docker::DockerContainerd,
    graphfs::GraphFs,
    jobfs::JobFs,
    settings::Settings,
};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{Query, State},
    http::{Method, StatusCode, Uri},
    response::{IntoResponse, Response},
};
use bollard::{API_DEFAULT_VERSION, Docker};
use event::task::Task;
use graph::types::{DAG, Node};
use jiff::Timestamp;
use scheduler::{Job, ResourceCapacity, actuator::NodeConfig};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::{
    net::TcpListener,
    sync::{Semaphore, broadcast, mpsc},
    task::JoinHandle,
    time::timeout,
};
use uuid::Uuid;

struct Event {
    path: String,
    body: Value,
}

struct MockState {
    jobs: Vec<Job>,
    events: mpsc::UnboundedSender<Event>,
    start: Semaphore,
    creates: AtomicUsize,
    fail_poll: AtomicBool,
    unavailable: AtomicBool,
    fail_report: AtomicBool,
    completed: Mutex<HashSet<Uuid>>,
    exit_code: i32,
    containers: Mutex<HashMap<String, Value>>,
    names: Mutex<HashMap<String, String>>,
    docker_events: broadcast::Sender<String>,
    missing_image: AtomicBool,
}

async fn handle(
    State(state): State<Arc<MockState>>,
    Query(query): Query<HashMap<String, String>>,
    method: Method,
    uri: Uri,
    body: Bytes,
) -> Response {
    let body: Value = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body).unwrap()
    };
    let path = uri
        .path()
        .strip_prefix(&format!("/v{API_DEFAULT_VERSION}"))
        .unwrap_or(uri.path());
    let _ = state.events.send(Event {
        path: path.to_owned(),
        body: body.clone(),
    });
    match path {
        "/events" => {
            let receiver = state.docker_events.subscribe();
            let stream = futures_util::stream::unfold(receiver, |mut receiver| async move {
                let event = receiver.recv().await.ok()?;
                if event.is_empty() {
                    return None;
                }
                Some((Ok::<_, std::io::Error>(event), receiver))
            });
            Body::from_stream(stream).into_response()
        }
        "/images/create" => {
            state.missing_image.store(false, Ordering::SeqCst);
            "{\"status\":\"Downloaded\"}\n".into_response()
        }
        path if path.starts_with("/images/") && state.missing_image.load(Ordering::SeqCst) => (
            StatusCode::NOT_FOUND,
            Json(json!({"message": "No such image"})),
        )
            .into_response(),
        path if path.starts_with("/images/") => Json(json!({"Id": "image"})).into_response(),
        "/jobs/poll" => {
            if state.unavailable.load(Ordering::SeqCst)
                || state.fail_poll.swap(false, Ordering::SeqCst)
            {
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
            let completed = state.completed.lock().unwrap();
            let jobs: Vec<_> = state
                .jobs
                .iter()
                .filter(|job| !completed.contains(&job.uuid))
                .collect();
            Json(json!({
                "jobs": jobs,
            }))
            .into_response()
        }
        "/containers/create" => {
            let name = query.get("name").unwrap();
            if state.names.lock().unwrap().contains_key(name) {
                return (
                    StatusCode::CONFLICT,
                    Json(json!({"message": "name already exists"})),
                )
                    .into_response();
            }
            let index = state.creates.fetch_add(1, Ordering::SeqCst);
            let id = format!("container-{index}");
            state
                .names
                .lock()
                .unwrap()
                .insert(name.to_owned(), id.clone());
            state
                .containers
                .lock()
                .unwrap()
                .insert(id.clone(), json!({"Status": "created", "Running": false}));
            (
                StatusCode::CREATED,
                Json(json!({ "Id": format!("container-{index}"), "Warnings": [] })),
            )
                .into_response()
        }
        path if path.ends_with("/start") => {
            state.start.acquire().await.unwrap().forget();
            let id = path
                .strip_prefix("/containers/")
                .unwrap()
                .strip_suffix("/start")
                .unwrap();
            let mut containers = state.containers.lock().unwrap();
            let Some(container) = containers.get_mut(id) else {
                return (StatusCode::NOT_FOUND, Json(json!({"message": "missing"})))
                    .into_response();
            };
            *container = json!({"Status": "running", "Running": true, "Pid": 123, "StartedAt": Timestamp::now().to_string()});
            StatusCode::NO_CONTENT.into_response()
        }
        path if path.ends_with("/kill") => {
            let id = path
                .strip_prefix("/containers/")
                .unwrap()
                .strip_suffix("/kill")
                .unwrap();
            let id = state
                .names
                .lock()
                .unwrap()
                .get(id)
                .cloned()
                .unwrap_or_else(|| id.to_owned());
            if let Some(container) = state.containers.lock().unwrap().get_mut(&id) {
                *container = json!({"Status": "exited", "Running": false, "ExitCode": 124});
            }
            StatusCode::NO_CONTENT.into_response()
        }
        path if path.starts_with("/containers/") && path.ends_with("/json") => {
            let name = path
                .strip_prefix("/containers/")
                .unwrap()
                .strip_suffix("/json")
                .unwrap();
            let id = state
                .names
                .lock()
                .unwrap()
                .get(name)
                .cloned()
                .unwrap_or_else(|| name.to_owned());
            match state.containers.lock().unwrap().get(&id) {
                Some(container) => Json(json!({"Id": id, "State": container})).into_response(),
                None => {
                    (StatusCode::NOT_FOUND, Json(json!({"message": "missing"}))).into_response()
                }
            }
        }
        path if path.starts_with("/containers/") && method == Method::DELETE => {
            let name = path.strip_prefix("/containers/").unwrap();
            let id = state
                .names
                .lock()
                .unwrap()
                .get(name)
                .cloned()
                .unwrap_or_else(|| name.to_owned());
            state.containers.lock().unwrap().remove(&id);
            state.names.lock().unwrap().retain(|_, value| value != &id);
            StatusCode::NO_CONTENT.into_response()
        }
        "/job/succeed" | "/job/fail" => {
            if state.unavailable.load(Ordering::SeqCst)
                || state.fail_report.swap(false, Ordering::SeqCst)
            {
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
            state
                .completed
                .lock()
                .unwrap()
                .insert(serde_json::from_value(body["job_id"].clone()).unwrap());
            StatusCode::NO_CONTENT.into_response()
        }
        _ => StatusCode::NOT_FOUND.into_response(),
    }
}

struct RunningAgent {
    state: Arc<MockState>,
    events: mpsc::UnboundedReceiver<Event>,
    server: JoinHandle<()>,
    agent: JoinHandle<Result<(), agent::errors::Error>>,
    workdir: TempDir,
    runtime: Arc<Agentic<DockerContainerd, GraphFs>>,
    settings: Settings,
}

impl RunningAgent {
    async fn new(jobs: Vec<Job>, exit_code: i32, fail_poll: bool, fail_report: bool) -> Self {
        let (sender, events) = mpsc::unbounded_channel();
        let state = Arc::new(MockState {
            jobs,
            events: sender,
            start: Semaphore::new(0),
            creates: AtomicUsize::new(0),
            fail_poll: AtomicBool::new(fail_poll),
            unavailable: AtomicBool::new(false),
            fail_report: AtomicBool::new(fail_report),
            completed: Mutex::new(HashSet::new()),
            exit_code,
            containers: Mutex::new(HashMap::new()),
            names: Mutex::new(HashMap::new()),
            docker_events: broadcast::channel(32).0,
            missing_image: AtomicBool::new(false),
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let router = Router::new().fallback(handle).with_state(state.clone());
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let workdir = tempfile::tempdir().unwrap();
        let mut settings = Settings::new(
            Uuid::new_v4(),
            address.clone(),
            workdir.path(),
            ResourceCapacity {
                cpu_cores: 4,
                memory_bytes: 4096,
            },
        );
        settings.poll_interval = Duration::from_millis(10);
        settings.request_timeout = Duration::from_secs(2);
        let docker = Docker::connect_with_http(&address, 5, API_DEFAULT_VERSION).unwrap();
        let client = Client::new(&settings).unwrap();
        let graph_fs = GraphFs::from_workdir(&settings.workdir);
        for job in &state.jobs {
            graph_fs.write_graph(job_graph(job)).await.unwrap();
        }
        let agentd = Agentd::open(DockerContainerd::new(docker), graph_fs, &settings.workdir)
            .await
            .unwrap();
        let runtime = Arc::new(Agentic::with_agent(agentd, client, settings.clone()).unwrap());
        let task = runtime.clone();
        let agent = tokio::task::spawn_local(async move { task.start().await });
        Self {
            state,
            events,
            server,
            agent,
            workdir,
            runtime,
            settings,
        }
    }

    async fn execution_agent(&self) -> Agentd<DockerContainerd, GraphFs> {
        let docker =
            Docker::connect_with_http(&self.settings.server_url, 5, API_DEFAULT_VERSION).unwrap();
        Agentd::open(
            DockerContainerd::new(docker),
            GraphFs::from_workdir(self.workdir.path()),
            self.workdir.path(),
        )
        .await
        .unwrap()
    }

    fn job_fs(&self, job_id: Uuid) -> JobFs {
        JobFs::new(self.workdir.path(), job_id)
    }

    async fn restart(&mut self) {
        if !self.agent.is_finished() {
            self.agent.abort();
            let _ = (&mut self.agent).await;
        }
        while self.events.try_recv().is_ok() {}
        self.runtime = Arc::new(
            Agentic::with_agent(
                self.execution_agent().await,
                Client::new(&self.settings).unwrap(),
                self.settings.clone(),
            )
            .unwrap(),
        );
        let runtime = self.runtime.clone();
        self.agent = tokio::task::spawn_local(async move { runtime.start().await });
    }

    async fn wait_running(&self, count: usize) {
        timeout(Duration::from_secs(5), async {
            loop {
                let running = self
                    .state
                    .containers
                    .lock()
                    .unwrap()
                    .values()
                    .filter(|container| container["Running"] == true)
                    .count();
                if running == count {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }

    fn finish_jobs(&self) {
        for container in self.state.containers.lock().unwrap().values_mut() {
            assert_eq!(container["Running"], true);
            *container =
                json!({"Status": "exited", "Running": false, "ExitCode": self.state.exit_code});
        }
    }

    async fn wait_cleaned(&self, job_id: Uuid) {
        timeout(Duration::from_secs(5), async {
            while self.job_fs(job_id).dir().join("job.json").exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert!(!self.job_fs(job_id).dir().join("result.json").exists());
        assert!(!self.job_fs(job_id).dir().join("container.json").exists());
    }

    async fn event(&mut self, suffix: &str) -> Event {
        let mut observed = Vec::new();
        timeout(Duration::from_secs(5), async {
            loop {
                let event = self.events.recv().await.unwrap();
                if event.path.ends_with(suffix) {
                    return event;
                }
                if observed.len() < 20 {
                    observed.push(event.path);
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("waiting for {suffix}; observed requests: {observed:?}"))
    }
}

impl Drop for RunningAgent {
    fn drop(&mut self) {
        self.agent.abort();
        self.server.abort();
    }
}

fn job_graph(job: &Job) -> DAG {
    DAG {
        id: job.run.get().graph_id,
        name: "test".into(),
        version: job.run.get().version,
        node_count: 1,
        nodes: vec![Node {
            id: job.node_run.get().node_id,
            name: "task".into(),
            node_type: "script".into(),
            settings: Default::default(),
            inputs: vec![],
            outputs: vec![],
        }],
        edges: vec![],
        ..DAG::default()
    }
}

fn job() -> Job {
    let task = Task::new(Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4(), 1);
    let mut job = Job::new(&task, Uuid::new_v4(), 1, 1);
    job.status = scheduler::actuator::Status::Polled;
    job.polled_at = Some(Timestamp::now());
    job.expired_at = Some(Timestamp::now() + Duration::from_secs(30));
    job.node_config = Some(NodeConfig {
        id: 1,
        node_id: task.node_id,
        version: 1,
        annotation_match: String::new(),
        config: json!({ "image": "alpine:latest" }),
        lease: Some(30_000),
        required_cpu_cores: 1,
        required_memory_bytes: 512,
        created_at: Timestamp::now(),
        updated_at: Timestamp::now(),
        created_by: String::new(),
        updated_by: String::new(),
    })
    .into();
    let now = Timestamp::now();
    let mut value = serde_json::to_value(&job).unwrap();
    value["run"] = json!({
        "id": 1, "uuid": job.run_id, "data_id": task.data_id,
        "graph_id": task.graph_id, "version": 1, "revision": 1,
        "created_at": now, "updated_at": now
    });
    value["node_run"] = json!({
        "id": task.node_run_id, "run_id": job.run_id,
        "data_id": task.data_id, "graph_id": task.graph_id,
        "version": 1, "node_id": task.node_id, "revision": 1,
        "status": "Running", "created_at": now, "updated_at": now
    });
    serde_json::from_value(value).unwrap()
}

#[tokio::test]
async fn client_polls_and_reports_without_an_agent() {
    let job = job();
    let job_id = job.uuid;
    let agent_id = Uuid::new_v4();
    let active_job = Uuid::new_v4();
    let (sender, mut requests) = mpsc::unbounded_channel();
    let router = Router::new().fallback(move |uri: Uri, Json(body): Json<Value>| {
        let sender = sender.clone();
        let job = job.clone();
        async move {
            sender.send((uri.path().to_owned(), body)).unwrap();
            match uri.path() {
                "/jobs/poll" => Json(json!({
                    "jobs": [job],
                }))
                .into_response(),
                "/job/fail" => StatusCode::NO_CONTENT.into_response(),
                "/job/succeed" => StatusCode::CONFLICT.into_response(),
                _ => StatusCode::NOT_FOUND.into_response(),
            }
        }
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}/", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let workdir = tempfile::tempdir().unwrap();
    let resources = ResourceCapacity {
        cpu_cores: 4,
        memory_bytes: 4096,
    };
    let mut settings = Settings::new(agent_id, address, workdir.path(), resources);
    settings.request_timeout = Duration::from_secs(2);
    let client = Client::new(&settings).unwrap();
    let response = client
        .poll(&PollRequest {
            agent_id,
            batch: 3,
            resources,
            active_jobs: vec![active_job],
        })
        .await
        .unwrap();
    assert_eq!(response.jobs.len(), 1);
    assert_eq!(response.jobs[0].uuid, job_id);
    let (path, body) = requests.try_recv().unwrap();
    assert_eq!(path, "/jobs/poll");
    assert_eq!(
        body,
        json!({ "agent_id": agent_id, "batch": 3, "resources": resources, "active_jobs": [active_job] })
    );

    client.fail(agent_id, job_id).await.unwrap();
    let (path, body) = requests.try_recv().unwrap();
    assert_eq!(path, "/job/fail");
    assert_eq!(body, json!({ "agent_id": agent_id, "job_id": job_id }));

    let error = client.succeed(agent_id, job_id).await.unwrap_err();
    assert!(matches!(error, agent::errors::Error::ClaimLost(id) if id == job_id));
    assert_eq!(requests.try_recv().unwrap().0, "/job/succeed");
    assert!(workdir.path().read_dir().unwrap().next().is_none());
    server.abort();
}

#[tokio::test]
async fn polling_continues_during_parallel_starts_and_execution_without_duplicates() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let jobs = vec![job(), job()];
            let mut agent = RunningAgent::new(jobs.clone(), 0, false, true).await;
            agent.event("/start").await;
            agent.event("/start").await;
            let poll = agent.event("/jobs/poll").await;
            let active: HashSet<Uuid> =
                serde_json::from_value::<Vec<Uuid>>(poll.body["active_jobs"].clone())
                    .unwrap()
                    .into_iter()
                    .collect();
            assert_eq!(active, jobs.iter().map(|job| job.uuid).collect());
            assert_eq!(agent.state.creates.load(Ordering::SeqCst), 2);

            agent.state.start.add_permits(2);
            agent.wait_running(2).await;
            let poll = agent.event("/jobs/poll").await;
            assert_eq!(poll.body["active_jobs"].as_array().unwrap().len(), 2);
            agent.finish_jobs();
            agent.event("/job/succeed").await;
            agent.event("/job/succeed").await;
            agent.event("/job/succeed").await;
            timeout(Duration::from_secs(5), async {
                loop {
                    let poll = agent.event("/jobs/poll").await;
                    if poll.body["active_jobs"].as_array().unwrap().is_empty() {
                        break;
                    }
                }
            })
            .await
            .unwrap();
            assert_eq!(agent.state.completed.lock().unwrap().len(), 2);
            assert_eq!(agent.state.creates.load(Ordering::SeqCst), 2);
            for job in jobs {
                agent.wait_cleaned(job.uuid).await;
            }
        })
        .await;
}

#[tokio::test]
async fn polling_retries_errors_and_reports_nonzero_exit_codes() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut agent = RunningAgent::new(vec![job()], 7, true, false).await;
            agent.event("/jobs/poll").await;
            agent.event("/start").await;
            agent.state.start.add_permits(1);
            agent.wait_running(1).await;
            agent.finish_jobs();
            agent.event("/job/fail").await;
            assert!(!agent.agent.is_finished());
        })
        .await;
}

#[tokio::test]
async fn successful_polls_do_not_extend_the_execution_timeout() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut job = job();
            job.expired_at = Some(Timestamp::now() + Duration::from_millis(300));
            let mut agent = RunningAgent::new(vec![job.clone()], 0, false, true).await;
            agent.event("/start").await;
            agent.state.start.add_permits(1);
            agent.wait_running(1).await;
            agent.event("/jobs/poll").await;
            agent.event("/kill").await;
            agent.event("/job/fail").await;
            let result: Value = serde_json::from_slice(
                &std::fs::read(
                    agent
                        .workdir
                        .path()
                        .join(job.uuid.to_string())
                        .join("result.json"),
                )
                .unwrap(),
            )
            .unwrap();
            assert_eq!(result["exit_code"], 124);
            assert_eq!(agent.state.creates.load(Ordering::SeqCst), 1);
        })
        .await;
}

#[tokio::test]
async fn a_server_outage_kills_at_expiry_and_retries_the_failure_report() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut job = job();
            job.expired_at = Some(Timestamp::now() + Duration::from_millis(300));
            let mut agent = RunningAgent::new(vec![job], 0, false, false).await;
            agent.event("/start").await;
            agent.state.start.add_permits(1);
            agent.wait_running(1).await;
            agent.state.unavailable.store(true, Ordering::SeqCst);
            agent.event("/kill").await;
            agent.event("/job/fail").await;
            assert!(agent.state.completed.lock().unwrap().is_empty());
            agent.state.unavailable.store(false, Ordering::SeqCst);
            agent.event("/job/fail").await;
            assert!(!agent.agent.is_finished());
        })
        .await;
}

#[tokio::test]
async fn startup_finishing_after_expiry_kills_the_container() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut job = job();
            job.expired_at = Some(Timestamp::now() + Duration::from_millis(300));
            let mut agent = RunningAgent::new(vec![job], 0, false, false).await;
            agent.event("/start").await;
            tokio::time::sleep(Duration::from_millis(350)).await;
            agent.state.start.add_permits(1);
            agent.event("/kill").await;
            agent.event("/job/fail").await;
            assert!(!agent.agent.is_finished());
        })
        .await;
}

#[tokio::test]
async fn expired_job_fails_without_starting_a_container() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut job = job();
            job.expired_at = Some(Timestamp::now() - Duration::from_secs(1));
            let mut agent = RunningAgent::new(vec![job], 0, false, false).await;
            agent.event("/job/fail").await;
            assert!(
                agent
                    .state
                    .containers
                    .lock()
                    .unwrap()
                    .values()
                    .all(|container| container["Running"] != true)
            );
        })
        .await;
}

#[tokio::test]
async fn prepare_pulls_missing_images_and_run_detaches_with_resource_limits() {
    tokio::task::LocalSet::new().run_until(async {
    let mut fixture = RunningAgent::new(vec![], 0, false, false).await;
    fixture.runtime.shutdown().await.unwrap();
    (&mut fixture.agent).await.unwrap().unwrap();
    fixture.state.missing_image.store(true, Ordering::SeqCst);
    let executor = fixture.execution_agent().await;
    let mut job = job();
    job.required_cpu_cores = 2;
    job.required_memory_bytes = 2048;
    GraphFs::from_workdir(fixture.workdir.path())
        .write_graph(job_graph(&job)).await.unwrap();

    executor.prepare(job.clone()).await.unwrap();
    fixture.event("/images/create").await;
    let job_fs = fixture.job_fs(job.uuid);
    assert_eq!(job_fs.job().await.unwrap().uuid, job.uuid);
    assert_eq!(fixture.state.creates.load(Ordering::SeqCst), 0);

    fixture.state.start.add_permits(1);
    executor.start(job.uuid).await.unwrap();
    let create = fixture.event("/containers/create").await;
    assert_eq!(create.body["HostConfig"]["NanoCpus"], 2_000_000_000_i64);
    assert_eq!(create.body["HostConfig"]["Memory"], 2048);
    assert_eq!(
        tokio::fs::read_to_string(job_fs.dir().join("pid"))
            .await
            .unwrap(),
        "123"
    );
    assert!(job_fs.started_at().await.unwrap() <= Timestamp::now());

    let mut redelivered = job.clone();
    redelivered.expired_at = Some(Timestamp::now() + Duration::from_secs(60));
    assert!(matches!(
        executor.prepare(redelivered).await,
        Err(agent::errors::Error::JobRunning(id)) if id == job.uuid
    ));
    executor.start(job.uuid).await.unwrap();
    assert_eq!(job_fs.job().await.unwrap().expired_at, job.expired_at);
    assert_eq!(fixture.state.creates.load(Ordering::SeqCst), 1);
    assert!(
        matches!(executor.remove(job.uuid).await.unwrap_err(), agent::errors::Error::JobRunning(id) if id == job.uuid)
    );

    let metadata = job_fs.container().await.unwrap();
    *fixture
        .state
        .containers
        .lock()
        .unwrap()
        .get_mut(&metadata.id)
        .unwrap() = json!({"Status": "exited", "ExitCode": 0});
    executor.remove(job.uuid).await.unwrap();
    assert!(!job_fs.dir().join("job.json").exists());
    assert!(job_fs.dir().join("lock").is_file());
    assert!(fixture.state.containers.lock().unwrap().is_empty());
    }).await;
}

#[tokio::test]
async fn shutdown_detaches_and_restart_recovers_a_running_container() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let job = job();
            let mut fixture = RunningAgent::new(vec![job.clone()], 0, false, false).await;
            fixture.event("/start").await;
            fixture.state.start.add_permits(1);
            fixture.wait_running(1).await;
            assert!(matches!(
                fixture.runtime.start().await.unwrap_err(),
                agent::errors::Error::AlreadyRunning
            ));
            timeout(Duration::from_secs(1), fixture.runtime.shutdown())
                .await
                .unwrap()
                .unwrap();
            (&mut fixture.agent).await.unwrap().unwrap();
            fixture.runtime.shutdown().await.unwrap();
            assert!(
                fixture.state.containers.lock().unwrap()["container-0"]["Running"]
                    .as_bool()
                    .unwrap()
            );
            assert!(fixture.state.completed.lock().unwrap().is_empty());

            fixture.restart().await;
            fixture.wait_running(1).await;
            fixture.finish_jobs();
            fixture.event("/job/succeed").await;
            assert_eq!(fixture.state.creates.load(Ordering::SeqCst), 1);
        })
        .await;
}

#[tokio::test]
async fn recovery_finds_running_container_when_metadata_was_not_written() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let job = job();
            let mut fixture = RunningAgent::new(vec![job.clone()], 0, false, false).await;
            fixture.event("/start").await;
            fixture.state.start.add_permits(1);
            fixture.wait_running(1).await;
            fixture.agent.abort();
            let _ = (&mut fixture.agent).await;
            tokio::fs::remove_file(
                fixture
                    .workdir
                    .path()
                    .join(job.uuid.to_string())
                    .join("container.json"),
            )
            .await
            .unwrap();

            fixture.restart().await;
            fixture.wait_running(1).await;
            let metadata = timeout(Duration::from_secs(5), async {
                loop {
                    match fixture.job_fs(job.uuid).container().await {
                        Ok(metadata) => break metadata,
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                            tokio::time::sleep(Duration::from_millis(5)).await;
                        }
                        Err(error) => panic!("read restored container metadata: {error}"),
                    }
                }
            })
            .await
            .unwrap();
            assert_eq!(fixture.state.creates.load(Ordering::SeqCst), 1);
            assert_eq!(metadata.id, format!("rayflow-{}", job.uuid));
            fixture.finish_jobs();
            fixture.event("/job/succeed").await;
        })
        .await;
}

#[tokio::test]
async fn recovery_retries_a_durable_result_without_starting_again() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let job = job();
            let mut fixture = RunningAgent::new(vec![job.clone()], 7, false, false).await;
            fixture.event("/start").await;
            fixture.state.start.add_permits(1);
            fixture.wait_running(1).await;
            fixture.state.unavailable.store(true, Ordering::SeqCst);
            fixture.finish_jobs();
            fixture.event("/job/fail").await;
            assert_eq!(
                fixture
                    .job_fs(job.uuid)
                    .job_result()
                    .await
                    .unwrap()
                    .exit_code,
                7
            );

            fixture.restart().await;
            fixture.event("/job/fail").await;
            fixture.state.unavailable.store(false, Ordering::SeqCst);
            fixture.event("/job/fail").await;
            assert_eq!(fixture.state.creates.load(Ordering::SeqCst), 1);
            fixture.wait_cleaned(job.uuid).await;
            fixture.restart().await;
            fixture.event("/jobs/poll").await;
            assert_eq!(fixture.state.creates.load(Ordering::SeqCst), 1);
        })
        .await;
}

#[tokio::test]
async fn recovery_enforces_persisted_expiry_during_an_api_outage() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut job = job();
            let mut fixture = RunningAgent::new(vec![job.clone()], 0, false, false).await;
            fixture.event("/start").await;
            fixture.state.start.add_permits(1);
            fixture.wait_running(1).await;
            fixture.agent.abort();
            let _ = (&mut fixture.agent).await;
            job.expired_at = Some(Timestamp::now() - Duration::from_secs(1));
            tokio::fs::write(
                fixture
                    .workdir
                    .path()
                    .join(job.uuid.to_string())
                    .join("job.json"),
                serde_json::to_vec(&job).unwrap(),
            )
            .await
            .unwrap();
            fixture.state.unavailable.store(true, Ordering::SeqCst);

            fixture.restart().await;
            fixture.event("/kill").await;
            fixture.event("/job/fail").await;
            assert_eq!(
                fixture
                    .job_fs(job.uuid)
                    .job_result()
                    .await
                    .unwrap()
                    .exit_code,
                124
            );
            assert_eq!(fixture.state.creates.load(Ordering::SeqCst), 1);
            fixture.state.unavailable.store(false, Ordering::SeqCst);
            fixture.event("/job/fail").await;
        })
        .await;
}

#[tokio::test]
async fn native_events_finish_jobs_and_reconnect_after_stream_closes() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut fixture = RunningAgent::new(vec![job()], 0, false, false).await;
            fixture.event("/start").await;
            fixture.state.start.add_permits(1);
            fixture.wait_running(1).await;
            timeout(Duration::from_secs(1), async {
                while fixture.state.docker_events.receiver_count() == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            fixture.state.docker_events.send(String::new()).unwrap();
            fixture.event("/events").await;
            timeout(Duration::from_secs(1), async {
                while fixture.state.docker_events.receiver_count() == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            *fixture
                .state
                .containers
                .lock()
                .unwrap()
                .get_mut("container-0")
                .unwrap() = json!({"Status": "exited", "ExitCode": 7});
            fixture
        .state
        .docker_events
        .send(
            "{\"Type\":\"container\",\"Action\":\"die\",\"Actor\":{\"ID\":\"container-0\"}}\n"
                .to_owned(),
        )
        .unwrap();
            fixture.event("/job/fail").await;
            assert_eq!(fixture.state.creates.load(Ordering::SeqCst), 1);
        })
        .await;
}
