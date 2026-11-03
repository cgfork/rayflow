use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use agent::containerd::{
    Container, ContainerAction, ContainerState, Containerd,
    docker::{DockerContainer, DockerContainerd, DockerSpec},
};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    http::{Method, StatusCode, Uri},
    response::IntoResponse,
};
use bollard::{API_DEFAULT_VERSION, Docker};
use futures_util::{StreamExt, stream};
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::mpsc, task::JoinHandle};

struct Request {
    method: Method,
    path: String,
    body: Option<Value>,
    query: Option<String>,
}

struct MockDaemon {
    docker: Docker,
    requests: mpsc::UnboundedReceiver<Request>,
    server: JoinHandle<()>,
}

impl MockDaemon {
    async fn new(responses: Vec<(StatusCode, Value)>) -> Self {
        let responses = Arc::new(Mutex::new(VecDeque::from(responses)));
        let (sender, requests) = mpsc::unbounded_channel();
        let router = Router::new().fallback(move |method: Method, uri: Uri, body: Bytes| {
            let responses = responses.clone();
            let sender = sender.clone();
            async move {
                let path = uri
                    .path()
                    .strip_prefix(&format!("/v{API_DEFAULT_VERSION}"))
                    .unwrap_or(uri.path())
                    .to_owned();
                sender
                    .send(Request {
                        method,
                        path,
                        body: (!body.is_empty()).then(|| serde_json::from_slice(&body).unwrap()),
                        query: uri.query().map(str::to_owned),
                    })
                    .unwrap();
                let (status, value) = responses.lock().unwrap().pop_front().unwrap();
                if status == StatusCode::NO_CONTENT {
                    status.into_response()
                } else if let Value::Array(events) = value {
                    let chunks = events
                        .into_iter()
                        .map(|event| Ok::<_, std::io::Error>(format!("{event}\n")));
                    (status, Body::from_stream(stream::iter(chunks))).into_response()
                } else {
                    (status, Json(value)).into_response()
                }
            }
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let docker = Docker::connect_with_http(
            &format!("http://{}", listener.local_addr().unwrap()),
            5,
            API_DEFAULT_VERSION,
        )
        .unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        Self {
            docker,
            requests,
            server,
        }
    }

    fn request(&mut self, method: Method, path: &str) -> Request {
        let request = self.requests.try_recv().unwrap();
        assert_eq!(request.method, method);
        assert_eq!(request.path, path);
        request
    }
}

impl Drop for MockDaemon {
    fn drop(&mut self) {
        self.server.abort();
    }
}

fn spec() -> DockerSpec {
    DockerSpec {
        name: "rayflow-test".into(),
        image: "alpine:latest".into(),
        command: Some(vec!["sh".into(), "-c".into(), "echo hello".into()]),
        env: Some(vec!["GREETING=hello".into()]),
        working_dir: Some("/tmp".into()),
        cpu_cores: 2,
        memory_bytes: 2048,
    }
}

#[test]
fn docker_spec_reads_configuration_from_the_pinned_job() {
    let task = event::task::Task::new(
        uuid::Uuid::new_v4(),
        uuid::Uuid::new_v4(),
        uuid::Uuid::new_v4(),
        1,
    );
    let job = scheduler::Job::new(&task, uuid::Uuid::new_v4(), 1, 0);
    let mut value = serde_json::to_value(&job).unwrap();
    value["required_cpu_cores"] = json!(2);
    value["required_memory_bytes"] = json!(2048);
    value["node_config"] = json!({
        "id": 1, "node_id": task.node_id, "version": 0, "annotation_match": "",
        "config": {"image": "alpine:3.21", "command": ["echo", "hello"], "env": ["MODE=test"], "working_dir": "/tmp"},
        "lease": 10000, "required_cpu_cores": 2, "required_memory_bytes": 2048,
        "created_at": job.created_at, "updated_at": job.updated_at, "created_by": "publisher", "updated_by": "publisher"
    });
    let configured: scheduler::Job = serde_json::from_value(value.clone()).unwrap();
    let actual = DockerSpec::try_from(&configured).unwrap();
    assert_eq!(actual.name, format!("rayflow-{}", job.uuid));
    assert_eq!(actual.image, "alpine:3.21");
    assert_eq!(actual.command, Some(vec!["echo".into(), "hello".into()]));
    assert_eq!(actual.env, Some(vec!["MODE=test".into()]));
    assert_eq!(actual.working_dir, Some("/tmp".into()));
    assert_eq!((actual.cpu_cores, actual.memory_bytes), (2, 2048));
    value["node_config"]["config"]["command"] = json!("invalid");
    let invalid: scheduler::Job = serde_json::from_value(value.clone()).unwrap();
    assert!(DockerSpec::try_from(&invalid).is_err());
    value["node_config"]["config"] = json!({"image": "alpine:3.21"});
    let legacy: scheduler::Job = serde_json::from_value(value).unwrap();
    let spec = DockerSpec::try_from(&legacy).unwrap();
    assert!(spec.command.is_none() && spec.env.is_none() && spec.working_dir.is_none());
}

#[tokio::test]
async fn creates_and_controls_a_container_with_limits() {
    let mut daemon = MockDaemon::new(vec![
        (
            StatusCode::CREATED,
            json!({"Id": "created", "Warnings": []}),
        ),
        (StatusCode::NO_CONTENT, Value::Null),
        (StatusCode::NO_CONTENT, Value::Null),
        (StatusCode::NO_CONTENT, Value::Null),
    ])
    .await;
    let provider = DockerContainerd::new(daemon.docker.clone());
    let container = provider.create(spec()).await.unwrap();
    assert_eq!(container.id(), "created");
    let request = daemon.request(Method::POST, "/containers/create");
    assert!(request.query.unwrap().contains("name=rayflow-test"));
    let body = request.body.unwrap();
    assert_eq!(body["Image"], "alpine:latest");
    assert_eq!(body["Cmd"], json!(["sh", "-c", "echo hello"]));
    assert_eq!(body["Env"], json!(["GREETING=hello"]));
    assert_eq!(body["WorkingDir"], "/tmp");
    assert_eq!(body["HostConfig"]["NanoCpus"], 2_000_000_000_i64);
    assert_eq!(body["HostConfig"]["Memory"], 2048);

    container.start().await.unwrap();
    daemon.request(Method::POST, "/containers/created/start");
    container.stop().await.unwrap();
    daemon.request(Method::POST, "/containers/created/stop");
    container.remove().await.unwrap();
    daemon.request(Method::DELETE, "/containers/created");
}

#[tokio::test]
async fn existing_name_reconnects_and_start_tolerates_running_container() {
    let mut daemon = MockDaemon::new(vec![
        (StatusCode::CONFLICT, json!({"message": "already exists"})),
        (
            StatusCode::NOT_MODIFIED,
            json!({"message": "already started"}),
        ),
    ])
    .await;
    let provider = DockerContainerd::new(daemon.docker.clone());
    let container = provider.create(spec()).await.unwrap();
    assert_eq!(container.id(), "rayflow-test");
    daemon.request(Method::POST, "/containers/create");
    container.start().await.unwrap();
    daemon.request(Method::POST, "/containers/rayflow-test/start");
}

#[tokio::test]
async fn provider_preserves_daemon_errors_and_tolerates_missing_containers() {
    let mut daemon = MockDaemon::new(vec![
        (StatusCode::NOT_FOUND, json!({"message": "image missing"})),
        (StatusCode::NOT_FOUND, json!({"message": "missing"})),
        (StatusCode::CONFLICT, json!({"message": "stopped"})),
        (StatusCode::NOT_FOUND, json!({"message": "missing"})),
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"message": "cannot start"}),
        ),
    ])
    .await;
    let provider = DockerContainerd::new(daemon.docker.clone());
    let error = provider.create(spec()).await.unwrap_err();
    assert!(error.to_string().contains("image missing"));
    daemon.request(Method::POST, "/containers/create");

    let container = provider.container("existing");
    container.stop().await.unwrap();
    daemon.request(Method::POST, "/containers/existing/stop");
    container.kill().await.unwrap();
    daemon.request(Method::POST, "/containers/existing/kill");
    container.remove().await.unwrap();
    daemon.request(Method::DELETE, "/containers/existing");
    assert!(
        container
            .start()
            .await
            .unwrap_err()
            .to_string()
            .contains("cannot start")
    );
    daemon.request(Method::POST, "/containers/existing/start");
}

#[tokio::test]
async fn prepares_missing_image_then_inspects_container_state() {
    let mut daemon = MockDaemon::new(vec![
        (StatusCode::NOT_FOUND, json!({"message": "missing image"})),
        (StatusCode::OK, json!([{"status": "Downloaded"}])),
        (
            StatusCode::OK,
            json!({"State": {"Status": "running", "Running": true, "Pid": 123}}),
        ),
        (
            StatusCode::OK,
            json!({"State": {"Status": "exited", "ExitCode": 7}}),
        ),
    ])
    .await;
    let provider = DockerContainerd::new(daemon.docker.clone());
    provider.prepare(spec()).await.unwrap();
    daemon.request(Method::GET, "/images/alpine:latest/json");
    daemon.request(Method::POST, "/images/create");
    let container = provider.container("existing");
    assert!(matches!(
        container.inspect().await.unwrap(),
        ContainerState::Running { pid: Some(123), .. }
    ));
    daemon.request(Method::GET, "/containers/existing/json");
    assert!(matches!(
        container.inspect().await.unwrap(),
        ContainerState::Exited { exit_code: 7, .. }
    ));
    daemon.request(Method::GET, "/containers/existing/json");
}

#[tokio::test]
async fn translates_container_events_and_reconnects_after_errors() {
    let mut daemon = MockDaemon::new(vec![
        (
            StatusCode::OK,
            json!([
                {"Type": "container", "Action": "start", "Actor": {"ID": "created"}},
                {"Type": "container", "Action": "die", "Actor": {"ID": "created"}},
            ]),
        ),
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"message": "events unavailable"}),
        ),
    ])
    .await;
    let provider = DockerContainerd::new(daemon.docker.clone());
    let mut events = provider.events();
    let started = events.next().await.unwrap().unwrap();
    assert_eq!(started.container_id, "created");
    assert_eq!(started.action, ContainerAction::Start);
    assert_eq!(
        events.next().await.unwrap().unwrap().action,
        ContainerAction::Die
    );
    assert!(events.next().await.is_none());
    daemon.request(Method::GET, "/events");

    let mut events = provider.events();
    assert!(
        events
            .next()
            .await
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("events unavailable")
    );
    daemon.request(Method::GET, "/events");
}

#[test]
fn docker_provider_and_container_are_thread_safe() {
    fn assert_provider<T: Containerd + Send + Sync>() {}
    fn assert_container<T: Container + Send + Sync>() {}
    assert_provider::<DockerContainerd>();
    assert_container::<DockerContainer>();
}
