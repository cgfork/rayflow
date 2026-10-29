use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use agent::container::{
    Config, Connector, Container, ContainerBackend, Error as ContainerError, Event, Provider,
    docker::{DockerConfig, DockerConnector, DockerContainer},
};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    http::{Method, StatusCode, Uri},
    response::IntoResponse,
};
use bollard::{API_DEFAULT_VERSION, Docker, errors::Error, models::ContainerCreateBody};
use futures_util::{Stream, StreamExt, stream};
use serde_json::{Value, json};
use tokio::{
    net::TcpListener,
    sync::mpsc::{self, UnboundedReceiver},
    task::JoinHandle,
};

struct Request {
    method: Method,
    uri: Uri,
    body: Option<Value>,
}

struct MockDaemon {
    docker: Docker,
    requests: UnboundedReceiver<Request>,
    server: JoinHandle<()>,
}

impl MockDaemon {
    async fn new(responses: Vec<(StatusCode, Value)>) -> Self {
        let responses = Arc::new(Mutex::new(VecDeque::from(responses)));
        let (sender, requests) = mpsc::unbounded_channel();
        let app = Router::new().fallback(move |method: Method, uri: Uri, body: Bytes| {
            let responses = responses.clone();
            let sender = sender.clone();
            async move {
                sender
                    .send(Request {
                        method,
                        uri,
                        body: if body.is_empty() {
                            None
                        } else {
                            Some(serde_json::from_slice(&body).unwrap())
                        },
                    })
                    .unwrap();
                let (status, body) = responses.lock().unwrap().pop_front().unwrap();
                if status == StatusCode::NO_CONTENT {
                    status.into_response()
                } else if let Value::Array(events) = body {
                    let chunks = events
                        .into_iter()
                        .map(|event| Ok::<_, std::io::Error>(format!("{event}\n")));
                    (status, Body::from_stream(stream::iter(chunks))).into_response()
                } else {
                    (status, Json(body)).into_response()
                }
            }
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let docker =
            Docker::connect_with_http(&format!("http://{address}"), 5, API_DEFAULT_VERSION)
                .unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            docker,
            requests,
            server,
        }
    }

    fn request(&mut self, method: Method, path: &str) -> Request {
        let request = self.requests.try_recv().unwrap();
        assert_eq!(request.method, method);
        let version_prefix = format!("/v{API_DEFAULT_VERSION}");
        let request_path = request
            .uri
            .path()
            .strip_prefix(&version_prefix)
            .unwrap_or(request.uri.path());
        assert_eq!(request_path, path);
        assert!(request.uri.query().is_none());
        request
    }
}

impl Drop for MockDaemon {
    fn drop(&mut self) {
        self.server.abort();
    }
}

fn assert_server_error(error: Error, expected_status: u16, expected_message: &str) {
    match error {
        Error::DockerResponseServerError {
            status_code,
            message,
        } => {
            assert_eq!(status_code, expected_status);
            assert_eq!(message, expected_message);
        }
        error => panic!("expected Docker server error, got {error:?}"),
    }
}

fn docker_error(error: ContainerError) -> Error {
    match error {
        ContainerError::DockerError(error) => error,
        error => panic!("expected Docker error, got {error:?}"),
    }
}

#[tokio::test]
async fn creates_and_controls_the_returned_container() {
    let mut daemon = MockDaemon::new(vec![
        (
            StatusCode::CREATED,
            json!({"Id": "created-container", "Warnings": []}),
        ),
        (StatusCode::NO_CONTENT, Value::Null),
        (StatusCode::NO_CONTENT, Value::Null),
        (StatusCode::NO_CONTENT, Value::Null),
    ])
    .await;
    let connector = DockerConnector::new(daemon.docker.clone());
    let config = DockerConfig {
        image: "alpine:latest".to_owned(),
    };
    let expected_body = serde_json::to_value(ContainerCreateBody {
        image: Some(config.image.clone()),
        ..Default::default()
    })
    .unwrap();
    let container = connector.create(config.clone()).await.unwrap();
    drop(connector);

    assert_eq!(container.name(), "created-container");
    let request = daemon.request(Method::POST, "/containers/create");
    assert_eq!(request.body, Some(expected_body));
    assert!(daemon.requests.try_recv().is_err());

    container.start(config.clone()).await.unwrap();
    let request = daemon.request(Method::POST, "/containers/created-container/start");
    assert!(request.body.is_none());

    container.stop(config.clone()).await.unwrap();
    let request = daemon.request(Method::POST, "/containers/created-container/stop");
    assert!(request.body.is_none());

    container.remove(config).await.unwrap();
    let request = daemon.request(Method::DELETE, "/containers/created-container");
    assert!(request.body.is_none());
    assert!(daemon.requests.try_recv().is_err());
}

#[tokio::test]
async fn create_propagates_docker_errors() {
    let mut daemon = MockDaemon::new(vec![(
        StatusCode::NOT_FOUND,
        json!({"message": "No such image"}),
    )])
    .await;
    let connector = DockerConnector::new(daemon.docker.clone());
    let error = connector
        .create(DockerConfig {
            image: "missing:latest".to_owned(),
        })
        .await
        .unwrap_err();

    assert_server_error(error, 404, "No such image");
    daemon.request(Method::POST, "/containers/create");
    assert!(daemon.requests.try_recv().is_err());
}

#[tokio::test]
async fn existing_container_propagates_lifecycle_errors() {
    let mut daemon = MockDaemon::new(vec![
        (
            StatusCode::NOT_FOUND,
            json!({"message": "No such container"}),
        ),
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"message": "Cannot stop container"}),
        ),
        (
            StatusCode::CONFLICT,
            json!({"message": "Container is running"}),
        ),
    ])
    .await;
    let container = DockerContainer::new(daemon.docker.clone(), "existing-container");
    assert_eq!(container.name(), "existing-container");

    assert_server_error(
        container.start(DockerConfig::default()).await.unwrap_err(),
        404,
        "No such container",
    );
    daemon.request(Method::POST, "/containers/existing-container/start");

    assert_server_error(
        container.stop(DockerConfig::default()).await.unwrap_err(),
        500,
        "Cannot stop container",
    );
    daemon.request(Method::POST, "/containers/existing-container/stop");

    assert_server_error(
        container.remove(DockerConfig::default()).await.unwrap_err(),
        409,
        "Container is running",
    );
    daemon.request(Method::DELETE, "/containers/existing-container");
    assert!(daemon.requests.try_recv().is_err());
}

#[test]
fn enum_backends_implement_the_traits_and_are_thread_safe() {
    fn assert_container<Handle: Container<Config = Config, Err = ContainerError> + Send + Sync>() {}
    fn assert_connector<
        Handle: Connector<Config = Config, Err = ContainerError, Container = ContainerBackend>
            + Send
            + Sync,
    >() {
    }
    fn assert_error<Failure: std::error::Error + Send + Sync + 'static>() {}

    assert_container::<ContainerBackend>();
    assert_connector::<Provider>();
    assert_error::<ContainerError>();
}

#[tokio::test]
async fn docker_connector_streams_events_in_order() {
    let mut daemon = MockDaemon::new(vec![(StatusCode::OK, json!([
        {"Type": "container", "Action": "start", "Actor": {"ID": "container-id"}, "time": 1},
        {"Type": "container", "Action": "die", "Actor": {"ID": "container-id", "Attributes": {"exitCode": "7"}}, "time": 2},
    ]))]).await;
    let connector = DockerConnector::new(daemon.docker.clone());
    let events = connector.events();
    futures_util::pin_mut!(events);
    let started = events.next().await.unwrap().unwrap();
    assert_eq!(started.action.as_deref(), Some("start"));
    assert_eq!(started.actor.unwrap().id.as_deref(), Some("container-id"));
    assert_eq!(started.time, Some(1));
    let finished = events.next().await.unwrap().unwrap();
    assert_eq!(finished.action.as_deref(), Some("die"));
    assert_eq!(finished.actor.unwrap().attributes.unwrap()["exitCode"], "7");
    assert_eq!(finished.time, Some(2));
    assert!(events.next().await.is_none());
    daemon.request(Method::GET, "/events");
}

#[tokio::test]
async fn provider_streams_events_and_preserves_daemon_errors() {
    let mut daemon = MockDaemon::new(vec![
        (
            StatusCode::OK,
            json!([{"Type": "container", "Action": "destroy", "Actor": {"ID": "container-id"}}]),
        ),
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"message": "Cannot read events"}),
        ),
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"message": "Cannot read events"}),
        ),
    ])
    .await;
    let provider: Provider = DockerConnector::new(daemon.docker.clone()).into();
    let events = provider.events();
    futures_util::pin_mut!(events);
    let Event::Docker(event) = events.next().await.unwrap().unwrap();
    assert_eq!(event.action.as_deref(), Some("destroy"));
    assert_eq!(event.actor.unwrap().id.as_deref(), Some("container-id"));
    assert!(events.next().await.is_none());
    daemon.request(Method::GET, "/events");

    let events = provider.events();
    futures_util::pin_mut!(events);
    let error = events.next().await.unwrap().unwrap_err();
    assert_server_error(docker_error(error), 500, "Cannot read events");
    daemon.request(Method::GET, "/events");

    let connector = DockerConnector::new(daemon.docker.clone());
    let events = connector.events();
    futures_util::pin_mut!(events);
    assert_server_error(
        events.next().await.unwrap().unwrap_err(),
        500,
        "Cannot read events",
    );
    daemon.request(Method::GET, "/events");
}

#[tokio::test]
async fn connector_events_can_borrow_from_the_connector() {
    struct BorrowingConnector(String);

    impl Connector for BorrowingConnector {
        type Err = std::io::Error;
        type Container = DockerContainer;
        type Config = ();
        type Event<'a> = &'a str;

        async fn create(&self, _: Self::Config) -> Result<Self::Container, Self::Err> {
            Err(std::io::ErrorKind::Unsupported.into())
        }

        fn events(&self) -> impl Stream<Item = Result<Self::Event<'_>, Self::Err>> + Send + '_ {
            stream::once(std::future::ready(Ok(self.0.as_str())))
        }
    }

    let connector = BorrowingConnector("container-event".to_owned());
    let events = connector.events();
    futures_util::pin_mut!(events);
    assert_eq!(events.next().await.unwrap().unwrap(), "container-event");
    assert!(events.next().await.is_none());
}

#[tokio::test]
async fn kill_accepts_stopped_containers_and_propagates_daemon_errors() {
    let mut daemon = MockDaemon::new(vec![
        (StatusCode::NO_CONTENT, Value::Null),
        (
            StatusCode::CONFLICT,
            json!({"message": "Container is not running"}),
        ),
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"message": "Cannot kill container"}),
        ),
    ])
    .await;
    let container: ContainerBackend =
        DockerContainer::new(daemon.docker.clone(), "timed-out").into();
    container.kill().await.unwrap();
    daemon.request(Method::POST, "/containers/timed-out/kill");
    container.kill().await.unwrap();
    daemon.request(Method::POST, "/containers/timed-out/kill");
    assert_server_error(
        docker_error(container.kill().await.unwrap_err()),
        500,
        "Cannot kill container",
    );
    daemon.request(Method::POST, "/containers/timed-out/kill");
}

#[tokio::test]
async fn enum_connector_creates_a_container_and_dispatches_its_lifecycle() {
    let mut daemon = MockDaemon::new(vec![
        (
            StatusCode::CREATED,
            json!({"Id": "enum-container", "Warnings": []}),
        ),
        (StatusCode::NO_CONTENT, Value::Null),
        (StatusCode::NO_CONTENT, Value::Null),
        (StatusCode::NO_CONTENT, Value::Null),
    ])
    .await;
    let connector = Provider::Docker(DockerConnector::new(daemon.docker.clone()));
    let config = Config::Docker(DockerConfig {
        image: "alpine:latest".to_owned(),
    });
    let cloned_connector = connector.clone();
    let create_config = config.clone();
    let container = tokio::spawn(async move { cloned_connector.create(create_config).await })
        .await
        .unwrap()
        .unwrap();
    drop(connector);

    assert!(matches!(&container, ContainerBackend::Docker(_)));
    assert_eq!(container.name(), "enum-container");
    let request = daemon.request(Method::POST, "/containers/create");
    assert_eq!(request.body.unwrap()["Image"], "alpine:latest");
    assert!(daemon.requests.try_recv().is_err());

    let cloned_container = container.clone();
    let start_config = config.clone();
    tokio::spawn(async move { cloned_container.start(start_config).await })
        .await
        .unwrap()
        .unwrap();
    let request = daemon.request(Method::POST, "/containers/enum-container/start");
    assert!(request.body.is_none());

    container.stop(config.clone()).await.unwrap();
    let request = daemon.request(Method::POST, "/containers/enum-container/stop");
    assert!(request.body.is_none());

    container.remove(config).await.unwrap();
    let request = daemon.request(Method::DELETE, "/containers/enum-container");
    assert!(request.body.is_none());
    assert!(daemon.requests.try_recv().is_err());
}

#[tokio::test]
async fn enum_connector_preserves_docker_creation_errors() {
    let mut daemon = MockDaemon::new(vec![(
        StatusCode::NOT_FOUND,
        json!({"message": "No such image"}),
    )])
    .await;
    let connector: Provider = DockerConnector::new(daemon.docker.clone()).into();
    let error = connector
        .create(Config::Docker(DockerConfig {
            image: "missing:latest".to_owned(),
        }))
        .await
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "Docker responded with status code 404: No such image"
    );
    let error = docker_error(error);
    assert_server_error(error, 404, "No such image");
    daemon.request(Method::POST, "/containers/create");
    assert!(daemon.requests.try_recv().is_err());
}

#[tokio::test]
async fn enum_existing_container_preserves_lifecycle_errors() {
    let mut daemon = MockDaemon::new(vec![
        (
            StatusCode::NOT_FOUND,
            json!({"message": "No such container"}),
        ),
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"message": "Cannot stop container"}),
        ),
        (
            StatusCode::CONFLICT,
            json!({"message": "Container is running"}),
        ),
    ])
    .await;
    let container: ContainerBackend =
        DockerContainer::new(daemon.docker.clone(), "existing-container").into();
    let config = Config::Docker(DockerConfig::default());
    assert_eq!(container.name(), "existing-container");

    let error = docker_error(container.start(config.clone()).await.unwrap_err());
    assert_server_error(error, 404, "No such container");
    daemon.request(Method::POST, "/containers/existing-container/start");

    let error = docker_error(container.stop(config.clone()).await.unwrap_err());
    assert_server_error(error, 500, "Cannot stop container");
    daemon.request(Method::POST, "/containers/existing-container/stop");

    let error = docker_error(container.remove(config).await.unwrap_err());
    assert_server_error(error, 409, "Container is running");
    daemon.request(Method::DELETE, "/containers/existing-container");
    assert!(daemon.requests.try_recv().is_err());
}
