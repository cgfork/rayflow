use super::*;
use std::{io, path::PathBuf, pin::Pin, sync::Arc, time::Duration};

use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post, put},
};
use catalog::{PresignOperation, PresignedUrl, model::Catalog};
use event::task::Task;
use futures_util::{Stream, stream};
use graph::types::{Node, NodeSettings, Output};
use jiff::Timestamp;
use runtime::command::NodeRun;
use scheduler::{JobArtifactName, ResourceCapacity};
use serde_json::{Value, json};
use tempfile::{TempDir, tempdir};
use tokio::{fs, net::TcpListener, task::JoinHandle};

use crate::{
    client::{Client, Transfer},
    containerd::{Container, ContainerEvent, ContainerState, ExitStatus},
    job_manager::{JobUsage, UsageSummary},
    settings::Settings,
};

const TOKEN: &str = "result-fixture-fleet-credential";

#[derive(Clone)]
struct NoContainers;

impl Container for NoContainers {
    fn id(&self) -> &str {
        "unused"
    }
    async fn start(&self) -> Result<(), Error> {
        unreachable!()
    }
    async fn wait(&self) -> Result<ExitStatus, Error> {
        unreachable!()
    }
    async fn inspect(&self) -> Result<ContainerState, Error> {
        Err(io::Error::from(io::ErrorKind::NotFound).into())
    }
    async fn stop(&self) -> Result<(), Error> {
        unreachable!()
    }
    async fn kill(&self) -> Result<(), Error> {
        unreachable!()
    }
    async fn remove(&self) -> Result<(), Error> {
        unreachable!()
    }
}

impl Containerd for NoContainers {
    type Container = Self;
    type Spec<'a> = ();
    fn spec<'a>(&'a self, _: &'a Job) -> Result<(), Error> {
        unreachable!()
    }
    async fn prepare(&self, _: Self::Spec<'_>) -> Result<(), Error> {
        unreachable!()
    }
    async fn create(&self, _: Self::Spec<'_>) -> Result<Self, Error> {
        unreachable!()
    }
    fn container(&self, _: &str) -> Self {
        self.clone()
    }
    fn events(&self) -> Pin<Box<dyn Stream<Item = Result<ContainerEvent, Error>> + Send>> {
        Box::pin(stream::pending())
    }
}

#[derive(Default)]
struct ArtifactTraffic {
    bytes: Vec<u8>,
    put_bodies: Vec<Vec<u8>>,
    prepared_size: Option<u64>,
    uploaded: bool,
    upload_failure: Option<StatusCode>,
    lose_commit_reply: bool,
}

#[derive(Default)]
struct Traffic {
    calls: Vec<String>,
    result: ArtifactTraffic,
    usage: ArtifactTraffic,
    graph_requests: usize,
    fail_graph: bool,
    prepare_error: Option<(StatusCode, &'static str)>,
    fail_action: bool,
    conflict_action: bool,
    output_prepare_error: Option<StatusCode>,
    output_commit_error: Option<StatusCode>,
    output_committed: bool,
}

impl Traffic {
    fn record(&mut self, filename: JobArtifactName, action: &str) -> &mut ArtifactTraffic {
        match filename {
            JobArtifactName::Result => {
                self.calls.push(action.into());
                &mut self.result
            }
            JobArtifactName::Usage => {
                self.calls.push(format!("usage {action}"));
                &mut self.usage
            }
        }
    }
}

struct Fixture {
    job: Job,
    agent_id: Uuid,
    endpoint: String,
    graph: DAG,
    catalog_id: Uuid,
    usage_catalog_id: Uuid,
    output_catalog_id: Uuid,
    traffic: std::sync::Mutex<Traffic>,
}

impl Fixture {
    fn catalog_id(&self, filename: JobArtifactName) -> Uuid {
        match filename {
            JobArtifactName::Result => self.catalog_id,
            JobArtifactName::Usage => self.usage_catalog_id,
        }
    }

    fn claim(&self, headers: &HeaderMap, body: &Value) {
        assert_eq!(headers["authorization"], format!("Bearer {TOKEN}"));
        assert_eq!(body["job_id"], json!(self.job.uuid));
        assert_eq!(body["agent_id"], json!(self.agent_id));
    }
}

async fn prepare(
    State(fixture): State<Arc<Fixture>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    fixture.claim(&headers, &body);
    let filename = serde_json::from_value(body["filename"].clone()).unwrap();
    let mut traffic = fixture.traffic.lock().unwrap();
    let prepare_error = traffic.prepare_error;
    let traffic = traffic.record(filename, "prepare");
    if let Some((status, code)) = prepare_error {
        return (
            status,
            Json(json!({"error_code": code, "error": "prepare failed"})),
        )
            .into_response();
    }
    let size = body["size"].as_u64().unwrap();
    if *traffic.prepared_size.get_or_insert(size) != size {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let mut catalog = Catalog::new(
        fixture.catalog_id(filename),
        format!("/jobs/{}/{}", fixture.job.uuid, filename.as_str()),
        format!("s3://results/{}", filename.as_str()),
        size,
        "application/json",
    );
    catalog.uploaded = traffic.uploaded;
    let transfer = Transfer {
        catalog,
        presigned: PresignedUrl {
            url: format!("{}/object/{}", fixture.endpoint, filename.as_str()),
            operation: if traffic.uploaded {
                PresignOperation::Download
            } else {
                PresignOperation::Upload
            },
            headers: [("x-result-signature".into(), "signed".into())].into(),
            expires_at: Timestamp::now()
                .checked_add(Duration::from_secs(60))
                .unwrap(),
        },
        multipart: false,
    };
    let mut body = json!(transfer);
    body["catalog_uuid"] = json!(fixture.catalog_id(filename));
    Json(body).into_response()
}

async fn upload(
    State(fixture): State<Arc<Fixture>>,
    Path(filename): Path<JobArtifactName>,
    headers: HeaderMap,
    bytes: Bytes,
) -> impl IntoResponse {
    assert!(!headers.contains_key("authorization"));
    assert_eq!(headers["x-result-signature"], "signed");
    assert_eq!(headers["content-length"], bytes.len().to_string());
    let mut traffic = fixture.traffic.lock().unwrap();
    let traffic = traffic.record(filename, "put");
    assert_eq!(traffic.prepared_size, Some(bytes.len() as u64));
    traffic.put_bodies.push(bytes.to_vec());
    match traffic.upload_failure.take() {
        Some(StatusCode::TEMPORARY_REDIRECT) => {
            return axum::response::Redirect::temporary("/moved/result.json").into_response();
        }
        Some(status) => return status.into_response(),
        None => {}
    }
    traffic.bytes = bytes.to_vec();
    StatusCode::OK.into_response()
}

async fn commit(
    State(fixture): State<Arc<Fixture>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> StatusCode {
    fixture.claim(&headers, &body);
    let filename = serde_json::from_value(body["filename"].clone()).unwrap();
    assert_eq!(body["catalog_uuid"], json!(fixture.catalog_id(filename)));
    let mut traffic = fixture.traffic.lock().unwrap();
    let traffic = traffic.record(filename, "commit");
    traffic.uploaded = true;
    if std::mem::take(&mut traffic.lose_commit_reply) {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    }
}

async fn action(
    State(fixture): State<Arc<Fixture>>,
    Path(action): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> StatusCode {
    fixture.claim(&headers, &body);
    let mut traffic = fixture.traffic.lock().unwrap();
    if action == "succeed" && !fixture.graph.nodes[0].outputs.is_empty() {
        assert!(
            traffic.output_committed,
            "outputs must be committed before success"
        );
    }
    traffic.calls.push(action);
    if std::mem::take(&mut traffic.fail_action) {
        StatusCode::SERVICE_UNAVAILABLE
    } else if traffic.conflict_action {
        StatusCode::CONFLICT
    } else {
        StatusCode::NO_CONTENT
    }
}

async fn read_graph(State(fixture): State<Arc<Fixture>>) -> impl IntoResponse {
    let mut traffic = fixture.traffic.lock().unwrap();
    traffic.graph_requests += 1;
    if traffic.fail_graph {
        StatusCode::INTERNAL_SERVER_ERROR.into_response()
    } else {
        Json(fixture.graph.clone()).into_response()
    }
}

async fn prepare_output(
    State(fixture): State<Arc<Fixture>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    fixture.claim(&headers, &body);
    let mut traffic = fixture.traffic.lock().unwrap();
    traffic.calls.push("output prepare".into());
    if let Some(status) = traffic.output_prepare_error {
        return status.into_response();
    }
    assert_eq!(body["filename"], "result.bin");
    assert_eq!(body["size"], 6);
    Json(Transfer {
        catalog: Catalog::new(
            fixture.output_catalog_id,
            "/result.bin",
            "s3://outputs/result.bin",
            6,
            "bin",
        ),
        presigned: PresignedUrl {
            url: format!("{}/output", fixture.endpoint),
            headers: Default::default(),
            expires_at: Timestamp::now()
                .checked_add(Duration::from_secs(60))
                .unwrap(),
            operation: PresignOperation::Upload,
        },
        multipart: false,
    })
    .into_response()
}

async fn put_output(
    State(fixture): State<Arc<Fixture>>,
    headers: HeaderMap,
    bytes: Bytes,
) -> StatusCode {
    assert!(!headers.contains_key("authorization"));
    assert_eq!(&bytes[..], b"output");
    fixture
        .traffic
        .lock()
        .unwrap()
        .calls
        .push("output put".into());
    StatusCode::OK
}

async fn commit_output(
    State(fixture): State<Arc<Fixture>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> StatusCode {
    fixture.claim(&headers, &body);
    assert_eq!(body["catalog_uuid"], json!(fixture.output_catalog_id));
    let mut traffic = fixture.traffic.lock().unwrap();
    traffic.calls.push("output commit".into());
    if let Some(status) = traffic.output_commit_error {
        return status;
    }
    traffic.output_committed = true;
    StatusCode::NO_CONTENT
}

struct Harness {
    _root: TempDir,
    settings: Settings,
    fixture: Arc<Fixture>,
    server: JoinHandle<()>,
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Harness {
    async fn new(outcome: &str, output: Option<&str>) -> Self {
        let root = tempdir().unwrap();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let mut settings = Settings::new(
            Uuid::new_v4(),
            format!("http://{}", listener.local_addr().unwrap()),
            root.path().join("jobs"),
            ResourceCapacity {
                cpu_cores: 1,
                memory_bytes: 1024,
            },
        );
        settings.auth_token = Some(TOKEN.into());
        settings.request_timeout = Duration::from_secs(5);
        let now = Timestamp::now();
        let node_id = Uuid::new_v4();
        let graph = DAG {
            id: Uuid::new_v4(),
            version: 1,
            nodes: vec![Node {
                id: node_id,
                name: "execute".into(),
                node_type: "test".into(),
                settings: NodeSettings::default(),
                inputs: vec![],
                outputs: if let Some(filename) = output {
                    vec![Output {
                        file_name: filename.into(),
                        kind: graph::types::Kind::File,
                        multiple: false,
                    }]
                } else {
                    vec![]
                },
            }],
            ..DAG::default()
        };
        let run_id = Uuid::new_v4();
        let task = Task::new(Uuid::new_v4(), graph.id, node_id, 1);
        let mut job = Job::new(&task, run_id, Uuid::new_v4(), Uuid::new_v4());
        job.node_run_id = Uuid::new_v4();
        job.node_run = NodeRun {
            id: 1,
            uuid: job.node_run_id,
            run_id,
            data_id: task.data_id,
            graph_id: graph.id,
            version: 1,
            node_id,
            revision: 0,
            status: "Running".into(),
            ineffective: 0,
            created_at: now,
            updated_at: now,
            run: Default::default(),
        }
        .into();
        let metadata = settings.workdir.join(job.uuid.to_string());
        let data = settings.data_dir.join(job.uuid.to_string());
        write_json(&metadata.join("job.json"), &job).await.unwrap();
        write_json(
            &metadata.join("result.json"),
            &JobResult {
                exit_code: if outcome == "fail" { 1 } else { 0 },
                outcome: Some(outcome.into()),
                finished_at: Some(now),
                duration_ms: Some(1000),
            },
        )
        .await
        .unwrap();
        write_json(&metadata.join("usage.json"), &JobUsage::default())
            .await
            .unwrap();
        fs::create_dir_all(data.join("inputs")).await.unwrap();
        fs::create_dir_all(data.join("outputs")).await.unwrap();
        fs::write(data.join("inputs/input.bin"), b"input")
            .await
            .unwrap();
        fs::write(data.join("outputs/result.bin"), b"output")
            .await
            .unwrap();
        let fixture = Arc::new(Fixture {
            job,
            agent_id: settings.id,
            endpoint: settings.server_url.clone(),
            graph,
            catalog_id: Uuid::new_v4(),
            usage_catalog_id: Uuid::new_v4(),
            output_catalog_id: Uuid::new_v4(),
            traffic: Default::default(),
        });
        let router = Router::new()
            .route("/api/v1/job/result/prepare", post(prepare))
            .route("/api/v1/job/result/commit", post(commit))
            .route("/api/v1/job/output/prepare", post(prepare_output))
            .route("/api/v1/job/output/commit", post(commit_output))
            .route("/output", put(put_output))
            .route("/api/v1/job/{action}", post(action))
            .route("/api/v1/graphs/{id}", get(read_graph))
            .route("/object/{filename}", put(upload))
            .route("/moved/{filename}", put(upload))
            .with_state(fixture.clone());
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        Self {
            _root: root,
            settings,
            fixture,
            server,
        }
    }

    async fn manager(&self) -> JobManager<NoContainers, Client> {
        let client = Client::new(self.settings.clone()).unwrap();
        let manager = JobManager::open(NoContainers, client.clone(), client)
            .await
            .unwrap();
        manager.recover().await.unwrap();
        manager
    }

    fn metadata(&self) -> PathBuf {
        self.settings
            .workdir
            .join(self.fixture.job.uuid.to_string())
    }

    fn data(&self) -> PathBuf {
        self.settings
            .data_dir
            .join(self.fixture.job.uuid.to_string())
    }

    async fn report(&self, manager: &JobManager<NoContainers, Client>) -> Result<(), Error> {
        manager.advance_completion(self.fixture.job.uuid).await
    }

    async fn upload_usage(
        &self,
        manager: &JobManager<NoContainers, Client>,
    ) -> Result<bool, Error> {
        manager
            .upload_artifact(self.fixture.job.uuid, JobArtifactName::Usage)
            .await
    }
}

#[tokio::test]
async fn reports_each_terminal_outcome_before_archiving_the_result() {
    for outcome in ["succeed", "fail", "yield", "terminate"] {
        let harness = Harness::new(outcome, None).await;
        let result_path = harness.metadata().join("result.json");
        let expected = fs::read(&result_path).await.unwrap();
        let manager = harness.manager().await;
        harness.report(&manager).await.unwrap();
        assert!(result_path.exists());
        let traffic = harness.fixture.traffic.lock().unwrap();
        assert_eq!(traffic.calls, [outcome, "prepare", "put", "commit"]);
        assert_eq!(traffic.result.bytes, expected);
    }
}

#[tokio::test]
async fn requires_committed_outputs_before_success_and_retries_archival_afterward() {
    let harness = Harness::new("succeed", Some("result.bin")).await;
    let id = harness.fixture.job.uuid;
    let manager = harness.manager().await;
    assert!(manager.active_job_ids().contains(&id));
    {
        let mut traffic = harness.fixture.traffic.lock().unwrap();
        traffic.output_prepare_error = Some(StatusCode::SERVICE_UNAVAILABLE);
        traffic.result.upload_failure = Some(StatusCode::SERVICE_UNAVAILABLE);
    }
    assert!(harness.report(&manager).await.is_err());
    assert!(manager.active_job_ids().contains(&id));
    {
        let mut traffic = harness.fixture.traffic.lock().unwrap();
        assert_eq!(std::mem::take(&mut traffic.calls), ["output prepare"]);
        traffic.output_prepare_error = None;
        traffic.output_commit_error = Some(StatusCode::SERVICE_UNAVAILABLE);
    }
    assert!(harness.report(&manager).await.is_err());
    assert!(manager.active_job_ids().contains(&id));
    {
        let mut traffic = harness.fixture.traffic.lock().unwrap();
        assert_eq!(
            std::mem::take(&mut traffic.calls),
            ["output prepare", "output put", "output commit"]
        );
        traffic.output_commit_error = None;
    }

    assert!(harness.report(&manager).await.is_err());
    assert!(!manager.active_job_ids().contains(&id));
    assert!(!harness.metadata().join("result.json.upload").exists());
    assert!(harness.data().join("outputs/result.bin").exists());
    assert_eq!(
        harness.fixture.traffic.lock().unwrap().calls,
        [
            "output prepare",
            "output put",
            "output commit",
            "succeed",
            "prepare",
            "put"
        ]
    );
    drop(manager);
    let recovered = harness.manager().await;
    assert!(!recovered.active_job_ids().contains(&id));
    harness.report(&recovered).await.unwrap();
    assert!(harness.metadata().join("result.json.upload").exists());
    assert!(harness.data().join("inputs/input.bin").exists());

    assert!(harness.upload_usage(&recovered).await.unwrap());
    harness.report(&recovered).await.unwrap();
    assert!(!harness.metadata().exists());
    assert!(!harness.data().exists());
    let traffic = harness.fixture.traffic.lock().unwrap();
    assert_eq!(
        traffic.calls,
        [
            "output prepare",
            "output put",
            "output commit",
            "succeed",
            "prepare",
            "put",
            "prepare",
            "put",
            "commit",
            "usage prepare",
            "usage put",
            "usage commit",
        ]
    );
    assert_eq!(traffic.result.put_bodies[0], traffic.result.put_bodies[1]);
    assert_eq!(traffic.usage.bytes, b"{}");
}

#[tokio::test]
async fn retries_only_usage_after_restart_and_reclaims_inputs_and_outputs() {
    let harness = Harness::new("succeed", None).await;
    let usage_path = harness.metadata().join("usage.json");
    fs::remove_file(&usage_path).await.unwrap();
    let manager = harness.manager().await;
    assert!(!harness.upload_usage(&manager).await.unwrap());
    harness.report(&manager).await.unwrap();
    assert!(harness.metadata().join("result.json.upload").exists());

    let summary = serde_json::from_value::<UsageSummary>(json!({
        "sampled_at": Timestamp::now(), "total_cpu_usage_ns": 1234,
        "cpu_millicores_percentiles": {"p50": 500, "p90": 800, "p95": 900, "p99": 1000},
    }))
    .unwrap();
    write_json(
        &usage_path,
        &JobUsage {
            summary: Some(summary),
        },
    )
    .await
    .unwrap();
    let expected = fs::read(&usage_path).await.unwrap();
    harness.fixture.traffic.lock().unwrap().usage.upload_failure =
        Some(StatusCode::SERVICE_UNAVAILABLE);
    assert!(harness.upload_usage(&manager).await.is_err());
    assert!(usage_path.exists());
    assert!(!harness.metadata().join("usage.json.upload").exists());
    drop(manager);

    let recovered = harness.manager().await;
    assert!(harness.upload_usage(&recovered).await.unwrap());
    harness.report(&recovered).await.unwrap();
    let traffic = harness.fixture.traffic.lock().unwrap();
    assert_eq!(
        traffic.calls,
        [
            "succeed",
            "prepare",
            "put",
            "commit",
            "usage prepare",
            "usage put",
            "usage prepare",
            "usage put",
            "usage commit",
        ]
    );
    assert_eq!(traffic.usage.put_bodies, [expected.clone(), expected]);
    assert!(!harness.metadata().exists());
    assert!(!harness.data().exists());
}

#[tokio::test]
async fn retains_local_files_when_archival_is_disabled_or_unavailable() {
    let harness = Harness::new("succeed", None).await;
    let result_path = harness.metadata().join("result.json");
    harness.fixture.traffic.lock().unwrap().prepare_error = Some((
        StatusCode::SERVICE_UNAVAILABLE,
        "RAYFLOW_STORAGE_CREDENTIALS_EXPIRED",
    ));
    let manager = harness.manager().await;
    assert!(harness.report(&manager).await.is_err());
    assert!(result_path.exists());

    harness.fixture.traffic.lock().unwrap().prepare_error = Some((
        StatusCode::SERVICE_UNAVAILABLE,
        "RAYFLOW_STORAGE_UNAVAILABLE",
    ));
    harness.report(&manager).await.unwrap();
    assert!(result_path.exists());
    assert!(!harness.metadata().join("result.json.upload").exists());
    assert_eq!(
        harness.fixture.traffic.lock().unwrap().calls,
        ["succeed", "prepare", "prepare"]
    );
}

#[tokio::test]
async fn archive_claim_loss_keeps_unuploaded_files() {
    let harness = Harness::new("succeed", None).await;
    let manager = harness.manager().await;
    for (status, code) in [
        (StatusCode::CONFLICT, "RAYFLOW_JOB_CONFLICT"),
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "RAYFLOW_JOB_RESULT_OWNER_UNKNOWN",
        ),
        (StatusCode::CONFLICT, "RAYFLOW_JOB_RESULT_OWNER_UNKNOWN"),
    ] {
        harness.fixture.traffic.lock().unwrap().prepare_error = Some((status, code));
        assert!(harness.report(&manager).await.is_err());
        assert!(harness.metadata().join("result.json").exists());
        assert!(!harness.metadata().join("result.json.upload").exists());
    }
    assert_eq!(
        harness.fixture.traffic.lock().unwrap().calls,
        ["succeed", "prepare", "prepare", "prepare"]
    );
}

#[tokio::test]
async fn redirected_result_upload_is_retried_with_the_same_bytes() {
    let harness = Harness::new("succeed", None).await;
    let result_path = harness.metadata().join("result.json");
    let expected = fs::read(&result_path).await.unwrap();
    harness
        .fixture
        .traffic
        .lock()
        .unwrap()
        .result
        .upload_failure = Some(StatusCode::TEMPORARY_REDIRECT);
    let manager = harness.manager().await;
    assert!(harness.report(&manager).await.is_err());
    assert!(result_path.exists());
    assert!(!harness.metadata().join("result.json.upload").exists());
    {
        let mut traffic = harness.fixture.traffic.lock().unwrap();
        assert_eq!(traffic.calls, ["succeed", "prepare", "put"]);
        traffic.fail_graph = true;
    }
    harness.report(&manager).await.unwrap();
    let traffic = harness.fixture.traffic.lock().unwrap();
    assert_eq!(
        traffic.calls,
        ["succeed", "prepare", "put", "prepare", "put", "commit"]
    );
    assert_eq!(traffic.graph_requests, 1);
    assert_eq!(traffic.result.put_bodies, [expected.clone(), expected]);
}

#[tokio::test]
async fn recovers_a_lost_commit_reply_without_uploading_the_result_twice() {
    let harness = Harness::new("fail", None).await;
    harness
        .fixture
        .traffic
        .lock()
        .unwrap()
        .result
        .lose_commit_reply = true;
    let manager = harness.manager().await;
    assert!(harness.report(&manager).await.is_err());
    assert!(!harness.metadata().join("result.json.upload").exists());
    drop(manager);
    let recovered = harness.manager().await;
    harness.report(&recovered).await.unwrap();
    assert!(harness.metadata().join("result.json.upload").exists());
    assert_eq!(
        harness.fixture.traffic.lock().unwrap().calls,
        ["fail", "prepare", "put", "commit", "prepare"]
    );
}

#[tokio::test]
async fn retries_a_terminal_action_after_restart_with_the_graph_offline() {
    let harness = Harness::new("succeed", None).await;
    harness.fixture.traffic.lock().unwrap().fail_action = true;
    let manager = harness.manager().await;
    assert!(harness.report(&manager).await.is_err());
    drop(manager);
    harness.fixture.traffic.lock().unwrap().fail_graph = true;
    let recovered = harness.manager().await;
    harness.report(&recovered).await.unwrap();
    let traffic = harness.fixture.traffic.lock().unwrap();
    assert_eq!(
        traffic.calls,
        ["succeed", "succeed", "prepare", "put", "commit"]
    );
    assert_eq!(traffic.graph_requests, 1);
}

#[tokio::test]
async fn retries_the_initial_graph_read_without_rewriting_a_successful_result() {
    let harness = Harness::new("succeed", None).await;
    let result_path = harness.metadata().join("result.json");
    let expected = fs::read(&result_path).await.unwrap();
    harness.fixture.traffic.lock().unwrap().fail_graph = true;
    let manager = harness.manager().await;
    assert!(matches!(
        harness.report(&manager).await,
        Err(Error::JobManager(_))
    ));
    assert_eq!(fs::read(&result_path).await.unwrap(), expected);
    assert!(!harness.metadata().join(COMPLETION_FILE).exists());
    {
        let mut traffic = harness.fixture.traffic.lock().unwrap();
        assert!(traffic.calls.is_empty());
        traffic.fail_graph = false;
    }
    harness.report(&manager).await.unwrap();
    let traffic = harness.fixture.traffic.lock().unwrap();
    assert_eq!(traffic.graph_requests, 2);
    assert_eq!(traffic.calls, ["succeed", "prepare", "put", "commit"]);
    assert_eq!(traffic.result.bytes, expected);
}

#[tokio::test]
async fn archives_the_output_validation_failure_even_after_terminal_conflict() {
    let harness = Harness::new("succeed", Some("../invalid")).await;
    harness.fixture.traffic.lock().unwrap().conflict_action = true;
    let manager = harness.manager().await;
    harness.report(&manager).await.unwrap();
    let traffic = harness.fixture.traffic.lock().unwrap();
    assert_eq!(traffic.calls, ["fail", "prepare", "put", "commit"]);
    let uploaded: JobResult = serde_json::from_slice(&traffic.result.bytes).unwrap();
    assert_eq!(uploaded.outcome.as_deref(), Some("fail"));
    assert_eq!(uploaded.exit_code, 1);
    assert!(harness.metadata().join("result.json").exists());
}

#[tokio::test]
async fn archives_result_but_retains_outputs_when_the_output_claim_expires() {
    let harness = Harness::new("succeed", Some("result.bin")).await;
    let expected = fs::read(harness.metadata().join("result.json"))
        .await
        .unwrap();
    harness.fixture.traffic.lock().unwrap().output_prepare_error = Some(StatusCode::CONFLICT);
    let manager = harness.manager().await;
    harness.report(&manager).await.unwrap();
    assert!(harness.upload_usage(&manager).await.unwrap());
    harness.report(&manager).await.unwrap();
    let traffic = harness.fixture.traffic.lock().unwrap();
    assert_eq!(
        traffic.calls,
        [
            "output prepare",
            "prepare",
            "put",
            "commit",
            "usage prepare",
            "usage put",
            "usage commit"
        ]
    );
    assert_eq!(traffic.result.bytes, expected);
    assert!(harness.data().join("outputs/result.bin").exists());
}

#[tokio::test]
async fn recovers_legacy_frozen_result_bytes_and_upload_receipt() {
    let harness = Harness::new("succeed", None).await;
    let dir = harness.metadata();
    let expected = fs::read_to_string(dir.join("result.json")).await.unwrap();
    write_json(
        &dir.join(LEGACY_PUBLICATION_FILE),
        &json!({
            "job_id": harness.fixture.job.uuid,
            "action": "succeeded",
            "json": expected,
        }),
    )
    .await
    .unwrap();
    // A different serialization would violate the size pinned by the old prepare.
    fs::write(dir.join("result.json"), format!("{expected}\n"))
        .await
        .unwrap();
    harness.fixture.traffic.lock().unwrap().fail_graph = true;
    let manager = harness.manager().await;
    harness
        .fixture
        .traffic
        .lock()
        .unwrap()
        .result
        .upload_failure = Some(StatusCode::SERVICE_UNAVAILABLE);
    assert!(harness.report(&manager).await.is_err());
    drop(manager);
    write_json(
        &dir.join(LEGACY_RECEIPT_FILE),
        &json!({
            "job_id": harness.fixture.job.uuid,
            "catalog_uuid": harness.fixture.catalog_id,
            "result": serde_json::from_str::<Value>(&expected).unwrap(),
        }),
    )
    .await
    .unwrap();
    let recovered = harness.manager().await;
    harness.report(&recovered).await.unwrap();
    assert!(dir.join("result.json.upload").exists());
    let traffic = harness.fixture.traffic.lock().unwrap();
    assert_eq!(traffic.calls, ["succeed", "prepare", "put"]);
    assert_eq!(traffic.result.put_bodies, [expected.as_bytes()]);
    assert_eq!(traffic.graph_requests, 0);
}

#[tokio::test]
async fn resumes_cleanup_after_partial_metadata_removal() {
    let harness = Harness::new("succeed", None).await;
    let dir = harness.metadata();
    write_json(&dir.join(CLEANUP_FILE), &true).await.unwrap();
    fs::remove_file(dir.join("job.json")).await.unwrap();
    fs::remove_file(dir.join("result.json")).await.unwrap();
    let manager = harness.manager().await;
    let cleaned = async {
        while fs::try_exists(&dir).await.unwrap() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    tokio::select! {
        result = manager.start_lifecycle() => panic!("lifecycle monitor stopped: {result:?}"),
        result = tokio::time::timeout(Duration::from_secs(5), cleaned) => result.unwrap(),
    }
    assert!(!harness.data().exists());
}

#[tokio::test]
async fn cleans_shared_metadata_and_data_directory() {
    let mut harness = Harness::new("succeed", Some("result.bin")).await;
    let metadata = harness.metadata();
    let data = harness.data();
    for entry in ["inputs", "outputs"] {
        fs::rename(data.join(entry), metadata.join(entry))
            .await
            .unwrap();
    }
    harness.settings.data_dir = harness.settings.workdir.clone();
    let manager = harness.manager().await;
    assert!(harness.upload_usage(&manager).await.unwrap());
    harness.report(&manager).await.unwrap();
    assert!(harness.fixture.traffic.lock().unwrap().output_committed);
    assert!(!metadata.exists());
}
