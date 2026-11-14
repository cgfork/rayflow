use agent::{client::Client, errors::Error, filed::Filed, settings::Settings};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
use catalog::{
    PresignOperation, PresignedUrl,
    artifact::{
        CommitUploadRequest, DIRECTORY_FORMAT, DIRECTORY_MANIFEST, DownloadRequest,
        PrepareUploadRequest, Transfer,
    },
    model::Catalog,
};
use event::task::Task;
use graph::types::{DAG, Input, Kind, Node, Output};
use jiff::Timestamp;
use scheduler::{Job, ResourceCapacity};
use serde_json::json;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tempfile::TempDir;
use tokio::{fs, net::TcpListener, task::JoinHandle};
use uuid::Uuid;

type Key = String;
#[derive(Default)]
struct Data {
    catalogs: HashMap<Uuid, Catalog>,
    objects: HashMap<Uuid, Vec<u8>>,
    published: HashMap<Key, Uuid>,
    identities: HashMap<Key, Uuid>,
    prepared: Vec<(Key, Uuid)>,
    commits: Vec<Key>,
    downloads: Vec<Key>,
    fail_put: bool,
    graphs: HashMap<u64, DAG>,
    nodes: Vec<serde_json::Value>,
}
#[derive(Clone)]
struct Api {
    base: String,
    data: Arc<Mutex<Data>>,
}
fn fleet(headers: &HeaderMap) {
    assert_eq!(headers.get("authorization").unwrap(), "Bearer fleet-secret");
}
fn transfer(api: &Api, catalog: Catalog, operation: PresignOperation) -> Transfer {
    Transfer {
        presigned: PresignedUrl {
            url: format!("{}/objects/{}?signature=secret", api.base, catalog.uuid),
            operation,
            headers: [("x-required".into(), "signed".into())].into(),
            expires_at: Timestamp::now() + Duration::from_secs(60),
        },
        catalog,
    }
}
async fn prepare(
    State(api): State<Api>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Json<Transfer> {
    fleet(&headers);
    assert!(body.get("catalog_uuid").is_none());
    let request: PrepareUploadRequest = serde_json::from_value(body).unwrap();
    let mut data = api.data.lock().unwrap();
    assert!(!request.job_id.is_nil());
    let key = request.key;
    let uuid = *data
        .identities
        .entry(key.clone())
        .or_insert_with(Uuid::new_v4);
    let catalog = Catalog::new(
        uuid,
        key.clone(),
        format!("s3://bucket/{uuid}"),
        request.size,
        request.format,
    );
    data.prepared.push((key, uuid));
    data.catalogs.insert(catalog.uuid, catalog.clone());
    Json(transfer(&api, catalog, PresignOperation::Upload))
}
async fn commit(
    State(api): State<Api>,
    headers: HeaderMap,
    Json(request): Json<CommitUploadRequest>,
) -> StatusCode {
    fleet(&headers);
    let mut data = api.data.lock().unwrap();
    assert_eq!(
        data.objects[&request.catalog_uuid].len() as u64,
        request.size
    );
    assert!(!request.job_id.is_nil());
    let key = request.key;
    data.published.insert(key.clone(), request.catalog_uuid);
    data.commits.push(key);
    StatusCode::NO_CONTENT
}
async fn download(
    State(api): State<Api>,
    headers: HeaderMap,
    Json(request): Json<DownloadRequest>,
) -> axum::response::Response {
    fleet(&headers);
    assert!(!request.job_id.is_nil());
    let mut data = api.data.lock().unwrap();
    data.downloads.push(request.key.clone());
    match data.published.get(&request.key) {
        Some(id) => Json(transfer(
            &api,
            data.catalogs[id].clone(),
            PresignOperation::Download,
        ))
        .into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}
async fn put_object(
    State(api): State<Api>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    bytes: Bytes,
) -> StatusCode {
    assert!(!headers.contains_key("authorization"));
    assert_eq!(headers.get("x-required").unwrap(), "signed");
    assert_eq!(
        headers["content-length"]
            .to_str()
            .unwrap()
            .parse::<usize>()
            .unwrap(),
        bytes.len()
    );
    let mut data = api.data.lock().unwrap();
    if std::mem::take(&mut data.fail_put) {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    data.objects.insert(id, bytes.to_vec());
    StatusCode::OK
}
async fn get_object(State(api): State<Api>, Path(id): Path<Uuid>, headers: HeaderMap) -> Vec<u8> {
    assert!(!headers.contains_key("authorization"));
    assert_eq!(headers.get("x-required").unwrap(), "signed");
    api.data.lock().unwrap().objects[&id].clone()
}
struct Fixture {
    root: TempDir,
    settings: Settings,
    filed: Filed,
    api: Api,
    server: JoinHandle<()>,
}
impl Fixture {
    async fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let api = Api {
            base: base.clone(),
            data: Default::default(),
        };
        let router = Router::new()
            .route("/catalog/prepare", post(prepare))
            .route("/catalog/commit", post(commit))
            .route("/catalog/download", post(download))
            .route("/graphs/{id}", get(|State(api): State<Api>, Query(query): Query<HashMap<String, String>>| async move {
                let version: u64 = query["version"].parse().unwrap();
                Json(api.data.lock().unwrap().graphs[&version].clone())
            }))
            .route("/runs/{id}", get(|State(api): State<Api>, Path(id): Path<Uuid>, Query(query): Query<HashMap<String, String>>, headers: HeaderMap| async move {
                fleet(&headers);
                let revision: u64 = query["revision"].parse().unwrap();
                let data = api.data.lock().unwrap();
                let nodes: Vec<_> = data.nodes.iter().filter(|n| n["revision"].as_u64().unwrap() <= revision).cloned().collect();
                Json(json!({"state":{"uuid":id,"nodes":nodes}}))
            }))
            .route("/objects/{id}", get(get_object).put(put_object))
            .with_state(api.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let mut settings = Settings::new(
            Uuid::new_v4(),
            base,
            root.path().join("jobs"),
            ResourceCapacity {
                cpu_cores: 1,
                memory_bytes: 1024,
            },
        );
        settings.data_dir = root.path().join("data");
        settings.auth_token = Some("fleet-secret".into());
        settings.request_timeout = Duration::from_secs(3);
        let filed = Filed::new(Client::new(&settings).unwrap(), &settings).unwrap();
        Self {
            root,
            settings,
            filed,
            api,
            server,
        }
    }
    fn graph(&self, version: u64, name: &str, nodes: Vec<Node>) {
        self.api.data.lock().unwrap().graphs.insert(
            version,
            DAG {
                id: Uuid::from_u128(77),
                name: name.into(),
                version,
                nodes,
                ..Default::default()
            },
        );
    }
    fn succeeded(&self, node: Uuid, version: u64, revision: u64) {
        self.api.data.lock().unwrap().nodes.push(json!({"node_id":node,"version":version,"revision":revision,"status":"Succeeded","ineffective":0}));
    }
    fn restart(&self) -> Filed {
        Filed::new(Client::new(&self.settings).unwrap(), &self.settings).unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}
fn job(run: Uuid, node: Uuid, revision: u64) -> Job {
    let task = Task::new(Uuid::new_v4(), Uuid::from_u128(77), node, 1);
    let job = Job::new(&task, run, 1, 1);
    let mut value = serde_json::to_value(job).unwrap();
    let now = Timestamp::now();
    value["run"] = json!({"id":1,"uuid":run,"data_id":task.data_id,"graph_id":task.graph_id,"version":1,"revision":revision,"created_at":now,"updated_at":now});
    value["node_run"] = json!({"id":1,"run_id":run,"data_id":task.data_id,"graph_id":task.graph_id,"version":1,"node_id":node,"revision":revision,"status":"Running","created_at":now,"updated_at":now});
    serde_json::from_value(value).unwrap()
}
fn node(id: Uuid) -> Node {
    Node {
        id,
        name: "producer".into(),
        node_type: "shell".into(),
        settings: Default::default(),
        inputs: vec![],
        outputs: vec![],
    }
}
fn output(name: &str, kind: Kind) -> Output {
    Output {
        file_name: name.into(),
        kind,
        multiple: false,
    }
}
fn input(name: &str, required: bool) -> Input {
    Input {
        file_name: name.into(),
        source: format!("producer.{name}"),
        required,
        format: None,
    }
}

#[tokio::test]
async fn transfers_files_and_directories_with_durable_retry_and_scoped_data() {
    let f = Fixture::new().await;
    let run = Uuid::new_v4();
    let node_id = Uuid::new_v4();
    let producer = job(run, node_id, 7);
    let data = f.filed.file_fs(&producer);
    assert_eq!(
        data.dir(),
        f.settings
            .data_dir
            .join(run.to_string())
            .join(node_id.to_string())
            .join("7")
    );
    assert_eq!(data.dir(), f.filed.file_fs(&job(run, node_id, 7)).dir());
    assert_ne!(
        data.dir(),
        f.filed.file_fs(&job(Uuid::new_v4(), node_id, 7)).dir()
    );
    assert_ne!(data.dir(), f.filed.file_fs(&job(run, node_id, 8)).dir());
    data.prepare().await.unwrap();
    let bytes = vec![0x71; 180_000];
    fs::write(data.outputs_dir().join("result"), &bytes)
        .await
        .unwrap();
    fs::create_dir_all(data.outputs_dir().join("tree/empty"))
        .await
        .unwrap();
    fs::create_dir_all(data.outputs_dir().join("tree/nested"))
        .await
        .unwrap();
    fs::write(
        data.outputs_dir().join("tree/nested/file.txt"),
        "nested bytes",
    )
    .await
    .unwrap();
    let mut producer_node = node(node_id);
    producer_node.outputs = vec![
        output("result", Kind::File),
        output("tree", Kind::Directory),
    ];
    producer_node.name = "old producer".into();
    f.graph(1, "old flow", vec![producer_node.clone()]);
    f.api.data.lock().unwrap().fail_put = true;
    let error = f
        .filed
        .upload_outputs(&producer, &producer_node)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Http(_)));
    assert!(!error.to_string().contains("signature="));
    assert!(f.api.data.lock().unwrap().commits.is_empty());
    let resumed = f.restart();
    resumed
        .upload_outputs(&producer, &producer_node)
        .await
        .unwrap();
    {
        let stored = f.api.data.lock().unwrap();
        assert_eq!(
            stored.prepared[0], stored.prepared[1],
            "retry must preserve catalog UUID"
        );
        assert_eq!(
            stored.commits.last().unwrap(),
            &format!("{run}/old%20flow/1/old%20producer/7/tree/{DIRECTORY_MANIFEST}"),
            "manifest is published last"
        );
        assert_eq!(stored.commits.len(), 3);
    }
    resumed
        .upload_outputs(&producer, &producer_node)
        .await
        .unwrap();
    assert_eq!(
        f.api.data.lock().unwrap().commits.len(),
        3,
        "recovery checkpoint prevents duplicate upload"
    );
    let consumer = job(run, Uuid::new_v4(), 9);
    let mut value = serde_json::to_value(consumer).unwrap();
    value["run"]["version"] = json!(2);
    value["node_run"]["version"] = json!(2);
    let consumer: Job = serde_json::from_value(value).unwrap();
    let mut consumer_node = node(consumer.node_run.get().node_id);
    consumer_node.inputs = vec![
        input("copied", true),
        input("tree", true),
        input("missing", false),
    ];
    consumer_node.inputs[0].source = "producer.result".into();
    producer_node.name = "producer".into();
    producer_node.outputs.push(output("missing", Kind::File));
    f.graph(2, "flow", vec![producer_node, consumer_node.clone()]);
    f.succeeded(node_id, 1, 7);
    resumed
        .download_inputs(&consumer, &consumer_node)
        .await
        .unwrap();
    let inputs = resumed.file_fs(&consumer).inputs_dir();
    assert_eq!(fs::read(inputs.join("copied")).await.unwrap(), bytes);
    assert_eq!(
        fs::read_to_string(inputs.join("tree/nested/file.txt"))
            .await
            .unwrap(),
        "nested bytes"
    );
    assert!(inputs.join("tree/empty").is_dir());
    assert!(inputs.join("tree").join(DIRECTORY_MANIFEST).is_file());
    assert!(!inputs.join("missing").exists());
    assert!(
        data.outputs_dir().join("result").is_file(),
        "producer bytes remain available"
    );
    assert!(
        !f.settings.workdir.exists(),
        "artifact transfers must not use JobFs"
    );
    assert!(f.root.path().join("data").is_dir());
}

#[tokio::test]
async fn rejects_required_missing_inputs_and_escaping_manifests() {
    let f = Fixture::new().await;
    let consumer = job(Uuid::new_v4(), Uuid::new_v4(), 1);
    let mut n = node(consumer.node_run.get().node_id);
    n.inputs = vec![input("missing", true)];
    let mut producer_node = node(Uuid::new_v4());
    producer_node.outputs = vec![
        output("missing", Kind::File),
        output("tree", Kind::Directory),
    ];
    f.graph(1, "flow", vec![producer_node.clone(), n.clone()]);
    f.succeeded(producer_node.id, 1, 0);
    assert!(matches!(
        f.filed.download_inputs(&consumer, &n).await,
        Err(Error::Artifact(_))
    ));
    let bytes = serde_json::to_vec(
        &json!({"version":1,"entries":[{"path":"../../outside","kind":"file","size":1}]}),
    )
    .unwrap();
    let id = Uuid::new_v4();
    {
        let mut data = f.api.data.lock().unwrap();
        data.catalogs.insert(
            id,
            Catalog::new(
                id,
                "/bad",
                "s3://bucket/bad",
                bytes.len() as u64,
                DIRECTORY_FORMAT,
            ),
        );
        data.objects.insert(id, bytes);
        data.published.insert(
            format!(
                "{}/flow/1/producer/0/tree/{DIRECTORY_MANIFEST}",
                consumer.run_id
            ),
            id,
        );
    }
    n.inputs = vec![input("tree", true)];
    assert!(matches!(
        f.filed.download_inputs(&consumer, &n).await,
        Err(Error::Artifact(_))
    ));
    assert!(!f.filed.file_fs(&consumer).dir().join("outside").exists());
}

#[tokio::test]
async fn does_not_download_outputs_of_unfinished_ineffective_or_future_producers() {
    let f = Fixture::new().await;
    let consumer = job(Uuid::new_v4(), Uuid::new_v4(), 1);
    let mut consumer_node = node(consumer.node_run.get().node_id);
    consumer_node.inputs = vec![input("result", true)];
    let mut producer = node(Uuid::new_v4());
    producer.outputs = vec![output("result", Kind::File)];
    f.graph(1, "flow", vec![producer.clone(), consumer_node.clone()]);
    let state = |revision, status, ineffective| json!({"node_id":producer.id,"version":1,"revision":revision,"status":status,"ineffective":ineffective});
    for nodes in [
        vec![state(0, "Running", 0)],
        vec![state(0, "Succeeded", 1)],
        vec![state(2, "Succeeded", 0)],
        vec![state(0, "Succeeded", 0), state(1, "Running", 0)],
    ] {
        f.api.data.lock().unwrap().nodes = nodes;
        assert!(matches!(
            f.filed.download_inputs(&consumer, &consumer_node).await,
            Err(Error::Artifact(_))
        ));
        assert!(f.api.data.lock().unwrap().downloads.is_empty());
    }
}

#[cfg(unix)]
#[tokio::test]
async fn rejects_output_symlinks_without_reading_the_target() {
    let f = Fixture::new().await;
    let producer = job(Uuid::new_v4(), Uuid::new_v4(), 1);
    let data = f.filed.file_fs(&producer);
    data.prepare().await.unwrap();
    let outside = f.root.path().join("private");
    fs::write(&outside, "private bytes").await.unwrap();
    tokio::fs::symlink(&outside, data.outputs_dir().join("result"))
        .await
        .unwrap();
    let mut n = node(producer.node_run.get().node_id);
    n.outputs = vec![output("result", Kind::File)];
    f.graph(1, "flow", vec![n.clone()]);
    assert!(matches!(
        f.filed.upload_outputs(&producer, &n).await,
        Err(Error::Artifact(_))
    ));
    assert!(f.api.data.lock().unwrap().prepared.is_empty());
}
