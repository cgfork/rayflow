//! Exercises real HTTP transfers against a loopback fixture. When using an HTTP
//! proxy, include `127.0.0.1` in `NO_PROXY`.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use agent::{
    client::{Client, Transfer},
    settings::Settings,
    transfer::JobTransfer,
};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post, put},
};
use catalog::{
    PresignOperation, PresignedUrl,
    directory::{
        DIRECTORY_FORMAT, DIRECTORY_MANIFEST, DirectoryEntry, DirectoryManifest, EntryKind,
    },
    model::Catalog,
};
use event::task::Task;
use graph::types::{DAG, Input, Kind, Node, NodeSettings, Output};
use jiff::Timestamp;
use runtime::command::{NodeRun, Run};
use scheduler::{Job, ResourceCapacity, actuator::Status};
use serde::Deserialize;
use serde_json::{Value, json};
use tempfile::tempdir;
use tokio::{fs, net::TcpListener};
use uuid::Uuid;

const SOURCE: &[u8] = b"input bytes\0\xff";
const OUTPUT: &[u8] = b"output bytes\0\xfe";
const MEMBER: &[u8] = b"nested file bytes";
const SIGNATURE: &str = "x-test-object-signature";

struct Fixture {
    graph: DAG,
    job: Job,
    agent_id: Uuid,
    endpoint: String,
    traffic: Mutex<Traffic>,
}

#[derive(Default)]
struct Traffic {
    calls: Vec<String>,
    uploads: BTreeMap<Uuid, Upload>,
}

struct Upload {
    filename: String,
    size: u64,
    format: String,
    bytes: Option<Bytes>,
}

impl Fixture {
    fn new(endpoint: String, agent_id: Uuid) -> Self {
        let node = |name: &str, inputs: Vec<Input>, outputs: Vec<Output>| Node {
            id: Uuid::new_v4(),
            name: name.into(),
            node_type: "docker".into(),
            settings: NodeSettings::default(),
            inputs,
            outputs,
        };
        let output = |name: &str, kind| Output {
            file_name: name.into(),
            kind,
        };
        let producer = node("producer", vec![], vec![output("original.bin", Kind::File)]);
        let consumer = node(
            "consumer",
            vec![Input {
                file_name: "staged/renamed.bin".into(),
                source: "producer.original.bin".into(),
                required: true,
                format: None,
            }],
            vec![
                output("result.bin", Kind::File),
                output("bundle", Kind::Directory),
            ],
        );
        let graph = DAG {
            id: Uuid::new_v4(),
            name: "transfer-integration".into(),
            version: 7,
            edges: vec![(producer.id, consumer.id)],
            nodes: vec![producer.clone(), consumer.clone()],
            ..DAG::default()
        };
        let now = Timestamp::now();
        let run = Run {
            id: 1,
            uuid: Uuid::new_v4(),
            data_id: Uuid::new_v4(),
            graph_id: graph.id,
            version: graph.version,
            revision: 3,
            created_at: now,
            updated_at: now,
        };
        let node_run = |id, node_id, status: &str| NodeRun {
            id,
            uuid: Uuid::new_v4(),
            run_id: run.uuid,
            data_id: run.data_id,
            graph_id: graph.id,
            version: graph.version,
            node_id,
            revision: run.revision,
            status: status.into(),
            ineffective: 0,
            created_at: now,
            updated_at: now,
            run: run.clone().into(),
        };
        let consumer = node_run(2, consumer.id, "Running");
        let task = Task::new(run.data_id, graph.id, consumer.node_id, consumer.id);
        let mut job = Job::new(&task, run.uuid, Uuid::new_v4(), Uuid::new_v4());
        job.node_run_id = consumer.uuid;
        job.status = Status::Polled;
        job.agent_id = Some(agent_id);
        job.run = run.clone().into();
        job.node_run = consumer.into();
        Self {
            graph,
            job,
            agent_id,
            endpoint,
            traffic: Mutex::default(),
        }
    }

    fn record(&self, call: impl Into<String>) {
        self.traffic.lock().unwrap().calls.push(call.into());
    }

    fn verify_claim(&self, request: &Claim) {
        assert_eq!(request.job_id, self.job.uuid);
        assert_eq!(request.agent_id, self.agent_id);
    }

    fn transfer(
        &self,
        uuid: Uuid,
        filename: &str,
        size: u64,
        format: &str,
        operation: PresignOperation,
        object: &str,
    ) -> Transfer {
        let mut catalog = Catalog::new(uuid, filename, format!("s3://test/{object}"), size, format);
        catalog.uploaded = operation == PresignOperation::Download;
        Transfer {
            catalog,
            presigned: PresignedUrl {
                url: format!("{}/objects/{object}", self.endpoint),
                operation,
                headers: BTreeMap::from([(SIGNATURE.into(), "fixture".into())]),
                expires_at: Timestamp::now()
                    .checked_add(Duration::from_secs(30))
                    .unwrap(),
            },
            multipart: false,
        }
    }
}

#[derive(Deserialize)]
struct Claim {
    job_id: Uuid,
    agent_id: Uuid,
    filename: String,
}

#[derive(Deserialize)]
struct Prepare {
    #[serde(flatten)]
    claim: Claim,
    size: u64,
    format: String,
}

#[derive(Deserialize)]
struct Commit {
    #[serde(flatten)]
    claim: Claim,
    catalog_uuid: Uuid,
}

async fn presign_input(
    State(fixture): State<Arc<Fixture>>,
    Json(claim): Json<Claim>,
) -> Json<Transfer> {
    fixture.verify_claim(&claim);
    assert_eq!(claim.filename, "staged/renamed.bin");
    fixture.record(format!("presign {}", claim.filename));
    Json(fixture.transfer(
        Uuid::new_v4(),
        "original.bin",
        SOURCE.len() as u64,
        "bin",
        PresignOperation::Download,
        "original.bin",
    ))
}

async fn download_object(
    State(fixture): State<Arc<Fixture>>,
    Path(object): Path<String>,
    headers: HeaderMap,
) -> &'static [u8] {
    assert_eq!(headers[SIGNATURE], "fixture");
    assert_eq!(object, "original.bin");
    fixture.record(format!("download {object}"));
    SOURCE
}

async fn prepare_output(
    State(fixture): State<Arc<Fixture>>,
    Json(request): Json<Prepare>,
) -> Json<Value> {
    fixture.verify_claim(&request.claim);
    let uuid = Uuid::new_v4();
    let filename = request.claim.filename;
    let transfer = fixture.transfer(
        uuid,
        &filename,
        request.size,
        &request.format,
        PresignOperation::Upload,
        &uuid.to_string(),
    );
    let mut traffic = fixture.traffic.lock().unwrap();
    traffic.calls.push(format!("prepare {filename}"));
    traffic.uploads.insert(
        uuid,
        Upload {
            filename,
            size: request.size,
            format: request.format,
            bytes: None,
        },
    );
    Json(json!({"transfer": transfer}))
}

async fn upload_object(
    State(fixture): State<Arc<Fixture>>,
    Path(uuid): Path<Uuid>,
    headers: HeaderMap,
    bytes: Bytes,
) -> StatusCode {
    assert_eq!(headers[SIGNATURE], "fixture");
    let mut traffic = fixture.traffic.lock().unwrap();
    let upload = traffic.uploads.get_mut(&uuid).unwrap();
    assert_eq!(upload.size, bytes.len() as u64);
    assert_eq!(headers["content-length"], upload.size.to_string().as_str());
    assert!(upload.bytes.replace(bytes).is_none());
    let filename = upload.filename.clone();
    traffic.calls.push(format!("upload {filename}"));
    StatusCode::OK
}

async fn commit_output(
    State(fixture): State<Arc<Fixture>>,
    Json(request): Json<Commit>,
) -> Json<Uuid> {
    fixture.verify_claim(&request.claim);
    let mut traffic = fixture.traffic.lock().unwrap();
    let upload = traffic.uploads.get(&request.catalog_uuid).unwrap();
    assert_eq!(upload.filename, request.claim.filename);
    assert!(upload.bytes.is_some());
    traffic
        .calls
        .push(format!("commit {}", request.claim.filename));
    Json(Uuid::new_v4())
}

#[tokio::test]
async fn transfers_renamed_input_and_commits_file_and_directory_outputs_once() {
    let root = tempdir().unwrap();
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let mut settings = Settings::new(
        Uuid::new_v4(),
        format!("http://{}", listener.local_addr().unwrap()),
        root.path().join("jobs"),
        ResourceCapacity {
            cpu_cores: 1,
            memory_bytes: 1,
        },
    );
    settings.request_timeout = Duration::from_secs(5);
    let fixture = Arc::new(Fixture::new(settings.server_url.clone(), settings.id));
    let routes = Router::new()
        .route("/api/v1/job/input/presign", post(presign_input))
        .route("/api/v1/job/output/prepare", post(prepare_output))
        .route("/api/v1/job/output/commit", post(commit_output))
        .route(
            "/objects/{object}",
            get(download_object).merge(put(upload_object)),
        )
        .with_state(fixture.clone());
    let server = tokio::spawn(async move { axum::serve(listener, routes).await.unwrap() });

    let data = settings.data_dir.join(fixture.job.uuid.to_string());
    let node = fixture
        .graph
        .nodes
        .iter()
        .find(|node| node.id == fixture.job.node_run.get().node_id)
        .unwrap();
    let transfer = JobTransfer::new(Client::new(settings.clone()).unwrap()).unwrap();
    let staged_input = data.join("inputs/staged/renamed.bin");
    transfer
        .prepare_input(&fixture.job, &data, node)
        .await
        .unwrap();
    assert_eq!(fs::read(&staged_input).await.unwrap(), SOURCE);
    assert!(!data.join("inputs/original.bin").exists());

    let output = data.join("outputs");
    fs::create_dir_all(output.join("bundle/empty"))
        .await
        .unwrap();
    fs::create_dir_all(output.join("bundle/nested"))
        .await
        .unwrap();
    fs::write(output.join("result.bin"), OUTPUT).await.unwrap();
    fs::write(output.join("bundle/nested/member.txt"), MEMBER)
        .await
        .unwrap();
    transfer
        .complete_output(&fixture.job, &data, node)
        .await
        .unwrap();
    let manifest_filename = format!("bundle/{DIRECTORY_MANIFEST}");
    let manifest_on_disk = fs::read(output.join(&manifest_filename)).await.unwrap();

    drop(transfer);
    // Reopen the same workdir with fresh handles and no HTTP service. Only
    // the durable completion markers can make these retries succeed.
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
    let restored_job: Job =
        serde_json::from_slice(&serde_json::to_vec(&fixture.job).unwrap()).unwrap();
    let transfer = JobTransfer::new(Client::new(settings).unwrap()).unwrap();
    transfer
        .prepare_input(&restored_job, &data, node)
        .await
        .unwrap();
    transfer
        .complete_output(&restored_job, &data, node)
        .await
        .unwrap();
    assert_eq!(fs::read(staged_input).await.unwrap(), SOURCE);

    let traffic = fixture.traffic.lock().unwrap();
    assert_eq!(
        traffic.calls,
        [
            "presign staged/renamed.bin",
            "download original.bin",
            "prepare result.bin",
            "upload result.bin",
            "commit result.bin",
            "prepare bundle/nested/member.txt",
            "upload bundle/nested/member.txt",
            "commit bundle/nested/member.txt",
            &format!("prepare {manifest_filename}"),
            &format!("upload {manifest_filename}"),
            &format!("commit {manifest_filename}"),
        ]
    );
    let uploaded = |filename| {
        let upload = traffic
            .uploads
            .values()
            .find(|upload| upload.filename == filename)
            .unwrap();
        (upload.format.as_str(), upload.bytes.as_deref().unwrap())
    };
    assert_eq!(uploaded("result.bin"), ("bin", OUTPUT));
    assert_eq!(uploaded("bundle/nested/member.txt"), ("txt", MEMBER));
    let (format, manifest_bytes) = uploaded(&manifest_filename);
    assert_eq!(format, DIRECTORY_FORMAT);
    assert_eq!(manifest_bytes, manifest_on_disk);
    let manifest: DirectoryManifest = serde_json::from_slice(manifest_bytes).unwrap();
    assert_eq!(
        manifest,
        DirectoryManifest {
            version: 1,
            entries: vec![
                DirectoryEntry {
                    path: "empty".into(),
                    kind: EntryKind::Directory,
                    size: 0,
                },
                DirectoryEntry {
                    path: "nested".into(),
                    kind: EntryKind::Directory,
                    size: 0,
                },
                DirectoryEntry {
                    path: "nested/member.txt".into(),
                    kind: EntryKind::File,
                    size: MEMBER.len() as u64,
                },
            ],
        }
    );
}
