use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use catalog::{
    CatalogSystem, FileKey, FileSystem,
    artifact::{DIRECTORY_FORMAT, DIRECTORY_MANIFEST},
    model::Catalog,
};
use event::task::Task;
use graph::{
    GraphStore,
    types::{DAG, Input, Kind, Node, Output},
};
use jiff::Timestamp;
use runtime::command::{NodeRun, Run};
use scheduler::{
    Job, JobWrite,
    actuator::{RunConfig, Status},
};
use serde_json::{Value, json};
use server::{App, Config};
use std::{convert::Infallible, time::Duration};
use toasty::Db;
use tower::ServiceExt;
use uuid::Uuid;

#[derive(Clone)]
struct Versions(Vec<DAG>);
impl GraphStore<Uuid> for Versions {
    type G = DAG;
    type Err = Infallible;
    async fn load(&mut self, _: Uuid, version: Option<u64>) -> Result<DAG, Infallible> {
        Ok(self
            .0
            .iter()
            .find(|g| Some(g.version) == version)
            .unwrap()
            .clone())
    }
    async fn graphs(
        &mut self,
        page: graph::PageRequest,
        _: Option<Uuid>,
    ) -> Result<graph::Page<DAG>, Infallible> {
        Ok(page.select(self.0.iter().map(|g| (g.version, g.clone()))))
    }
}

struct Fixture {
    app: App<Versions>,
    db: Db,
    producer: Job,
    consumer: Job,
    agent: Uuid,
    run: Uuid,
}

async fn setup(kind: Kind) -> Option<Fixture> {
    let url = std::env::var("DATABASE_URL").ok()?;
    let mut builder = Db::builder();
    builder.models(server::models());
    builder.table_name_prefix(&format!("artifacts_{}_", Uuid::new_v4().simple()));
    let mut db = builder.connect(&url).await.unwrap();
    db.push_schema().await.unwrap();
    let now = Timestamp::now();
    let graph_id = Uuid::new_v4();
    let producer = Node {
        id: Uuid::new_v4(),
        name: "producer".into(),
        node_type: "shell".into(),
        settings: Default::default(),
        inputs: vec![],
        outputs: vec![Output {
            file_name: "result".into(),
            kind,
            multiple: false,
        }],
    };
    let consumer = Node {
        id: Uuid::new_v4(),
        name: "consumer".into(),
        node_type: "shell".into(),
        settings: Default::default(),
        inputs: vec![Input {
            file_name: "input".into(),
            source: "producer.result".into(),
            required: true,
            format: None,
        }],
        outputs: vec![],
    };
    let current = DAG {
        id: graph_id,
        name: "flow".into(),
        version: 2,
        nodes: vec![producer.clone(), consumer.clone()],
        edges: vec![(producer.id, consumer.id)],
        ..Default::default()
    };
    let mut previous = current.clone();
    previous.version = 1;
    previous.name = "old flow".into();
    previous.nodes[0].name = "old producer".into();
    let run = Run::create()
        .uuid(Uuid::new_v4())
        .data_id(Uuid::new_v4())
        .graph_id(graph_id)
        .version(2)
        .revision(9)
        .created_at(now)
        .updated_at(now)
        .exec(&mut db)
        .await
        .unwrap();
    let agent = Uuid::new_v4();
    let mut jobs = Vec::new();
    for (node, version, revision) in [(producer, 1, 2), (consumer, 2, 7)] {
        let node_run = NodeRun::create()
            .run_id(run.uuid)
            .data_id(run.data_id)
            .graph_id(graph_id)
            .version(version)
            .node_id(node.id)
            .revision(revision)
            .status("Running")
            .created_at(now)
            .updated_at(now)
            .exec(&mut db)
            .await
            .unwrap();
        let config = RunConfig::create()
            .run_id(run.uuid)
            .annotation(toasty::Json(std::collections::HashMap::new()))
            .created_at(now)
            .updated_at(now)
            .created_by("")
            .updated_by("")
            .exec(&mut db)
            .await
            .unwrap();
        let task = Task::new(run.data_id, graph_id, node.id, node_run.id);
        let mut job = Job::new(&task, run.uuid, 0, config.id);
        job.status = Status::Polled;
        job.agent_id = Some(agent);
        job.expired_at = Some(now + Duration::from_secs(300));
        job.write(&mut db).await.unwrap();
        jobs.push(job);
    }
    let config = Config { storage: Some(serde_json::from_value(json!({"endpoint":"http://localhost:9000", "bucket":"artifacts", "prefix":"prefix", "access_key":"access", "secret_key":"secret"})).unwrap()), ..Default::default() };
    Some(Fixture {
        app: App::new(db.clone(), config, Versions(vec![previous, current])),
        db,
        producer: jobs.remove(0),
        consumer: jobs.remove(0),
        agent,
        run: run.uuid,
    })
}

async fn request(f: &Fixture, method: &str, path: &str, body: Value) -> (StatusCode, Value) {
    let response = server::router(f.app.clone())
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 2 * 1024 * 1024)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn action(f: &Fixture, job: Uuid, action: &str, mut body: Value) -> (StatusCode, Value) {
    body["job_id"] = json!(job);
    request(f, "POST", &format!("/catalog/{action}"), body).await
}

#[tokio::test]
async fn catalog_operations_use_agent_supplied_keys_and_acknowledge_publication() {
    let Some(mut f) = setup(Kind::File).await else {
        return;
    };
    Run::filter_by_uuid(f.run)
        .update()
        .version(1)
        .revision(2)
        .exec(&mut f.db)
        .await
        .unwrap();
    let object_key = format!("{}/old%20flow/1/old%20producer/2/result", f.run);
    let prepare = json!({"agent_id":f.agent, "key":object_key, "size":12,"format":"csv"});
    let mut upload = prepare.clone();
    let (status, transfer) = action(&f, f.producer.uuid, "prepare", upload.clone()).await;
    assert_eq!(status, StatusCode::OK, "{transfer}");
    let uuid = serde_json::from_value(transfer["catalog"]["uuid"].clone()).unwrap();
    upload["catalog_uuid"] = json!(uuid);
    // A lost prepare response can be retried before any File has been published.
    let (status, retry) = action(&f, f.producer.uuid, "prepare", prepare.clone()).await;
    assert_eq!(status, StatusCode::OK, "{retry}");
    assert_eq!(retry["catalog"]["uuid"], json!(uuid));
    assert_eq!(Catalog::all().exec(&mut f.db).await.unwrap().len(), 1);
    let key = format!("/prefix/{}/old%20flow/1/old%20producer/2/result", f.run);
    assert_eq!(transfer["catalog"]["key"], key);
    assert_eq!(
        transfer["catalog"]["url"],
        format!("s3://artifacts{}", key.replace('%', "%25"))
    );
    assert_eq!(transfer["presigned"]["operation"], "upload");
    assert_eq!(transfer["catalog"]["uploaded"], false);
    assert!(!f.db.get_catalog(uuid).await.unwrap().unwrap().uploaded);
    assert!(
        transfer["presigned"]["url"]
            .as_str()
            .unwrap()
            .contains("X-Amz-Signature=")
    );
    assert!(
        f.db.get_file(FileKey::Fullpath(&key))
            .await
            .unwrap()
            .is_none()
    );
    let mut invalid = upload.clone();
    invalid["size"] = json!(13);
    assert_eq!(
        action(&f, f.producer.uuid, "commit", invalid).await.0,
        StatusCode::BAD_REQUEST
    );
    assert!(!f.db.get_catalog(uuid).await.unwrap().unwrap().uploaded);
    let (status, body) = action(&f, f.producer.uuid, "commit", upload.clone()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["fullpath"], key);
    let file_uuid = body["uuid"].as_str().unwrap().to_owned();
    assert_ne!(file_uuid, uuid.to_string());
    assert!(f.db.get_catalog(uuid).await.unwrap().unwrap().uploaded);
    let (status, retry) = action(&f, f.producer.uuid, "commit", upload.clone()).await;
    assert_eq!(status, StatusCode::OK, "{retry}");
    assert_eq!(retry["uuid"], file_uuid);
    assert!(f.db.get_catalog(uuid).await.unwrap().unwrap().uploaded);
    assert_eq!(
        action(&f, f.producer.uuid, "prepare", prepare).await.1["catalog"]["uuid"],
        uuid.to_string()
    );
    assert!(
        !f.db.get_catalog(uuid).await.unwrap().unwrap().uploaded,
        "replacement PUT must be acknowledged again"
    );
    assert_eq!(
        action(&f, f.producer.uuid, "commit", upload).await.0,
        StatusCode::OK
    );
    NodeRun::filter_by_id(f.producer.node_run_id)
        .update()
        .status("Succeeded")
        .exec(&mut f.db)
        .await
        .unwrap();
    Job::filter_by_uuid(f.producer.uuid)
        .update()
        .status(Status::Succeeded)
        .exec(&mut f.db)
        .await
        .unwrap();
    Run::filter_by_uuid(f.run)
        .update()
        .version(2)
        .revision(9)
        .exec(&mut f.db)
        .await
        .unwrap();
    let download = json!({"agent_id":f.agent,"key":object_key});
    let (status, body) = action(&f, f.consumer.uuid, "download", download.clone()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["catalog"]["uuid"], uuid.to_string());
    assert_eq!(body["presigned"]["operation"], "download");
    assert_eq!(body["catalog"]["uploaded"], true);
    Catalog::filter_by_uuid(uuid)
        .update()
        .uploaded(false)
        .exec(&mut f.db)
        .await
        .unwrap();
    assert_eq!(
        action(&f, f.consumer.uuid, "download", download.clone())
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    Catalog::filter_by_uuid(uuid)
        .update()
        .uploaded(true)
        .exec(&mut f.db)
        .await
        .unwrap();
    assert_eq!(
        action(&f, f.consumer.uuid, "download", download.clone())
            .await
            .0,
        StatusCode::OK
    );
    let mut wrong = download.clone();
    wrong["agent_id"] = json!(Uuid::new_v4());
    assert_eq!(
        action(&f, f.consumer.uuid, "download", wrong).await.0,
        StatusCode::CONFLICT
    );
    let mut wrong = download.clone();
    wrong["key"] = json!(format!("{}/other/1/node/0/result", Uuid::new_v4()));
    assert_eq!(
        action(&f, f.consumer.uuid, "download", wrong).await.0,
        StatusCode::BAD_REQUEST
    );
    let mut missing = download;
    missing["key"] = json!(format!("{}/missing/1/node/0/result", f.run));
    assert_eq!(
        action(&f, f.consumer.uuid, "download", missing).await.0,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn directory_uploads_reject_escape_and_expired_claims() {
    let Some(mut f) = setup(Kind::Directory).await else {
        return;
    };
    let key = format!("{}/flow/2/producer/2/result/{DIRECTORY_MANIFEST}", f.run);
    let mut upload = json!({"agent_id":f.agent,"key":key,"size":12,"format":DIRECTORY_FORMAT});
    let (status, body) = action(&f, f.producer.uuid, "prepare", upload.clone()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let uuid = serde_json::from_value(body["catalog"]["uuid"].clone()).unwrap();
    assert!(
        body["catalog"]["url"]
            .as_str()
            .unwrap()
            .ends_with(DIRECTORY_MANIFEST)
    );
    for path in [
        format!("{}/../outside", f.run),
        "/absolute".into(),
        key.clone(),
        format!("{}/{DIRECTORY_MANIFEST}/child", f.run),
    ] {
        upload["key"] = json!(path);
        upload["format"] = json!("bin");
        assert_eq!(
            action(&f, f.producer.uuid, "prepare", upload.clone())
                .await
                .0,
            StatusCode::BAD_REQUEST
        );
    }
    Job::filter_by_uuid(f.producer.uuid)
        .update()
        .expired_at(Some(Timestamp::now() - Duration::from_secs(1)))
        .exec(&mut f.db)
        .await
        .unwrap();
    upload["key"] = json!(key);
    upload["format"] = json!(DIRECTORY_FORMAT);
    upload["catalog_uuid"] = json!(uuid);
    assert_eq!(
        action(&f, f.producer.uuid, "commit", upload).await.0,
        StatusCode::CONFLICT
    );
    assert!(!f.db.get_catalog(uuid).await.unwrap().unwrap().uploaded);
}

#[tokio::test]
async fn run_inspection_exposes_producer_versions_at_the_requested_revision() {
    let Some(mut f) = setup(Kind::File).await else {
        return;
    };
    let producer = NodeRun::filter_by_id(f.producer.node_run_id)
        .get(&mut f.db)
        .await
        .unwrap();
    NodeRun::create()
        .run_id(f.run)
        .data_id(producer.data_id)
        .graph_id(producer.graph_id)
        .version(2)
        .node_id(producer.node_id)
        .revision(5)
        .status("Running")
        .created_at(Timestamp::now())
        .updated_at(Timestamp::now())
        .exec(&mut f.db)
        .await
        .unwrap();
    let (status, body) = request(
        &f,
        "GET",
        &format!("/runs/{}?revision=3", f.run),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["state"]["revision"], 3);
    assert_eq!(body["state"]["nodes"].as_array().unwrap().len(), 1);
    assert_eq!(body["state"]["nodes"][0]["version"], 1);
    assert_eq!(body["state"]["nodes"][0]["revision"], 2);
    let (status, body) = request(
        &f,
        "GET",
        &format!("/runs/{}?revision=5", f.run),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["state"]["nodes"][0]["version"], 2);
    assert_eq!(body["state"]["nodes"][0]["revision"], 5);
    assert_eq!(
        request(
            &f,
            "GET",
            &format!("/runs/{}?revision=10", f.run),
            Value::Null
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn catalog_api_exposes_only_transfer_operations() {
    let Some(f) = setup(Kind::File).await else {
        return;
    };
    for action in ["prepare", "commit", "download"] {
        assert_eq!(
            request(
                &f,
                "POST",
                &format!("/catalog/{action}"),
                json!({"agent_id":f.agent})
            )
            .await
            .0,
            StatusCode::UNPROCESSABLE_ENTITY
        );
        assert_eq!(
            request(&f, "GET", &format!("/catalog/{action}"), json!({}))
                .await
                .0,
            StatusCode::METHOD_NOT_ALLOWED
        );
    }
    let id = Uuid::new_v4();
    for path in [
        "/catalog".to_owned(),
        format!("/catalog/{id}"),
        "/files".to_owned(),
        "/files?fullpath=example".to_owned(),
        format!("/files/{id}"),
        "/catalog/complete".into(),
        format!("/jobs/{}/artifacts/prepare", f.producer.uuid),
        format!("/jobs/{}/artifacts/commit", f.producer.uuid),
        format!("/jobs/{}/artifacts/download", f.producer.uuid),
    ] {
        for method in ["GET", "POST", "PUT"] {
            assert_eq!(
                request(&f, method, &path, json!({})).await.0,
                StatusCode::NOT_FOUND,
                "{method} {path}"
            );
        }
    }
}
