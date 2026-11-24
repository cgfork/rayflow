mod support;

use std::time::Duration;

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Method, Request, StatusCode},
};
use catalog::{
    CatalogOptionalData, CatalogSystem, FileInput, FileSystem,
    directory::{DIRECTORY_FORMAT, DIRECTORY_MANIFEST},
};
use graph::store::{GraphDraftRecord, GraphRecord};
use graph::types::{DAG, Input, Kind, Node, NodeSettings, Output, Resource};
use runtime::{backend::Backend, command::NodeRun};
use scheduler::{Actuator, Job, actuator::Status};
use serde_json::{Value, json};
use server::{App, Config, api::PollJobsResponse, config::StorageConfig, router};
use tower::ServiceExt;
use uuid::Uuid;

async fn request(
    app: &Router,
    method: Method,
    path: &str,
    body: Value,
    expected: StatusCode,
) -> Value {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method.clone())
                .uri(format!("/api/v1{path}"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    assert_eq!(
        status,
        expected,
        "{method} {path} response: {}",
        String::from_utf8_lossy(&bytes)
    );
    if bytes.is_empty() {
        return Value::Null;
    }
    serde_json::from_slice(&bytes).unwrap()
}

async fn post(app: &Router, path: &str, body: Value, expected: StatusCode) -> Value {
    request(app, Method::POST, path, body, expected).await
}

async fn poll(app: &Router, agent_id: Uuid) -> Vec<Job> {
    let response: PollJobsResponse = serde_json::from_value(
        post(
            app,
            "/jobs/poll",
            json!({
                "agent_id": agent_id,
                "batch": 8,
                "resources": {"cpu_cores": 8, "memory_bytes": 4096},
                "wait_ms": 0
            }),
            StatusCode::OK,
        )
        .await,
    )
    .unwrap();
    response.jobs
}

async fn poll_one(app: &Router, agent_id: Uuid, node_id: Uuid) -> Job {
    let mut jobs = poll(app, agent_id).await;
    assert_eq!(jobs.len(), 1);
    let job = jobs.pop().unwrap();
    assert_eq!(job.node_run.get().node_id, node_id);
    assert_eq!(job.node_run_id, job.node_run.get().uuid);
    assert_eq!(
        job.node_config_id,
        job.node_config.get().as_ref().unwrap().uuid
    );
    assert_eq!(job.agent_id, Some(agent_id));
    assert_eq!(job.status, Status::Polled);
    job
}

async fn read_nodes(app: &Router, run_id: Uuid) -> Vec<(NodeRun, Node)> {
    let mut response = request(
        app,
        Method::GET,
        &format!("/runs/{run_id}"),
        Value::Null,
        StatusCode::OK,
    )
    .await;
    assert_eq!(response["run"]["uuid"], json!(run_id));
    assert_eq!(response["run"]["revision"], 0);
    serde_json::from_value(response["node_runs"].take()).unwrap()
}

fn node_run(nodes: &[(NodeRun, Node)], node_id: Uuid) -> &NodeRun {
    let (run, node) = nodes.iter().find(|(_, node)| node.id == node_id).unwrap();
    assert_eq!(run.node_id, node.id);
    run
}

fn workflow() -> DAG {
    let mut nodes: Vec<_> = ["build.v2", "consume", "finish"]
        .into_iter()
        .map(|name| Node {
            id: Uuid::new_v4(),
            name: name.into(),
            node_type: "test".into(),
            settings: NodeSettings {
                config: json!({}),
                resource: Resource {
                    cpu_cores: 1,
                    memory_bytes: 512,
                },
                timeout: Duration::from_secs(120),
            },
            inputs: vec![],
            outputs: vec![],
        })
        .collect();
    nodes[0].outputs.push(Output {
        file_name: "result.txt".into(),
        kind: Kind::File,
        multiple: false,
    });
    nodes[0].outputs.push(Output {
        file_name: "report".into(),
        kind: Kind::Directory,
        multiple: false,
    });
    nodes[1].inputs.push(Input {
        file_name: "input.txt".into(),
        source: "build.v2.result.txt".into(),
        required: true,
        format: Some("text/plain".into()),
    });
    nodes[1].inputs.push(Input {
        file_name: "incoming/report".into(),
        source: "build.v2.report".into(),
        required: true,
        format: None,
    });
    DAG {
        id: Uuid::new_v4(),
        name: "server-workflow".into(),
        edges: vec![(nodes[0].id, nodes[1].id), (nodes[1].id, nodes[2].id)],
        nodes,
        ..DAG::default()
    }
}

async fn commit_artifact(
    app: &Router,
    job_id: Uuid,
    agent_id: Uuid,
    filename: &str,
    size: u64,
    format: &str,
) -> Uuid {
    let prepared = post(
        app,
        "/job/output/prepare",
        json!({"job_id": job_id, "agent_id": agent_id, "filename": filename, "size": size, "format": format}),
        StatusCode::OK,
    )
    .await;
    let catalog_uuid: Uuid = serde_json::from_value(prepared["catalog_uuid"].clone()).unwrap();
    assert_eq!(prepared["catalog"]["uuid"], json!(catalog_uuid));
    assert_eq!(prepared["catalog"]["uploaded"], false);
    assert_eq!(prepared["presigned"]["operation"], "upload");
    post(
        app,
        "/job/output/commit",
        json!({"job_id": job_id, "agent_id": agent_id, "filename": filename, "catalog_uuid": catalog_uuid}),
        StatusCode::OK,
    )
    .await;
    catalog_uuid
}

fn storage_config() -> Config {
    Config {
        storage: Some(StorageConfig {
            endpoint: "http://127.0.0.1:9000".into(),
            bucket: "workflow-test".into(),
            prefix: "artifacts".into(),
            region: "us-east-1".into(),
            access_key: "test-key".into(),
            secret_key: "test-secret".into(),
            multipart_enabled: false,
            session_token: None,
            credential_expiry: None,
            presign_ttl: Duration::from_secs(900),
        }),
        ..Config::default()
    }
}

async fn run_workflow(db: toasty::Db) {
    let app = router(App::new(db.clone(), storage_config(), db.clone()));
    let mut actuator = Actuator::new(Backend::new(db.clone(), db), 64);
    let graph = workflow();
    let graph_id = graph.id;
    let producer = graph.nodes[0].id;
    let consumer = graph.nodes[1].id;
    let data_id = Uuid::new_v4();
    let owner = Uuid::new_v4();
    let other_agent = Uuid::new_v4();
    request(
        &app,
        Method::PUT,
        &format!("/graphs/{graph_id}/draft"),
        json!({"graph": graph, "actor": "workflow-test"}),
        StatusCode::OK,
    )
    .await;
    post(
        &app,
        &format!("/graphs/{graph_id}/publish"),
        json!({"actor": "workflow-test"}),
        StatusCode::CREATED,
    )
    .await;
    post(
        &app,
        "/runs/start",
        json!({"data_id": data_id, "graph_id": graph_id}),
        StatusCode::ACCEPTED,
    )
    .await;
    assert_eq!(actuator.tick().await.unwrap().tasks, 1);
    let initial = poll_one(&app, owner, producer).await;

    let rejection = post(
        &app,
        "/job/yield",
        json!({"job_id": initial.uuid, "agent_id": other_agent}),
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(rejection["error_code"], "RAYFLOW_JOB_CONFLICT");
    assert_eq!(actuator.tick().await.unwrap().events, 0);
    assert_eq!(
        node_run(&read_nodes(&app, initial.run_id).await, producer).status,
        "Running"
    );

    let yield_body = json!({"job_id": initial.uuid, "agent_id": owner});
    post(
        &app,
        "/job/yield",
        yield_body.clone(),
        StatusCode::NO_CONTENT,
    )
    .await;
    post(&app, "/job/yield", yield_body, StatusCode::NO_CONTENT).await;
    assert_eq!(actuator.tick().await.unwrap().events, 1);
    let nodes = read_nodes(&app, initial.run_id).await;
    assert_eq!(nodes.len(), 1);
    assert_eq!(node_run(&nodes, producer).status, "Pending");
    assert!(poll(&app, owner).await.is_empty());

    post(
        &app,
        "/runs/resume",
        json!({"data_id": data_id, "graph_id": graph_id, "node": producer}),
        StatusCode::ACCEPTED,
    )
    .await;
    assert_eq!(actuator.tick().await.unwrap().tasks, 1);
    let resumed = poll_one(&app, owner, producer).await;
    assert_ne!(resumed.uuid, initial.uuid);
    assert_ne!(resumed.task_id, initial.task_id);
    assert_eq!(resumed.node_run_id, initial.node_run_id);
    assert_eq!(resumed.run_id, initial.run_id);

    post(
        &app,
        "/job/output/prepare",
        json!({
            "job_id": resumed.uuid,
            "agent_id": owner,
            "filename": "report-other/file.txt",
            "size": 4,
            "format": "text/plain"
        }),
        StatusCode::BAD_REQUEST,
    )
    .await;
    let catalog_uuid =
        commit_artifact(&app, resumed.uuid, owner, "result.txt", 4, "text/plain").await;
    let member_uuid = commit_artifact(
        &app,
        resumed.uuid,
        owner,
        "report/nested/file.txt",
        4,
        "text/plain",
    )
    .await;
    let manifest_uuid = commit_artifact(
        &app,
        resumed.uuid,
        owner,
        &format!("report/{DIRECTORY_MANIFEST}"),
        100,
        DIRECTORY_FORMAT,
    )
    .await;
    post(
        &app,
        "/job/succeed",
        json!({"job_id": resumed.uuid, "agent_id": owner}),
        StatusCode::NO_CONTENT,
    )
    .await;
    assert_eq!(actuator.tick().await.unwrap().events, 1);
    let nodes = read_nodes(&app, resumed.run_id).await;
    assert_eq!(node_run(&nodes, producer).status, "Succeeded");
    assert_eq!(node_run(&nodes, consumer).status, "Ready");
    assert_eq!(actuator.tick().await.unwrap().tasks, 1);

    let downstream = poll_one(&app, owner, consumer).await;
    let input = post(
        &app,
        "/job/input/presign",
        json!({"job_id": downstream.uuid, "agent_id": owner, "filename": "input.txt"}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(input["catalog"]["uuid"], json!(catalog_uuid));
    assert_eq!(input["catalog"]["uploaded"], true);
    assert_eq!(input["catalog"]["size"], 4);
    assert_eq!(input["presigned"]["operation"], "download");

    let browser = post(
        &app,
        &format!("/runs/{}/files/download", resumed.run_id),
        json!({"node_id": consumer, "file_name": "input.txt", "direction": "input"}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(browser["catalog"]["uuid"], json!(catalog_uuid));
    assert_eq!(browser["presigned"]["operation"], "download");

    let browser_directory = post(
        &app,
        &format!("/runs/{}/files/download", resumed.run_id),
        json!({"node_id": consumer, "file_name": "incoming/report", "direction": "input"}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(browser_directory["catalog"]["uuid"], json!(manifest_uuid));
    assert_eq!(browser_directory["presigned"]["operation"], "download");

    for (filename, expected_catalog_uuid) in [
        ("incoming/report", manifest_uuid),
        ("incoming/report/nested/file.txt", member_uuid),
    ] {
        let transfer = post(
            &app,
            "/job/input/presign",
            json!({"job_id": downstream.uuid, "agent_id": owner, "filename": filename}),
            StatusCode::OK,
        )
        .await;
        assert_eq!(transfer["catalog"]["uuid"], json!(expected_catalog_uuid));
        assert_eq!(transfer["catalog"]["uploaded"], true);
        assert_eq!(transfer["presigned"]["operation"], "download");
    }

    post(
        &app,
        "/job/fail",
        json!({"job_id": downstream.uuid, "agent_id": owner}),
        StatusCode::NO_CONTENT,
    )
    .await;
    assert_eq!(actuator.tick().await.unwrap().events, 1);
    assert_eq!(actuator.tick().await.unwrap().tasks, 0);
    let nodes = read_nodes(&app, resumed.run_id).await;
    assert_eq!(
        nodes.len(),
        2,
        "failure must leave the final node unstarted"
    );
    assert_eq!(node_run(&nodes, producer).status, "Succeeded");
    assert_eq!(node_run(&nodes, consumer).status, "Failed");
    assert!(poll(&app, owner).await.is_empty());

    let jobs = request(
        &app,
        Method::GET,
        &format!("/jobs?run_id={}", resumed.run_id),
        Value::Null,
        StatusCode::OK,
    )
    .await;
    let jobs = jobs["items"].as_array().unwrap();
    assert_eq!(jobs.len(), 3);
    for (job, status) in [
        (initial, "Yielded"),
        (resumed, "Succeeded"),
        (downstream, "Failed"),
    ] {
        let terminal = jobs
            .iter()
            .find(|row| row["uuid"] == json!(job.uuid))
            .unwrap();
        assert_eq!(terminal["status"], status);
        assert!(terminal["agent_id"].is_null());
    }
}

#[tokio::test]
async fn workflow_yields_resumes_presigns_artifacts_and_stops_at_failure() {
    support::with_test_database(run_workflow).await;
}

#[tokio::test]
async fn job_result_upload_survives_expiry_and_preserves_catalog_ownership() {
    support::with_test_database(|mut db| async move {
        let app = router(App::new(db.clone(), storage_config(), db.clone()));
        let mut actuator = Actuator::new(Backend::new(db.clone(), db.clone()), 64);
        let mut graph = workflow();
        let graph_id = graph.id;
        graph.nodes[0].outputs.push(Output {
            file_name: "result.json".into(),
            kind: Kind::File,
            multiple: false,
        });
        request(
            &app, Method::PUT, &format!("/graphs/{graph_id}/draft"),
            json!({"graph": graph, "actor": "workflow-test"}), StatusCode::OK,
        ).await;
        post(
            &app, &format!("/graphs/{graph_id}/publish"),
            json!({"actor": "workflow-test"}), StatusCode::CREATED,
        ).await;
        post(
            &app, "/runs/start",
            json!({"data_id": Uuid::new_v4(), "graph_id": graph_id}), StatusCode::ACCEPTED,
        ).await;
        assert_eq!(actuator.tick().await.unwrap().tasks, 1);

        let owner = Uuid::new_v4();
        let job = Job::filter(Job::fields().status().eq(Status::Scheduled))
            .first()
            .exec(&mut db)
            .await
            .unwrap()
            .unwrap();
        let now = jiff::Timestamp::now();
        // Reproduce a claim delivered by an older server without a result row.
        Job::filter_by_id(job.id)
            .update()
            .status(Status::Polled)
            .agent_id(Some(owner))
            .polled_at(Some(now))
            .expired_at(Some(now.checked_add(Duration::from_secs(120)).unwrap()))
            .exec(&mut db)
            .await
            .unwrap();
        let node = NodeRun::filter_by_uuid(job.node_run_id).get(&mut db).await.unwrap();
        let output_key = format!(
            "/artifacts/{}/{}/{}/{}/{}/result.json",
            node.run_id, node.graph_id, node.version, node.node_id, node.revision,
        );
        let output = db.upsert_catalog(CatalogOptionalData {
            url: Some(format!("s3://workflow-test{output_key}")),
            key: Some(output_key),
            size: Some(16),
            format: Some("application/json".into()),
            uploaded: Some(true),
        }).await.unwrap();
        db.upsert_file(&FileInput::new(node.uuid, "result.json", output))
            .await.unwrap();
        let prepare = |agent_id, size| json!({"job_id": job.uuid, "agent_id": agent_id, "size": size});
        let commit = |agent_id, catalog_uuid| json!({"job_id": job.uuid, "agent_id": agent_id, "catalog_uuid": catalog_uuid});
        let other_agent = Uuid::new_v4();
        let conflict = post(&app, "/job/result/prepare", prepare(other_agent, 42), StatusCode::CONFLICT).await;
        assert_eq!(conflict["error_code"], "RAYFLOW_JOB_CONFLICT");
        post(&app, "/job/result/commit", commit(owner, output), StatusCode::BAD_REQUEST).await;

        let without_storage = router(App::new(db.clone(), Config::default(), db.clone()));
        let error = post(&without_storage, "/job/result/prepare", prepare(owner, 42), StatusCode::SERVICE_UNAVAILABLE).await;
        assert_eq!(error["error_code"], "RAYFLOW_STORAGE_UNAVAILABLE");

        Job::filter_by_id(job.id).update().expired_at(Some(jiff::Timestamp::UNIX_EPOCH))
            .exec(&mut db).await.unwrap();
        let prepared = post(&app, "/job/result/prepare", prepare(owner, 42), StatusCode::OK).await;
        let result_id: Uuid = serde_json::from_value(prepared["catalog_uuid"].clone()).unwrap();
        assert_ne!(result_id, output);
        assert_eq!(prepared["catalog"]["key"], format!("/artifacts/jobs/{}/result.json", job.uuid));
        assert_eq!(prepared["catalog"]["size"], 42);
        assert_eq!(prepared["catalog"]["format"], "application/json");
        assert_eq!(prepared["catalog"]["uploaded"], false);
        assert_eq!(prepared["presigned"]["operation"], "upload");
        assert_eq!(prepared["multipart"], false);
        let retry = post(&app, "/job/result/prepare", prepare(owner, 42), StatusCode::OK).await;
        assert_eq!(retry["catalog"], prepared["catalog"]);
        post(&app, "/job/result/prepare", prepare(owner, 43), StatusCode::BAD_REQUEST).await;

        assert_eq!(scheduler::agent::fail_expired_in(&mut db, Duration::ZERO).await.unwrap(), 1);
        let expired = Job::filter_by_id(job.id).get(&mut db).await.unwrap();
        assert_eq!(expired.status, Status::Failed);
        assert_eq!(expired.agent_id, None);
        let conflict = post(&app, "/job/result/prepare", prepare(other_agent, 42), StatusCode::CONFLICT).await;
        assert_eq!(conflict["error_code"], "RAYFLOW_JOB_CONFLICT");
        post(&app, "/job/result/commit", commit(other_agent, result_id), StatusCode::CONFLICT).await;
        post(&app, "/job/result/commit", commit(owner, output), StatusCode::BAD_REQUEST).await;
        let prepared_after_expiry = post(&app, "/job/result/prepare", prepare(owner, 42), StatusCode::OK).await;
        assert_eq!(prepared_after_expiry["catalog_uuid"], json!(result_id));
        for _ in 0..2 {
            let acknowledged = post(&app, "/job/result/commit", commit(owner, result_id), StatusCode::OK).await;
            assert_eq!(acknowledged, json!(result_id));
        }
        let committed = post(&app, "/job/result/prepare", prepare(owner, 42), StatusCode::OK).await;
        assert_eq!(committed["catalog_uuid"], json!(result_id));
        assert_eq!(committed["catalog"]["uploaded"], true);
        assert_eq!(committed["presigned"]["operation"], "download");
        let usage_prepare = |agent_id, size| json!({
            "job_id": job.uuid, "agent_id": agent_id, "filename": "usage.json", "size": size,
        });
        let usage_commit = |catalog_uuid| json!({
            "job_id": job.uuid, "agent_id": owner, "filename": "usage.json", "catalog_uuid": catalog_uuid,
        });
        post(&app, "/job/result/prepare", usage_prepare(other_agent, 21), StatusCode::CONFLICT).await;
        post(&app, "/job/result/commit", usage_commit(result_id), StatusCode::BAD_REQUEST).await;
        let usage = post(&app, "/job/result/prepare", usage_prepare(owner, 21), StatusCode::OK).await;
        let usage_id: Uuid = serde_json::from_value(usage["catalog_uuid"].clone()).unwrap();
        assert_ne!(usage_id, result_id);
        assert_eq!(usage["catalog"]["key"], format!("/artifacts/jobs/{}/usage.json", job.uuid));
        assert_eq!(usage["presigned"]["operation"], "upload");
        post(&app, "/job/result/prepare", usage_prepare(owner, 22), StatusCode::BAD_REQUEST).await;
        post(&app, "/job/result/commit", commit(owner, usage_id), StatusCode::BAD_REQUEST).await;
        post(&app, "/job/result/commit", usage_commit(usage_id), StatusCode::OK).await;
        let usage = post(&app, "/job/result/prepare", usage_prepare(owner, 21), StatusCode::OK).await;
        assert_eq!(usage["catalog_uuid"], json!(usage_id));
        assert_eq!(usage["presigned"]["operation"], "download");
        let invalid = json!({
            "job_id": job.uuid, "agent_id": owner, "filename": "../usage.json", "size": 21,
        });
        let response = app.clone().oneshot(Request::post("/api/v1/job/result/prepare")
            .header("content-type", "application/json")
            .body(Body::from(invalid.to_string())).unwrap()).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let files = catalog::model::File::filter_by_node_run_id(job.node_run_id)
            .exec(&mut db)
            .await
            .unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].filename, "result.json");
        assert_eq!(files[0].catalog_uuid, output);

        post(
            &app, "/runs/start",
            json!({"data_id": Uuid::new_v4(), "graph_id": graph_id}), StatusCode::ACCEPTED,
        ).await;
        assert_eq!(actuator.tick().await.unwrap().tasks, 1);
        let legacy = Job::filter(Job::fields().status().eq(Status::Scheduled))
            .first()
            .exec(&mut db)
            .await
            .unwrap()
            .unwrap();
        let legacy_prepare = json!({"job_id": legacy.uuid, "agent_id": owner, "size": 42});
        let conflict = post(&app, "/job/result/prepare", legacy_prepare.clone(), StatusCode::CONFLICT).await;
        assert_eq!(conflict["error_code"], "RAYFLOW_JOB_CONFLICT");
        // Reproduce a job polled before upgrade and expired before any result request.
        Job::filter_by_id(legacy.id)
            .update()
            .status(Status::Polled)
            .agent_id(Some(owner))
            .polled_at(Some(now))
            .expired_at(Some(jiff::Timestamp::UNIX_EPOCH))
            .exec(&mut db)
            .await
            .unwrap();
        assert_eq!(scheduler::agent::fail_expired_in(&mut db, Duration::ZERO).await.unwrap(), 1);
        let conflict = post(&app, "/job/result/prepare", legacy_prepare, StatusCode::CONFLICT).await;
        assert_eq!(conflict["error_code"], "RAYFLOW_JOB_RESULT_OWNER_UNKNOWN");
        let conflict = post(
            &app, "/job/result/commit",
            json!({"job_id": legacy.uuid, "agent_id": owner, "catalog_uuid": result_id}),
            StatusCode::CONFLICT,
        ).await;
        assert_eq!(conflict["error_code"], "RAYFLOW_JOB_CONFLICT");
    }).await;
}

#[tokio::test]
async fn graph_responses_expose_persisted_audit_fields() {
    support::with_test_database(|mut db| async move {
        let app = router(App::new(db.clone(), Config::default(), db.clone()));
        let mut graph = serde_json::to_value(workflow()).unwrap();
        let graph_id: Uuid = serde_json::from_value(graph["id"].clone()).unwrap();
        let draft_path = format!("/graphs/{graph_id}/draft");
        graph["created_by"] = json!("payload-actor");
        graph["updated_by"] = json!("payload-actor");
        graph["created_at"] = json!("1970-01-01T00:00:00Z");
        graph["updated_at"] = json!("1970-01-01T00:00:00Z");

        let created = request(
            &app,
            Method::PUT,
            &draft_path,
            json!({"graph": graph, "actor": "alice"}),
            StatusCode::OK,
        )
        .await;
        assert_eq!(created["id"], graph["id"]);
        assert_eq!(created["nodes"], graph["nodes"]);
        assert_eq!(created["edges"], graph["edges"]);
        assert_eq!(created["created_by"], "alice");
        assert_eq!(created["updated_by"], "alice");
        assert_eq!(created["created_at"], created["updated_at"]);
        let created_at: jiff::Timestamp = created["created_at"].as_str().unwrap().parse().unwrap();
        assert!(created_at > jiff::Timestamp::UNIX_EPOCH);

        let draft_row = GraphDraftRecord::filter_by_uuid(graph_id)
            .first()
            .exec(&mut db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(serde_json::to_value(draft_row.graph.0).unwrap(), created);
        // Use JSON_REMOVE to reproduce a legacy payload while preserving the row audit columns.
        toasty::sql::statement(
            "UPDATE graph_draft SET graph = JSON_REMOVE(graph, '$.created_by', '$.created_at', \
             '$.updated_by', '$.updated_at'), updated_at = updated_at WHERE id = ?",
        )
        .bind(draft_row.id)
        .exec(&mut db)
        .await
        .unwrap();
        let legacy_draft =
            request(&app, Method::GET, &draft_path, Value::Null, StatusCode::OK).await;
        assert_eq!(legacy_draft, created);

        graph["name"] = json!("updated workflow");
        let updated = request(
            &app,
            Method::PUT,
            &draft_path,
            json!({"graph": graph, "actor": "bob"}),
            StatusCode::OK,
        )
        .await;
        assert_eq!(updated["name"], graph["name"]);
        assert_eq!(updated["created_by"], "alice");
        assert_eq!(updated["created_at"], created["created_at"]);
        assert_eq!(updated["updated_by"], "bob");
        let updated_at: jiff::Timestamp = updated["updated_at"].as_str().unwrap().parse().unwrap();
        assert!(updated_at >= created_at);
        let draft_row = GraphDraftRecord::filter_by_uuid(graph_id)
            .first()
            .exec(&mut db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(serde_json::to_value(draft_row.graph.0).unwrap(), updated);

        let draft = request(&app, Method::GET, &draft_path, Value::Null, StatusCode::OK).await;
        assert_eq!(draft, updated);
        let drafts = request(
            &app,
            Method::GET,
            "/graph_drafts",
            Value::Null,
            StatusCode::OK,
        )
        .await;
        assert_eq!(drafts, json!({"items": [draft], "next_cursor": null}));

        let published = post(
            &app,
            &format!("/graphs/{graph_id}/publish"),
            json!({"actor": "carol"}),
            StatusCode::CREATED,
        )
        .await;
        assert_eq!(published["created_by"], "carol");
        assert_eq!(published["updated_by"], "carol");
        assert_eq!(published["created_at"], published["updated_at"]);
        let published_at: jiff::Timestamp =
            published["created_at"].as_str().unwrap().parse().unwrap();
        assert!(published_at >= updated_at);

        let graph_row = GraphRecord::filter_by_uuid(graph_id)
            .first()
            .exec(&mut db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(serde_json::to_value(graph_row.graph.0).unwrap(), published);
        // Reproduce the legacy published payload with the same column audit values.
        toasty::sql::statement(
            "UPDATE graph SET graph = JSON_REMOVE(graph, '$.created_by', '$.created_at', \
             '$.updated_by', '$.updated_at'), updated_at = updated_at WHERE id = ?",
        )
        .bind(graph_row.id)
        .exec(&mut db)
        .await
        .unwrap();

        let version = published["version"].as_u64().unwrap();
        let read = request(
            &app,
            Method::GET,
            &format!("/graphs/{graph_id}?version={version}"),
            Value::Null,
            StatusCode::OK,
        )
        .await;
        assert_eq!(read, published);
        let graphs = request(
            &app,
            Method::GET,
            &format!("/graphs?graph_id={graph_id}"),
            Value::Null,
            StatusCode::OK,
        )
        .await;
        assert_eq!(graphs, json!({"items": [read], "next_cursor": null}));
    })
    .await;
}
