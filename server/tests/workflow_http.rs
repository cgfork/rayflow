mod support;

use std::time::Duration;

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Method, Request, StatusCode},
};
use catalog::{
    CatalogOptionalData, CatalogSystem, FileInput, FileSystem, PresignOperation, PresignedUrl,
    directory::{DIRECTORY_FORMAT, DIRECTORY_MANIFEST},
};
use event::{
    Emit,
    command::{Command, CommandEntry},
};
use graph::store::{GraphDraftRecord, GraphRecord};
use graph::types::{
    AnnotationMatchNodeSettings, DAG, Input, Kind, Node, NodeSettings, Output, Resource,
};
use runtime::{
    backend::Backend,
    command::{CommandResult, NodeRun, Run, RunConfig},
};
use scheduler::{
    Actuator, Job,
    actuator::{NodeConfig, Status},
};
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
                overrides: Default::default(),
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

async fn run_workflow(mut db: toasty::Db) {
    let app = router(App::new(db.clone(), storage_config(), db.clone()));
    let mut actuator = Actuator::new(Backend::new(db.clone(), db.clone()), 64);
    let graph = workflow();
    let graph_id = graph.id;
    let producer = graph.nodes[0].id;
    let consumer = graph.nodes[1].id;
    let normal_priority = "has(annotations.priority) && annotations.priority == 'normal'";
    let mut graph = json!(graph);
    graph["nodes"][0]["settings"]["overrides"] = json!({
        "large": {
            "annotation_match": normal_priority,
            "resource": {"cpu_cores": 4, "memory_bytes": 1024}
        },
        "short": {"config": {"image": "alternate"}, "timeout": "5s"}
    });
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
    let published = post(
        &app,
        &format!("/graphs/{graph_id}/publish"),
        json!({"actor": "workflow-test"}),
        StatusCode::CREATED,
    )
    .await;
    let version = published["version"].as_u64().unwrap();
    let configs = NodeConfig::filter_by_node_id(producer)
        .filter(NodeConfig::fields().version().eq(version))
        .exec(&mut db)
        .await
        .unwrap();
    assert_eq!(configs.len(), 3);
    for (name, annotation_match, config, lease, cpu, memory) in [
        ("default", "", json!({}), 120_000, 1, 512),
        ("large", normal_priority, json!({}), 120_000, 4, 1024),
        ("short", "", json!({"image": "alternate"}), 5_000, 1, 512),
    ] {
        let row = configs.iter().find(|row| row.name == name).unwrap();
        assert_eq!(row.annotation_match, annotation_match);
        assert_eq!(row.config, config);
        assert_eq!(row.lease, Some(lease));
        assert_eq!(row.required_cpu_cores, cpu);
        assert_eq!(row.required_memory_bytes, memory);
        assert_eq!(row.created_by, "workflow-test");
        assert_eq!(row.updated_by, "workflow-test");
    }
    let annotations = json!({"dataset": "filetest", "priority": "normal"});
    let started = post(
        &app,
        "/runs/start",
        json!({"data_id": data_id, "graph_id": graph_id, "annotations": annotations}),
        StatusCode::ACCEPTED,
    )
    .await;
    let command_id: Uuid = serde_json::from_value(started["id"].clone()).unwrap();
    let entry = CommandEntry::filter_by_id(command_id)
        .get(&mut db)
        .await
        .unwrap();
    let Command::Start {
        run_config_id: Some(config_id),
        ..
    } = entry.command.0
    else {
        panic!("Start command must reference the saved config");
    };
    let config = RunConfig::filter_by_uuid(config_id)
        .get(&mut db)
        .await
        .unwrap();
    assert_eq!(config.run_id, None);
    assert_eq!(json!(config.annotation.0), annotations);

    assert_eq!(actuator.tick().await.unwrap().tasks, 1);
    let initial = poll_one(&app, owner, producer).await;
    assert_eq!(initial.node_config.get().as_ref().unwrap().name, "large");
    assert_eq!(initial.required_cpu_cores, 4);
    assert_eq!(initial.required_memory_bytes, 1024);
    assert_eq!(initial.run_config_id, config_id);
    let config = RunConfig::filter_by_run_id(initial.run_id)
        .get(&mut db)
        .await
        .unwrap();
    assert_eq!(config.uuid, config_id);
    assert_eq!(config.run_id, Some(initial.run_id));
    assert_eq!(json!(config.annotation.0), annotations);

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

    let archived = post(
        &app,
        "/job/result/prepare",
        json!({"job_id": initial.uuid, "agent_id": owner, "size": 32}),
        StatusCode::OK,
    )
    .await;
    post(
        &app,
        "/job/result/commit",
        json!({"job_id": initial.uuid, "agent_id": owner, "catalog_uuid": archived["catalog_uuid"]}),
        StatusCode::OK,
    )
    .await;

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
    assert_eq!(resumed.run_config_id, config_id);
    assert_eq!(resumed.node_config_id, initial.node_config_id);
    let result = request(
        &app,
        Method::GET,
        &format!(
            "/runs/{}/node-runs/{}/artifacts/result.json",
            resumed.run_id, resumed.node_run_id
        ),
        Value::Null,
        StatusCode::OK,
    )
    .await;
    assert!(result.is_null());

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
    assert_eq!(downstream.run_config_id, config_id);
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
async fn job_config_matching_and_fallback_use_the_node_run_version() {
    support::with_test_database(|mut db| async move {
        let app = router(App::new(db.clone(), Config::default(), db.clone()));
        let mut actuator = Actuator::new(Backend::new(db.clone(), db.clone()), 64);
        let graph = workflow();
        let graph_id = graph.id;
        let node_id = graph.nodes[0].id;
        let mut graph = json!(graph);
        let high_priority = "annotations.priority == 'high'";
        for (version, overrides, cases) in [
            (
                1,
                json!({
                    "aardvark": {"timeout": "1s"},
                    "zeta": {"annotation_match": high_priority},
                    "alpha": {"annotation_match": high_priority, "timeout": "5s"}
                }),
                vec![
                    (Some(json!({"priority": "high"})), "alpha"),
                    (Some(json!({"priority": "low"})), "default"),
                    (Some(json!({"dataset": "filetest"})), "default"),
                    (None, "default"),
                ],
            ),
            (
                2,
                json!({
                    "empty": {"annotation_match": "size(annotations) == 0"},
                    "legacy": {"annotation_match": "has(annotations.other)"},
                    "tagged": {"annotation_match": "'dataset-id' in annotations && annotations['dataset-id'].startsWith('file')"}
                }),
                vec![
                    (Some(json!({"priority": "high"})), "default"),
                    (Some(json!({"priority": "high", "dataset-id": "filetest"})), "tagged"),
                    (Some(json!({})), "empty"),
                    (None, "default"),
                ],
            ),
        ] {
            graph["nodes"][0]["settings"]["overrides"] = overrides;
            request(
                &app,
                Method::PUT,
                &format!("/graphs/{graph_id}/draft"),
                json!({"graph": graph}),
                StatusCode::OK,
            )
            .await;
            let published = post(
                &app,
                &format!("/graphs/{graph_id}/publish"),
                json!({}),
                StatusCode::CREATED,
            )
            .await;
            assert_eq!(published["version"], version);

            if version == 2 {
                // Simulate an override published while JSONPath was supported.
                NodeConfig::filter_by_node_id_and_version_and_name(node_id, version, "legacy")
                    .update()
                    .annotation_match("$.priority")
                    .exec(&mut db)
                    .await
                    .unwrap();
            }
            for (annotations, expected) in cases {
                post(
                    &app,
                    "/runs/start",
                    json!({"data_id": Uuid::new_v4(), "graph_id": graph_id, "annotations": annotations}),
                    StatusCode::ACCEPTED,
                )
                .await;
                assert_eq!(actuator.tick().await.unwrap().tasks, 1);
                let job = poll_one(&app, Uuid::new_v4(), node_id).await;
                let config = job.node_config.get().as_ref().unwrap();
                assert_eq!(job.node_run.get().version, version);
                assert_eq!(config.version, version);
                assert_eq!(config.name, expected);
                assert_eq!(config.lease, Some(if expected == "alpha" { 5_000 } else { 120_000 }));
            }
        }
    })
    .await;
}

#[tokio::test]
async fn upgrade_restarts_a_shared_node_after_a_settings_only_publish() {
    support::with_test_database(|db| async move {
        let app = router(App::new(db.clone(), Config::default(), db.clone()));
        let mut actuator = Actuator::new(Backend::new(db.clone(), db), 64);
        let mut graph = workflow();
        let graph_id = graph.id;
        let node_ids: Vec<_> = graph.nodes.iter().map(|node| node.id).collect();
        let node_id = node_ids[0];
        let data_id = Uuid::new_v4();
        let owner = Uuid::new_v4();
        let draft = format!("/graphs/{graph_id}/draft");
        let publish = format!("/graphs/{graph_id}/publish");

        request(
            &app,
            Method::PUT,
            &draft,
            json!({"graph": graph}),
            StatusCode::OK,
        )
        .await;
        post(&app, &publish, json!({}), StatusCode::CREATED).await;
        post(
            &app,
            "/runs/start",
            json!({"data_id": data_id, "graph_id": graph_id}),
            StatusCode::ACCEPTED,
        )
        .await;
        assert_eq!(actuator.tick().await.unwrap().tasks, 1);
        let original = poll_one(&app, owner, node_id).await;
        assert_eq!(original.node_run.get().version, 1);
        post(
            &app,
            "/job/fail",
            json!({"job_id": original.uuid, "agent_id": owner}),
            StatusCode::NO_CONTENT,
        )
        .await;
        assert_eq!(actuator.tick().await.unwrap().events, 1);

        graph.nodes[0].settings.timeout = Duration::from_secs(30);
        request(
            &app,
            Method::PUT,
            &draft,
            json!({"graph": graph}),
            StatusCode::OK,
        )
        .await;
        post(&app, &publish, json!({}), StatusCode::CREATED).await;
        let shared = request(
            &app,
            Method::GET,
            &format!("/graphs/{graph_id}/shared-nodes?from_version=1&version=2"),
            Value::Null,
            StatusCode::OK,
        )
        .await;
        assert_eq!(shared, json!(node_ids));

        post(
            &app,
            "/runs/upgrade",
            json!({"data_id": data_id, "graph_id": graph_id, "version": 2, "node": node_id}),
            StatusCode::ACCEPTED,
        )
        .await;
        assert_eq!(actuator.tick().await.unwrap().tasks, 1);
        let upgraded = poll_one(&app, owner, node_id).await;
        assert_eq!(upgraded.run_id, original.run_id);
        assert_eq!(upgraded.node_run.get().revision, 1);
        assert_eq!(upgraded.node_run.get().version, 2);
        assert_eq!(
            upgraded.node_config.get().as_ref().unwrap().lease,
            Some(30_000)
        );
    })
    .await;
}

#[tokio::test]
async fn invalid_node_overrides_leave_the_graph_unpublished() {
    support::with_test_database(|mut db| async move {
        let app = router(App::new(db.clone(), Config::default(), db.clone()));
        let graph = workflow();
        let graph_id = graph.id;
        let node_id = graph.nodes[0].id;
        let mut graph = json!(graph);
        for (name, value) in [
            ("default", json!({})),
            ("short", json!({"timeout": "1us"})),
            (
                "broken",
                json!({"annotation_match": "has(annotations.priority"}),
            ),
            (
                "nonboolean",
                json!({"annotation_match": "annotations.priority"}),
            ),
            ("unknown", json!({"annotation_match": "priority == 'high'"})),
            ("jsonpath", json!({"annotation_match": "$.priority"})),
        ] {
            graph["nodes"][0]["settings"]["overrides"] = json!({(name): value});
            request(
                &app,
                Method::PUT,
                &format!("/graphs/{graph_id}/draft"),
                json!({"graph": graph}),
                StatusCode::OK,
            )
            .await;
            let error = post(
                &app,
                &format!("/graphs/{graph_id}/publish"),
                json!({}),
                StatusCode::BAD_REQUEST,
            )
            .await;
            assert_eq!(error["error_code"], "RAYFLOW_INVALID_GRAPH");
            assert!(error["error"].as_str().unwrap().contains(name));
            if name == "nonboolean" {
                assert!(
                    error["error"]
                        .as_str()
                        .unwrap()
                        .contains("must return a boolean")
                );
            }
        }
        assert!(
            GraphRecord::filter_by_uuid(graph_id)
                .first()
                .exec(&mut db)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            NodeConfig::filter_by_node_id(node_id)
                .first()
                .exec(&mut db)
                .await
                .unwrap()
                .is_none()
        );
    })
    .await;
}

#[tokio::test]
async fn start_rejects_missing_and_attached_config_ids() {
    support::with_test_database(|mut db| async move {
        let app = router(App::new(db.clone(), Config::default(), db.clone()));
        let mut actuator = Actuator::new(Backend::new(db.clone(), db.clone()), 64);
        let graph = workflow();
        let graph_id = graph.id;
        request(
            &app,
            Method::PUT,
            &format!("/graphs/{graph_id}/draft"),
            json!({"graph": graph}),
            StatusCode::OK,
        )
        .await;
        post(
            &app,
            &format!("/graphs/{graph_id}/publish"),
            json!({}),
            StatusCode::CREATED,
        )
        .await;
        post(
            &app,
            "/runs/start",
            json!({"data_id": Uuid::new_v4(), "graph_id": graph_id, "annotations": {}}),
            StatusCode::ACCEPTED,
        )
        .await;
        assert_eq!(actuator.tick().await.unwrap().tasks, 1);
        let job = poll_one(&app, Uuid::new_v4(), graph.nodes[0].id).await;
        let config_id = job.run_config_id;

        for (run_config_id, code) in [
            (Uuid::new_v4(), "RAYFLOW_RUN_CONFIG_NOT_FOUND"),
            (config_id, "RAYFLOW_RUN_CONFIG_IN_USE"),
        ] {
            let data_id = Uuid::new_v4();
            let entry = CommandEntry::new(
                data_id,
                graph_id,
                Command::Start {
                    nodes: vec![],
                    run_config_id: Some(run_config_id),
                },
            );
            entry.emit(&mut db).await.unwrap();
            let step = actuator.tick().await.unwrap();
            assert_eq!(step.commands, 1);
            assert_eq!(step.tasks, 0);
            let failure = CommandResult::filter_by_command_id(entry.id)
                .get(&mut db)
                .await
                .unwrap();
            assert_eq!(failure.error_code, code);
            assert!(
                Run::filter_by_data_id_and_graph_id(data_id, graph_id)
                    .first()
                    .exec(&mut db)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        let config = RunConfig::filter_by_uuid(config_id)
            .get(&mut db)
            .await
            .unwrap();
        assert_eq!(config.run_id, Some(job.run_id));
        assert!(config.annotation.0.is_empty());
    })
    .await;
}

#[tokio::test]
async fn job_result_upload_survives_expiry_and_preserves_catalog_ownership() {
    support::with_test_database(|mut db| async move {
        let mut config = storage_config();
        // A committed archive can be signed without contacting storage.
        let endpoint = "https://storage.example.invalid";
        config.storage.as_mut().unwrap().endpoint = endpoint.into();
        let app = router(App::new(db.clone(), config, db.clone()));
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
        assert!(job.run_config_id.is_nil());
        assert!(RunConfig::all().first().exec(&mut db).await.unwrap().is_none());
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
        let archive_path = |filename| format!("/runs/{}/node-runs/{}/artifacts/{filename}", job.run_id, node.uuid);
        let assert_signed_archive = |filename: &str, body: Value| {
            let signed: PresignedUrl = serde_json::from_value(body.clone()).unwrap();
            assert_eq!(body, serde_json::to_value(&signed).unwrap());
            assert_eq!(signed.operation, PresignOperation::Download);
            let url = reqwest::Url::parse(&signed.url).unwrap();
            assert_eq!(url.origin().ascii_serialization(), endpoint);
            assert_eq!(url.path(), format!("/workflow-test/artifacts/jobs/{}/{filename}", job.uuid));
            assert!(url.query_pairs().any(|(name, value)| name == "X-Amz-Signature" && !value.is_empty()));
        };
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
        assert!(request(&app, Method::GET, &archive_path("result.json"), Value::Null, StatusCode::OK).await.is_null());
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
        assert_signed_archive(
            "result.json",
            request(&app, Method::GET, &archive_path("result.json"), Value::Null, StatusCode::OK).await,
        );
        let different_run = format!("/runs/{}/node-runs/{}/artifacts/result.json", Uuid::new_v4(), node.uuid);
        assert!(request(&app, Method::GET, &different_run, Value::Null, StatusCode::OK).await.is_null());
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
        assert!(request(&app, Method::GET, &archive_path("usage.json"), Value::Null, StatusCode::OK).await.is_null());
        post(&app, "/job/result/prepare", usage_prepare(owner, 22), StatusCode::BAD_REQUEST).await;
        post(&app, "/job/result/commit", commit(owner, usage_id), StatusCode::BAD_REQUEST).await;
        post(&app, "/job/result/commit", usage_commit(usage_id), StatusCode::OK).await;
        let usage = post(&app, "/job/result/prepare", usage_prepare(owner, 21), StatusCode::OK).await;
        assert_eq!(usage["catalog_uuid"], json!(usage_id));
        assert_eq!(usage["presigned"]["operation"], "download");
        assert_signed_archive(
            "usage.json",
            request(&app, Method::GET, &archive_path("usage.json"), Value::Null, StatusCode::OK).await,
        );
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

#[tokio::test]
async fn graph_diff_compares_exact_published_versions() {
    support::with_test_database(|db| async move {
        let app = router(App::new(db.clone(), Config::default(), db));
        let mut before = workflow();
        before.nodes[1].settings.timeout = Duration::from_secs(10);
        let graph_id = before.id;
        let node_id = before.nodes[1].id.to_string();
        let expression = "has(annotations.priority) && annotations.priority == 'high'";
        let mut after = before.clone();
        after.nodes[1].name = "process".into();
        after.nodes[1].inputs[0].required = false;
        after.nodes[1].inputs[0].format = None;
        let settings = &mut after.nodes[1].settings;
        settings.config = json!({"image": "build:v2", "args": ["--safe"]});
        settings.resource.cpu_cores = 2;
        settings.resource.memory_bytes = 1024;
        settings.timeout = Duration::from_secs(20);
        settings.overrides.insert("high".into(), AnnotationMatchNodeSettings {
            annotation_match: Some(expression.into()),
            timeout: Some(Duration::from_secs(5)),
            ..Default::default()
        });
        let other = workflow();
        let other_id = other.id;

        for (graph, version) in [(&before, 1), (&after, 2), (&other, 1)] {
            let id = graph.id;
            request(
                &app,
                Method::PUT,
                &format!("/graphs/{id}/draft"),
                json!({"graph": graph}),
                StatusCode::OK,
            )
            .await;
            let published = post(
                &app,
                &format!("/graphs/{id}/publish"),
                json!({}),
                StatusCode::CREATED,
            )
            .await;
            assert_eq!(published["version"], version);
        }

        let diff = request(
            &app,
            Method::GET,
            &format!("/graphs/{graph_id}/diff?from_version=1&version=2"),
            Value::Null,
            StatusCode::OK,
        )
        .await;
        assert_eq!(diff["before_node_names"][&node_id], "consume");
        assert_eq!(diff["after_node_names"][&node_id], "process");
        let changes = diff["changes"].as_array().unwrap();
        assert!(changes.contains(&json!({"field": "version", "change": [1, 2]})));
        let structure = changes
            .iter()
            .find(|item| item["field"] == "structure")
            .unwrap();
        assert_eq!(
            structure["change"]["node_diffs"],
            json!([[node_id, [
                {"field": "name", "change": ["consume", "process"]},
                {"field": "settings", "change": [
                    {"field": "config", "change": [{}, {"image": "build:v2", "args": ["--safe"]}]},
                    {"field": "resource", "change": [
                        {"field": "cpu_cores", "change": [1, 2]},
                        {"field": "memory_bytes", "change": [512, 1024]},
                    ]},
                    {"field": "timeout", "change": ["10s", "20s"]},
                    {"field": "overrides", "change": [{
                        "name": "high", "before": null,
                        "after": {"annotation_match": expression, "config": null, "resource": null, "timeout": "5s"},
                    }]},
                ]},
                {"field": "inputs", "change": [{"kind": "changed", "index": 0, "changes": [
                    {"field": "required", "change": [true, false]},
                    {"field": "format", "change": ["text/plain", null]},
                ]}]},
            ]]]),
        );

        let diff = request(
            &app,
            Method::GET,
            &format!("/graphs/{graph_id}/diff?from_graph_id={other_id}&from_version=1&version=2"),
            Value::Null,
            StatusCode::OK,
        )
        .await;
        let changes = diff["changes"].as_array().unwrap();
        assert!(changes.contains(&json!({"field": "id", "change": [other_id, graph_id]})));

        let missing = request(
            &app,
            Method::GET,
            &format!("/graphs/{graph_id}/diff?from_version=1&version=3"),
            Value::Null,
            StatusCode::NOT_FOUND,
        )
        .await;
        assert_eq!(missing["error_code"], "RAYFLOW_GRAPH_NOT_FOUND");
    })
    .await;
}
