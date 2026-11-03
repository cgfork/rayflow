use axum::body::Body;
use axum::http::{Request, StatusCode};
use graph::types::{DAG, Node};
use server::{App, Config, router};
use tower::ServiceExt;
use uuid::Uuid;

#[tokio::test]
async fn published_settings_pin_job_configuration_to_the_node_run_version() {
    use event::{Emit, task::Task};
    use jiff::Timestamp;
    use runtime::command::{NodeRun, Run};
    use scheduler::{Job, actuator::NodeConfig};

    let Some((app, mut db)) = app().await else {
        return;
    };
    let mut draft = graph(Uuid::new_v4());
    app.write_graph_draft(draft.id, draft.clone(), "editor")
        .await
        .unwrap();
    app.publish_graph_draft(draft.id, "publisher")
        .await
        .unwrap();
    draft.nodes[0].settings.config =
        serde_json::json!({"image": "busybox", "command": ["echo", "new"]});
    draft.nodes[0].settings.resource.cpu_cores = 4;
    draft.nodes[0].settings.resource.memory_bytes = 536_870_912;
    draft.nodes[0].settings.timeout = std::time::Duration::from_secs(10);
    app.write_graph_draft(draft.id, draft.clone(), "editor")
        .await
        .unwrap();
    let published = app
        .publish_graph_draft(draft.id, "publisher")
        .await
        .unwrap();
    assert_eq!(published.version, 2);

    for (version, image, cores, memory, lease) in [
        (1, "alpine:3.21", 1, 268_435_456, 30_000),
        (2, "busybox", 4, 536_870_912, 10_000),
    ] {
        let now = Timestamp::now();
        let run = Run::create()
            .uuid(Uuid::new_v4())
            .data_id(Uuid::new_v4())
            .graph_id(draft.id)
            .version(version)
            .revision(1)
            .created_at(now)
            .updated_at(now)
            .exec(&mut db)
            .await
            .unwrap();
        let node_run = NodeRun::create()
            .run_id(run.uuid)
            .data_id(run.data_id)
            .graph_id(run.graph_id)
            .version(version)
            .node_id(draft.nodes[0].id)
            .revision(1)
            .status("Running")
            .created_at(now)
            .updated_at(now)
            .exec(&mut db)
            .await
            .unwrap();
        let task = Task::new(run.data_id, run.graph_id, node_run.node_id, node_run.id);
        task.emit(&mut db).await.unwrap();
        let job = Job::create_for_task(&task, &mut db).await.unwrap();
        let config = NodeConfig::filter_by_id(job.node_config_id)
            .get(&mut db)
            .await
            .unwrap();
        assert_eq!(config.version, version);
        assert_eq!(config.config["image"], image);
        assert_eq!(config.lease, Some(lease));
        assert_eq!(
            (job.required_cpu_cores, job.required_memory_bytes),
            (cores, memory)
        );
        assert_eq!(config.created_by, "publisher");
    }

    let mut collision = draft.clone();
    collision.id = Uuid::new_v4();
    app.write_graph_draft(collision.id, collision.clone(), "editor")
        .await
        .unwrap();
    assert!(
        app.publish_graph_draft(collision.id, "publisher")
            .await
            .is_err()
    );
    assert!(app.read_graph(collision.id, None).await.is_err());
    assert_eq!(NodeConfig::all().exec(&mut db).await.unwrap().len(), 2);
}

#[tokio::test]
async fn invalid_settings_remain_editable_but_cannot_be_published() {
    let Some((app, mut db)) = app().await else {
        return;
    };
    let mut draft = graph(Uuid::new_v4());
    draft.nodes[0].settings.timeout = std::time::Duration::ZERO;
    app.write_graph_draft(draft.id, draft.clone(), "editor")
        .await
        .unwrap();
    assert!(
        app.publish_graph_draft(draft.id, "publisher")
            .await
            .is_err()
    );
    assert!(app.read_graph(draft.id, None).await.is_err());
    assert!(
        scheduler::actuator::NodeConfig::all()
            .exec(&mut db)
            .await
            .unwrap()
            .is_empty()
    );
    draft.nodes[0].settings.timeout = std::time::Duration::from_secs(1_000_000_000_000);
    app.write_graph_draft(draft.id, draft.clone(), "editor")
        .await
        .unwrap();
    assert!(
        app.publish_graph_draft(draft.id, "publisher")
            .await
            .is_err()
    );
    assert!(app.read_graph(draft.id, None).await.is_err());
    draft.nodes[0].settings.timeout = std::time::Duration::from_secs(10);
    app.write_graph_draft(draft.id, draft.clone(), "editor")
        .await
        .unwrap();
    assert_eq!(
        app.publish_graph_draft(draft.id, "publisher")
            .await
            .unwrap()
            .version,
        1
    );
}

fn graph(id: Uuid) -> DAG {
    let node = Node {
        id: Uuid::new_v4(),
        name: "task".into(),
        node_type: "docker".into(),
        settings: graph::types::NodeSettings {
            config: serde_json::json!({"image": "alpine:3.21"}),
            resource: graph::types::Resource {
                cpu_cores: 1,
                memory_bytes: 268_435_456,
            },
            timeout: std::time::Duration::from_secs(30),
        },
        inputs: Vec::new(),
        outputs: Vec::new(),
    };
    DAG {
        id,
        name: "draft".into(),
        version: 0,
        nodes: vec![node],
        edges: Vec::new(),
        node_count: 1,
        ..DAG::default()
    }
}

async fn app() -> Option<(server::App<toasty::Db>, toasty::Db)> {
    let url = std::env::var("DATABASE_URL").ok()?;
    let prefix = format!("draft_{}_", Uuid::new_v4().simple());
    let mut builder = toasty::Db::builder();
    builder.models(server::models());
    builder.table_name_prefix(&prefix);
    let db = builder
        .connect(&url)
        .await
        .expect("connect to DATABASE_URL");
    db.push_schema().await.expect("push schema");

    Some((App::new(db.clone(), Config::default(), db.clone()), db))
}

#[tokio::test]
async fn graph_draft_api_round_trips_and_publishes() {
    let Some((app, _db)) = app().await else {
        eprintln!("skipping graph draft API test: DATABASE_URL is not set");
        return;
    };
    let graph_id = Uuid::new_v4();
    let draft = graph(graph_id);
    let path = format!("/graphs/{graph_id}/draft");

    let response = router(app.clone())
        .oneshot(
            Request::put(&path)
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({
                        "graph": draft,
                        "actor": "editor"
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(serde_json::from_slice::<DAG>(&body).unwrap(), draft);

    let response = router(app.clone())
        .oneshot(Request::get(&path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["id"], graph_id.to_string());
    assert_eq!(value["node_count"], 1);
    assert!(value.get("graph").is_none());
    for field in ["updated_at", "updated_by", "created_at", "created_by"] {
        assert!(value.get(field).is_none(), "{field}");
    }
    let stored = app.read_graph_draft(graph_id).await.unwrap();
    assert_eq!(stored.updated_by, "editor");
    assert_eq!(stored.created_by, "editor");

    let publish_path = format!("/graphs/{graph_id}/publish");
    let response = router(app.clone())
        .oneshot(
            Request::post(&publish_path)
                .header("content-type", "application/json")
                .body(Body::from(r#"{"actor":"publisher"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["version"], 1);
    assert_eq!(value["id"], graph_id.to_string());
    assert_eq!(value["node_count"], 1);
    assert!(value.get("graph").is_none());
    assert!(value.get("created_by").is_none());
    let stored = app.read_graph(graph_id, Some(1)).await.unwrap();
    assert_eq!(stored.updated_by, "publisher");
    assert_eq!(stored.created_by, "publisher");

    let graph_path = format!("/graphs/{graph_id}?version=1");
    let response = router(app)
        .oneshot(Request::get(&graph_path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["id"], graph_id.to_string());
    assert_eq!(value["version"], 1);
}

#[tokio::test]
async fn graph_draft_api_rejects_invalid_publish_and_missing_drafts() {
    let Some((app, _db)) = app().await else {
        eprintln!("skipping graph draft API test: DATABASE_URL is not set");
        return;
    };
    let graph_id = Uuid::new_v4();
    let draft = graph(graph_id);
    let path = format!("/graphs/{graph_id}/draft");
    let publish_path = format!("/graphs/{graph_id}/publish");

    let response = router(app.clone())
        .oneshot(Request::get(&path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let mut invalid = draft;
    invalid
        .edges
        .push((invalid.nodes[0].id, invalid.nodes[0].id));
    let response = router(app.clone())
        .oneshot(
            Request::put(&path)
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({
                        "graph": invalid,
                        "actor": "editor"
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = router(app)
        .oneshot(
            Request::post(&publish_path)
                .header("content-type", "application/json")
                .body(Body::from(r#"{"actor":"publisher"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
