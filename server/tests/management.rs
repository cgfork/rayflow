use std::time::Duration;

use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
};
use event::{Emit, task::Task};
use graph::{
    DraftGraphStore, GraphStore,
    types::{DAG, Node},
};
use jiff::Timestamp;
use runtime::command::{NodeRun, Run};
use scheduler::{
    AgentRef, Job, JobWrite, ResourceCapacity,
    actuator::{NodeConfig, Status},
};
use serde_json::{Value, json};
use server::{App, Config, app::AgentInstance};
use toasty::Db;
use tower::ServiceExt;
use uuid::Uuid;

async fn setup() -> Option<(App<Db>, Db)> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping management API tests: DATABASE_URL is not set");
        return None;
    };
    let mut builder = Db::builder();
    builder.models(server::models());
    builder.table_name_prefix(&format!("ui_{}_", Uuid::new_v4().simple()));
    let db = builder.connect(&url).await.unwrap();
    db.push_schema().await.unwrap();
    let app = App::new(db.clone(), Config::default(), db.clone());
    Some((app, db))
}

async fn request(app: &App<Db>, method: Method, path: &str, body: Value) -> (StatusCode, Value) {
    let response = server::router(app.clone())
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 2 * 1024 * 1024)
        .await
        .unwrap();
    let value =
        serde_json::from_slice(&bytes).unwrap_or_else(|_| json!(String::from_utf8_lossy(&bytes)));
    (status, value)
}

async fn get(app: &App<Db>, path: &str) -> Value {
    let (status, value) = request(app, Method::GET, path, Value::Null).await;
    assert_eq!(status, StatusCode::OK, "{path}: {value}");
    value
}

fn graph(name: &str) -> DAG {
    DAG {
        id: Uuid::new_v4(),
        name: name.into(),
        version: 0,
        nodes: vec![Node {
            id: Uuid::new_v4(),
            name: "start".into(),
            node_type: "docker".into(),
            settings: graph::types::NodeSettings {
                config: json!({"image": "alpine:3.21"}),
                resource: graph::types::Resource {
                    cpu_cores: 1,
                    memory_bytes: 1024,
                },
                timeout: std::time::Duration::from_secs(30),
            },
            inputs: Vec::new(),
            outputs: Vec::new(),
        }],
        edges: Vec::new(),
        node_count: 1,
        ..DAG::default()
    }
}

#[tokio::test]
async fn graph_lists_paginate_drafts_and_published_versions() {
    let Some((app, _db)) = setup().await else {
        return;
    };
    let first = graph("first");
    let second = graph("second");
    for definition in [&first, &second] {
        let (status, draft) = request(
            &app,
            Method::PUT,
            &format!("/graphs/{}/draft", definition.id),
            json!({ "graph": definition, "actor": "editor" }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{draft}");
        assert_eq!(draft, serde_json::to_value(definition).unwrap());
        assert_eq!(
            app.read_graph_draft(definition.id)
                .await
                .unwrap()
                .updated_by,
            "editor"
        );
    }
    for version in [1, 2] {
        let (status, published) = request(
            &app,
            Method::POST,
            &format!("/graphs/{}/publish", first.id),
            json!({ "actor": "publisher" }),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{published}");
        assert_eq!(published["version"], version);
    }
    let page = get(&app, "/graph_drafts?limit=1").await;
    assert_eq!(page["items"].as_array().unwrap().len(), 1);
    assert_eq!(page["items"][0]["id"], second.id.to_string());
    assert_eq!(page["items"][0]["version"], 0);
    assert_eq!(page["items"][0]["node_count"], 1);
    assert!(page["items"][0].get("graph").is_none());
    assert_eq!(page["items"][0], serde_json::to_value(&second).unwrap());
    let next = get(
        &app,
        &format!("/graph_drafts?limit=1&before={}", page["next_cursor"]),
    )
    .await;
    assert_eq!(next["items"][0]["id"], first.id.to_string());
    assert_eq!(next["next_cursor"], Value::Null);
    let published = get(&app, &format!("/graphs?limit=1&graph_id={}", first.id)).await;
    assert_eq!(published["items"][0]["version"], 2);
    assert_eq!(
        published["items"][0]["nodes"],
        serde_json::to_value(&first.nodes).unwrap()
    );
    assert_eq!(published["items"][0]["node_count"], first.nodes.len());
    let older = get(
        &app,
        &format!("/graphs?limit=1&before={}", published["next_cursor"]),
    )
    .await;
    assert_eq!(older["items"][0]["version"], 1);
    for path in [
        "/graphs?limit=0",
        "/graph_drafts?limit=201",
        "/runs?limit=0",
        "/jobs?limit=201",
        "/agents?limit=0",
        "/jobs?status=Unknown",
    ] {
        let (status, _) = request(&app, Method::GET, path, Value::Null).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{path}");
    }
}

#[tokio::test]
async fn run_detail_reads_the_selected_version_and_latest_node_revision() {
    let Some((app, mut db)) = setup().await else {
        return;
    };
    let mut definition = graph("version-zero");
    db.write(&definition, "editor").await.unwrap();
    db.publish(definition.id, "editor").await.unwrap();
    let first = db.load(definition.id, None).await.unwrap();
    definition.name = "version-one".into();
    db.write(&definition, "editor").await.unwrap();
    db.publish(definition.id, "editor").await.unwrap();
    let second = db.load(definition.id, None).await.unwrap();
    let now = Timestamp::now();
    let mut runs = Vec::new();
    for version in [first.version, second.version] {
        runs.push(
            Run::create()
                .uuid(Uuid::new_v4())
                .data_id(Uuid::new_v4())
                .graph_id(definition.id)
                .version(version)
                .revision(2)
                .created_at(now)
                .updated_at(now)
                .exec(&mut db)
                .await
                .unwrap(),
        );
    }
    for (revision, status) in [(1, "Succeeded"), (2, "Pending"), (3, "Failed")] {
        NodeRun::create()
            .run_id(runs[0].uuid)
            .data_id(runs[0].data_id)
            .graph_id(definition.id)
            .version(first.version)
            .node_id(definition.nodes[0].id)
            .revision(revision)
            .status(status.to_owned())
            .created_at(now)
            .updated_at(now)
            .exec(&mut db)
            .await
            .unwrap();
    }
    let old = get(&app, &format!("/runs/{}", runs[0].uuid)).await;
    assert_eq!(old["graph"]["name"], "version-zero");
    assert_eq!(old["state"]["uuid"], runs[0].uuid.to_string());
    assert_eq!(old["state"]["version"], first.version);
    assert_eq!(old["state"]["nodes"][0]["status"], "Pending");
    assert_eq!(old["current"], true);
    let current = get(&app, &format!("/runs/{}", runs[1].uuid)).await;
    assert_eq!(current["current"], true);
    assert_eq!(current["graph"]["name"], "version-one");
    assert!(current["state"]["nodes"].as_array().unwrap().is_empty());
    let page = get(&app, &format!("/runs?limit=1&graph_id={}", definition.id)).await;
    assert_eq!(page["items"][0]["uuid"], runs[1].uuid.to_string());
    let (status, _) = request(
        &app,
        Method::GET,
        &format!("/runs/{}", Uuid::new_v4()),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn job_list_combines_status_owner_and_run_filters_without_claiming() {
    let Some((app, mut db)) = setup().await else {
        return;
    };
    let run_id = Uuid::new_v4();
    let agent_id = Uuid::new_v4();
    let task = Task::new(Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4(), 1);
    for (status, owner, run) in [
        (Status::Scheduled, None, run_id),
        (Status::Polled, Some(agent_id), run_id),
        (Status::Polled, Some(Uuid::new_v4()), run_id),
        (Status::Polled, Some(agent_id), Uuid::new_v4()),
    ] {
        let mut job = Job::new(&task, run, 1, 1);
        job.status = status;
        job.agent_id = owner;
        job.write(&mut db).await.unwrap();
    }
    let page = get(
        &app,
        &format!("/jobs?status=Polled&agent_id={agent_id}&run_id={run_id}&limit=1"),
    )
    .await;
    assert_eq!(page["items"].as_array().unwrap().len(), 1);
    assert_eq!(page["items"][0]["agent_id"], agent_id.to_string());
    let queue = get(&app, "/jobs?status=Scheduled").await;
    assert_eq!(queue["items"].as_array().unwrap().len(), 1);
    assert_eq!(queue["items"][0]["agent_id"], Value::Null);
}

#[tokio::test]
async fn agent_instances_are_observed_from_valid_polls_and_become_offline() {
    let Some((app, mut db)) = setup().await else {
        return;
    };
    let id = Uuid::new_v4();
    let body = json!({ "agent_id": id, "batch": 0, "wait_ms": 0,
        "resources": { "cpu_cores": 0, "memory_bytes": 1024 } });
    let (status, _) = request(&app, Method::POST, "/jobs/poll", body.clone()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        get(&app, "/agents").await["items"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let mut valid = body;
    valid["batch"] = json!(1);
    let (status, _) = request(&app, Method::POST, "/jobs/poll", valid).await;
    assert_eq!(status, StatusCode::OK);
    let first = get(&app, "/agents?online=true").await;
    assert_eq!(first["items"][0]["uuid"], id.to_string());
    assert_eq!(first["items"][0]["online"], true);
    let capacity = ResourceCapacity {
        cpu_cores: 4,
        memory_bytes: 4096,
    };
    let mut first_poll = AgentRef::new(id, capacity, vec![Uuid::new_v4(), Uuid::new_v4()]);
    let mut second_poll = first_poll.clone();
    let (left, right) = tokio::join!(app.poll(&mut first_poll, 1), app.poll(&mut second_poll, 1));
    left.unwrap();
    right.unwrap();
    let updated = get(&app, "/agents?online=true&limit=1").await;
    assert_eq!(updated["items"].as_array().unwrap().len(), 1);
    assert_eq!(updated["next_cursor"], Value::Null);
    assert_eq!(updated["items"][0]["id"], first["items"][0]["id"]);
    assert_eq!(
        updated["items"][0]["first_seen_at"],
        first["items"][0]["first_seen_at"]
    );
    assert_eq!(updated["items"][0]["cpu_cores"], 4);
    assert_eq!(updated["items"][0]["active_jobs"], 2);
    AgentInstance::filter_by_uuid(id)
        .update()
        .online_until(Timestamp::now() - Duration::from_secs(1))
        .exec(&mut db)
        .await
        .unwrap();
    assert!(
        get(&app, "/agents?online=true").await["items"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let offline = get(&app, "/agents?online=false").await;
    assert_eq!(offline["items"][0]["online"], false);
    assert_eq!(offline["items"][0]["uuid"], id.to_string());
}

#[tokio::test]
async fn poll_commits_or_rolls_back_agent_observations_with_job_delivery() {
    let Some((app, mut db)) = setup().await else {
        return;
    };
    let id = Uuid::new_v4();
    let mut idle = AgentRef::new(
        id,
        ResourceCapacity {
            cpu_cores: 0,
            memory_bytes: 0,
        },
        Vec::new(),
    );
    assert!(app.poll(&mut idle, 2).await.unwrap().is_empty());
    let original = AgentInstance::filter_by_uuid(id)
        .get(&mut db)
        .await
        .unwrap();
    let now = Timestamp::now();
    let run = Run::create()
        .uuid(Uuid::new_v4())
        .data_id(Uuid::new_v4())
        .graph_id(Uuid::new_v4())
        .version(0)
        .revision(1)
        .created_at(now)
        .updated_at(now)
        .exec(&mut db)
        .await
        .unwrap();
    let mut jobs = Vec::new();
    for lease in [Some(30_000), None] {
        let node_id = Uuid::new_v4();
        let node = NodeRun::create()
            .run_id(run.uuid)
            .data_id(run.data_id)
            .graph_id(run.graph_id)
            .version(0)
            .node_id(node_id)
            .revision(1)
            .status("Running".to_owned())
            .created_at(now)
            .updated_at(now)
            .exec(&mut db)
            .await
            .unwrap();
        let config = NodeConfig::create()
            .node_id(node_id)
            .version(1)
            .annotation_match(String::new())
            .config(json!({}))
            .lease(lease)
            .required_cpu_cores(1)
            .required_memory_bytes(512)
            .created_at(now)
            .updated_at(now)
            .created_by(String::new())
            .updated_by(String::new())
            .exec(&mut db)
            .await
            .unwrap();
        let task = Task::new(run.data_id, run.graph_id, node_id, node.id);
        task.emit(&mut db).await.unwrap();
        let mut job = Job::new(&task, run.uuid, config.id, 0);
        job.status = Status::Polled;
        job.agent_id = Some(id);
        job.required_cpu_cores = 1;
        job.required_memory_bytes = 512;
        job.polled_at = Some(now);
        job.expired_at = Some(now + Duration::from_secs(120));
        job.write(&mut db).await.unwrap();
        jobs.push(Job::filter_by_uuid(job.uuid).get(&mut db).await.unwrap());
    }
    let capacity = ResourceCapacity {
        cpu_cores: 4,
        memory_bytes: 4096,
    };
    let mut agent = AgentRef::new(id, capacity, Vec::new());
    assert!(app.poll(&mut agent, 2).await.is_err());
    let unchanged = AgentInstance::filter_by_uuid(id)
        .get(&mut db)
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(unchanged).unwrap(),
        serde_json::to_value(original).unwrap()
    );
    for job in &jobs {
        let unchanged = Job::filter_by_uuid(job.uuid).get(&mut db).await.unwrap();
        assert_eq!(unchanged.expired_at, job.expired_at);
        assert_eq!(unchanged.polled_at, job.polled_at);
        assert_eq!(unchanged.updated_at, job.updated_at);
    }
    assert_eq!(agent.resource(), capacity);

    AgentInstance::filter_by_uuid(id)
        .delete()
        .exec(&mut db)
        .await
        .unwrap();
    assert!(app.poll(&mut agent, 2).await.is_err());
    assert!(
        AgentInstance::filter_by_uuid(id)
            .first()
            .exec(&mut db)
            .await
            .unwrap()
            .is_none()
    );

    NodeConfig::filter_by_id(jobs[1].node_config_id)
        .update()
        .lease(Some(30_000))
        .exec(&mut db)
        .await
        .unwrap();
    assert_eq!(app.poll(&mut agent, 2).await.unwrap().len(), 2);
    let observed = AgentInstance::filter_by_uuid(id)
        .get(&mut db)
        .await
        .unwrap();
    assert_eq!(observed.cpu_cores, capacity.cpu_cores);
    assert_eq!(observed.memory_bytes, capacity.memory_bytes);
    assert_eq!(observed.active_jobs, 0);
    assert_eq!(agent.resource().cpu_cores, 2);
}

#[tokio::test]
async fn graph_list_uses_the_configured_store() {
    let Some((_app, db)) = setup().await else {
        return;
    };
    let mut store = server::Graphs::new();
    let definition = graph("in-memory");
    store.insert(definition.clone());
    let app = App::new(db, Config::default(), store);
    let response = server::router(app)
        .oneshot(
            Request::get(format!("/graphs?graph_id={}", definition.id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let page: graph::Page<DAG> = serde_json::from_slice(&body).unwrap();
    assert_eq!(page.items, [definition]);
    assert_eq!(page.next_cursor, None);
}
