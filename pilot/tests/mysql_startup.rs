use std::time::Duration;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use coordinator::mysql::MySqlLeaderElection;
use pilot::{Pilot, Step};
use runtime::{
    backend::Backend,
    command::{NodeRun, Run},
};
use scheduler::{Job, actuator::Status};
use server::StaticGraphStore;
use tower::ServiceExt;
use uuid::Uuid;

#[tokio::test]
async fn startup_models_support_leadership_and_database_graph_execution() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!(
            "skipping: set DATABASE_URL to an isolated MySQL database; this test resets its schema"
        );
        return;
    };
    let mut db = toasty::Db::builder()
        .models(pilot::models())
        .connect(&url)
        .await
        .unwrap();
    db.reset_db().await.unwrap();
    db.push_schema().await.unwrap();

    let template_id = Uuid::from_u128(0x00000001_0000_0000_0000_000000000000);
    let graph_id = Uuid::new_v4();
    let mut graph = StaticGraphStore::new().graph(template_id).unwrap().clone();
    graph.id = graph_id;
    for node in &mut graph.nodes {
        node.node_type = "docker".into();
        node.settings = graph::types::NodeSettings {
            config: serde_json::json!({"image": "alpine:3.21"}),
            resource: graph::types::Resource {
                cpu_cores: 1,
                memory_bytes: 1024,
            },
            timeout: Duration::from_secs(30),
        };
    }
    let app = server::App::new(db.clone(), server::Config::default(), db.clone());
    app.write_graph_draft(graph_id, graph, "editor")
        .await
        .unwrap();
    let published = app
        .publish_graph_draft(graph_id, "publisher")
        .await
        .unwrap();

    let data_id = Uuid::new_v4();
    let response = server::router(app)
        .oneshot(
            Request::post("/runs/start")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"data_id": data_id, "graph_id": graph_id}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let election = MySqlLeaderElection::new(
        db.clone(),
        format!("startup-{}", Uuid::new_v4()),
        "pilot",
        Duration::from_secs(30),
    );
    let mut pilot = Pilot::new(
        election,
        Backend::new(db.clone(), db.clone()),
        64,
        Duration::from_millis(10),
    );

    let step = pilot.step().await.unwrap();
    assert_eq!(step.commands, 1);
    assert!(step.tasks > 0);
    assert!(pilot.is_leader().await.unwrap());
    let runs = Run::all().exec(&mut db).await.unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].data_id, data_id);
    assert_eq!(runs[0].graph_id, graph_id);
    assert_eq!(runs[0].version, published.version);
    let jobs = Job::all().exec(&mut db).await.unwrap();
    assert_eq!(jobs.len(), step.tasks);
    for job in jobs {
        assert_eq!(job.run_id, runs[0].uuid);
        assert_eq!(job.status, Status::Scheduled);
        let node = NodeRun::filter_by_id(job.node_run_id)
            .get(&mut db)
            .await
            .unwrap();
        assert_eq!(node.status, "Running");
        assert_eq!(node.version, published.version);
    }
    assert_eq!(pilot.step().await.unwrap(), Step::default());
    pilot.election().resign().await.unwrap();
    assert!(!pilot.is_leader().await.unwrap());
}
