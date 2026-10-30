use axum::body::Body;
use axum::http::{Request, StatusCode};
use graph::store::MySqlGraphStore;
use graph::types::{DAG, Node};
use server::{App, Config, router};
use tower::ServiceExt;
use uuid::Uuid;

fn graph(id: Uuid) -> DAG {
    let node = Node {
        id: Uuid::new_v4(),
        name: "task".into(),
        node_type: "script".into(),
        inputs: Vec::new(),
        outputs: Vec::new(),
    };
    DAG {
        id,
        name: "draft".into(),
        version: 0,
        nodes: vec![node],
        edges: Vec::new(),
    }
}

async fn app() -> Option<server::App<MySqlGraphStore>> {
    let url = std::env::var("DATABASE_URL").ok()?;
    let prefix = format!("server_graph_draft_test_{}_", Uuid::new_v4().simple());
    let mut builder = toasty::Db::builder();
    builder.models(server::models());
    builder.table_name_prefix(&prefix);
    let db = builder
        .connect(&url)
        .await
        .expect("connect to DATABASE_URL");
    db.push_schema().await.expect("push schema");

    Some(App::new(
        db.clone(),
        Config::default(),
        MySqlGraphStore::new(db),
    ))
}

#[tokio::test]
async fn graph_draft_api_round_trips_and_publishes() {
    let Some(app) = app().await else {
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

    let response = router(app.clone())
        .oneshot(Request::get(&path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["uuid"], graph_id.to_string());
    assert_eq!(value["updated_by"], "editor");
    assert_eq!(value["graph"]["id"], graph_id.to_string());

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
    assert_eq!(value["version"], 0);
    assert_eq!(value["created_by"], "publisher");

    let graph_path = format!("/graphs/{graph_id}?version=0");
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
    assert_eq!(value["version"], 0);
}

#[tokio::test]
async fn graph_draft_api_rejects_invalid_publish_and_missing_drafts() {
    let Some(app) = app().await else {
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
