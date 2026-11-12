use std::sync::{
    Arc,
    atomic::{AtomicU16, Ordering},
};

use agent::{client::Client, errors::Error, settings::Settings};
use axum::{
    Json, Router,
    http::{HeaderMap, StatusCode, Uri},
};
use scheduler::ResourceCapacity;
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::mpsc};
use uuid::Uuid;

#[tokio::test]
async fn explicit_job_actions_send_ids_and_preserve_http_errors() {
    let (sender, mut requests) = mpsc::unbounded_channel();
    let response_status = Arc::new(AtomicU16::new(204));
    let status = response_status.clone();
    let router = Router::new().fallback(
        move |uri: Uri, headers: HeaderMap, Json(body): Json<Value>| {
            let sender = sender.clone();
            let status = status.clone();
            async move {
                assert_eq!(headers["authorization"], "Bearer test-fleet-token");
                sender.send((uri.path().to_owned(), body)).unwrap();
                StatusCode::from_u16(status.load(Ordering::SeqCst)).unwrap()
            }
        },
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}/", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let workdir = tempfile::tempdir().unwrap();
    let agent_id = Uuid::new_v4();
    let job_id = Uuid::new_v4();
    let mut settings = Settings::new(
        agent_id,
        address,
        workdir.path(),
        ResourceCapacity::default(),
    );
    settings.auth_token = Some("test-fleet-token".into());
    let client = Client::new(&settings).unwrap();

    for status in [204, 409, 404, 503] {
        response_status.store(status, Ordering::SeqCst);
        for action in ["cancel", "fail", "succeed", "terminate"] {
            let result = match action {
                "cancel" => client.cancel(agent_id, job_id).await,
                "fail" => client.fail(agent_id, job_id).await,
                "succeed" => client.succeed(agent_id, job_id).await,
                "terminate" => client.terminate(agent_id, job_id).await,
                _ => unreachable!(),
            };
            match status {
                204 => result.unwrap(),
                409 => assert!(matches!(result, Err(Error::ClaimLost(id)) if id == job_id)),
                _ => assert!(matches!(result, Err(Error::Http(error))
                    if error.status().unwrap().as_u16() == status)),
            }
            let (path, body) = requests.try_recv().unwrap();
            assert_eq!(path, format!("/job/{action}"));
            assert_eq!(body, json!({ "agent_id": agent_id, "job_id": job_id }));
        }
    }
    assert!(requests.try_recv().is_err());
    server.abort();
}

#[tokio::test]
async fn graphs_reads_one_page_directly_and_preserves_cursor_and_errors() {
    use graph::{
        GraphStore, PageRequest,
        types::{DAG, Node},
    };

    let (sender, mut requests) = mpsc::unbounded_channel();
    let graph_id = Uuid::new_v4();
    let expected = DAG {
        id: graph_id,
        name: "listed".into(),
        version: 3,
        node_count: 1,
        nodes: vec![Node {
            id: Uuid::new_v4(),
            name: "root".into(),
            node_type: "script".into(),
            settings: Default::default(),
            inputs: vec![],
            outputs: vec![],
        }],
        edges: vec![],
        ..DAG::default()
    };
    let definition = expected.clone();
    let response_status = Arc::new(AtomicU16::new(200));
    let status = response_status.clone();
    let router = Router::new().route(
        "/graphs",
        axum::routing::get(move |uri: Uri| {
            let sender = sender.clone();
            let definition = definition.clone();
            let status = status.clone();
            async move {
                let empty = uri
                    .query()
                    .unwrap()
                    .split('&')
                    .any(|pair| pair == "before=8");
                sender.send(uri).unwrap();
                let page = if empty {
                    json!({"items": [], "next_cursor": null})
                } else {
                    json!({
                        "items": [definition],
                        "next_cursor": 8
                    })
                };
                (
                    StatusCode::from_u16(status.load(Ordering::SeqCst)).unwrap(),
                    Json(page),
                )
            }
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let workdir = tempfile::tempdir().unwrap();
    let settings = Settings::new(
        Uuid::new_v4(),
        address,
        workdir.path(),
        ResourceCapacity::default(),
    );
    let mut client = Client::new(&settings).unwrap();
    let page = client
        .graphs(PageRequest::new(Some(12), 1).unwrap(), Some(graph_id))
        .await
        .unwrap();
    assert_eq!(page.items.as_slice(), std::slice::from_ref(&expected));
    assert_eq!(page.next_cursor, Some(8));
    let uri = requests.try_recv().unwrap();
    let query: std::collections::HashMap<_, _> = uri
        .query()
        .unwrap()
        .split('&')
        .map(|pair| pair.split_once('=').unwrap())
        .collect();
    assert_eq!(query["before"], "12");
    assert_eq!(query["limit"], "1");
    assert_eq!(query["graph_id"], graph_id.to_string());
    assert!(requests.try_recv().is_err());
    assert_eq!(
        client
            .graphs(PageRequest::default(), None)
            .await
            .unwrap()
            .items,
        [expected]
    );
    assert_eq!(requests.try_recv().unwrap().query(), Some("limit=50"));
    assert!(requests.try_recv().is_err());

    let empty = client
        .graphs(
            PageRequest::new(page.next_cursor, 1).unwrap(),
            Some(graph_id),
        )
        .await
        .unwrap();
    assert!(empty.items.is_empty());
    assert_eq!(empty.next_cursor, None);
    assert_eq!(requests.try_recv().unwrap().path(), "/graphs");
    assert!(requests.try_recv().is_err());

    response_status.store(503, Ordering::SeqCst);
    assert!(matches!(client.graphs(PageRequest::default(), None).await,
        Err(Error::Http(error)) if error.status() == Some(StatusCode::SERVICE_UNAVAILABLE)));
    server.abort();
}
