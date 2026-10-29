use std::sync::{
    Arc,
    atomic::{AtomicU16, Ordering},
};

use agent::{client::Client, errors::Error, settings::Settings};
use axum::{
    Json, Router,
    http::{StatusCode, Uri},
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
    let router = Router::new().fallback(move |uri: Uri, Json(body): Json<Value>| {
        let sender = sender.clone();
        let status = status.clone();
        async move {
            sender.send((uri.path().to_owned(), body)).unwrap();
            StatusCode::from_u16(status.load(Ordering::SeqCst)).unwrap()
        }
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}/", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let workdir = tempfile::tempdir().unwrap();
    let agent_id = Uuid::new_v4();
    let job_id = Uuid::new_v4();
    let settings = Settings::new(
        agent_id,
        address,
        workdir.path(),
        ResourceCapacity::default(),
    );
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
