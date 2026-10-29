use std::process::{Child, Command, Stdio};

use tempfile::tempdir;

struct Process(Child);

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn main_reports_invalid_configuration_before_connecting() {
    let root = tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_agent"))
        .env_clear()
        .current_dir(root.path())
        .env("RAYFLOW_AGENT_ID", "invalid")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("id"));
}

#[cfg(unix)]
#[tokio::test]
async fn main_polls_with_configured_identity_and_exits_on_shutdown_signals() {
    use std::{convert::Infallible, time::Duration};

    use axum::{
        Json, Router,
        body::{Body, Bytes},
        http::Uri,
        response::{IntoResponse, Response},
    };
    use serde_json::{Value, json};
    use tokio::{
        net::TcpListener,
        sync::mpsc,
        time::{sleep, timeout},
    };
    use uuid::Uuid;

    for signal in ["-INT", "-TERM"] {
        let (sender, mut requests) = mpsc::unbounded_channel();
        let router = Router::new().fallback(move |uri: Uri, body: Bytes| {
            let sender = sender.clone();
            async move {
                match uri.path() {
                    "/jobs/poll" => {
                        sender
                            .send(serde_json::from_slice::<Value>(&body).unwrap())
                            .unwrap();
                        Json(json!({"jobs": []})).into_response()
                    }
                    _ => Response::new(Body::from_stream(futures_util::stream::pending::<
                        Result<Bytes, Infallible>,
                    >())),
                }
            }
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let root = tempdir().unwrap();
        let id = Uuid::new_v4();
        let path = root.path().join("agent.yaml");
        std::fs::write(
            &path,
            format!(
                "id: {}\nresources:\n  cpu_cores: 2\n  memory_bytes: 2048\n",
                Uuid::new_v4()
            ),
        )
        .unwrap();
        let mut process = Process(
            Command::new(env!("CARGO_BIN_EXE_agent"))
                .env_clear()
                .current_dir(root.path())
                .env("RAYFLOW_AGENT_CONFIG", path)
                .env("RAYFLOW_AGENT_ID", id.to_string())
                .env("RAYFLOW_AGENT_SERVER_URL", &address)
                .env("RAYFLOW_AGENT_POLL_INTERVAL", "10ms")
                .env("RAYFLOW_AGENT_REQUEST_TIMEOUT", "1s")
                .env("DOCKER_HOST", &address)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
        let request = timeout(Duration::from_secs(5), requests.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(request["agent_id"], id.to_string());
        assert_eq!(
            request["resources"],
            json!({"cpu_cores": 2, "memory_bytes": 2048})
        );
        assert!(root.path().join(".rayflow/jobs").is_dir());

        assert!(
            Command::new("/bin/kill")
                .args([signal, &process.0.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        let status = timeout(Duration::from_secs(5), async {
            loop {
                if let Some(status) = process.0.try_wait().unwrap() {
                    break status;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(status.success(), "{signal}: {status}");
        server.abort();
    }
}
