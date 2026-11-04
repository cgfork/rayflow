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
    use std::io::Read;
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

    for (signal, log_filter, config_variable) in [
        ("-INT", None, None),
        ("-TERM", Some("off"), Some("RAYFLOW_AGENT_CONFIG")),
        ("-TERM", None, Some("RAYFLOW_CONFIG")),
    ] {
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
        let path = root.path().join(if config_variable.is_some() {
            "agent.yaml"
        } else {
            "rayflow.yaml"
        });
        std::fs::write(
            &path,
            format!(
                "id: {}\nresources:\n  cpu_cores: 2\n  memory_bytes: 2048\n",
                Uuid::new_v4()
            ),
        )
        .unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_agent"));
        command.env_clear();
        if let Some(filter) = log_filter {
            command.env("RUST_LOG", filter);
        }
        if let Some(variable) = config_variable {
            command.env(variable, path);
        }
        let mut process = Process(
            command
                .current_dir(root.path())
                .env("RAYFLOW_AGENT_ID", id.to_string())
                .env("RAYFLOW_AGENT_SERVER_URL", &address)
                .env("RAYFLOW_AGENT_POLL_INTERVAL", "10ms")
                .env("RAYFLOW_AGENT_REQUEST_TIMEOUT", "1s")
                .env("DOCKER_HOST", &address)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
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
        let mut stdout = String::new();
        process
            .0
            .stdout
            .take()
            .unwrap()
            .read_to_string(&mut stdout)
            .unwrap();
        assert!(stdout.is_empty(), "{stdout}");
        let mut stderr = String::new();
        process
            .0
            .stderr
            .take()
            .unwrap()
            .read_to_string(&mut stderr)
            .unwrap();
        if log_filter.is_none() {
            assert!(stderr.contains("INFO"), "{stderr}");
            assert!(stderr.contains("rayflow agent started"), "{stderr}");
            assert!(stderr.contains("rayflow agent shutting down"), "{stderr}");
            assert!(stderr.contains(&id.to_string()), "{stderr}");
            assert!(stderr.contains(&address), "{stderr}");
        } else {
            assert!(stderr.is_empty(), "{stderr}");
        }
        server.abort();
    }
}
