use super::*;
use axum::{
    Json, Router,
    body::Bytes,
    extract::{Query, State},
    http::HeaderMap,
    response::{IntoResponse, Response},
    routing::post,
};
use catalog::model::Catalog;
use scheduler::ResourceCapacity;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tempfile::TempDir;
use tokio::{net::TcpListener, task::JoinHandle};
use uuid::Uuid;

const PAYLOAD_SIZE: usize = 11 * 1024 * 1024 + 7;

#[test]
fn transfer_configuration_defaults_and_limits() {
    let defaults: Config = serde_json::from_str("{}").unwrap();
    assert_eq!(defaults.multipart_threshold, 64 * MIB);
    assert_eq!(defaults.part_size, 8 * MIB);
    defaults.validate().unwrap();
    for config in [
        Config {
            part_size: MIB,
            ..Default::default()
        },
        Config {
            part_size: MAX_PART + 1,
            ..Default::default()
        },
        Config {
            multipart_threshold: 0,
            ..Default::default()
        },
        Config {
            concurrency: 0,
            ..Default::default()
        },
        Config {
            concurrency: 33,
            ..Default::default()
        },
        Config {
            max_attempts: 0,
            ..Default::default()
        },
    ] {
        assert!(config.validate().is_err());
    }
}

struct Data {
    payload: Vec<u8>,
    parts: BTreeMap<u32, Vec<u8>>,
    attempts: BTreeMap<u32, usize>,
    completed: bool,
    aborted: bool,
    completion_error: bool,
    fatal_part: bool,
    invalid_range: bool,
    change_identity: bool,
    ranges: Vec<(u64, u64)>,
}

#[derive(Clone)]
struct Api {
    url: String,
    data: Arc<Mutex<Data>>,
}

fn signed(api: Api, headers: HeaderMap, operation: PresignOperation) -> Json<PresignedUrl> {
    assert_eq!(headers[header::AUTHORIZATION], "Bearer fleet-token");
    let query = match &operation {
        PresignOperation::CreateMultipart => "action=create".into(),
        PresignOperation::UploadPart {
            upload_id,
            part_number,
        } => {
            assert_eq!(upload_id, "test-upload");
            format!("part={part_number}")
        }
        PresignOperation::CompleteMultipart { upload_id } => {
            assert_eq!(upload_id, "test-upload");
            "action=complete".into()
        }
        PresignOperation::AbortMultipart { upload_id } => {
            assert_eq!(upload_id, "test-upload");
            "action=abort".into()
        }
        _ => panic!("unexpected operation"),
    };
    Json(PresignedUrl {
        url: format!("{}/object?{query}", api.url),
        operation,
        headers: Default::default(),
        expires_at: jiff::Timestamp::now() + Duration::from_secs(60),
    })
}

async fn sign_create(
    State(api): State<Api>,
    headers: HeaderMap,
    Json(_body): Json<serde_json::Value>,
) -> Json<PresignedUrl> {
    signed(api, headers, PresignOperation::CreateMultipart)
}
async fn sign_upload(
    State(api): State<Api>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Json<PresignedUrl> {
    signed(
        api,
        headers,
        PresignOperation::UploadPart {
            upload_id: body["upload_id"].as_str().unwrap().into(),
            part_number: u32::try_from(body["part_number"].as_u64().unwrap()).unwrap(),
        },
    )
}
async fn sign_complete(
    State(api): State<Api>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Json<PresignedUrl> {
    signed(
        api,
        headers,
        PresignOperation::CompleteMultipart {
            upload_id: body["upload_id"].as_str().unwrap().into(),
        },
    )
}
async fn sign_abort(
    State(api): State<Api>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Json<PresignedUrl> {
    signed(
        api,
        headers,
        PresignOperation::AbortMultipart {
            upload_id: body["upload_id"].as_str().unwrap().into(),
        },
    )
}

async fn object(
    State(api): State<Api>,
    method: axum::http::Method,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
    body: Bytes,
) -> Response {
    assert!(
        !headers.contains_key(header::AUTHORIZATION),
        "fleet token leaked to object storage"
    );
    let delay =
        { api.data.lock().unwrap().fatal_part && query.get("part").is_some_and(|n| n == "3") };
    if delay {
        sleep(Duration::from_millis(100)).await;
    }
    let mut data = api.data.lock().unwrap();
    if method == axum::http::Method::PUT {
        let part = query["part"].parse().unwrap();
        assert!(!data.aborted, "part completed after abort");
        if data.fatal_part && part == 1 {
            return StatusCode::FORBIDDEN.into_response();
        }
        let attempt = data.attempts.entry(part).or_default();
        *attempt += 1;
        if part == 2 && *attempt == 1 {
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
        assert_eq!(
            headers[header::CONTENT_LENGTH]
                .to_str()
                .unwrap()
                .parse::<usize>()
                .unwrap(),
            body.len()
        );
        data.parts.insert(part, body.to_vec());
        return ([(header::ETAG, format!("\"part-{part}&etag\""))], "").into_response();
    }
    if method == axum::http::Method::DELETE {
        data.aborted = true;
        data.parts.clear();
        return StatusCode::NO_CONTENT.into_response();
    }
    if method == axum::http::Method::POST {
        if query["action"] == "create" {
            return "<InitiateMultipartUploadResult><UploadId>test-upload</UploadId></InitiateMultipartUploadResult>".into_response();
        }
        if data.completion_error {
            return "<Error><Code>InternalError</Code></Error>".into_response();
        }
        let xml = std::str::from_utf8(&body).unwrap();
        assert!(xml.contains("&amp;etag"), "ETags must be XML escaped");
        let bytes: Vec<_> = data.parts.values().flatten().copied().collect();
        assert_eq!(bytes, data.payload);
        let one = xml.find("<PartNumber>1</PartNumber>").unwrap();
        let two = xml.find("<PartNumber>2</PartNumber>").unwrap();
        let three = xml.find("<PartNumber>3</PartNumber>").unwrap();
        assert!(one < two && two < three);
        data.completed = true;
        return "<CompleteMultipartUploadResult><ETag>done</ETag></CompleteMultipartUploadResult>"
            .into_response();
    }
    let range = headers[header::RANGE]
        .to_str()
        .unwrap()
        .strip_prefix("bytes=")
        .unwrap();
    let (start, end) = range.split_once('-').unwrap();
    let (start, end): (u64, u64) = (start.parse().unwrap(), end.parse().unwrap());
    data.ranges.push((start, end));
    if start != 0 {
        assert_eq!(headers[header::IF_MATCH], "\"original\"");
    }
    if data.invalid_range {
        return (StatusCode::OK, data.payload.clone()).into_response();
    }
    let identity = if data.change_identity && start != 0 {
        "\"changed\""
    } else {
        "\"original\""
    };
    (
        StatusCode::PARTIAL_CONTENT,
        [
            (header::ETAG, identity.to_owned()),
            (
                header::CONTENT_RANGE,
                format!("bytes {start}-{end}/{}", data.payload.len()),
            ),
        ],
        data.payload[start as usize..=end as usize].to_vec(),
    )
        .into_response()
}

struct Fixture {
    root: TempDir,
    api: Api,
    client: Client,
    s3: S3TransferClient,
    task: JoinHandle<()>,
}

impl Fixture {
    async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let api = Api {
            url: format!("http://{}", listener.local_addr().unwrap()),
            data: Arc::new(Mutex::new(Data {
                payload: (0..PAYLOAD_SIZE).map(|n| (n % 251) as u8).collect(),
                parts: Default::default(),
                attempts: Default::default(),
                completed: false,
                aborted: false,
                completion_error: false,
                fatal_part: false,
                invalid_range: false,
                change_identity: false,
                ranges: vec![],
            })),
        };
        let router = Router::new()
            .route("/api/v1/catalog/multipart/create", post(sign_create))
            .route("/api/v1/catalog/multipart/upload", post(sign_upload))
            .route("/api/v1/catalog/multipart/complete", post(sign_complete))
            .route("/api/v1/catalog/multipart/abort", post(sign_abort))
            .route("/object", axum::routing::any(object))
            .layer(axum::extract::DefaultBodyLimit::max(6 * 1024 * 1024))
            .with_state(api.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let root = tempfile::tempdir().unwrap();
        let mut settings = crate::settings::Settings::new(
            Uuid::new_v4(),
            api.url.clone(),
            root.path(),
            ResourceCapacity {
                cpu_cores: 1,
                memory_bytes: 1024,
            },
        );
        settings.auth_token = Some("fleet-token".into());
        let client = Client::new(&settings).unwrap();
        let s3 = S3TransferClient::new(
            reqwest::Client::builder().no_proxy().build().unwrap(),
            Config {
                multipart_threshold: 5 * MIB,
                part_size: 5 * MIB,
                max_attempts: 2,
                ..Default::default()
            },
        )
        .unwrap();
        Self {
            root,
            api,
            client,
            s3,
            task,
        }
    }

    fn transfer(&self, operation: PresignOperation) -> Transfer {
        Transfer {
            catalog: Catalog::new(
                Uuid::new_v4(),
                "key",
                "s3://bucket/key",
                PAYLOAD_SIZE as u64,
                "bin",
            ),
            multipart: true,
            presigned: PresignedUrl {
                url: format!("{}/object", self.api.url),
                operation,
                headers: Default::default(),
                expires_at: jiff::Timestamp::now() + Duration::from_secs(60),
            },
        }
    }

    fn request(&self) -> UploadContext {
        UploadContext {
            job_id: Uuid::new_v4(),
            agent_id: Uuid::new_v4(),
            key: "key".into(),
            catalog_id: Uuid::new_v4(),
        }
    }

    async fn source(&self) -> std::path::PathBuf {
        let path = self.root.path().join("source");
        let bytes = self.api.data.lock().unwrap().payload.clone();
        fs::write(&path, bytes).await.unwrap();
        path
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test]
async fn multipart_retries_only_failed_parts_and_completes_in_order() {
    let fixture = Fixture::new().await;
    let path = fixture.source().await;
    let checkpoint = fixture.root.path().join("state.json");
    fixture
        .s3
        .upload(
            &fixture.client,
            &fixture.request(),
            &fixture.transfer(PresignOperation::Upload),
            &path,
            &checkpoint,
        )
        .await
        .unwrap();
    let data = fixture.api.data.lock().unwrap();
    assert!(data.completed && !data.aborted);
    assert_eq!(data.attempts, BTreeMap::from([(1, 1), (2, 2), (3, 1)]));
    assert!(!checkpoint.exists());
}

#[tokio::test]
async fn resumes_checkpointed_parts_after_restart() {
    let fixture = Fixture::new().await;
    let path = fixture.source().await;
    let checkpoint = fixture.root.path().join("state.json");
    {
        let mut data = fixture.api.data.lock().unwrap();
        let first = data.payload[..(5 * MIB) as usize].to_vec();
        data.parts.insert(1, first);
    }
    write_json(
        &checkpoint,
        &UploadState {
            upload_id: "test-upload".into(),
            size: PAYLOAD_SIZE as u64,
            modified: fs::metadata(&path).await.unwrap().modified().unwrap(),
            part_size: 5 * MIB,
            parts: BTreeMap::from([(1, "\"part-1&etag\"".into())]),
        },
    )
    .await
    .unwrap();
    fixture
        .s3
        .upload(
            &fixture.client,
            &fixture.request(),
            &fixture.transfer(PresignOperation::Upload),
            &path,
            &checkpoint,
        )
        .await
        .unwrap();
    let data = fixture.api.data.lock().unwrap();
    assert!(data.completed);
    assert!(!data.attempts.contains_key(&1));
}

#[tokio::test]
async fn embedded_completion_errors_abort_without_publishing() {
    let fixture = Fixture::new().await;
    fixture.api.data.lock().unwrap().completion_error = true;
    let path = fixture.source().await;
    let checkpoint = fixture.root.path().join("state.json");
    assert!(
        fixture
            .s3
            .upload(
                &fixture.client,
                &fixture.request(),
                &fixture.transfer(PresignOperation::Upload),
                &path,
                &checkpoint
            )
            .await
            .is_err()
    );
    let data = fixture.api.data.lock().unwrap();
    assert!(data.aborted && !data.completed);
    assert!(!checkpoint.exists());
}

#[tokio::test]
async fn failed_upload_settles_inflight_parts_before_abort() {
    let fixture = Fixture::new().await;
    fixture.api.data.lock().unwrap().fatal_part = true;
    let path = fixture.source().await;
    let checkpoint = fixture.root.path().join("state.json");
    assert!(
        fixture
            .s3
            .upload(
                &fixture.client,
                &fixture.request(),
                &fixture.transfer(PresignOperation::Upload),
                &path,
                &checkpoint
            )
            .await
            .is_err()
    );
    let data = fixture.api.data.lock().unwrap();
    assert!(data.aborted && !data.completed && data.parts.is_empty());
}

#[tokio::test]
async fn ranged_download_reassembles_exact_bytes() {
    let fixture = Fixture::new().await;
    let destination = fixture.root.path().join("download");
    fs::write(&destination, []).await.unwrap();
    fixture
        .s3
        .download(&fixture.transfer(PresignOperation::Download), &destination)
        .await
        .unwrap();
    assert_eq!(
        fs::read(destination).await.unwrap(),
        fixture.api.data.lock().unwrap().payload
    );
    assert_eq!(fixture.api.data.lock().unwrap().ranges.len(), 3);
}

#[tokio::test]
async fn ranged_download_rejects_ignored_ranges_and_changed_objects() {
    for change in [false, true] {
        let fixture = Fixture::new().await;
        {
            let mut data = fixture.api.data.lock().unwrap();
            data.invalid_range = !change;
            data.change_identity = change;
        }
        let destination = fixture.root.path().join("download");
        fs::write(&destination, []).await.unwrap();
        assert!(
            fixture
                .s3
                .download(&fixture.transfer(PresignOperation::Download), &destination)
                .await
                .is_err()
        );
    }
}
