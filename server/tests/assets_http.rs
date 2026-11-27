mod support;

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    body::{Body, Bytes},
    extract::{Request, State},
    http::{Method, StatusCode, header},
    response::{IntoResponse, Response},
};
use catalog::model::{Asset, AssetPromotion};
use graph::types::{AssetRef, DAG, Node, NodeSettings, Resource};
use jiff::Timestamp;
use runtime::backend::Backend;
use scheduler::{Actuator, Job, actuator::Status};
use serde_json::{Value, json};
use server::{App, Config, config::StorageConfig, router};
use sha2::{Digest, Sha256};
use tokio::{net::TcpListener, sync::Mutex};
use tower::ServiceExt;
use uuid::Uuid;

type Objects = Arc<Mutex<HashMap<String, Bytes>>>;
type CopyCount = Arc<AtomicUsize>;
type DeleteFailures = Arc<Mutex<HashSet<String>>>;

/// In-memory S3-style service. Copy reads the source as it exists at the time
/// of the request, and a repeated browser upload can overwrite staging bytes.
async fn object(
    State((objects, copies, delete_failures)): State<(Objects, CopyCount, DeleteFailures)>,
    request: Request,
) -> Response {
    let key = request.uri().path().to_owned();
    match *request.method() {
        Method::PUT => {
            let copying = request.headers().contains_key("x-amz-copy-source");
            let body = if let Some(source) = request.headers().get("x-amz-copy-source") {
                copies.fetch_add(1, Ordering::Relaxed);
                assert!(request.uri().query().unwrap().contains("x-amz-copy-source"));
                let Some(bytes) = objects.lock().await.get(source.to_str().unwrap()).cloned()
                else {
                    return StatusCode::NOT_FOUND.into_response();
                };
                bytes
            } else {
                axum::body::to_bytes(request.into_body(), 1024)
                    .await
                    .unwrap()
            };
            objects.lock().await.insert(key, body);
            if copying {
                "<CopyObjectResult><ETag>fixture</ETag></CopyObjectResult>".into_response()
            } else {
                StatusCode::OK.into_response()
            }
        }
        Method::GET | Method::HEAD => match objects.lock().await.get(&key) {
            Some(bytes) if *request.method() == Method::HEAD => {
                ([(header::CONTENT_LENGTH, bytes.len().to_string())], "").into_response()
            }
            Some(bytes) => bytes.clone().into_response(),
            None => StatusCode::NOT_FOUND.into_response(),
        },
        Method::DELETE => {
            if delete_failures.lock().await.contains(&key) {
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
            objects.lock().await.remove(&key);
            StatusCode::NO_CONTENT.into_response()
        }
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

async fn fixture(db: toasty::Db) -> (App<toasty::Db>, Objects, CopyCount, DeleteFailures) {
    let objects = Objects::default();
    let copies = CopyCount::default();
    let delete_failures = DeleteFailures::default();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let storage = Router::new().fallback(object).with_state((
        objects.clone(),
        copies.clone(),
        delete_failures.clone(),
    ));
    tokio::spawn(async move { axum::serve(listener, storage).await.unwrap() });
    let config = Config {
        storage: Some(StorageConfig {
            endpoint,
            bucket: "asset-tests".into(),
            prefix: "artifacts".into(),
            asset_import_prefix: Some("shared-imports".into()),
            region: "us-east-1".into(),
            access_key: "test-key".into(),
            secret_key: "test-secret".into(),
            multipart_enabled: false,
            session_token: None,
            credential_expiry: None,
            presign_ttl: Duration::from_secs(900),
        }),
        ..Config::default()
    };
    (
        App::new(db.clone(), config, db),
        objects,
        copies,
        delete_failures,
    )
}

async fn request(
    app: &Router,
    method: Method,
    path: &str,
    json: Value,
    status: StatusCode,
) -> Value {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method.clone())
                .uri(format!("/api/v1{path}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let actual = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1_000_000)
        .await
        .unwrap();
    assert_eq!(
        actual,
        status,
        "{method} {path}: {}",
        String::from_utf8_lossy(&bytes)
    );
    if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    }
}

async fn post(app: &Router, path: &str, json: Value, status: StatusCode) -> Value {
    request(app, Method::POST, path, json, status).await
}

async fn upload(client: &reqwest::Client, prepared: &Value, bytes: &'static [u8]) {
    let put = &prepared["upload"];
    let mut request = client.put(put["url"].as_str().unwrap());
    for (name, value) in put["headers"].as_object().unwrap() {
        request = request.header(name, value.as_str().unwrap());
    }
    request
        .body(bytes)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
}

fn graph(asset_id: Uuid) -> DAG {
    DAG {
        id: Uuid::new_v4(),
        name: "asset workflow".into(),
        node_count: 1,
        nodes: vec![Node {
            id: Uuid::new_v4(),
            name: "run".into(),
            node_type: "docker".into(),
            settings: NodeSettings {
                config: json!({"image":"rayflow-runlua:local", "command":["/data/assets/script.lua"]}),
                resource: Resource {
                    cpu_cores: 1,
                    memory_bytes: 1024,
                },
                timeout: Duration::from_secs(60),
                ..Default::default()
            },
            inputs: vec![],
            outputs: vec![],
            assets: vec![AssetRef {
                file_name: "script.lua".into(),
                asset_id,
            }],
        }],
        ..Default::default()
    }
}

#[tokio::test]
async fn assets_publish_only_verified_bytes_and_isolate_upload_replays() {
    support::with_test_database(|mut db| async move {
        const SCRIPT: &[u8] = b"return 'a'";
        const OTHER: &[u8] = b"return 'b'";
        let (state, objects, copies, _) = fixture(db.clone()).await;
        let app = router(state.clone());
        let browser = reqwest::Client::builder().no_proxy().build().unwrap();
        let sha256 = format!("{:x}", Sha256::digest(SCRIPT));
        let prepare = json!({
            "name":"script.lua",
            "version":1,
            "size":SCRIPT.len(),
            "format":"text/x-lua",
            "sha256":sha256,
        });
        let pending = post(&app, "/assets/prepare", prepare.clone(), StatusCode::OK).await;
        assert_eq!(pending["state"], "pending");
        assert_eq!(pending["upload"]["operation"], "upload");
        assert!(pending["upload"]["headers"].get("content-length").is_none());
        let upload_url = url::Url::parse(pending["upload"]["url"].as_str().unwrap()).unwrap();
        assert!(upload_url.query_pairs().any(|(key, value)| {
            key == "X-Amz-SignedHeaders" && value.split(';').any(|part| part == "content-length")
        }));
        let asset_id: Uuid = serde_json::from_value(pending["asset_id"].clone()).unwrap();
        let commit = format!("/assets/{asset_id}/commit");
        let upload_ref = json!({"upload_id":pending["upload_id"]});

        let listing = request(&app, Method::GET, "/assets", Value::Null, StatusCode::OK).await;
        assert_eq!(listing["items"], json!([]));
        let mut graph = graph(asset_id);
        let draft = format!("/graphs/{}/draft", graph.id);
        let publish = format!("/graphs/{}/publish", graph.id);
        request(
            &app,
            Method::PUT,
            &draft,
            json!({"graph":graph}),
            StatusCode::OK,
        )
        .await;
        let rejected = post(&app, &publish, json!({}), StatusCode::NOT_FOUND).await;
        assert_eq!(rejected["error_code"], "RAYFLOW_ASSET_NOT_FOUND");

        let incomplete = post(&app, &commit, upload_ref.clone(), StatusCode::CONFLICT).await;
        assert_eq!(incomplete["error_code"], "RAYFLOW_ASSET_UPLOAD_INCOMPLETE");
        // Simulate a legacy or out-of-band staged PUT with the wrong length.
        objects.lock().await.insert(
            upload_url.path().into(),
            Bytes::from_static(b"oversized file"),
        );
        let bad = post(
            &app,
            &commit,
            upload_ref.clone(),
            StatusCode::UNPROCESSABLE_ENTITY,
        )
        .await;
        assert_eq!(bad["error_code"], "RAYFLOW_ASSET_INTEGRITY");
        assert_eq!(copies.load(Ordering::Relaxed), 0);
        upload(&browser, &pending, OTHER).await;
        let bad = post(
            &app,
            &commit,
            upload_ref.clone(),
            StatusCode::UNPROCESSABLE_ENTITY,
        )
        .await;
        assert_eq!(bad["error_code"], "RAYFLOW_ASSET_INTEGRITY");
        upload(&browser, &pending, SCRIPT).await;
        let (published, replay) = tokio::join!(
            post(&app, &commit, upload_ref.clone(), StatusCode::OK),
            post(&app, &commit, upload_ref.clone(), StatusCode::OK),
        );
        assert_eq!(published, replay);
        assert_eq!(published["asset_id"], json!(asset_id));
        assert_eq!(published["sha256"], sha256);
        assert_eq!(
            objects
                .lock()
                .await
                .keys()
                .filter(|key| key.contains("/assets/"))
                .count(),
            1,
        );

        upload(&browser, &pending, OTHER).await;
        let download = post(
            &app,
            &format!("/assets/{asset_id}/download"),
            json!({}),
            StatusCode::OK,
        )
        .await;
        let download_url = url::Url::parse(download["presigned"]["url"].as_str().unwrap()).unwrap();
        assert!(download_url.query_pairs().any(|(key, value)| {
            key == "response-content-disposition" && value == "attachment; filename=\"script.lua\""
        }));
        assert_eq!(download["presigned"]["operation"], "download");
        let canonical = download_url.path();
        let winner = Uuid::parse_str(canonical.rsplit('/').next().unwrap()).unwrap();
        assert!(
            AssetPromotion::filter_by_uuid(winner)
                .first()
                .exec(&mut db)
                .await
                .unwrap()
                .is_none()
        );
        let bytes = browser
            .get(download["presigned"]["url"].as_str().unwrap())
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(&bytes[..], SCRIPT);
        let retried = post(&app, "/assets/prepare", prepare.clone(), StatusCode::OK).await;
        assert_eq!(retried["state"], "published");
        assert_eq!(retried["asset_id"], json!(asset_id));
        let mut conflicting = prepare;
        conflicting["sha256"] = json!(format!("{:x}", Sha256::digest(OTHER)));
        let conflict = post(&app, "/assets/prepare", conflicting, StatusCode::CONFLICT).await;
        assert_eq!(conflict["error_code"], "RAYFLOW_ASSET_CONFLICT");
        post(
            &app,
            "/assets/script.lua/versions/1/cancel",
            json!({}),
            StatusCode::CONFLICT,
        )
        .await;

        let orphan = Uuid::new_v4();
        let active = Uuid::new_v4();
        for (candidate, age) in [(orphan, -60), (active, 60)] {
            let key = format!("artifacts/assets/{asset_id}/{candidate}");
            AssetPromotion::create()
                .uuid(candidate)
                .url(format!("s3://asset-tests/{key}"))
                .expires_at(Some(
                    Timestamp::now()
                        .checked_add(jiff::SignedDuration::from_secs(age))
                        .unwrap(),
                ))
                .exec(&mut db)
                .await
                .unwrap();
            objects
                .lock()
                .await
                .insert(format!("/asset-tests/{key}"), Bytes::from_static(SCRIPT));
        }
        state.reap_asset_promotions().await.unwrap();
        let objects_after_sweep = objects.lock().await;
        assert!(objects_after_sweep.contains_key(canonical));
        assert!(
            !objects_after_sweep
                .keys()
                .any(|key| key.ends_with(&orphan.to_string()))
        );
        assert!(
            objects_after_sweep
                .keys()
                .any(|key| key.ends_with(&active.to_string()))
        );
        drop(objects_after_sweep);
        assert!(
            AssetPromotion::filter_by_uuid(orphan)
                .first()
                .exec(&mut db)
                .await
                .unwrap()
                .is_none()
        );

        let snapshot = post(&app, &publish, json!({}), StatusCode::CREATED).await;
        assert_eq!(snapshot["version"], 1);
        assert_eq!(
            snapshot["nodes"][0]["assets"][0]["asset_id"],
            json!(asset_id)
        );

        let mut actuator = Actuator::new(Backend::new(db.clone(), db.clone()), 64);
        post(
            &app,
            "/runs/start",
            json!({"data_id": Uuid::new_v4(), "graph_id": graph.id}),
            StatusCode::ACCEPTED,
        )
        .await;
        assert_eq!(actuator.tick().await.unwrap().tasks, 1);
        let job = Job::all().first().exec(&mut db).await.unwrap().unwrap();
        let owner = Uuid::new_v4();
        let now = Timestamp::now();
        Job::filter_by_id(job.id)
            .update()
            .status(Status::Polled)
            .agent_id(Some(owner))
            .polled_at(Some(now))
            .expired_at(Some(now.checked_add(Duration::from_secs(120)).unwrap()))
            .exec(&mut db)
            .await
            .unwrap();
        graph.nodes[0].assets.clear();
        request(
            &app,
            Method::PUT,
            &draft,
            json!({"graph":graph}),
            StatusCode::OK,
        )
        .await;
        assert_eq!(
            post(&app, &publish, json!({}), StatusCode::CREATED).await["version"],
            2
        );
        let presign =
            |owner, path: &str| json!({"job_id":job.uuid, "agent_id":owner, "filename":path});
        let route = "/job/asset/presign";
        post(
            &app,
            route,
            presign(Uuid::new_v4(), "script.lua"),
            StatusCode::CONFLICT,
        )
        .await;
        post(
            &app,
            route,
            presign(owner, "other.lua"),
            StatusCode::NOT_FOUND,
        )
        .await;
        let signed = post(&app, route, presign(owner, "script.lua"), StatusCode::OK).await;
        assert_eq!(signed["asset_id"], json!(asset_id));
        assert_eq!(signed["sha256"], sha256);
        assert_eq!(signed["presigned"]["operation"], "download");
    })
    .await;
}

#[tokio::test]
async fn imported_asset_is_bound_to_its_verified_copy() {
    support::with_test_database(|db| async move {
        const BYTES: &[u8] = b"shared data";
        let (state, objects, _, _) = fixture(db).await;
        let app = router(state);
        let source = "/asset-tests/shared-imports/examples/data.json";
        objects
            .lock()
            .await
            .insert(source.into(), Bytes::from_static(b"incorrect file"));
        let path = "/assets/demo/versions/3/import";
        let import = json!({
            "source_key":"examples/data.json",
            "format":"application/json",
            "sha256":format!("{:x}", Sha256::digest(BYTES)),
        });
        post(&app, path, import.clone(), StatusCode::UNPROCESSABLE_ENTITY).await;
        objects
            .lock()
            .await
            .insert(source.into(), Bytes::from_static(BYTES));
        let imported = post(&app, path, import.clone(), StatusCode::OK).await;
        objects.lock().await.remove(source);
        let retry = post(&app, path, import, StatusCode::OK).await;
        assert_eq!(imported, retry);
        let id = imported["asset_id"].as_str().unwrap();
        let download = post(
            &app,
            &format!("/assets/{id}/download"),
            json!({}),
            StatusCode::OK,
        )
        .await;
        let storage_key = url::Url::parse(download["presigned"]["url"].as_str().unwrap()).unwrap();
        assert_eq!(objects.lock().await[storage_key.path()], BYTES);
    })
    .await;
}

#[tokio::test]
async fn cancel_releases_only_the_owners_unpublished_version() {
    support::with_test_database(|mut db| async move {
        let (state, objects, _, _) = fixture(db.clone()).await;
        let app = router(state);
        let request = |bytes: &[u8]| {
            json!({
                "name": "script.lua",
                "version": 1,
                "size": bytes.len(),
                "format": "text/x-lua",
                "sha256": format!("{:x}", Sha256::digest(bytes)),
            })
        };
        let old = post(&app, "/assets/prepare", request(b"old"), StatusCode::OK).await;
        let old_id = old["asset_id"].as_str().unwrap().parse::<Uuid>().unwrap();
        let path = "/assets/script.lua/versions/1/cancel";
        Asset::filter_by_uuid(old_id)
            .update()
            .created_by("other")
            .exec(&mut db)
            .await
            .unwrap();
        post(&app, path, json!({}), StatusCode::FORBIDDEN).await;
        Asset::filter_by_uuid(old_id)
            .update()
            .created_by("local")
            .exec(&mut db)
            .await
            .unwrap();
        let browser = reqwest::Client::builder().no_proxy().build().unwrap();
        upload(&browser, &old, b"old").await;
        post(&app, path, json!({}), StatusCode::NO_CONTENT).await;
        post(&app, path, json!({}), StatusCode::NO_CONTENT).await;
        assert!(objects.lock().await.is_empty());

        let replacement = post(
            &app,
            "/assets/prepare",
            request(b"new version"),
            StatusCode::OK,
        )
        .await;
        assert_ne!(old["asset_id"], replacement["asset_id"]);
        assert_ne!(old["upload_id"], replacement["upload_id"]);
        post(
            &app,
            &format!("/assets/{old_id}/commit"),
            json!({"upload_id": old["upload_id"]}),
            StatusCode::NOT_FOUND,
        )
        .await;
    })
    .await;
}

#[tokio::test]
async fn publication_failure_keeps_the_copy_durable_for_cleanup() {
    support::with_test_database(|mut db| async move {
        const BYTES: &[u8] = b"return true";
        let (state, objects, _, delete_failures) = fixture(db.clone()).await;
        let app = router(state.clone());
        let prepare = json!({
            "name": "script.lua", "version": 1, "size": BYTES.len(), "format": "text/x-lua",
            "sha256": format!("{:x}", Sha256::digest(BYTES)),
        });
        let pending = post(&app, "/assets/prepare", prepare, StatusCode::OK).await;
        let asset_id = pending["asset_id"].as_str().unwrap();
        let commit = format!("/assets/{asset_id}/commit");
        let request = json!({"upload_id": pending["upload_id"]});
        let browser = reqwest::Client::builder().no_proxy().build().unwrap();
        upload(&browser, &pending, BYTES).await;

        toasty::sql::statement(
            "CREATE TRIGGER fail_asset_publish BEFORE INSERT ON catalog FOR EACH ROW \
             SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'fixture publication failure'",
        )
        .exec(&mut db)
        .await
        .unwrap();
        post(
            &app,
            &commit,
            request.clone(),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
        toasty::sql::statement("DROP TRIGGER fail_asset_publish")
            .exec(&mut db)
            .await
            .unwrap();

        let orphan = AssetPromotion::all()
            .first()
            .exec(&mut db)
            .await
            .unwrap()
            .unwrap();
        post(&app, &commit, request, StatusCode::OK).await;
        let download = post(
            &app,
            &format!("/assets/{asset_id}/download"),
            json!({}),
            StatusCode::OK,
        )
        .await;
        let canonical = url::Url::parse(download["presigned"]["url"].as_str().unwrap()).unwrap();
        let orphan_url = url::Url::parse(&orphan.url).unwrap();
        let orphan_key = format!("/asset-tests{}", orphan_url.path());
        assert_ne!(canonical.path(), orphan_key);
        assert!(objects.lock().await.contains_key(&orphan_key));

        AssetPromotion::filter_by_uuid(orphan.uuid)
            .update()
            .expires_at(Some(
                Timestamp::now()
                    .checked_sub(Duration::from_secs(1))
                    .unwrap(),
            ))
            .exec(&mut db)
            .await
            .unwrap();
        delete_failures.lock().await.insert(orphan_key.clone());
        state.reap_asset_promotions().await.unwrap();
        let fenced = AssetPromotion::filter_by_uuid(orphan.uuid)
            .get(&mut db)
            .await
            .unwrap();
        assert!(fenced.expires_at.is_none());
        assert!(objects.lock().await.contains_key(&orphan_key));

        delete_failures.lock().await.remove(&orphan_key);
        state.reap_asset_promotions().await.unwrap();
        assert!(
            AssetPromotion::filter_by_uuid(orphan.uuid)
                .first()
                .exec(&mut db)
                .await
                .unwrap()
                .is_none()
        );
        let objects = objects.lock().await;
        assert!(!objects.contains_key(&orphan_key));
        assert_eq!(objects[canonical.path()], BYTES);
    })
    .await;
}
