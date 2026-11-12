use argon2::{Argon2, PasswordHasher, password_hash::SaltString};
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use jiff::Timestamp;
use serde_json::{Value, json};
use server::{
    App, Config,
    auth::{Auth, AuthConfig, LocalUser, Session, User},
};
use toasty::Db;
use tower::ServiceExt;
use uuid::Uuid;

async fn setup() -> Option<(Router, App<Db>, Db)> {
    let url = std::env::var("DATABASE_URL").ok()?;
    let mut builder = Db::builder();
    builder.models(server::models());
    builder.table_name_prefix(&format!("auth_{}_", Uuid::new_v4().simple()));
    let db = builder.connect(&url).await.unwrap();
    db.push_schema().await.unwrap();
    let salt = SaltString::encode_b64(b"test salt only!!").unwrap();
    let hash = Argon2::default()
        .hash_password(b"correct password", &salt)
        .unwrap()
        .to_string();
    let config = Config {
        auth: AuthConfig {
            enabled: true,
            public_url: "https://rayflow.example.com".into(),
            company_name: "Example company".into(),
            agent_token: Some("test-agent-token-with-at-least-32-characters".into()),
            local_users: vec![LocalUser {
                username: "alice".into(),
                name: "Alice".into(),
                password_hash: hash,
            }],
            ..AuthConfig::default()
        },
        ..Config::default()
    };
    let auth = Auth::new(config.auth.clone(), db.clone()).await.unwrap();
    // The attached Auth config is authoritative, even if App used defaults.
    let app = App::new(db.clone(), Config::default(), db.clone()).with_auth(auth);
    Some((server::router(app.clone()), app, db))
}

async fn request(
    router: &Router,
    method: &str,
    path: &str,
    body: Value,
    cookie: Option<&str>,
    csrf: bool,
) -> (StatusCode, axum::http::HeaderMap, Value) {
    let mut builder = Request::builder()
        .method(method)
        .uri(path)
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(cookie) = cookie {
        builder = builder.header(header::COOKIE, cookie);
    }
    if csrf {
        builder = builder.header("x-rayflow-request", "1");
    }
    let response = router
        .clone()
        .oneshot(builder.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, headers, value)
}

async fn login(router: &Router) -> (String, User) {
    let (status, headers, value) = request(
        router,
        "POST",
        "/auth/login",
        json!({"provider":"local", "username":"alice", "password":"correct password"}),
        None,
        true,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let cookie = headers[header::SET_COOKIE].to_str().unwrap();
    assert!(
        cookie.contains("HttpOnly") && cookie.contains("SameSite=Lax") && cookie.contains("Secure")
    );
    assert_eq!(headers[header::CACHE_CONTROL], "no-store");
    (
        cookie.split(';').next().unwrap().into(),
        serde_json::from_value(value).unwrap(),
    )
}

#[tokio::test]
async fn login_protects_direct_api_requests_and_records_verified_audit_identity() {
    let Some((router, app, mut db)) = setup().await else {
        return;
    };
    for path in ["/graphs", "/runs", "/agents", "/jobs", "/auth/me"] {
        assert_eq!(
            request(&router, "GET", path, Value::Null, None, false)
                .await
                .0,
            StatusCode::UNAUTHORIZED,
            "{path}"
        );
    }
    let (status, _, providers) =
        request(&router, "GET", "/auth/providers", Value::Null, None, false).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(providers["company_name"], "Example company");
    assert!(!providers.to_string().contains("password_hash"));
    let (cookie, user) = login(&router).await;
    assert_eq!(user, User::new("local", "alice", "Alice".into(), None));
    assert_eq!(
        request(
            &router,
            "GET",
            "/auth/me",
            Value::Null,
            Some(&cookie),
            false
        )
        .await
        .2["id"],
        user.id
    );
    assert_eq!(
        request(&router, "GET", "/graphs", Value::Null, Some(&cookie), false)
            .await
            .0,
        StatusCode::OK
    );
    let graph = graph::types::DAG {
        id: Uuid::new_v4(),
        name: "audit".into(),
        nodes: vec![graph::types::Node {
            id: Uuid::new_v4(),
            name: "start".into(),
            node_type: "docker".into(),
            settings: graph::types::NodeSettings {
                config: json!({"image":"alpine:3.21"}),
                resource: graph::types::Resource {
                    cpu_cores: 1,
                    memory_bytes: 1024,
                },
                timeout: std::time::Duration::from_secs(30),
            },
            inputs: vec![],
            outputs: vec![],
        }],
        node_count: 1,
        ..Default::default()
    };
    let path = format!("/graphs/{}/draft", graph.id);
    let (status, _, value) = request(
        &router,
        "PUT",
        &path,
        json!({"graph":graph,"actor":"forged-admin"}),
        Some(&cookie),
        true,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(
        app.read_graph_draft(graph.id).await.unwrap().updated_by,
        user.id
    );
    let (status, _, published) = request(
        &router,
        "POST",
        &format!("/graphs/{}/publish", graph.id),
        json!({"actor":"forged-publisher"}),
        Some(&cookie),
        true,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{published}");
    let published_record = graph::store::GraphRecord::filter_by_uuid(graph.id)
        .first()
        .exec(&mut db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(published_record.created_by, user.id);
    let rows = Session::all().exec(&mut db).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_ne!(rows[0].token_hash, cookie.split_once('=').unwrap().1);
    // An enabled API must fail closed if its providers were not initialized.
    let mut config = Config::default();
    config.auth.enabled = true;
    let fail_closed = server::router(App::new(db.clone(), config, db));
    assert_eq!(
        request(
            &fail_closed,
            "GET",
            "/graphs",
            Value::Null,
            Some(&cookie),
            false
        )
        .await
        .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test]
async fn credentials_csrf_expiry_rotation_and_logout_are_enforced() {
    let Some((router, _, mut db)) = setup().await else {
        return;
    };
    let credentials =
        json!({"provider":"local", "username":"alice", "password":"correct password"});
    assert_eq!(
        request(
            &router,
            "POST",
            "/auth/login",
            credentials.clone(),
            None,
            false
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    for (username, password) in [
        ("alice", "wrong"),
        ("missing", "correct password"),
        ("alice", ""),
    ] {
        assert_eq!(
            request(
                &router,
                "POST",
                "/auth/login",
                json!({"provider":"local", "username":username, "password":password}),
                None,
                true
            )
            .await
            .0,
            StatusCode::UNAUTHORIZED
        );
    }
    let (cookie, _) = login(&router).await;
    assert_eq!(
        request(
            &router,
            "POST",
            "/auth/logout",
            json!({}),
            Some(&cookie),
            false
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let (status, headers, _) = request(
        &router,
        "POST",
        "/auth/login",
        credentials,
        Some(&cookie),
        true,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let rotated = headers[header::SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap();
    assert_ne!(cookie, rotated);
    assert_eq!(
        request(
            &router,
            "GET",
            "/auth/me",
            Value::Null,
            Some(&cookie),
            false
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    Session::all()
        .update()
        .expires_at(Timestamp::now() - std::time::Duration::from_secs(1))
        .exec(&mut db)
        .await
        .unwrap();
    assert_eq!(
        request(
            &router,
            "GET",
            "/auth/me",
            Value::Null,
            Some(rotated),
            false
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    let (cookie, _) = login(&router).await;
    let response = request(
        &router,
        "POST",
        "/auth/logout",
        json!({}),
        Some(&cookie),
        true,
    )
    .await;
    assert_eq!(response.0, StatusCode::NO_CONTENT);
    assert!(
        response.1[header::SET_COOKIE]
            .to_str()
            .unwrap()
            .contains("Max-Age=0")
    );
    assert_eq!(
        request(
            &router,
            "GET",
            "/auth/me",
            Value::Null,
            Some(&cookie),
            false
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    let cross_origin = Request::post("/auth/logout")
        .header("x-rayflow-request", "1")
        .header(header::ORIGIN, "https://evil.example")
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        router.oneshot(cross_origin).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
}

#[tokio::test]
async fn fleet_credentials_cannot_access_management_or_bypass_login() {
    let Some((router, _, _)) = setup().await else {
        return;
    };
    for (method, path, expected) in [
        ("GET", "/graphs", StatusCode::OK),
        (
            "GET",
            "/graphs/00000000-0000-0000-0000-000000000000/draft",
            StatusCode::FORBIDDEN,
        ),
        ("GET", "/runs", StatusCode::FORBIDDEN),
        ("POST", "/runs/start", StatusCode::FORBIDDEN),
        ("POST", "/task_events", StatusCode::FORBIDDEN),
        ("POST", "/jobs/poll", StatusCode::UNSUPPORTED_MEDIA_TYPE),
    ] {
        let request = Request::builder()
            .method(method)
            .uri(path)
            .header(
                header::AUTHORIZATION,
                "Bearer test-agent-token-with-at-least-32-characters",
            )
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            router.clone().oneshot(request).await.unwrap().status(),
            expected,
            "{path}"
        );
    }
    let request = Request::get("/graphs")
        .header(header::AUTHORIZATION, "Bearer wrong")
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        router.oneshot(request).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
}
