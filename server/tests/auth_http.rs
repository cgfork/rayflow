mod support;

use argon2::{Argon2, PasswordHasher, password_hash::SaltString};
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{HeaderMap, Request, StatusCode, header},
};
use serde_json::{Value, json};
use server::{
    App, Config,
    auth::{Auth, AuthConfig, LocalUser, User, UserRecord},
    router,
};
use toasty::Db;
use tower::ServiceExt;

const PUBLIC_ORIGIN: &str = "http://192.0.2.10:5173";
const FLEET_TOKEN: &str = "rayflow-auth-http-test-agent-token";

async fn request(app: &Router, request: Request<Body>, expected: StatusCode) -> (HeaderMap, Value) {
    let route = format!("{} {}", request.method(), request.uri());
    let (parts, body) = app.clone().oneshot(request).await.unwrap().into_parts();
    let bytes = to_bytes(body, 1024 * 1024).await.unwrap();
    assert_eq!(
        parts.status,
        expected,
        "{route} response: {}",
        String::from_utf8_lossy(&bytes)
    );
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (parts.headers, body)
}

async fn check_user_resolution(app: &Router, mut db: Db, cookie: &str) {
    let author = User::new(
        "local",
        "author",
        "Workflow Author".into(),
        Some("author@example.test".into()),
    );
    let malformed_id = "a".repeat(64);
    let unknown_id = "b".repeat(64);
    for (id, profile) in [
        (author.id.clone(), json!(author)),
        (malformed_id.clone(), json!({"name": false})),
    ] {
        UserRecord::create()
            .user_id(id)
            .profile(profile)
            .updated_at(jiff::Timestamp::now())
            .exec(&mut db)
            .await
            .unwrap();
    }

    let resolve = || {
        Request::post("/api/v1/users/resolve")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::ORIGIN, PUBLIC_ORIGIN)
    };
    let session_resolve = || {
        resolve()
            .header(header::COOKIE, cookie)
            .header("x-rayflow-request", "1")
    };
    let ids = json!({"ids": [author.id, malformed_id, author.id, unknown_id]});
    let lookup = || Body::from(ids.to_string());

    request(
        app,
        resolve().body(lookup()).unwrap(),
        StatusCode::UNAUTHORIZED,
    )
    .await;
    request(
        app,
        resolve()
            .header(header::AUTHORIZATION, format!("Bearer {FLEET_TOKEN}"))
            .body(lookup())
            .unwrap(),
        StatusCode::FORBIDDEN,
    )
    .await;
    request(
        app,
        resolve()
            .header(header::COOKIE, cookie)
            .body(lookup())
            .unwrap(),
        StatusCode::FORBIDDEN,
    )
    .await;

    let (_, names) = request(
        app,
        session_resolve().body(lookup()).unwrap(),
        StatusCode::OK,
    )
    .await;
    assert_eq!(names, json!({(author.id.as_str()): author.name}));

    for ids in [
        vec!["invalid".into()],
        vec!["x".repeat(64)],
        vec![author.id.clone(); 101],
    ] {
        let (_, error) = request(
            app,
            session_resolve()
                .body(Body::from(json!({"ids": ids}).to_string()))
                .unwrap(),
            StatusCode::BAD_REQUEST,
        )
        .await;
        assert_eq!(error["error_code"], "RAYFLOW_INVALID_REQUEST");
    }

    let public_app = router(App::new(db.clone(), Config::default(), db));
    let (_, names) = request(
        &public_app,
        resolve().body(lookup()).unwrap(),
        StatusCode::OK,
    )
    .await;
    assert_eq!(names, json!({}));
}

async fn login_workflow(db: Db) {
    let credentials =
        json!({"provider": "local", "username": "developer", "password": "test-password"});
    let salt = SaltString::encode_b64(b"rayflow-test-salt").unwrap();
    let password_hash = Argon2::default()
        .hash_password(credentials["password"].as_str().unwrap().as_bytes(), &salt)
        .unwrap()
        .to_string();
    let auth = Auth::new(
        AuthConfig {
            enabled: true,
            public_url: format!("{PUBLIC_ORIGIN}/"),
            allow_insecure_http: true,
            agent_token: Some(FLEET_TOKEN.into()),
            local_users: vec![LocalUser {
                username: "developer".into(),
                name: "Rayflow Developer".into(),
                password_hash,
            }],
            ..AuthConfig::default()
        },
        db.clone(),
    )
    .await
    .unwrap();
    let app = router(App::new(db.clone(), Config::default(), db.clone()).with_auth(auth));

    let (_, providers) = request(
        &app,
        Request::get("/api/v1/auth/providers")
            .body(Body::empty())
            .unwrap(),
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        providers,
        json!({
            "enabled": true,
            "company_name": "Rayflow",
            "public_url": PUBLIC_ORIGIN,
            "providers": [{"id": "local", "label": "Rayflow account", "kind": "password"}],
        })
    );

    let login = |origin| {
        Request::post("/api/v1/auth/login")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::ORIGIN, origin)
    };
    let login_body = || Body::from(credentials.to_string());
    request(
        &app,
        login("http://127.0.0.1:5173")
            .header("x-rayflow-request", "1")
            .body(login_body())
            .unwrap(),
        StatusCode::FORBIDDEN,
    )
    .await;
    request(
        &app,
        login(PUBLIC_ORIGIN).body(login_body()).unwrap(),
        StatusCode::FORBIDDEN,
    )
    .await;

    let (headers, user) = request(
        &app,
        login(PUBLIC_ORIGIN)
            .header("x-rayflow-request", "1")
            .body(login_body())
            .unwrap(),
        StatusCode::OK,
    )
    .await;
    assert_eq!(user["name"], "Rayflow Developer");
    let cookie = headers[header::SET_COOKIE].to_str().unwrap();
    assert!(cookie.contains("; HttpOnly; SameSite=Lax;"));
    let cookie = cookie.split_once(';').unwrap().0;
    let me = || {
        Request::get("/api/v1/auth/me")
            .header(header::COOKIE, cookie)
            .body(Body::empty())
            .unwrap()
    };
    let (_, current_user) = request(&app, me(), StatusCode::OK).await;
    assert_eq!(current_user, user);
    check_user_resolution(&app, db, cookie).await;

    let (headers, _) = request(
        &app,
        Request::post("/api/v1/auth/logout")
            .header(header::ORIGIN, PUBLIC_ORIGIN)
            .header(header::COOKIE, cookie)
            .header("x-rayflow-request", "1")
            .body(Body::empty())
            .unwrap(),
        StatusCode::NO_CONTENT,
    )
    .await;
    assert!(
        headers[header::SET_COOKIE]
            .to_str()
            .unwrap()
            .contains("Max-Age=0")
    );
    request(&app, me(), StatusCode::UNAUTHORIZED).await;
}

#[tokio::test]
async fn local_sign_in_uses_the_configured_browser_origin() {
    support::with_test_database(login_workflow).await;
}
