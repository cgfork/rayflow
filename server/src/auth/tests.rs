use super::*;

#[tokio::test]
async fn ldap_empty_credentials_never_attempt_an_anonymous_bind() {
    let provider = password::LdapProvider(LdapConfig {
        url: "ldaps://unreachable.invalid".into(),
        bind_dn: "service".into(),
        bind_password: "secret".into(),
        base_dn: "dc=example".into(),
        user_filter: "(uid={username})".into(),
        name_attribute: "cn".into(),
        email_attribute: "mail".into(),
    });
    for (username, password) in [("alice", ""), ("", "password")] {
        assert_eq!(
            provider.authenticate(username, password).await.unwrap(),
            None
        );
    }
}

#[test]
fn cookie_parser_rejects_ambiguous_and_malformed_credentials() {
    let token = random_token();
    let mut headers = HeaderMap::new();
    headers.insert(
        header::COOKIE,
        HeaderValue::from_str(&format!("unrelated=1; rayflow_session={token}")).unwrap(),
    );
    assert_eq!(
        cookie_value(&headers, "rayflow_session"),
        Some(token.as_str())
    );
    headers.append(
        header::COOKIE,
        HeaderValue::from_str(&format!("rayflow_session={token}")).unwrap(),
    );
    assert_eq!(cookie_value(&headers, "rayflow_session"), None);
    headers.insert(
        header::COOKIE,
        HeaderValue::from_static("rayflow_session=short"),
    );
    assert_eq!(cookie_value(&headers, "rayflow_session"), None);
}

#[test]
fn identity_is_scoped_to_issuer_and_subject_not_display_name_or_email() {
    let alice = User::new(
        "issuer-a",
        "alice",
        "Alice".into(),
        Some("same@example.com".into()),
    );
    assert_eq!(alice.id.len(), 64);
    assert_ne!(
        alice.id,
        User::new("issuer-b", "alice", alice.name.clone(), alice.email.clone()).id
    );
    assert_ne!(
        alice.id,
        User::new("issuer-a", "bob", alice.name.clone(), alice.email.clone()).id
    );
    assert_eq!(
        alice.id,
        User::new("issuer-a", "alice", "Renamed".into(), None).id
    );
}

#[test]
fn configuration_requires_secure_provider_connections_and_valid_secrets() {
    let mut config = AuthConfig {
        enabled: true,
        oidc: Some(OidcConfig {
            issuer_url: "https://login.example.com/tenant".into(),
            client_id: "client".into(),
            client_secret: "secret".into(),
        }),
        ..AuthConfig::default()
    };
    assert!(config.validate().is_ok());
    config.public_url = "http://rayflow.example.com".into();
    assert!(matches!(config.validate(), Err(AuthConfigError::UnsafeUrl)));
    config.public_url = "https://rayflow.example.com".into();
    config.oidc.as_mut().unwrap().issuer_url = "http://login.example.com".into();
    assert!(matches!(config.validate(), Err(AuthConfigError::UnsafeUrl)));
    config.oidc = None;
    assert!(matches!(
        config.validate(),
        Err(AuthConfigError::MissingProvider)
    ));
    config.ldap = Some(LdapConfig {
        url: "ldap://directory.example.com".into(),
        bind_dn: "cn=rayflow,dc=example,dc=com".into(),
        bind_password: "secret".into(),
        base_dn: "dc=example,dc=com".into(),
        user_filter: "(uid={username})".into(),
        name_attribute: "cn".into(),
        email_attribute: "mail".into(),
    });
    assert!(config.validate().is_ok());
    config.ldap.as_mut().unwrap().user_filter = "(uid=*)".into();
    assert!(matches!(
        config.validate(),
        Err(AuthConfigError::InvalidLdapConfiguration)
    ));
    assert!(!format!("{config:?}").contains("secret"));
}

#[test]
fn configuration_errors_retain_url_parse_causes_and_service_status() {
    use std::error::Error;

    let config = AuthConfig {
        enabled: true,
        public_url: "invalid-url".into(),
        ..AuthConfig::default()
    };
    let error = AuthError::from(config.validate().unwrap_err());
    let cause = error.source().unwrap();
    assert!(matches!(
        cause.downcast_ref::<AuthConfigError>(),
        Some(AuthConfigError::InvalidUrl(_))
    ));
    assert!(cause.source().unwrap().is::<url::ParseError>());
    assert_eq!(
        error.to_string(),
        "invalid authentication configuration: invalid authentication URL"
    );
    assert_eq!(
        error.into_response().status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test]
async fn oidc_state_is_browser_bound_expiring_and_consumed_once_across_instances() {
    let Some(mut db) = crate::app::test::setup().await else {
        return;
    };
    let auth = Auth::new(AuthConfig::default(), db.clone()).await.unwrap();
    let other = Auth::new(AuthConfig::default(), db.clone()).await.unwrap();
    let browser = random_token();
    let state = random_token();
    LoginFlow::create()
        .state_hash(digest(&state))
        .browser_hash(digest(&browser))
        .nonce("nonce")
        .verifier("verifier")
        .expires_at(Timestamp::now() + Duration::from_secs(300))
        .exec(&mut db)
        .await
        .unwrap();
    assert!(matches!(
        other.consume_flow(&state, &random_token()).await,
        Err(AuthError::InvalidLogin)
    ));
    let (first, second) = tokio::join!(
        auth.consume_flow(&state, &browser),
        other.consume_flow(&state, &browser)
    );
    assert_ne!(first.is_ok(), second.is_ok());
    assert!(matches!(
        auth.consume_flow(&state, &browser).await,
        Err(AuthError::InvalidLogin)
    ));
    LoginFlow::create()
        .state_hash(digest(&state))
        .browser_hash(digest(&browser))
        .nonce("nonce")
        .verifier("verifier")
        .expires_at(Timestamp::now() - Duration::from_secs(1))
        .exec(&mut db)
        .await
        .unwrap();
    assert!(matches!(
        auth.consume_flow(&state, &browser).await,
        Err(AuthError::InvalidLogin)
    ));
}

#[tokio::test]
async fn sessions_survive_server_recreation_and_logout_revokes_them_on_other_instances() {
    let Some(db) = crate::app::test::setup().await else {
        return;
    };
    let auth = Auth::new(AuthConfig::default(), db.clone()).await.unwrap();
    let user = User::new("test", "subject", "User".into(), None);
    let cookie = auth
        .establish(&HeaderMap::new(), user.clone())
        .await
        .unwrap();
    let mut headers = HeaderMap::new();
    headers.insert(
        header::COOKIE,
        HeaderValue::from_str(cookie.to_str().unwrap().split(';').next().unwrap()).unwrap(),
    );
    drop(auth);
    let first = Auth::new(AuthConfig::default(), db.clone()).await.unwrap();
    let second = Auth::new(AuthConfig::default(), db).await.unwrap();
    assert_eq!(first.user(&headers).await.unwrap(), user);
    second.revoke(&headers).await.unwrap();
    assert!(matches!(
        first.user(&headers).await,
        Err(AuthError::Unauthorized)
    ));
}

#[tokio::test]
async fn attempts_are_bounded_and_custom_providers_do_not_skip_configuration_validation() {
    let Some(db) = crate::app::test::setup().await else {
        return;
    };
    let auth = Auth::new(AuthConfig::default(), db.clone()).await.unwrap();
    for _ in 0..10 {
        auth.limit("alice").unwrap();
    }
    assert!(matches!(auth.limit("alice"), Err(AuthError::RateLimited)));
    auth.limit("bob").unwrap();
    let local = password::LocalProvider::new(Vec::new()).await.unwrap();
    let config = AuthConfig {
        enabled: true,
        public_url: "http://insecure.example.com".into(),
        ..AuthConfig::default()
    };
    assert!(
        Auth::with_providers(config, db, vec![Arc::new(local)])
            .await
            .is_err()
    );
}
#[test]
fn fleet_catalog_routes_are_limited_to_transfer_operations() {
    let run_path = format!("/runs/{}", uuid::Uuid::new_v4());
    assert!(super::agent_route(&axum::http::Method::GET, &run_path));
    assert!(!super::agent_route(&axum::http::Method::POST, &run_path));
    assert!(!super::agent_route(&axum::http::Method::GET, "/runs"));
    for action in ["prepare", "commit", "download"] {
        assert!(super::agent_route(
            &axum::http::Method::POST,
            &format!("/catalog/{action}")
        ));
        assert!(!super::agent_route(
            &axum::http::Method::GET,
            &format!("/catalog/{action}")
        ));
    }
    for path in [
        "/catalog",
        "/files",
        "/catalog/complete",
        "/catalog/remove",
        "/jobs/not-a-uuid/artifacts/prepare",
        "/jobs/00000000-0000-0000-0000-000000000000/artifacts/complete",
        "/jobs/00000000-0000-0000-0000-000000000000/artifacts/remove",
    ] {
        assert!(!super::agent_route(&axum::http::Method::POST, path));
    }
}
