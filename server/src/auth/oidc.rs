use openidconnect::{
    AccessTokenHash, AuthenticationFlow, AuthorizationCode, ClientId, ClientSecret, CsrfToken,
    EndpointMaybeSet, EndpointNotSet, EndpointSet, IssuerUrl, Nonce, OAuth2TokenResponse,
    PkceCodeChallenge, PkceCodeVerifier, RedirectUrl, Scope, TokenResponse,
    core::{CoreClient, CoreProviderMetadata, CoreResponseType},
};

use super::{AuthConfig, AuthError, LoginFlow, User, config::web_url};

type Client = CoreClient<
    EndpointSet,
    EndpointNotSet,
    EndpointNotSet,
    EndpointNotSet,
    EndpointMaybeSet,
    EndpointMaybeSet,
>;

pub(super) struct OidcProvider {
    client: Client,
    http: reqwest::Client,
    issuer: String,
}

impl OidcProvider {
    pub async fn new(config: &AuthConfig) -> Result<Self, AuthError> {
        let oidc = config.oidc.as_ref().ok_or(AuthError::Unavailable)?;
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(|_| AuthError::Unavailable)?;
        let metadata = CoreProviderMetadata::discover_async(
            IssuerUrl::new(oidc.issuer_url.clone()).map_err(|_| AuthError::Unavailable)?,
            &http,
        )
        .await
        .map_err(|_| AuthError::Unavailable)?;
        // Discovery endpoints must preserve transport security as well as the issuer.
        web_url(metadata.authorization_endpoint().as_str()).map_err(|_| AuthError::Unavailable)?;
        web_url(
            metadata
                .token_endpoint()
                .ok_or(AuthError::Unavailable)?
                .as_str(),
        )
        .map_err(|_| AuthError::Unavailable)?;
        web_url(metadata.jwks_uri().as_str()).map_err(|_| AuthError::Unavailable)?;
        let client = CoreClient::from_provider_metadata(
            metadata,
            ClientId::new(oidc.client_id.clone()),
            Some(ClientSecret::new(oidc.client_secret.clone())),
        )
        .set_redirect_uri(
            RedirectUrl::new(format!(
                "{}/api/auth/oidc/callback",
                config.public_url.trim_end_matches('/')
            ))
            .map_err(|_| AuthError::Unavailable)?,
        );
        Ok(Self {
            client,
            http,
            issuer: oidc.issuer_url.clone(),
        })
    }

    pub fn start(&self) -> (String, String, String, String) {
        let (challenge, verifier) = PkceCodeChallenge::new_random_sha256();
        let (url, state, nonce) = self
            .client
            .authorize_url(
                AuthenticationFlow::<CoreResponseType>::AuthorizationCode,
                CsrfToken::new_random,
                Nonce::new_random,
            )
            .add_scope(Scope::new("profile".into()))
            .add_scope(Scope::new("email".into()))
            .set_pkce_challenge(challenge)
            .url();
        (
            url.to_string(),
            state.secret().clone(),
            nonce.secret().clone(),
            verifier.secret().clone(),
        )
    }

    pub async fn finish(&self, code: String, flow: LoginFlow) -> Result<User, AuthError> {
        let response = self
            .client
            .exchange_code(AuthorizationCode::new(code))
            .map_err(|_| AuthError::Unavailable)?
            .set_pkce_verifier(PkceCodeVerifier::new(flow.verifier))
            .request_async(&self.http)
            .await
            .map_err(|_| AuthError::InvalidLogin)?;
        let token = response.id_token().ok_or(AuthError::InvalidLogin)?;
        let verifier = self.client.id_token_verifier();
        let claims = token
            .claims(&verifier, &Nonce::new(flow.nonce))
            .map_err(|_| AuthError::InvalidLogin)?;
        if let Some(expected) = claims.access_token_hash() {
            let actual = AccessTokenHash::from_token(
                response.access_token(),
                token.signing_alg().map_err(|_| AuthError::InvalidLogin)?,
                token
                    .signing_key(&verifier)
                    .map_err(|_| AuthError::InvalidLogin)?,
            )
            .map_err(|_| AuthError::InvalidLogin)?;
            if &actual != expected {
                return Err(AuthError::InvalidLogin);
            }
        }
        // Identity is issuer + subject. Email and display name never grant access.
        let name = claims
            .name()
            .and_then(|name| name.get(None))
            .map(|name| name.to_string())
            .or_else(|| claims.preferred_username().map(|name| name.to_string()))
            .unwrap_or_else(|| claims.subject().to_string());
        Ok(User::new(
            &self.issuer,
            claims.subject().as_str(),
            name,
            claims.email().map(|email| email.to_string()),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Json, Router,
        routing::{get, post},
    };
    use openidconnect::core::{
        CoreHmacKey, CoreIdToken, CoreIdTokenClaims, CoreJwsSigningAlgorithm,
    };
    use serde_json::{Value, json};
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn discovery_pkce_and_signed_tokens_verify_issuer_audience_expiry_and_nonce() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let issuer = format!("http://{}", listener.local_addr().unwrap());
        let metadata = json!({"issuer": issuer, "authorization_endpoint": format!("{issuer}/authorize"),
            "token_endpoint": format!("{issuer}/token"), "jwks_uri": format!("{issuer}/keys"),
            "response_types_supported": ["code"], "subject_types_supported": ["public"],
            "id_token_signing_alg_values_supported": ["HS256"], "token_endpoint_auth_methods_supported": ["client_secret_basic"] });
        let token = Arc::new(Mutex::new(String::new()));
        let served_token = token.clone();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen_requests = requests.clone();
        let router = Router::new()
            .route("/.well-known/openid-configuration", get(move || { let metadata = metadata.clone(); async move { Json(metadata) } }))
            .route("/keys", get(|| async { Json(json!({"keys": []})) }))
            .route("/token", post(move |body: String| {
                let token = served_token.lock().unwrap().clone();
                seen_requests.lock().unwrap().push(body);
                async move { Json(json!({"access_token":"access-token", "token_type":"Bearer", "id_token": token})) }
            }));
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let config = AuthConfig {
            enabled: true,
            oidc: Some(super::super::OidcConfig {
                issuer_url: issuer.clone(),
                client_id: "rayflow-test".into(),
                client_secret: "test-secret".into(),
            }),
            ..AuthConfig::default()
        };
        let provider = OidcProvider::new(&config).await.unwrap();
        let (url, state, nonce, verifier) = provider.start();
        let url = url::Url::parse(&url).unwrap();
        let params: std::collections::HashMap<_, _> = url.query_pairs().collect();
        assert_eq!(params["state"], state);
        assert_eq!(params["nonce"], nonce);
        assert_eq!(params["code_challenge_method"], "S256");
        assert!(params["scope"].split(' ').any(|scope| scope == "openid"));
        assert_eq!(
            params["redirect_uri"],
            "http://127.0.0.1:5173/api/auth/oidc/callback"
        );
        let now = jiff::Timestamp::now().as_second();
        let claims = json!({"iss": issuer, "aud": "rayflow-test", "sub": "alice-id", "name":"Alice",
            "iat": now, "exp": now + 300, "nonce": nonce });
        let sign = |claims: Value, secret: &str| {
            let claims: CoreIdTokenClaims = serde_json::from_value(claims).unwrap();
            CoreIdToken::new(
                claims,
                &CoreHmacKey::new(secret),
                CoreJwsSigningAlgorithm::HmacSha256,
                None,
                None,
            )
            .unwrap()
            .to_string()
        };
        let flow = || LoginFlow {
            id: 0,
            state_hash: String::new(),
            browser_hash: String::new(),
            nonce: nonce.clone(),
            verifier: verifier.clone(),
            expires_at: jiff::Timestamp::now(),
        };
        *token.lock().unwrap() = sign(claims.clone(), "test-secret");
        let user = provider.finish("auth-code".into(), flow()).await.unwrap();
        assert_eq!(user.subject, "alice-id");
        assert_eq!(user.provider, issuer);
        assert_eq!(user.name, "Alice");
        let body = requests.lock().unwrap()[0].clone();
        let fields: std::collections::HashMap<_, _> =
            url::form_urlencoded::parse(body.as_bytes()).collect();
        assert_eq!(fields["code_verifier"], verifier);
        assert_eq!(fields["code"], "auth-code");
        for (field, invalid) in [
            ("iss", json!("https://other.example.com")),
            ("aud", json!("other-client")),
            ("exp", json!(now - 60)),
            ("nonce", json!("wrong-nonce")),
        ] {
            let mut changed = claims.clone();
            changed[field] = invalid;
            *token.lock().unwrap() = sign(changed, "test-secret");
            assert!(
                provider.finish("code".into(), flow()).await.is_err(),
                "accepted invalid {field}"
            );
        }
        *token.lock().unwrap() = sign(claims, "incorrect-signing-key");
        assert!(provider.finish("code".into(), flow()).await.is_err());
        server.abort();
    }
}
