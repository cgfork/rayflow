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
        Self::new_with_redirect(config, &config.public_url).await
    }

    pub async fn new_with_redirect(
        config: &AuthConfig,
        public_url: &str,
    ) -> Result<Self, AuthError> {
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
                "{}/api/v1/auth/oidc/callback",
                public_url.trim_end_matches('/')
            ))
            .map_err(|_| AuthError::Unavailable)?,
        );
        Ok(Self {
            client,
            http,
            issuer: oidc.issuer_url.clone(),
        })
    }

    pub fn start(&self, public_url: &str) -> Result<(String, String, String, String), AuthError> {
        let (challenge, verifier) = PkceCodeChallenge::new_random_sha256();
        let client = self.client.clone().set_redirect_uri(
            RedirectUrl::new(format!(
                "{}/api/v1/auth/oidc/callback",
                public_url.trim_end_matches('/')
            ))
            .map_err(|_| AuthError::Unavailable)?,
        );
        let (url, state, nonce) = client
            .authorize_url(
                AuthenticationFlow::<CoreResponseType>::AuthorizationCode,
                CsrfToken::new_random,
                Nonce::new_random,
            )
            .add_scope(Scope::new("profile".into()))
            .add_scope(Scope::new("email".into()))
            .set_pkce_challenge(challenge)
            .url();
        Ok((
            url.to_string(),
            state.secret().clone(),
            nonce.secret().clone(),
            verifier.secret().clone(),
        ))
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
