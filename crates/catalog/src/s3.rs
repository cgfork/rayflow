//! S3-compatible object storage using path-style URLs and AWS Signature Version 4.
//!
//! Enable the `s3` feature and wrap any [`FileSystem`] with [`S3`].
//! Signing is local; uploading/downloading bytes and creating buckets are left
//! to the caller. Stored locations have the form `s3://bucket/object-key`.

use std::time::{Duration, SystemTime};

pub use aws_credential_types::Credentials;
use aws_sigv4::{
    http_request::{
        PercentEncodingMode, SignableBody, SignableRequest, SignatureLocation, SigningSettings,
        UriPathNormalizationMode, sign,
    },
    sign::v4,
};
use jiff::Timestamp;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_decode_str, percent_encode};

use crate::{
    Catalog, CatalogSystem, FileKey, FileSystem, FileWithCatalog, ObjectStorage, PresignOperation,
    PresignedUrl,
};

const MAX_EXPIRES_SECONDS: u64 = 604_800;

#[cfg(test)]
mod tests;

const KEY_ENCODING: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~')
    .remove(b'/');

/// Connection details for an S3-compatible endpoint supporting path-style URLs.
#[derive(Clone, Debug)]
pub struct Config {
    /// HTTP(S) origin, for example `http://localhost:9000`. It must be reachable
    /// by the clients using the returned URLs. Path prefixes are not supported.
    pub endpoint: String,
    /// Signing region; defaults to `us-east-1` and must match the provider.
    pub region: String,
    /// Access key, secret key, and optionally a session token and its expiry.
    /// The caller is responsible for obtaining and refreshing credentials.
    pub credentials: Credentials,
}

impl Config {
    pub fn new(endpoint: impl Into<String>, credentials: Credentials) -> Self {
        Self {
            endpoint: endpoint.into(),
            region: "us-east-1".into(),
            credentials,
        }
    }

    fn validate(&mut self) -> Result<(), ConfigError> {
        let endpoint = url::Url::parse(&self.endpoint)
            .map_err(|_| ConfigError("endpoint must be an absolute HTTP(S) origin"))?;
        if !matches!(endpoint.scheme(), "http" | "https")
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.path() != "/"
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || self.endpoint.chars().any(char::is_control)
            || self.endpoint.contains('\\')
        {
            return Err(ConfigError(
                "endpoint must be an HTTP(S) origin without credentials, path, query, or fragment",
            ));
        }
        if self.region.is_empty()
            || !self
                .region
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-')
        {
            return Err(ConfigError(
                "region must contain only ASCII letters, digits, or hyphens",
            ));
        }
        if self.credentials.access_key_id().is_empty()
            || self.credentials.secret_access_key().is_empty()
            || self.credentials.session_token() == Some("")
        {
            return Err(ConfigError(
                "access key, secret key, and any supplied session token must be nonempty",
            ));
        }
        self.endpoint = endpoint.as_str().trim_end_matches('/').to_owned();
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid S3 configuration: {0}")]
pub struct ConfigError(&'static str);

#[derive(Debug, thiserror::Error)]
pub enum Error<E> {
    #[error("metadata filesystem failed: {0}")]
    FileSystem(E),
    #[error("catalog {0} was not found")]
    CatalogNotFound(String),
    #[error("expected a s3://bucket/object-key URL without credentials, port, query, or fragment")]
    InvalidObjectUrl,
    #[error("presign lifetime must be a whole number of seconds from 1 to 604800")]
    InvalidExpiry,
    #[error("credentials have expired or have less than one signing second remaining")]
    ExpiredCredentials,
    #[error("could not build signing parameters: {0}")]
    SigningParameters(#[from] v4::signing_params::BuildError),
    #[error("could not sign S3 request: {0}")]
    Signing(#[from] aws_sigv4::http_request::SigningError),
    #[error("could not build S3 request: {0}")]
    Http(#[from] http::Error),
    #[error("invalid signing timestamp: {0}")]
    Timestamp(#[from] jiff::Error),
}

/// Adds S3 signing to an existing metadata filesystem, preserving its GATs.
pub struct S3<S> {
    filesystem: S,
    config: Config,
}

impl<S> S3<S> {
    pub fn new(filesystem: S, mut config: Config) -> Result<Self, ConfigError> {
        config.validate()?;
        Ok(Self { filesystem, config })
    }

    pub fn get_ref(&self) -> &S {
        &self.filesystem
    }

    /// Access the underlying filesystem, for example to manage record lifecycle.
    pub fn get_mut(&mut self) -> &mut S {
        &mut self.filesystem
    }

    pub fn into_inner(self) -> S {
        self.filesystem
    }
}

impl<S: CatalogSystem> S3<S> {
    /// Sign already-loaded object data without a second filesystem lookup.
    pub fn presign_catalog(
        &self,
        catalog: &(impl Catalog + ?Sized),
        operation: PresignOperation,
        expires_in: Duration,
    ) -> Result<PresignedUrl, Error<S::Err>> {
        let uri = object_uri(&self.config.endpoint, catalog.url())?;
        sign_request(&self.config, &uri, operation, expires_in, SystemTime::now())
    }
}

impl<S: CatalogSystem> CatalogSystem for S3<S> {
    type Err = Error<S::Err>;
    type CatalogId<'a>
        = S::CatalogId<'a>
    where
        Self: 'a;
    type CatalogData<'a>
        = S::CatalogData<'a>
    where
        Self: 'a;

    type CatalogOptionalData<'a>
        = S::CatalogOptionalData<'a>
    where
        Self: 'a;

    async fn upsert_catalog<'a, C: Into<Self::CatalogOptionalData<'a>>>(
        &'a mut self,
        data: C,
    ) -> Result<Self::CatalogId<'a>, Self::Err>
    where
        Self: 'a,
    {
        self.filesystem
            .upsert_catalog(data)
            .await
            .map_err(Error::FileSystem)
    }
    async fn update_catalog<'a, C: Into<Self::CatalogOptionalData<'a>>>(
        &'a mut self,
        id: Self::CatalogId<'a>,
        data: C,
    ) -> Result<(), Self::Err>
    where
        Self: 'a,
    {
        self.filesystem
            .update_catalog(id, data)
            .await
            .map_err(Error::FileSystem)
    }

    async fn get_catalog<'a>(
        &'a mut self,
        id: Self::CatalogId<'a>,
    ) -> Result<Option<Self::CatalogData<'a>>, Self::Err>
    where
        Self: 'a,
    {
        self.filesystem
            .get_catalog(id)
            .await
            .map_err(Error::FileSystem)
    }
}

impl<S: FileSystem> FileSystem for S3<S> {
    type FileId<'a>
        = S::FileId<'a>
    where
        Self: 'a;
    type FileData<'a>
        = S::FileData<'a>
    where
        Self: 'a;

    type FileOptionalData<'a>
        = S::FileOptionalData<'a>
    where
        Self: 'a;

    async fn upsert_file<'a, F: Into<Self::FileOptionalData<'a>>>(
        &'a mut self,
        data: F,
    ) -> Result<Self::FileId<'a>, Self::Err>
    where
        Self: 'a,
    {
        self.filesystem
            .upsert_file(data)
            .await
            .map_err(Error::FileSystem)
    }
    async fn update_file<'a, F: Into<Self::FileOptionalData<'a>>>(
        &'a mut self,
        id: Self::FileId<'a>,
        data: F,
    ) -> Result<(), Self::Err>
    where
        Self: 'a,
    {
        self.filesystem
            .update_file(id, data)
            .await
            .map_err(Error::FileSystem)
    }

    async fn get_file<'a>(
        &'a mut self,
        key: FileKey<'_, Self::FileId<'a>>,
    ) -> Result<Option<FileWithCatalog<Self::FileData<'a>, Self::CatalogData<'a>>>, Self::Err>
    where
        Self: 'a,
    {
        self.filesystem
            .get_file(key)
            .await
            .map_err(Error::FileSystem)
    }
}

impl<S: FileSystem> ObjectStorage for S3<S> {
    /// Sign GET or PUT for a catalog. Lifetimes must be whole seconds
    /// in `1..=604800`; temporary credentials can shorten the effective lifetime.
    async fn presign<'a>(
        &'a mut self,
        catalog_id: Self::CatalogId<'a>,
        operation: PresignOperation,
        expires_in: Duration,
    ) -> Result<PresignedUrl, Self::Err>
    where
        Self: 'a,
    {
        let missing_id = catalog_id.to_string();
        let catalog = self
            .filesystem
            .get_catalog(catalog_id)
            .await
            .map_err(Error::FileSystem)?
            .ok_or(Error::CatalogNotFound(missing_id))?;
        let uri = object_uri(&self.config.endpoint, catalog.url())?;
        sign_request(&self.config, &uri, operation, expires_in, SystemTime::now())
    }
}

fn object_uri<E>(endpoint: &str, location: &str) -> Result<String, Error<E>> {
    // Do not parse object keys with Url: it removes dot segments, which changes
    // S3 object identity. Decode once, then apply S3's encoding without normalizing.
    let (scheme, object) = location.split_once("://").ok_or(Error::InvalidObjectUrl)?;
    let (bucket, key) = object.split_once('/').ok_or(Error::InvalidObjectUrl)?;
    if !scheme.eq_ignore_ascii_case("s3")
        || bucket.is_empty()
        || !bucket
            .bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-' || c == b'.')
        || !bucket.as_bytes()[0].is_ascii_alphanumeric()
        || !bucket.as_bytes()[bucket.len() - 1].is_ascii_alphanumeric()
        || bucket.contains("..")
        || key.is_empty()
        || key.contains(['?', '#'])
        || key.chars().any(char::is_control)
    {
        return Err(Error::InvalidObjectUrl);
    }
    for (index, byte) in key.bytes().enumerate() {
        if byte == b'%'
            && !key
                .as_bytes()
                .get(index + 1..index + 3)
                .is_some_and(|hex| hex.iter().all(u8::is_ascii_hexdigit))
        {
            return Err(Error::InvalidObjectUrl);
        }
    }
    let decoded = percent_decode_str(key)
        .decode_utf8()
        .map_err(|_| Error::InvalidObjectUrl)?;
    Ok(format!(
        "{endpoint}/{bucket}/{}",
        percent_encode(decoded.as_bytes(), KEY_ENCODING)
    ))
}

fn sign_request<E>(
    config: &Config,
    uri: &str,
    operation: PresignOperation,
    expires_in: Duration,
    now: SystemTime,
) -> Result<PresignedUrl, Error<E>> {
    if !(1..=MAX_EXPIRES_SECONDS).contains(&expires_in.as_secs()) || expires_in.subsec_nanos() != 0
    {
        return Err(Error::InvalidExpiry);
    }
    // SigV4 encodes time at second precision. Use the same rounded instant for
    // credential limits and expires_at, so the returned expiry is exact.
    let signed_at = Timestamp::from_second(Timestamp::try_from(now)?.as_second())?;
    let signing_time = SystemTime::from(signed_at);
    let mut seconds = expires_in.as_secs();
    if let Some(expiry) = config.credentials.expiry() {
        let remaining = expiry
            .duration_since(signing_time)
            .map_err(|_| Error::ExpiredCredentials)?;
        seconds = seconds.min(remaining.as_secs());
        if seconds == 0 {
            return Err(Error::ExpiredCredentials);
        }
    }
    let expires_at = signed_at.checked_add(Duration::from_secs(seconds))?;
    let mut settings = SigningSettings::default();
    settings.signature_location = SignatureLocation::QueryParams;
    settings.expires_in = Some(Duration::from_secs(seconds));
    settings.percent_encoding_mode = PercentEncodingMode::Single;
    settings.uri_path_normalization_mode = UriPathNormalizationMode::Disabled;
    let identity = config.credentials.clone().into();
    let params = v4::SigningParams::builder()
        .identity(&identity)
        .region(&config.region)
        .name("s3")
        .time(signing_time)
        .settings(settings)
        .build()?
        .into();
    let signable = SignableRequest::new(
        operation.method(),
        uri,
        std::iter::empty(),
        SignableBody::UnsignedPayload,
    )?;
    let (instructions, _) = sign(signable, &params)?.into_parts();
    let headers = instructions
        .headers()
        .map(|(name, value)| (name.to_owned(), value.to_owned()))
        .collect();
    let mut request = http::Request::builder()
        .method(operation.method())
        .uri(uri)
        .body(())?;
    instructions.apply_to_request_http1x(&mut request);
    Ok(PresignedUrl {
        url: request.uri().to_string(),
        operation,
        headers,
        expires_at,
    })
}
