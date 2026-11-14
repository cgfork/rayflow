use std::{collections::BTreeMap, convert::Infallible};

use super::*;

fn config() -> Config {
    Config::new(
        "https://objects.example.com:9000",
        Credentials::new(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            None,
            None,
            "test",
        ),
    )
}

fn now() -> SystemTime {
    SystemTime::from("2013-05-24T00:00:00Z".parse::<Timestamp>().unwrap())
}

fn query(result: &PresignedUrl) -> BTreeMap<String, String> {
    url::Url::parse(&result.url)
        .unwrap()
        .query_pairs()
        .into_owned()
        .collect()
}

fn presign(config: &Config, operation: PresignOperation, seconds: u64) -> PresignedUrl {
    let uri = object_uri::<Infallible>(&config.endpoint, "s3://bucket/test.txt").unwrap();
    sign_request::<Infallible>(config, &uri, operation, Duration::from_secs(seconds), now())
        .unwrap()
}

#[test]
fn matches_aws_published_s3_signature() {
    // Independent reference vector, including UNSIGNED-PAYLOAD and S3 settings:
    // https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-query-string-auth.html
    let signed = sign_request::<Infallible>(
        &config(),
        "https://examplebucket.s3.amazonaws.com/test.txt",
        PresignOperation::Download,
        Duration::from_secs(86400),
        now(),
    )
    .unwrap();
    assert_eq!(
        query(&signed)["X-Amz-Signature"],
        "aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
    );
    assert_eq!(
        signed.expires_at,
        "2013-05-25T00:00:00Z".parse::<Timestamp>().unwrap()
    );
    assert!(signed.headers.is_empty());
}

#[test]
fn signs_both_methods_with_configured_region_and_port() {
    let mut config = config();
    config.region = "eu-west-1".into();
    let download = presign(&config, PresignOperation::Download, 60);
    let upload = presign(&config, PresignOperation::Upload, 60);
    assert_eq!(download.operation.method(), "GET");
    assert_eq!(upload.operation.method(), "PUT");
    assert!(
        upload
            .url
            .starts_with("https://objects.example.com:9000/bucket/test.txt?")
    );
    assert_eq!(
        query(&upload)["X-Amz-Credential"],
        "AKIAIOSFODNN7EXAMPLE/20130524/eu-west-1/s3/aws4_request"
    );
    assert_eq!(query(&upload)["X-Amz-SignedHeaders"], "host");
    assert_ne!(
        query(&upload)["X-Amz-Signature"],
        query(&download)["X-Amz-Signature"]
    );
    config.endpoint = "https://objects.example.com".into();
    assert_ne!(
        query(&presign(&config, PresignOperation::Upload, 60))["X-Amz-Signature"],
        query(&upload)["X-Amz-Signature"]
    );
}

#[test]
fn encodes_keys_once_and_preserves_s3_paths() {
    for (key, path) in [
        ("a b/+雪%25%3F%23%2fend", "a%20b/%2B%E9%9B%AA%25%3F%23/end"),
        ("a%20b/%252F", "a%20b/%252F"),
        ("//a/./b/../c/", "//a/./b/../c/"),
        ("a/%2E%2e/b", "a/../b"),
        ("-._~", "-._~"),
    ] {
        let uri =
            object_uri::<Infallible>(&config().endpoint, &format!("s3://bucket/{key}")).unwrap();
        let signed = sign_request::<Infallible>(
            &config(),
            &uri,
            PresignOperation::Download,
            Duration::from_secs(60),
            now(),
        )
        .unwrap();
        // Unlike url::Url, http::Uri does not collapse dot segments.
        assert_eq!(
            signed.url.parse::<http::Uri>().unwrap().path(),
            format!("/bucket/{path}")
        );
    }
}

#[test]
fn rejects_unsupported_or_ambiguous_object_locations() {
    for location in [
        "rustfs://bucket/key",
        "oss://bucket/key",
        "file:///tmp/key",
        "https://bucket/key",
        "s3://bucket",
        "s3://bucket/",
        "s3:///key",
        "s3://user:secret@bucket/key",
        "s3://bucket:9000/key",
        "s3://../key",
        "s3://bucket/key?x=y",
        "s3://bucket/key#fragment",
        "s3://bucket/%",
        "s3://bucket/%0g",
        "s3://bucket/%ff",
    ] {
        assert!(
            matches!(
                object_uri::<Infallible>(&config().endpoint, location),
                Err(Error::InvalidObjectUrl)
            ),
            "{location}"
        );
    }
}

#[test]
fn bounds_lifetimes_and_reports_exact_expiry() {
    for duration in [
        Duration::ZERO,
        Duration::from_millis(1500),
        Duration::from_secs(604801),
        Duration::MAX,
    ] {
        assert!(matches!(
            sign_request::<Infallible>(
                &config(),
                "https://example.com/bucket/key",
                PresignOperation::Upload,
                duration,
                now()
            ),
            Err(Error::InvalidExpiry)
        ));
    }
    for seconds in [1, MAX_EXPIRES_SECONDS] {
        assert_eq!(
            query(&presign(&config(), PresignOperation::Download, seconds))["X-Amz-Expires"],
            seconds.to_string()
        );
    }
    let signed = sign_request::<Infallible>(
        &config(),
        "https://example.com/bucket/key",
        PresignOperation::Upload,
        Duration::from_secs(60),
        now() + Duration::from_millis(999),
    )
    .unwrap();
    assert_eq!(
        signed.expires_at,
        "2013-05-24T00:01:00Z".parse::<Timestamp>().unwrap()
    );
}

#[test]
fn temporary_credentials_limit_expiry_and_sign_the_token() {
    let mut config = config();
    config.credentials = Credentials::new(
        "access",
        "secret",
        Some("token+/=".into()),
        Some(now() + Duration::from_millis(30500)),
        "test",
    );
    let signed = presign(&config, PresignOperation::Upload, 60);
    assert_eq!(query(&signed)["X-Amz-Security-Token"], "token+/=");
    assert_eq!(query(&signed)["X-Amz-Expires"], "30");
    assert_eq!(
        signed.expires_at,
        "2013-05-24T00:00:30Z".parse::<Timestamp>().unwrap()
    );
    let short = presign(&config, PresignOperation::Download, 10);
    assert_eq!(query(&short)["X-Amz-Expires"], "10");
    config.credentials = Credentials::new(
        "access",
        "secret",
        Some("different-token".into()),
        config.credentials.expiry(),
        "test",
    );
    assert_ne!(
        query(&signed)["X-Amz-Signature"],
        query(&presign(&config, PresignOperation::Upload, 60))["X-Amz-Signature"]
    );
    for expiry in [
        now() - Duration::from_secs(1),
        now(),
        now() + Duration::from_millis(999),
    ] {
        config.credentials = Credentials::new("access", "secret", None, Some(expiry), "test");
        assert!(matches!(
            sign_request::<Infallible>(
                &config,
                "https://example.com/bucket/key",
                PresignOperation::Download,
                Duration::from_secs(60),
                now()
            ),
            Err(Error::ExpiredCredentials)
        ));
    }
}

#[test]
fn validates_configuration_without_disclosing_secrets() {
    for endpoint in [
        "",
        "file:///tmp",
        "http://user:secret@example.com",
        "http://example.com/prefix",
        "http://example.com/?x=1",
        "http://example.com/#fragment",
    ] {
        let mut config = config();
        config.endpoint = endpoint.into();
        assert!(S3::new((), config).is_err());
    }
    let mut invalid = config();
    invalid.region = "us/east/1".into();
    assert!(S3::new((), invalid).is_err());
    for credentials in [
        Credentials::new("", "secret", None, None, "test"),
        Credentials::new("access", "", None, None, "test"),
        Credentials::new("access", "secret", Some("".into()), None, "test"),
    ] {
        let mut config = config();
        config.credentials = credentials;
        assert!(S3::new((), config).is_err());
    }
    let mut config = config();
    config.credentials = Credentials::new(
        "access",
        "private-secret",
        Some("private-token".into()),
        None,
        "test",
    );
    assert!(!format!("{config:?}").contains("private-"));
    config.endpoint = "http://[::1]:9000/".into();
    let storage = S3::new((), config).unwrap();
    assert_eq!(storage.config.endpoint, "http://[::1]:9000");
}
