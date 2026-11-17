//! Assets compiled into the server; the UI uses hash-based navigation.

use axum::{
    Router,
    body::Body,
    extract::OriginalUri,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};

#[derive(rust_embed::RustEmbed)]
#[folder = "../ui/dist/"]
struct Assets;

pub(super) fn attach(router: Router) -> Router {
    router.fallback_service(Router::new().fallback(get(asset)))
}

async fn asset(OriginalUri(uri): OriginalUri) -> Response {
    let path = uri.path().trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };
    let Some(file) = Assets::get(path) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let content_type = mime_guess::from_path(path).first_or_octet_stream();
    (
        [
            (header::CONTENT_TYPE, content_type.as_ref()),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        Body::from(file.data.into_owned()),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Request;
    use tower::ServiceExt;

    #[tokio::test]
    async fn serves_embedded_assets_and_rejects_missing_files() {
        let api = Router::new().route("/auth/providers", get(|| async { "providers" }));
        let router = attach(crate::service_routes().merge(crate::versioned_api(api)));
        for (path, expected, mime) in [
            ("/", StatusCode::OK, Some("text/html")),
            ("/favicon.svg", StatusCode::OK, Some("image/svg+xml")),
            ("/assets/missing.js", StatusCode::NOT_FOUND, None),
            ("/api/v1/missing", StatusCode::NOT_FOUND, None),
            ("/api/v1/favicon.svg", StatusCode::NOT_FOUND, None),
            ("/api/v1/health", StatusCode::OK, None),
            ("/health", StatusCode::OK, None),
            ("/api/v1/auth/providers", StatusCode::OK, None),
            ("/auth/providers", StatusCode::NOT_FOUND, None),
        ] {
            let response = router
                .clone()
                .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), expected);
            if let Some(mime) = mime {
                assert_eq!(response.headers()[header::CONTENT_TYPE], mime);
                assert!(
                    !axum::body::to_bytes(response.into_body(), usize::MAX)
                        .await
                        .unwrap()
                        .is_empty()
                );
            }
        }
    }
}
