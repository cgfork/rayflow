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
