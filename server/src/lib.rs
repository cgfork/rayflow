//! Rayflow HTTP API.
//!
//! Exposes the runtime over HTTP: [`router`] merges the service routes with the
//! REST API in [`api`], which the server binary serves.

#![feature(return_type_notation)]

pub mod api;
pub mod app;
pub mod auth;
pub mod config;
pub mod migration;
#[cfg(feature = "embed-ui")]
mod ui;

pub use app::App;
pub use config::Config;
pub use metrics::Metrics as PrometheusMetrics;

use ::graph::GraphStore;
use ::graph::types::DAG;
use axum::extract::{MatchedPath, State};
use axum::http::Request;
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::get;
use axum::{Extension, Router, response::IntoResponse};
use metrics::{HttpMetrics, Metrics, PROMETHEUS_CONTENT_TYPE};
use std::sync::Arc;
use toasty::schema::Model;
use uuid::Uuid;

/// Registers catalog, graph, runtime, scheduling, and authentication models.
pub fn models() -> toasty::ModelSet {
    let mut models = scheduler::models();
    for model in scheduler::agent_registry::models() {
        models.add(model);
    }
    models.add(auth::Session::schema());
    models.add(auth::LoginFlow::schema());
    models.add(auth::UserRecord::schema());
    models.add(app::job_artifact::JobArtifact::schema());
    for model in ::graph::models() {
        models.add(model);
    }
    for model in catalog::models() {
        models.add(model);
    }
    models
}

/// Builds the application router: service routes plus the REST API.
pub fn router<S>(state: App<S>) -> Router
where
    S: GraphStore<Uuid, G = DAG> + Clone + Send + Sync + 'static,
    S::load(..): Send,
    S::graphs(..): Send,
    <S as GraphStore<Uuid>>::Err: Send + Sync + 'static,
{
    router_with_metrics(state, Arc::new(Metrics::with_build_info()))
}

/// Builds the application router with a caller-owned metrics registry.
pub fn router_with_metrics<S>(state: App<S>, metrics: Arc<Metrics>) -> Router
where
    S: GraphStore<Uuid, G = DAG> + Clone + Send + Sync + 'static,
    S::load(..): Send,
    S::graphs(..): Send,
    <S as GraphStore<Uuid>>::Err: Send + Sync + 'static,
{
    let http_metrics = Arc::new(metrics.register_http_metrics());
    let api = api::router(state);
    let router =
        service_routes(metrics)
            .merge(versioned_api(api))
            .layer(middleware::from_fn_with_state(
                http_metrics,
                observe_request,
            ));
    #[cfg(feature = "embed-ui")]
    let router = ui::attach(router);
    router
}

async fn observe_request(
    State(metrics): State<Arc<HttpMetrics>>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let method = request.method().to_string();
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(|path| path.as_str().to_owned())
        .unwrap_or_else(|| "unmatched".into());
    let started = std::time::Instant::now();
    let response = next.run(request).await;
    metrics.observe(
        method,
        route,
        response.status().as_u16(),
        started.elapsed().as_secs_f64(),
    );
    response
}

/// Mounts both browser and agent APIs under the same versioned prefix.
fn versioned_api(api: Router) -> Router {
    Router::new().nest(
        "/api/v1",
        api.route("/health", get(health))
            .fallback(|| async { axum::http::StatusCode::NOT_FOUND }),
    )
}

/// Builds the routes that do not need backend state.
fn service_routes(metrics: Arc<Metrics>) -> Router {
    let router = Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics_handler))
        .layer(Extension(metrics));
    #[cfg(not(feature = "embed-ui"))]
    let router = router.route("/", get(index));
    router
}

async fn metrics_handler(Extension(metrics): Extension<Arc<Metrics>>) -> impl IntoResponse {
    match metrics.encode() {
        Ok(output) => (
            [(axum::http::header::CONTENT_TYPE, PROMETHEUS_CONTENT_TYPE)],
            output,
        )
            .into_response(),
        Err(error) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to encode metrics: {error}"),
        )
            .into_response(),
    }
}

/// Reports the service name at the root.
#[cfg(not(feature = "embed-ui"))]
async fn index() -> &'static str {
    "rayflow"
}

/// Liveness probe.
async fn health() -> &'static str {
    "ok"
}

#[cfg(test)]
mod tests {
    use super::{Metrics, PROMETHEUS_CONTENT_TYPE, health, observe_request, service_routes};
    use axum::{Router, body::Body, http::Request, middleware, routing::get};
    use std::sync::Arc;
    use tower::ServiceExt;

    #[tokio::test]
    async fn metrics_endpoint_exposes_prometheus_text() {
        let app = service_routes(Arc::new(Metrics::with_build_info()));
        let response = app
            .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        assert_eq!(
            response.headers()[axum::http::header::CONTENT_TYPE],
            PROMETHEUS_CONTENT_TYPE
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert!(
            String::from_utf8(body.to_vec())
                .unwrap()
                .contains("rayflow_build_info 1")
        );
    }

    #[tokio::test]
    async fn request_duration_metrics_use_route_templates() {
        let metrics = Arc::new(Metrics::new());
        let http = Arc::new(metrics.register_http_metrics());
        let app = Router::new()
            .route("/health", get(health))
            .layer(middleware::from_fn_with_state(http, observe_request));
        let response = app
            .oneshot(Request::get("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let output = metrics.encode().unwrap();
        assert!(output.contains("rayflow_http_requests_total"));
        assert!(output.contains("rayflow_http_request_duration_seconds_count"));
        assert!(output.contains("route=\"/health\""));
    }
}
