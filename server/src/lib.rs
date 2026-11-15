//! Rayflow HTTP API.
//!
//! Exposes the runtime over HTTP: [`router`] merges the service routes with the
//! REST API in [`api`], which the server binary serves.

#![feature(return_type_notation)]

pub mod api;
pub mod app;
pub mod auth;
pub mod config;
pub mod graph;
pub mod migration;
pub mod storage;
#[cfg(feature = "embed-ui")]
mod ui;

pub use app::App;
pub use config::Config;
pub use graph::{Graphs, StaticGraphStore};

use ::graph::GraphStore;
use ::graph::types::DAG;
use axum::Router;
use axum::routing::get;
use toasty::schema::Model;
use uuid::Uuid;

/// Registers catalog, graph, runtime, scheduling, and authentication models.
pub fn models() -> toasty::ModelSet {
    let mut models = scheduler::models();
    models.add(app::AgentInstance::schema());
    models.add(auth::Session::schema());
    models.add(auth::LoginFlow::schema());
    models.add(auth::UserRecord::schema());
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
    let api = api::router(state);
    let router = service_routes().merge(versioned_api(api));
    #[cfg(feature = "embed-ui")]
    let router = ui::attach(router);
    router
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
fn service_routes() -> Router {
    let router = Router::new().route("/health", get(health));
    #[cfg(not(feature = "embed-ui"))]
    let router = router.route("/", get(index));
    router
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
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    use super::*;

    #[test]
    fn models_include_catalog_graph_runtime_and_scheduler() {
        let models = models();
        for model in scheduler::models()
            .into_iter()
            .chain(::graph::models())
            .chain(catalog::models())
        {
            assert!(models.contains(model.id()));
        }
    }

    #[tokio::test]
    async fn health_returns_ok() {
        let response = service_routes()
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn unknown_route_returns_not_found() {
        let response = service_routes()
            .oneshot(
                Request::builder()
                    .uri("/missing")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn browser_and_agent_routes_require_the_versioned_prefix() {
        let api = Router::new()
            .route("/auth/providers", get(|| async { "providers" }))
            .route("/jobs/poll", axum::routing::post(|| async { "jobs" }));
        let router = service_routes().merge(versioned_api(api));
        for (method, path, expected) in [
            ("GET", "/api/v1/auth/providers", StatusCode::OK),
            ("POST", "/api/v1/jobs/poll", StatusCode::OK),
            ("GET", "/api/v1/health", StatusCode::OK),
            ("GET", "/health", StatusCode::OK),
            ("GET", "/auth/providers", StatusCode::NOT_FOUND),
            ("POST", "/jobs/poll", StatusCode::NOT_FOUND),
            ("GET", "/api/auth/providers", StatusCode::NOT_FOUND),
            ("POST", "/api/jobs/poll", StatusCode::NOT_FOUND),
        ] {
            let response = router
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(path)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), expected, "{method} {path}");
        }
    }
}
