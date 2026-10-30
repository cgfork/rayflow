//! Rayflow HTTP API.
//!
//! Exposes the runtime over HTTP: [`router`] merges the service routes with the
//! REST API in [`api`], which the server binary serves.

pub mod api;
pub mod app;
pub mod config;
pub mod graph;

pub use app::App;
pub use config::Config;
pub use graph::{Graphs, StaticGraphStore};

use ::graph::GraphStore;
use ::graph::types::DAG;
use axum::Router;
use axum::routing::get;
use uuid::Uuid;

/// Registers graph, runtime, and scheduling models together for the API.
pub fn models() -> toasty::ModelSet {
    let mut models = scheduler::models();
    for model in ::graph::models() {
        models.add(model);
    }
    models
}

/// Builds the application router: service routes plus the REST API.
pub fn router<S>(state: App<S>) -> Router
where
    S: GraphStore<Uuid, G = DAG> + Clone + Send + Sync + 'static,
    <S as GraphStore<Uuid>>::Err: Send + Sync + 'static,
{
    service_routes().merge(api::router(state))
}

/// Builds the routes that do not need backend state.
fn service_routes() -> Router {
    Router::new()
        .route("/", get(index))
        .route("/health", get(health))
}

/// Reports the service name at the root.
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
    fn models_include_graph_runtime_and_scheduler() {
        let models = models();
        for model in scheduler::models().into_iter().chain(::graph::models()) {
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
}
