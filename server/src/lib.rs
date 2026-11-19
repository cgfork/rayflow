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

use ::graph::GraphStore;
use ::graph::types::DAG;
use axum::Router;
use axum::routing::get;
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
