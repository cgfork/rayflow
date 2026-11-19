use std::collections::HashMap;

use axum::{Json, Router, extract::State, routing::post};
use serde::Deserialize;

use crate::app::{App, AppError};

#[derive(Deserialize)]
struct ResolveUsersRequest {
    ids: Vec<String>,
}

pub(super) fn routes<S>() -> Router<App<S>>
where
    S: Clone + Send + Sync + 'static,
{
    Router::new().route("/users/resolve", post(resolve_users::<S>))
}

async fn resolve_users<S>(
    State(app): State<App<S>>,
    Json(request): Json<ResolveUsersRequest>,
) -> Result<Json<HashMap<String, String>>, AppError> {
    app.resolve_users(request.ids).await.map(Json)
}
