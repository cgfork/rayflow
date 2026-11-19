use axum::{
    Json, Router,
    extract::{Query, State},
    routing::get,
};
use serde::Deserialize;

use crate::app::{AgentView, App, AppError, Page, PageQuery};

#[derive(Debug, Deserialize)]
pub struct ListAgentsQuery {
    pub online: Option<bool>,
}

pub(super) fn routes<S>() -> Router<App<S>>
where
    S: Clone + Send + Sync + 'static,
{
    Router::new().route("/agents", get(list_agents::<S>))
}

async fn list_agents<S>(
    State(app): State<App<S>>,
    Query(page): Query<PageQuery>,
    Query(query): Query<ListAgentsQuery>,
) -> Result<Json<Page<AgentView>>, AppError> {
    app.list_agents(page, query.online).await.map(Json)
}
