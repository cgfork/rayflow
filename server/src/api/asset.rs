//! Library asset publication and job-scoped asset downloads.

use ::graph::{GraphStore, types::DAG};
use axum::{
    Extension, Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
};
use catalog::{PresignedUrl, model::Catalog};
use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    app::{App, AppError, Page, PageQuery},
    auth::User,
};

pub(super) fn routes<S>() -> Router<App<S>>
where
    S: GraphStore<Uuid, G = DAG> + Clone + Send + Sync + 'static,
    S::load(..): Send,
    S::Err: Send + Sync + 'static,
{
    Router::new()
        .route("/assets", get(list::<S>))
        .route("/assets/prepare", post(prepare::<S>))
        .route("/assets/{asset_id}", get(read::<S>))
        .route("/assets/{asset_id}/commit", post(commit::<S>))
        .route("/assets/{asset_id}/download", post(download::<S>))
        .route(
            "/assets/{name}/versions/{version}/cancel",
            post(cancel::<S>),
        )
        .route(
            "/assets/{name}/versions/{version}/import",
            post(import::<S>),
        )
        .route("/job/asset/presign", post(presign_job_asset::<S>))
}

#[derive(Debug, Serialize)]
pub struct AssetDescriptor {
    pub asset_id: Uuid,
    pub name: String,
    pub version: u64,
    pub size: u64,
    pub format: String,
    pub sha256: String,
    pub created_by: String,
    pub created_at: Timestamp,
    pub published_at: Timestamp,
}

#[derive(Debug, Deserialize)]
pub struct PrepareAssetRequest {
    pub name: String,
    pub version: u64,
    pub size: u64,
    pub format: String,
    pub sha256: String,
}

#[derive(Debug, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum PrepareAssetResponse {
    Pending {
        asset_id: Uuid,
        upload_id: Uuid,
        upload: PresignedUrl,
    },
    Published {
        #[serde(flatten)]
        asset: AssetDescriptor,
    },
}

#[derive(Debug, Deserialize)]
pub struct CommitAssetRequest {
    pub upload_id: Uuid,
}

#[derive(Debug, Deserialize)]
pub struct ImportAssetRequest {
    pub source_key: String,
    pub format: String,
    pub sha256: String,
}

#[derive(Debug, Deserialize)]
pub struct PresignJobAssetRequest {
    pub job_id: Uuid,
    pub agent_id: Uuid,
    pub filename: String,
}

#[derive(Debug, Serialize)]
pub struct AssetTransfer {
    pub asset_id: Uuid,
    pub sha256: String,
    pub catalog: Catalog,
    pub presigned: PresignedUrl,
    pub multipart: bool,
}

#[derive(Debug, Deserialize)]
struct AssetQuery {
    name: Option<String>,
}

async fn list<S>(
    State(app): State<App<S>>,
    Query(page): Query<PageQuery>,
    Query(query): Query<AssetQuery>,
) -> Result<Json<Page<AssetDescriptor>>, AppError> {
    app.list_assets(page, query.name).await.map(Json)
}

async fn read<S>(
    State(app): State<App<S>>,
    Path(asset_id): Path<Uuid>,
) -> Result<Json<AssetDescriptor>, AppError> {
    app.read_asset(asset_id).await.map(Json)
}

async fn prepare<S>(
    State(app): State<App<S>>,
    user: Option<Extension<User>>,
    Json(request): Json<PrepareAssetRequest>,
) -> Result<Json<PrepareAssetResponse>, AppError> {
    app.prepare_asset(request, actor(&user)).await.map(Json)
}

async fn commit<S>(
    State(app): State<App<S>>,
    Path(asset_id): Path<Uuid>,
    user: Option<Extension<User>>,
    Json(request): Json<CommitAssetRequest>,
) -> Result<Json<AssetDescriptor>, AppError> {
    app.commit_asset(asset_id, request.upload_id, actor(&user))
        .await
        .map(Json)
}

async fn cancel<S>(
    State(app): State<App<S>>,
    Path((name, version)): Path<(String, u64)>,
    user: Option<Extension<User>>,
) -> Result<StatusCode, AppError> {
    app.cancel_asset(&name, version, actor(&user)).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn download<S>(
    State(app): State<App<S>>,
    Path(asset_id): Path<Uuid>,
) -> Result<Json<AssetTransfer>, AppError> {
    app.download_asset(asset_id).await.map(Json)
}

async fn import<S>(
    State(app): State<App<S>>,
    Path((name, version)): Path<(String, u64)>,
    user: Option<Extension<User>>,
    Json(request): Json<ImportAssetRequest>,
) -> Result<Json<AssetDescriptor>, AppError> {
    app.import_asset(name, version, request, actor(&user))
        .await
        .map(Json)
}

async fn presign_job_asset<S>(
    State(app): State<App<S>>,
    Json(request): Json<PresignJobAssetRequest>,
) -> Result<Json<AssetTransfer>, AppError>
where
    S: GraphStore<Uuid, G = DAG> + Clone,
    S::load(..): Send,
    S::Err: Send + Sync + 'static,
{
    app.presign_job_asset(request).await.map(Json)
}

fn actor(user: &Option<Extension<User>>) -> &str {
    user.as_ref().map_or("local", |user| user.id.as_str())
}
