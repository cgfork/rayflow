//! Prepare, commit, and download artifacts for a claimed job.

use crate::app::{App, AppError};
use axum::{Json, Router, extract::State, routing::post};
use catalog::{
    PresignedUrl,
    model::{Catalog, File},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Prepare an upload. The server creates or reuses the catalog by key.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrepareUploadRequest {
    pub job_id: Uuid,
    pub agent_id: Uuid,
    /// Complete object key relative to the configured storage prefix.
    pub key: String,
    pub size: u64,
    pub format: String,
}

/// Commit an upload with the catalog UUID returned by prepare.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommitUploadRequest {
    pub job_id: Uuid,
    pub agent_id: Uuid,
    pub key: String,
    pub catalog_uuid: Uuid,
    pub size: u64,
    pub format: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DownloadRequest {
    pub job_id: Uuid,
    pub agent_id: Uuid,
    pub key: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Transfer {
    pub catalog: Catalog,
    pub presigned: PresignedUrl,
    /// The configured provider supports S3 multipart uploads and range downloads.
    #[serde(default)]
    pub multipart: bool,
}

/// Identity and claim shared by the four multipart endpoints.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MultipartRequest {
    pub job_id: Uuid,
    pub agent_id: Uuid,
    pub key: String,
    pub catalog_uuid: Uuid,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MultipartUploadRequest {
    #[serde(flatten)]
    pub context: MultipartRequest,
    pub upload_id: String,
    pub part_number: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MultipartSessionRequest {
    #[serde(flatten)]
    pub context: MultipartRequest,
    pub upload_id: String,
}

pub(super) fn routes<S>() -> Router<App<S>>
where
    S: Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/catalog/prepare", post(prepare::<S>))
        .route("/catalog/commit", post(commit::<S>))
        .route("/catalog/download", post(download::<S>))
        .route("/catalog/multipart/create", post(multipart_create::<S>))
        .route("/catalog/multipart/upload", post(multipart_upload::<S>))
        .route("/catalog/multipart/complete", post(multipart_complete::<S>))
        .route("/catalog/multipart/abort", post(multipart_abort::<S>))
}

async fn multipart_create<S>(
    State(app): State<App<S>>,
    Json(body): Json<MultipartRequest>,
) -> Result<Json<catalog::PresignedUrl>, AppError> {
    app.multipart_artifact(body, catalog::PresignOperation::CreateMultipart)
        .await
        .map(Json)
}

async fn multipart_upload<S>(
    State(app): State<App<S>>,
    Json(body): Json<MultipartUploadRequest>,
) -> Result<Json<catalog::PresignedUrl>, AppError> {
    let operation = catalog::PresignOperation::UploadPart {
        upload_id: body.upload_id,
        part_number: body.part_number,
    };
    app.multipart_artifact(body.context, operation)
        .await
        .map(Json)
}

async fn multipart_complete<S>(
    State(app): State<App<S>>,
    Json(body): Json<MultipartSessionRequest>,
) -> Result<Json<catalog::PresignedUrl>, AppError> {
    let operation = catalog::PresignOperation::CompleteMultipart {
        upload_id: body.upload_id,
    };
    app.multipart_artifact(body.context, operation)
        .await
        .map(Json)
}

async fn multipart_abort<S>(
    State(app): State<App<S>>,
    Json(body): Json<MultipartSessionRequest>,
) -> Result<Json<catalog::PresignedUrl>, AppError> {
    let operation = catalog::PresignOperation::AbortMultipart {
        upload_id: body.upload_id,
    };
    app.multipart_artifact(body.context, operation)
        .await
        .map(Json)
}

async fn prepare<S>(
    State(app): State<App<S>>,
    Json(body): Json<PrepareUploadRequest>,
) -> Result<Json<Transfer>, AppError> {
    app.prepare_artifact_upload(body).await.map(Json)
}

async fn commit<S>(
    State(app): State<App<S>>,
    Json(body): Json<CommitUploadRequest>,
) -> Result<Json<File>, AppError> {
    app.commit_artifact_upload(body).await.map(Json)
}

async fn download<S>(
    State(app): State<App<S>>,
    Json(body): Json<DownloadRequest>,
) -> Result<Json<Transfer>, AppError> {
    app.download_artifact(body).await.map(Json)
}
