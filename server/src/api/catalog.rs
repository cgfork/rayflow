//! Prepare, commit, and download artifacts for a claimed job.

use crate::app::{App, AppError};
use axum::{Json, Router, extract::State, routing::post};
use catalog::{
    artifact::{
        CommitUploadRequest, DownloadRequest, MultipartRequest, PrepareUploadRequest, Transfer,
    },
    model::File,
};

pub(super) fn routes<S>() -> Router<App<S>>
where
    S: Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/catalog/prepare", post(prepare::<S>))
        .route("/catalog/commit", post(commit::<S>))
        .route("/catalog/download", post(download::<S>))
        .route("/catalog/multipart", post(multipart::<S>))
}

async fn multipart<S>(
    State(app): State<App<S>>,
    Json(body): Json<MultipartRequest>,
) -> Result<Json<catalog::PresignedUrl>, AppError> {
    app.multipart_artifact(body).await.map(Json)
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
