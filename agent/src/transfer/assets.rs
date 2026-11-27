use super::{JobTransfer, artifact, check_digest};
use crate::{
    client::{AssetTransfer, Transfer},
    errors::Error,
};
use catalog::{PresignOperation, PresignedUrl, path::validate_relative_path};
use graph::{
    types::{AssetRef, Node},
    validator::validate_assets,
};
use scheduler::Job;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
};
use uuid::Uuid;

mod cache;

use cache::{AssetCache, is_sha256};

pub(super) struct AssetDownload<'a> {
    job_id: Uuid,
    asset: &'a AssetRef,
    pub(super) sha256: &'a str,
}

impl JobTransfer {
    pub(crate) async fn ensure_job_asset_dir(&self, job_id: Uuid) -> Result<PathBuf, Error> {
        let settings = self.client.settings();
        AssetCache::new(settings)
            .ensure_job_dir(settings.job_asset_dir(job_id))
            .await?;
        settings.canonical_job_asset_dir(job_id).await
    }

    pub async fn prepare_assets(&self, job: &Job, node: &Node) -> Result<(), Error> {
        if node.assets.is_empty() {
            return Ok(());
        }
        validate_assets(node).map_err(|error| artifact(error.to_string()))?;

        let root = self.ensure_job_asset_dir(job.uuid).await?;
        // A pre-start retry replaces the job's aliases and releases stale pins.
        fs::remove_dir_all(&root).await?;
        fs::create_dir(&root).await?;

        let settings = self.client.settings();
        let cache = AssetCache::new(settings);
        cache.initialize().await?;
        for asset in &node.assets {
            let destination = root.join(&asset.file_name);
            let entry = cache.lock_asset(asset.asset_id).await?;
            let (source, publish_to) = match entry.verified_file().await? {
                Some(path) => (path, None),
                None => {
                    let grant = self.asset_grant(job.uuid, asset).await?;
                    let path = entry.staging_path();
                    if let Some(source) = &settings.asset_source_dir {
                        stage_mounted_asset(source, &grant, &path).await?;
                    } else {
                        let download = AssetDownload {
                            job_id: job.uuid,
                            asset,
                            sha256: &grant.sha256,
                        };
                        self.download_file_checked(&grant.transfer, &path, Some(&download))
                            .await?;
                    }
                    (path, Some(entry.verified_path(&grant.sha256)))
                }
            };
            entry.link(source, publish_to, destination).await?;
        }
        if let Err(error) = cache.trim().await {
            tracing::warn!(%error, "failed to trim the asset cache");
        }
        Ok(())
    }

    pub(crate) async fn trim_asset_cache(&self) -> Result<(), Error> {
        AssetCache::new(self.client.settings()).trim().await
    }

    async fn asset_grant(&self, job_id: Uuid, asset: &AssetRef) -> Result<AssetTransfer, Error> {
        let grant = self
            .client
            .download_asset(job_id, self.agent_id, &asset.file_name)
            .await?;
        if grant.asset_id != asset.asset_id
            || grant.transfer.presigned.operation != PresignOperation::Download
            || !grant.transfer.catalog.uploaded
            || !is_sha256(&grant.sha256)
        {
            return Err(artifact(format!(
                "invalid download grant for asset {}",
                asset.file_name
            )));
        }
        Ok(grant)
    }

    pub(super) async fn refresh_asset_download(
        &self,
        download: &AssetDownload<'_>,
        pinned: &Transfer,
    ) -> Result<PresignedUrl, Error> {
        let grant = self.asset_grant(download.job_id, download.asset).await?;
        let object = &grant.transfer.catalog;
        if object.uuid != pinned.catalog.uuid
            || object.key != pinned.catalog.key
            || object.url != pinned.catalog.url
            || object.size != pinned.catalog.size
            || !grant.sha256.eq_ignore_ascii_case(download.sha256)
        {
            return Err(artifact(format!(
                "download grant changed the pinned version for asset {}",
                download.asset.file_name
            )));
        }
        Ok(grant.transfer.presigned)
    }
}

async fn stage_mounted_asset(
    root: &Path,
    grant: &AssetTransfer,
    destination: &Path,
) -> Result<(), Error> {
    let catalog = &grant.transfer.catalog;
    // StorageConfig treats the leading slash as a logical key prefix, not
    // part of the object name inside the mounted bucket.
    let key = catalog.key.strip_prefix('/').unwrap_or(&catalog.key);
    validate_relative_path(key)
        .map_err(|error| artifact(format!("invalid mounted asset key: {error}")))?;

    let mut source: PathBuf = root.components().collect();
    if !fs::symlink_metadata(&source).await?.is_dir() {
        return Err(artifact("mounted asset source must be a directory"));
    }
    let mut parts = key.split('/').peekable();
    while let Some(part) = parts.next() {
        source.push(part);
        let metadata = fs::symlink_metadata(&source)
            .await
            .map_err(|error| artifact(format!("cannot access mounted asset {key}: {error}")))?;
        let valid = if parts.peek().is_some() {
            metadata.is_dir()
        } else {
            metadata.is_file()
        };
        if !valid {
            return Err(artifact(format!("invalid mounted asset path for {key}")));
        }
    }

    let mut source = fs::File::open(source).await?;
    let metadata = source.metadata().await?;
    if !metadata.is_file() || metadata.len() != catalog.size {
        return Err(artifact(
            "mounted asset size does not match the published version",
        ));
    }

    let mut staged = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)
        .await?;
    let result = async {
        let mut digest = Sha256::new();
        let mut remaining = catalog.size;
        let mut buffer = vec![0; 64 * 1024];
        loop {
            let read = source.read(&mut buffer).await?;
            if read == 0 {
                break;
            }
            remaining = remaining
                .checked_sub(read as u64)
                .ok_or_else(|| artifact("mounted asset exceeds the published size"))?;
            digest.update(&buffer[..read]);
            staged.write_all(&buffer[..read]).await?;
        }
        if remaining != 0 {
            return Err(artifact("incomplete mounted asset"));
        }
        check_digest(Some(digest), Some(&grant.sha256))?;
        staged.sync_all().await?;
        Ok(())
    }
    .await;
    drop(staged);
    if result.is_err() {
        let _ = fs::remove_file(destination).await;
    }
    result
}
