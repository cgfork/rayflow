use super::{ASSET_DIR, JobTransfer, artifact, check_digest, safe_existing, state_dir};
use crate::{
    client::{AssetTransfer, Transfer},
    errors::Error,
    read_json, write_json,
};
use catalog::{PresignOperation, PresignedUrl};
use graph::{
    types::{AssetRef, Node},
    validator::validate_assets,
};
use scheduler::Job;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{io, path::Path};
use tokio::{fs, io::AsyncReadExt};
use uuid::Uuid;

pub(super) struct AssetDownload<'a> {
    job_id: Uuid,
    asset: &'a AssetRef,
    pub(super) sha256: &'a str,
}

#[derive(Serialize, Deserialize)]
struct CachedAsset {
    #[serde(flatten)]
    asset: AssetRef,
    size: u64,
    sha256: String,
}

impl JobTransfer {
    pub async fn prepare_assets(
        &self,
        job: &Job,
        data_dir: &Path,
        node: &Node,
    ) -> Result<(), Error> {
        if node.assets.is_empty() {
            return Ok(());
        }
        validate_assets(node).map_err(|error| artifact(error.to_string()))?;

        let root = data_dir.join(ASSET_DIR);
        let checkpoint = state_dir(data_dir, job.uuid).join("assets-ready.json");
        let cached: Vec<CachedAsset> = match read_json(&checkpoint).await {
            Ok(cached) => cached,
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::InvalidData
                ) =>
            {
                Vec::new()
            }
            Err(error) => return Err(error),
        };
        if cached.iter().map(|entry| &entry.asset).eq(&node.assets)
            && assets_match(&root, &cached).await
        {
            return Ok(());
        }

        // Clear prior readiness before rebuilding the directory. A crash during
        // a new transfer must never leave an old checkpoint covering partial files.
        match fs::remove_file(&checkpoint).await {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        match fs::symlink_metadata(&root).await {
            Ok(metadata) if metadata.is_dir() => fs::remove_dir_all(&root).await?,
            Ok(_) => return Err(artifact("asset root is not a directory")),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        fs::create_dir(&root).await?;

        let mut verified = Vec::with_capacity(node.assets.len());
        for asset in &node.assets {
            let destination = root.join(&asset.file_name);
            let grant = self.asset_grant(job.uuid, asset).await?;
            let download = AssetDownload {
                job_id: job.uuid,
                asset,
                sha256: &grant.sha256,
            };
            self.download_file_checked(&grant.transfer, &destination, Some(&download))
                .await?;
            verified.push(CachedAsset {
                asset: asset.clone(),
                size: grant.transfer.catalog.size,
                sha256: grant.sha256,
            });
        }
        write_json(&checkpoint, &verified).await?;
        Ok(())
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

fn is_sha256(digest: &str) -> bool {
    digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
}

async fn assets_match(root: &Path, assets: &[CachedAsset]) -> bool {
    let mut buffer = vec![0; 64 * 1024];
    for asset in assets {
        let Ok(path) = safe_existing(root, &asset.asset.file_name).await else {
            return false;
        };
        if !fs::symlink_metadata(&path)
            .await
            .is_ok_and(|metadata| metadata.is_file() && metadata.len() == asset.size)
            || !is_sha256(&asset.sha256)
        {
            return false;
        }
        let Ok(file) = fs::File::open(path).await else {
            return false;
        };
        let mut file = file.take(asset.size.saturating_add(1));
        let mut digest = Sha256::new();
        let mut size = 0;
        loop {
            let Ok(read) = file.read(&mut buffer).await else {
                return false;
            };
            if read == 0 {
                break;
            }
            size += read as u64;
            digest.update(&buffer[..read]);
        }
        if size != asset.size || check_digest(Some(digest), Some(&asset.sha256)).is_err() {
            return false;
        }
    }
    true
}
