use super::{ASSET_DIR, JobTransfer, artifact, safe_existing, state_dir};
use crate::{errors::Error, read_json, write_json};
use catalog::PresignOperation;
use graph::{
    types::{AssetRef, Node},
    validator::validate_assets,
};
use scheduler::Job;
use std::{io, path::Path};
use tokio::fs;

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
        let cached: Vec<AssetRef> = match read_json(&checkpoint).await {
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
        if cached == node.assets && assets_present(&root, &cached).await {
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

        for asset in &node.assets {
            let destination = root.join(&asset.file_name);
            let mut can_refresh = true;
            loop {
                let grant = self
                    .client
                    .download_asset(job.uuid, self.agent_id, &asset.file_name)
                    .await?;
                if grant.asset_id != asset.asset_id
                    || grant.transfer.presigned.operation != PresignOperation::Download
                    || !grant.transfer.catalog.uploaded
                    || grant.sha256.len() != 64
                    || !grant.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
                {
                    return Err(artifact(format!(
                        "invalid download grant for asset {}",
                        asset.file_name
                    )));
                }
                match self
                    .download_file_checked(&grant.transfer, &destination, Some(&grant.sha256))
                    .await
                {
                    Err(Error::Http(error))
                        if can_refresh
                            && error.status() == Some(reqwest::StatusCode::FORBIDDEN) =>
                    {
                        can_refresh = false;
                    }
                    result => {
                        result?;
                        break;
                    }
                }
            }
        }
        write_json(&checkpoint, &node.assets).await?;
        Ok(())
    }
}

async fn assets_present(root: &Path, assets: &[AssetRef]) -> bool {
    for asset in assets {
        let Ok(path) = safe_existing(root, &asset.file_name).await else {
            return false;
        };
        if !fs::symlink_metadata(path)
            .await
            .is_ok_and(|metadata| metadata.is_file())
        {
            return false;
        }
    }
    true
}
