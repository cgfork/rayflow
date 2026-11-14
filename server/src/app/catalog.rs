use catalog::{
    CatalogInput, CatalogOptionalData, CatalogSystem, FileInput, FileKey, FileSystem,
    PresignOperation,
    artifact::{
        CommitUploadRequest, DIRECTORY_FORMAT, DIRECTORY_MANIFEST, DownloadRequest,
        MAX_MANIFEST_BYTES, PrepareUploadRequest, Transfer, validate_relative_path,
    },
    model::{Catalog, File},
    s3::S3,
};
use jiff::Timestamp;
use scheduler::{Job, actuator::Status};
use toasty::{Executor, schema::Model};
use uuid::Uuid;

use super::{App, AppError};

impl<S> App<S> {
    /// Creates upload metadata for the key supplied by the agent.
    pub async fn prepare_artifact_upload(
        &self,
        request: PrepareUploadRequest,
    ) -> Result<Transfer, AppError> {
        validate_upload(&request.key, request.size, &request.format)?;
        let mut db = self.db.clone();
        let mut tx = db.transaction().await?;
        let job = lock_claim(&mut tx, request.job_id, request.agent_id).await?;
        let (fullpath, url) = self.artifact_location(&job, &request.key)?;
        if let Some(existing) = tx.get_file(FileKey::Fullpath(&fullpath)).await?
            && existing.catalog.key != fullpath
        {
            return Err(AppError::InvalidRequest(
                "artifact name belongs to another object".into(),
            ));
        }
        // Every prepared PUT needs acknowledgement, even for identical metadata.
        let id = tx
            .upsert_catalog(&CatalogInput::new(
                fullpath,
                url,
                request.size,
                request.format,
            ))
            .await?;
        let catalog = tx
            .get_catalog(id)
            .await?
            .ok_or_else(|| AppError::ArtifactNotFound(id.to_string()))?;
        let transfer = self.transfer(catalog, PresignOperation::Upload, &job)?;
        tx.commit().await?;
        Ok(transfer)
    }

    /// Acknowledges a completed PUT and publishes its logical name atomically.
    pub async fn commit_artifact_upload(
        &self,
        request: CommitUploadRequest,
    ) -> Result<File, AppError> {
        validate_upload(&request.key, request.size, &request.format)?;
        let mut db = self.db.clone();
        let mut tx = db.transaction().await?;
        let job = lock_claim(&mut tx, request.job_id, request.agent_id).await?;
        let (fullpath, url) = self.artifact_location(&job, &request.key)?;
        // Toasty has no locking read. Serialize acknowledgement with another prepare.
        let table = tx.schema().table_for(Catalog::id()).name.replace('`', "``");
        toasty::sql::query(format!(
            "SELECT id FROM `{table}` WHERE uuid = ? FOR UPDATE"
        ))
        .bind(request.catalog_uuid)
        .exec(&mut tx)
        .await?;
        let catalog = tx
            .get_catalog(request.catalog_uuid)
            .await?
            .ok_or_else(|| AppError::ArtifactNotFound(request.catalog_uuid.to_string()))?;
        if catalog.key != fullpath
            || catalog.url != url
            || catalog.size != request.size
            || catalog.format != request.format
        {
            return Err(AppError::InvalidRequest(
                "upload does not match prepared catalog metadata".into(),
            ));
        }
        if let Some(existing) = tx.get_file(FileKey::Fullpath(&fullpath)).await?
            && existing.catalog.uuid != catalog.uuid
        {
            return Err(AppError::InvalidRequest(
                "artifact name already has a catalog UUID".into(),
            ));
        }
        tx.update_catalog(
            catalog.uuid,
            CatalogOptionalData {
                uploaded: Some(true),
                ..Default::default()
            },
        )
        .await?;
        let filename = request.key.rsplit('/').next().unwrap().to_owned();
        let id = tx
            .upsert_file(&FileInput::new(fullpath, filename, catalog.uuid))
            .await?;
        let file = File::filter_by_uuid(id).get(&mut tx).await?;
        tx.commit().await?;
        Ok(file)
    }

    /// Signs a published object identified by the agent's resolved key.
    pub async fn download_artifact(&self, request: DownloadRequest) -> Result<Transfer, AppError> {
        let mut db = self.db.clone();
        let job = Job::filter_by_uuid(request.job_id)
            .first()
            .exec(&mut db)
            .await?
            .ok_or(AppError::JobNotFound(request.job_id))?;
        check_claim(&job, request.agent_id)?;
        let (fullpath, url) = self.artifact_location(&job, &request.key)?;
        let loaded = db
            .get_file(FileKey::Fullpath(&fullpath))
            .await?
            .ok_or_else(|| AppError::ArtifactNotFound(fullpath))?;
        if loaded.catalog.url != url {
            return Err(AppError::InvalidRequest(
                "artifact catalog location does not match its key".into(),
            ));
        }
        self.transfer(loaded.catalog, PresignOperation::Download, &job)
    }

    fn artifact_location(&self, job: &Job, key: &str) -> Result<(String, String), AppError> {
        validate_relative_path(key)?;
        // Agents choose graph/node names and versions, but may only access their run.
        if !key.starts_with(&format!("{}/", job.run_id)) {
            return Err(AppError::InvalidRequest(
                "artifact key belongs to another run".into(),
            ));
        }
        let config = self
            .config
            .storage
            .as_ref()
            .ok_or(AppError::StorageUnavailable)?;
        let fullpath = format!("/{}/{key}", config.prefix.trim_matches('/'));
        if fullpath.chars().count() > 512 {
            return Err(AppError::InvalidRequest(
                "artifact full path is too long".into(),
            ));
        }
        let url = config.object_url(&fullpath)?;
        Ok((fullpath, url))
    }

    fn transfer(
        &self,
        catalog: Catalog,
        operation: PresignOperation,
        job: &Job,
    ) -> Result<Transfer, AppError> {
        if operation == PresignOperation::Download && !catalog.uploaded {
            return Err(AppError::ArtifactNotFound(catalog.uuid.to_string()));
        }
        let config = self
            .config
            .storage
            .as_ref()
            .ok_or(AppError::StorageUnavailable)?;
        let remaining = job.expired_at.unwrap().as_second() - Timestamp::now().as_second();
        if remaining < 1 {
            return Err(AppError::JobConflict(job.uuid));
        }
        let ttl = config
            .presign_ttl
            .min(std::time::Duration::from_secs(remaining as u64));
        let storage =
            S3::new(self.db.clone(), config.signer()).map_err(crate::storage::Error::from)?;
        let presigned = storage.presign_catalog(&catalog, operation, ttl)?;
        Ok(Transfer { catalog, presigned })
    }
}

fn validate_upload(key: &str, size: u64, format: &str) -> Result<(), AppError> {
    let manifest = key.rsplit('/').next() == Some(DIRECTORY_MANIFEST);
    if (manifest && (format != DIRECTORY_FORMAT || size > MAX_MANIFEST_BYTES))
        || (!manifest && format == DIRECTORY_FORMAT)
        || key
            .split('/')
            .rev()
            .skip(1)
            .any(|part| part == DIRECTORY_MANIFEST)
    {
        return Err(AppError::InvalidRequest(
            "invalid artifact upload metadata".into(),
        ));
    }
    Ok(())
}

fn check_claim(job: &Job, agent_id: Uuid) -> Result<(), AppError> {
    if job.agent_id != Some(agent_id)
        || job.status != Status::Polled
        || job
            .expired_at
            .is_none_or(|expiry| expiry <= Timestamp::now())
    {
        return Err(AppError::JobConflict(job.uuid));
    }
    Ok(())
}

async fn lock_claim(
    tx: &mut toasty::Transaction<'_>,
    job_id: Uuid,
    agent_id: Uuid,
) -> Result<Job, AppError> {
    let table = tx.schema().table_for(Job::id()).name.replace('`', "``");
    // Toasty cannot express FOR UPDATE. Serialize publication with claim changes.
    let rows = toasty::sql::query(format!("SELECT id FROM `{table}` WHERE uuid = ? AND agent_id = ? AND status = 'Polled' AND expired_at > ? FOR UPDATE"))
        .bind(job_id).bind(agent_id).bind(Timestamp::now()).exec(tx).await?;
    if rows.is_empty() {
        return Err(AppError::JobConflict(job_id));
    }
    Ok(Job::filter_by_uuid(job_id).get(tx).await?)
}
