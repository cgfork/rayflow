//! Storage and claim checks for job metadata archives.

use catalog::{
    CatalogOptionalData, CatalogSystem, ObjectStorage, PresignOperation, model::Catalog,
};
use jiff::Timestamp;
use scheduler::{Job, JobArtifactName, actuator::Status};
use toasty::{Executor, Transaction, schema::Model};
use uuid::Uuid;

use super::{App, AppError};
use crate::api::run::{JobArtifactDownload, Transfer};

const FORMAT: &str = "application/json";

#[derive(Debug, toasty::Model)]
#[table = "job_artifact"]
#[unique(job_uuid, filename)]
pub(crate) struct JobArtifact {
    #[key]
    #[auto]
    pub id: u64,
    pub job_uuid: Uuid,
    #[column(type = varchar(255))]
    pub filename: String,
    pub agent_id: Uuid,
    #[unique]
    pub catalog_uuid: Option<Uuid>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

/// Record the successful delivery before the poll transaction commits.
pub(super) async fn record_assignment(
    tx: &mut Transaction<'_>,
    job_id: Uuid,
    agent_id: Uuid,
) -> Result<(), AppError> {
    let table = tx
        .schema()
        .table_for(JobArtifact::id())
        .name
        .replace('`', "``");
    // The owner and any prepared catalog must survive job redelivery.
    // Toasty does not express an atomic insert with a no-op on duplicate.
    toasty::sql::statement(format!(
        "INSERT INTO `{table}` (job_uuid, filename, agent_id, created_at, updated_at) \
         VALUES (?, ?, ?, CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6)) \
         ON DUPLICATE KEY UPDATE job_uuid = VALUES(job_uuid)",
    ))
    .bind(job_id)
    .bind(JobArtifactName::Result.as_str())
    .bind(agent_id)
    .exec(tx)
    .await?;
    Ok(())
}

async fn lock_assignment(
    tx: &mut Transaction<'_>,
    job_id: Uuid,
    agent_id: Uuid,
) -> Result<Option<JobArtifact>, AppError> {
    let table = tx.schema().table_for(Job::id()).name.replace('`', "``");
    // Lock the job in the same order as poll, including before an artifact exists.
    let locked = toasty::sql::query(format!(
        "SELECT id FROM `{table}` WHERE uuid = ? FOR UPDATE"
    ))
    .bind(job_id)
    .column_types([toasty::stmt::Type::U64])
    .exec(&mut *tx)
    .await?;
    if locked.is_empty() {
        return Err(AppError::JobNotFound(job_id));
    }
    let job = Job::filter_by_uuid(job_id).get(&mut *tx).await?;
    if job.agent_id.is_some_and(|owner| owner != agent_id) {
        return Err(AppError::JobConflict(job_id));
    }
    let assignment =
        || JobArtifact::filter_by_job_uuid_and_filename(job_id, JobArtifactName::Result.as_str());
    if let Some(result) = assignment().first().exec(&mut *tx).await? {
        if result.agent_id == agent_id {
            return Ok(Some(result));
        }
        return Err(AppError::JobConflict(job_id));
    }
    if job.status.is_terminal() && job.agent_id.is_none() {
        return Ok(None);
    }
    // An older server might have delivered the job without recording the
    // result owner. Only its current claim can establish ownership here.
    if job.status != Status::Polled || job.agent_id != Some(agent_id) {
        return Err(AppError::JobConflict(job_id));
    }
    record_assignment(tx, job_id, agent_id).await?;
    Ok(Some(assignment().get(tx).await?))
}

async fn find_artifact(
    tx: &mut Transaction<'_>,
    assignment: JobArtifact,
    filename: JobArtifactName,
) -> Result<Option<JobArtifact>, AppError> {
    if filename == JobArtifactName::Result {
        return Ok(Some(assignment));
    }
    Ok(
        JobArtifact::filter_by_job_uuid_and_filename(assignment.job_uuid, filename.as_str())
            .first()
            .exec(tx)
            .await?,
    )
}

impl<S> App<S> {
    /// Signs an uploaded archive for the latest job attempt of the given node run.
    pub(crate) async fn presign_run_job_artifact(
        &self,
        run_id: Uuid,
        node_run_id: Uuid,
        filename: JobArtifactName,
    ) -> Result<Option<JobArtifactDownload>, AppError> {
        let mut db = self.db.clone();
        let Some(job) = Job::filter_by_node_run_id(node_run_id)
            .filter(Job::fields().run_id().eq(run_id))
            .order_by(Job::fields().id().desc())
            .first()
            .exec(&mut db)
            .await?
        else {
            return Ok(None);
        };
        let artifact = JobArtifact::filter_by_job_uuid_and_filename(job.uuid, filename.as_str())
            .first()
            .exec(&mut db)
            .await?;
        let Some(catalog_uuid) = artifact.and_then(|artifact| artifact.catalog_uuid) else {
            return Ok(None);
        };
        let Some(catalog) = db
            .get_catalog(catalog_uuid)
            .await?
            .filter(|catalog| catalog.uploaded)
        else {
            return Ok(None);
        };
        let config = self
            .config
            .storage
            .as_ref()
            .ok_or(AppError::StorageUnavailable)?;
        let storage = config.object_storage(self.db.clone())?;
        let presigned = storage
            .presign_catalog(&catalog, PresignOperation::Download, config.presign_ttl)
            .map_err(|error| AppError::Storage(Box::new(error)))?;
        Ok(Some(JobArtifactDownload { catalog, presigned }))
    }

    pub async fn prepare_job_artifact(
        &self,
        job_id: Uuid,
        agent_id: Uuid,
        filename: JobArtifactName,
        size: u64,
    ) -> Result<Transfer, AppError> {
        if size == 0 {
            return Err(AppError::InvalidRequest(
                "job artifact must not be empty".into(),
            ));
        }
        let mut db = self.db.clone();
        let mut tx = db.transaction().await?;
        let assignment = lock_assignment(&mut tx, job_id, agent_id)
            .await?
            .ok_or(AppError::JobResultOwnerUnknown(job_id))?;
        let config = self
            .config
            .storage
            .as_ref()
            .ok_or(AppError::StorageUnavailable)?;
        let artifact = find_artifact(&mut tx, assignment, filename).await?;
        let catalog_uuid = match artifact.as_ref().and_then(|artifact| artifact.catalog_uuid) {
            Some(id) => id,
            None => {
                let prefix = config.prefix.trim_matches('/');
                let key = format!("/{prefix}/jobs/{job_id}/{}", filename.as_str());
                if tx.get_catalog_by_key(&key).await?.is_some() {
                    return Err(AppError::JobConflict(job_id));
                }
                let url = config.object_url(&key)?;
                let id = tx
                    .upsert_catalog(CatalogOptionalData {
                        key: Some(key),
                        url: Some(url),
                        size: Some(size),
                        format: Some(FORMAT.into()),
                        uploaded: None,
                    })
                    .await?;
                let now = Timestamp::now();
                if let Some(artifact) = artifact {
                    JobArtifact::filter_by_id(artifact.id)
                        .update()
                        .catalog_uuid(Some(id))
                        .updated_at(now)
                        .exec(&mut tx)
                        .await?;
                } else {
                    JobArtifact::create()
                        .job_uuid(job_id)
                        .filename(filename.as_str())
                        .agent_id(agent_id)
                        .catalog_uuid(Some(id))
                        .created_at(now)
                        .updated_at(now)
                        .exec(&mut tx)
                        .await?;
                }
                id
            }
        };
        let catalog = tx
            .get_catalog(catalog_uuid)
            .await?
            .ok_or_else(|| AppError::ArtifactNotFound(catalog_uuid.to_string()))?;
        if catalog.size != size {
            return Err(AppError::InvalidRequest(
                "job artifact size does not match its prepared upload".into(),
            ));
        }
        let operation = if catalog.uploaded {
            // A retry can identify the committed object without a new write URL.
            PresignOperation::Download
        } else {
            PresignOperation::Upload
        };
        let storage = config.object_storage(self.db.clone())?;
        let presigned = storage
            .presign_catalog(&catalog, operation, config.presign_ttl)
            .map_err(|error| AppError::Storage(Box::new(error)))?;
        tx.commit().await?;
        Ok(Transfer {
            catalog,
            presigned,
            multipart: false,
        })
    }

    pub async fn commit_job_artifact(
        &self,
        job_id: Uuid,
        agent_id: Uuid,
        filename: JobArtifactName,
        catalog_uuid: Uuid,
    ) -> Result<Catalog, AppError> {
        let mut db = self.db.clone();
        let mut tx = db.transaction().await?;
        let assignment = lock_assignment(&mut tx, job_id, agent_id)
            .await?
            .ok_or(AppError::JobConflict(job_id))?;
        let prepared_catalog = find_artifact(&mut tx, assignment, filename)
            .await?
            .and_then(|artifact| artifact.catalog_uuid);
        if prepared_catalog != Some(catalog_uuid) {
            return Err(AppError::InvalidRequest(
                "catalog does not match the prepared job artifact".into(),
            ));
        }
        let catalog = tx
            .get_catalog(catalog_uuid)
            .await?
            .ok_or_else(|| AppError::ArtifactNotFound(catalog_uuid.to_string()))?;
        if !catalog.uploaded {
            tx.update_catalog(
                catalog_uuid,
                CatalogOptionalData {
                    uploaded: Some(true),
                    ..Default::default()
                },
            )
            .await?;
        }
        let catalog = tx
            .get_catalog(catalog_uuid)
            .await?
            .ok_or_else(|| AppError::ArtifactNotFound(catalog_uuid.to_string()))?;
        tx.commit().await?;
        Ok(catalog)
    }
}
