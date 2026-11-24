use std::{io, path::Path};

use graph::{
    GraphStore,
    types::{DAG, Node},
};
use scheduler::{Job, JobArtifactName};
use serde::{Deserialize, Serialize};
use tokio::fs;
use uuid::Uuid;

use super::{JobManager, JobOutcome, JobResult, is_not_found, read_optional_json};
use crate::{
    containerd::{Container, ContainerState, Containerd},
    errors::Error,
    transfer::JobTransfer,
    write_json,
};

const COMPLETION_FILE: &str = "completion.json";
pub(super) const CLEANUP_FILE: &str = "cleanup-ready.json";
const LEGACY_PUBLICATION_FILE: &str = "result-publication.json";
const LEGACY_RECEIPT_FILE: &str = "result-upload.json";

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Completion {
    Pending(JobOutcome),
    Reported,
    ClaimLost,
}

impl Completion {
    fn settled(self) -> bool {
        matches!(self, Self::Reported | Self::ClaimLost)
    }
}

#[derive(Deserialize)]
struct LegacyPublication {
    job_id: Uuid,
    action: Option<JobOutcome>,
    json: String,
}

impl LegacyPublication {
    async fn load(dir: &Path, job_id: Uuid) -> Result<Option<Self>, Error> {
        let publication: Option<Self> =
            read_optional_json(&dir.join(LEGACY_PUBLICATION_FILE)).await?;
        if let Some(publication) = &publication {
            if publication.job_id != job_id {
                return Err(Error::JobManager(format!(
                    "result publication does not belong to job {job_id}"
                )));
            }
            serde_json::from_str::<JobResult>(&publication.json)?;
        }
        Ok(publication)
    }
}

#[derive(Deserialize)]
struct LegacyReceipt {
    job_id: Uuid,
    result: JobResult,
}

impl<C, R> JobManager<C, R>
where
    C: Containerd,
    R: GraphStore<Uuid, G = DAG> + Clone + Send + Sync,
{
    pub(super) async fn is_completed(&self, job_id: Uuid) -> Result<bool, Error> {
        let dir = self.job_dir(job_id);
        if fs::try_exists(dir.join(CLEANUP_FILE)).await? {
            return Ok(true);
        }
        if let Some(completion) =
            read_optional_json::<Completion>(&dir.join(COMPLETION_FILE)).await?
        {
            return Ok(completion.settled());
        }
        Ok(LegacyPublication::load(&dir, job_id)
            .await?
            .is_some_and(|publication| publication.action.is_none()))
    }

    pub(super) async fn advance_completion(&self, job_id: Uuid) -> Result<(), Error> {
        let dir = self.job_dir(job_id);
        if fs::try_exists(dir.join(CLEANUP_FILE)).await? {
            return self.cleanup(job_id).await;
        }

        let result_path = dir.join(JobArtifactName::Result.as_str());
        let Some(result) = read_optional_json::<JobResult>(&result_path).await? else {
            return Ok(());
        };
        let completion_path = dir.join(COMPLETION_FILE);
        let mut completion = match read_optional_json(&completion_path).await? {
            Some(completion) => completion,
            None => {
                let completion = self.prepare_completion(job_id, &result).await?;
                write_json(&completion_path, &completion).await?;
                completion
            }
        };

        if let Completion::Pending(outcome) = completion {
            completion = match self.report(job_id, outcome).await {
                Ok(()) => Completion::Reported,
                Err(Error::ClaimLost(_)) => {
                    tracing::warn!(%job_id, "server no longer accepts the job outcome");
                    Completion::ClaimLost
                }
                Err(error) => return Err(error),
            };
            write_json(&completion_path, &completion).await?;
        }

        self.release(job_id);
        if self
            .upload_artifact(job_id, JobArtifactName::Result)
            .await?
            && matches!(completion, Completion::Reported)
            && fs::try_exists(dir.join("usage.json.upload")).await?
        {
            self.cleanup(job_id).await?;
        }
        Ok(())
    }

    async fn prepare_completion(
        &self,
        job_id: Uuid,
        result: &JobResult,
    ) -> Result<Completion, Error> {
        let dir = self.job_dir(job_id);
        let data_dir = self.data_dir(job_id);
        if let Some(publication) = LegacyPublication::load(&dir, job_id).await? {
            if let Some(outcome) = publication.action {
                return Ok(Completion::Pending(outcome));
            }
            // Old agents cleared this action both after reporting and after an
            // output claim conflict. A successful job still needs output evidence.
            return if result.outcome()? != JobOutcome::Succeeded
                || JobTransfer::outputs_uploaded(&data_dir, job_id).await?
            {
                Ok(Completion::Reported)
            } else {
                let job = self.read_job(job_id).await?;
                let node = self.completion_node(&job).await?;
                Ok(if node.outputs.is_empty() {
                    Completion::Reported
                } else {
                    Completion::ClaimLost
                })
            };
        }

        let outcome = result.outcome()?;
        if outcome == JobOutcome::Succeeded
            && !JobTransfer::outputs_uploaded(&data_dir, job_id).await?
        {
            let job = self.read_job(job_id).await?;
            let node = self.completion_node(&job).await?;
            match self
                .inner
                .transfer
                .complete_output(&job, &data_dir, &node)
                .await
            {
                Ok(()) => {}
                Err(Error::ClaimLost(_)) => {
                    tracing::warn!(%job_id, "server no longer accepts the job outputs");
                    return Ok(Completion::ClaimLost);
                }
                Err(Error::Artifact(message)) => {
                    tracing::error!(%job_id, %message, "job outputs are invalid");
                    let _lock = self.lock_job(job_id).await?;
                    self.preserve_legacy_usage(job_id).await?;
                    let result = JobResult {
                        exit_code: 1,
                        outcome: Some(JobOutcome::Failed.as_str().into()),
                        ..result.clone()
                    };
                    write_json(&dir.join(JobArtifactName::Result.as_str()), &result).await?;
                    return Ok(Completion::Pending(JobOutcome::Failed));
                }
                Err(error) => return Err(error),
            }
        }

        Ok(Completion::Pending(outcome))
    }

    async fn completion_node(&self, job: &Job) -> Result<Node, Error> {
        let graph = self.load_graph(job).await?;
        let node_id = job.node_run.get().node_id;
        graph
            .nodes
            .into_iter()
            .find(|node| node.id == node_id)
            .ok_or_else(|| Error::JobManager("job node is missing from graph".into()))
    }

    async fn report(&self, job_id: Uuid, outcome: JobOutcome) -> Result<(), Error> {
        let client = &self.inner.client;
        let agent_id = client.settings().id;
        match outcome {
            JobOutcome::Succeeded => client.succeed(agent_id, job_id).await,
            JobOutcome::Failed => client.fail(agent_id, job_id).await,
            JobOutcome::Yielded => client.yield_job(agent_id, job_id).await,
            JobOutcome::Terminated => client.terminate(agent_id, job_id).await,
        }
    }

    /// A committed artifact remains eligible for cleanup across restarts.
    /// No receipt is recorded if storage is disabled or the file is absent.
    pub(super) async fn upload_artifact(
        &self,
        job_id: Uuid,
        filename: JobArtifactName,
    ) -> Result<bool, Error> {
        let dir = self.job_dir(job_id);
        let receipt = dir.join(format!("{}.upload", filename.as_str()));
        if fs::try_exists(&receipt).await? {
            return Ok(true);
        }
        let mut bytes = match fs::read(dir.join(filename.as_str())).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        let legacy_uploaded = if filename == JobArtifactName::Result {
            if let Some(publication) = LegacyPublication::load(&dir, job_id).await? {
                bytes = publication.json.into_bytes();
            }
            let result = serde_json::from_slice::<JobResult>(&bytes)?;
            read_optional_json::<LegacyReceipt>(&dir.join(LEGACY_RECEIPT_FILE))
                .await?
                .is_some_and(|receipt| receipt.job_id == job_id && receipt.result == result)
        } else {
            false
        };
        if !legacy_uploaded
            && !self
                .inner
                .transfer
                .upload_artifact(job_id, filename, bytes)
                .await?
        {
            return Ok(false);
        }
        let _lock = self.lock_job(job_id).await?;
        write_json(&receipt, &true).await?;
        Ok(true)
    }

    async fn cleanup(&self, job_id: Uuid) -> Result<(), Error> {
        let _lock = self.lock_job(job_id).await?;
        let dir = self.job_dir(job_id);
        let ready = dir.join(CLEANUP_FILE);
        if !fs::try_exists(&ready).await? {
            write_json(&ready, &true).await?;
        }

        if let Some(metadata) = self.read_container(job_id).await? {
            let handle = self.container(&metadata)?;
            match handle.inspect().await {
                Ok(ContainerState::Running { .. }) => return Err(Error::JobRunning(job_id)),
                Ok(_) => handle.remove().await?,
                Err(error) if is_not_found(&error) => {}
                Err(error) => return Err(error),
            }
            self.forget_container(job_id);
        }

        remove_job_data(&self.data_dir(job_id), &dir).await?;

        // Keep cleanup discoverable until all job data and metadata has gone.
        let mut entries = fs::read_dir(&dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            if entry.file_name() == CLEANUP_FILE || entry.file_name() == "lock" {
                continue;
            }
            if entry.file_type().await?.is_dir() {
                fs::remove_dir_all(entry.path()).await?;
            } else {
                fs::remove_file(entry.path()).await?;
            }
        }
        fs::remove_file(dir.join("lock")).await?;
        fs::remove_file(&ready).await?;
        fs::remove_dir(&dir).await?;
        Ok(())
    }
}

async fn remove_job_data(data_dir: &Path, metadata_dir: &Path) -> Result<(), Error> {
    match fs::symlink_metadata(data_dir).await {
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "job cleanup path is not a directory: {}",
                    data_dir.display()
                ),
            )
            .into());
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    }
    // When the roots share one directory, the metadata sweep also removes the
    // job data and keeps the cleanup marker discoverable until then.
    if fs::canonicalize(data_dir).await? != fs::canonicalize(metadata_dir).await? {
        fs::remove_dir_all(data_dir).await?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "completion_tests.rs"]
mod tests;
