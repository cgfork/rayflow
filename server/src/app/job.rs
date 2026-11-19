//! Job delivery and conversion of agent status reports into task events.

use std::borrow::Cow;

use catalog::directory::DIRECTORY_MANIFEST;
use catalog::path::validate_relative_path;
use catalog::{
    CatalogOptionalData, CatalogSystem, FileInput, FileSystem, ObjectStorage, PresignOperation,
};
use graph::{
    Graph, GraphStore,
    types::{DAG, Kind, Output},
};
use runtime::command::{NodeRun, Run};
use scheduler::{
    AgentRef, Job, JobPoll, actuator::NodeConfig, cancel_job, fail_job, succeed_job, terminate_job,
    yield_job,
};
use uuid::Uuid;

use super::{App, AppError, Page, PageQuery};

fn is_directory_output(output: &Output) -> bool {
    output.kind == Kind::Directory || output.multiple
}

pub(super) fn output_catalog_filename(output: &Output) -> Cow<'_, str> {
    if is_directory_output(output) {
        format!("{}/{DIRECTORY_MANIFEST}", output.file_name).into()
    } else {
        output.file_name.as_str().into()
    }
}

pub(super) fn resolve_input_source<'a>(
    graph: &'a DAG,
    source: &str,
) -> Result<(Uuid, &'a Output), AppError> {
    let mut outputs = graph.nodes.iter().filter_map(|node| {
        let filename = source.strip_prefix(&node.name)?.strip_prefix('.')?;
        node.outputs
            .iter()
            .find(|output| output.file_name == filename)
            .map(|output| (node.id, output))
    });
    let output = outputs
        .next()
        .ok_or_else(|| AppError::InvalidRequest("input producer output is not declared".into()))?;
    if outputs.next().is_some() {
        return Err(AppError::InvalidRequest("input source is ambiguous".into()));
    }
    Ok(output)
}

impl<S> App<S> {
    async fn validate_output_filename(
        &self,
        node_run: &NodeRun,
        filename: &str,
    ) -> Result<(), AppError>
    where
        S: GraphStore<Uuid, G = DAG> + Clone,
        S::load(..): Send,
        S::Err: Send + Sync + 'static,
    {
        let graph = self
            .read_graph(node_run.graph_id, Some(node_run.version))
            .await?;
        let node = graph
            .node(node_run.node_id)
            .ok_or(AppError::NodeNotFound(node_run.node_id))?;
        if !node.data.outputs.iter().any(|output| {
            output.file_name == filename
                || (is_directory_output(output)
                    && filename
                        .strip_prefix(output.file_name.as_str())
                        .is_some_and(|suffix| suffix.starts_with('/')))
        }) {
            return Err(AppError::InvalidRequest(
                "filename is not a declared output".into(),
            ));
        }
        Ok(())
    }

    fn output_location(
        &self,
        node_run: &NodeRun,
        filename: &str,
    ) -> Result<(String, String), AppError> {
        validate_relative_path(filename)
            .map_err(|error| AppError::InvalidRequest(error.to_string()))?;
        let prefix = self
            .config
            .storage
            .as_ref()
            .ok_or(AppError::StorageUnavailable)?
            .prefix
            .trim_matches('/');
        let key = format!(
            "/{prefix}/{}/{}/{}/{}/{}/{}",
            node_run.run_id,
            node_run.graph_id,
            node_run.version,
            node_run.node_id,
            node_run.revision,
            filename
        );
        let config = self
            .config
            .storage
            .as_ref()
            .ok_or(AppError::StorageUnavailable)?;
        let url = config.object_url(&key)?;
        Ok((key, url))
    }

    fn validate_output_job(job: &Job, agent_id: Uuid) -> Result<(), AppError> {
        if job.status != scheduler::actuator::Status::Polled
            || job.agent_id != Some(agent_id)
            || job
                .expired_at
                .is_none_or(|expiry| expiry <= jiff::Timestamp::now())
        {
            return Err(AppError::JobConflict(job.uuid));
        }
        Ok(())
    }

    pub async fn prepare_output(
        &self,
        job_id: Uuid,
        agent_id: Uuid,
        filename: String,
        size: Option<u64>,
        format: Option<String>,
    ) -> Result<crate::api::run::Transfer, AppError>
    where
        S: GraphStore<Uuid, G = DAG> + Clone,
        S::load(..): Send,
        S::Err: Send + Sync + 'static,
    {
        let size =
            size.ok_or_else(|| AppError::InvalidRequest("output size is required".into()))?;
        let format =
            format.ok_or_else(|| AppError::InvalidRequest("output format is required".into()))?;
        let mut db = self.db.clone();
        let mut tx = db.transaction().await?;
        let job = Job::filter_by_uuid(job_id)
            .include(Job::fields().node_run())
            .first()
            .exec(&mut tx)
            .await?
            .ok_or(AppError::JobNotFound(job_id))?;
        Self::validate_output_job(&job, agent_id)?;
        let node_run = job.node_run.get();
        self.validate_output_filename(node_run, &filename).await?;
        let (key, url) = self.output_location(node_run, &filename)?;
        let id = tx
            .upsert_catalog(CatalogOptionalData {
                key: Some(key),
                url: Some(url),
                size: Some(size),
                format: Some(format),
                uploaded: Some(false),
            })
            .await?;
        tx.upsert_file(&FileInput::new(node_run.uuid, filename, id))
            .await?;
        let catalog = tx
            .get_catalog(id)
            .await?
            .ok_or_else(|| AppError::ArtifactNotFound(id.to_string()))?;
        let config = self
            .config
            .storage
            .as_ref()
            .ok_or(AppError::StorageUnavailable)?;
        let storage = config.object_storage(self.db.clone())?;
        let presigned = storage
            .presign_catalog(&catalog, PresignOperation::Upload, config.presign_ttl)
            .map_err(|error| AppError::Storage(Box::new(error)))?;
        tx.commit().await?;
        Ok(crate::api::run::Transfer {
            catalog,
            presigned,
            multipart: false,
        })
    }

    pub async fn commit_output(
        &self,
        job_id: Uuid,
        agent_id: Uuid,
        filename: String,
        catalog_uuid: Uuid,
    ) -> Result<Uuid, AppError>
    where
        S: GraphStore<Uuid, G = DAG> + Clone,
        S::load(..): Send,
        S::Err: Send + Sync + 'static,
    {
        let mut db = self.db.clone();
        let mut tx = db.transaction().await?;
        let job = Job::filter_by_uuid(job_id)
            .include(Job::fields().node_run())
            .first()
            .exec(&mut tx)
            .await?
            .ok_or(AppError::JobNotFound(job_id))?;
        Self::validate_output_job(&job, agent_id)?;
        let node_run = job.node_run.get();
        self.validate_output_filename(node_run, &filename).await?;
        let (key, _) = self.output_location(node_run, &filename)?;
        let catalog = tx
            .get_catalog(catalog_uuid)
            .await?
            .ok_or_else(|| AppError::ArtifactNotFound(catalog_uuid.to_string()))?;
        if catalog.key != key {
            return Err(AppError::InvalidRequest(
                "output catalog does not match the job output".into(),
            ));
        }
        tx.update_catalog(
            catalog_uuid,
            CatalogOptionalData {
                uploaded: Some(true),
                ..Default::default()
            },
        )
        .await?;
        let file = tx
            .upsert_file(&FileInput::new(node_run.uuid, filename, catalog_uuid))
            .await?;
        tx.commit().await?;
        Ok(file)
    }

    pub async fn presign_input(
        &self,
        job_id: Uuid,
        agent_id: Uuid,
        filename: String,
    ) -> Result<crate::api::run::Transfer, AppError>
    where
        S: GraphStore<Uuid, G = DAG> + Clone,
        S::Err: Send + Sync + 'static,
    {
        validate_relative_path(&filename)
            .map_err(|error| AppError::InvalidRequest(error.to_string()))?;
        let mut db = self.db.clone();
        let job = Job::filter_by_uuid(job_id)
            .include(Job::fields().node_run())
            .first()
            .exec(&mut db)
            .await?
            .ok_or(AppError::JobNotFound(job_id))?;
        Self::validate_output_job(&job, agent_id)?;
        let node_run = job.node_run.get();
        let graph = self
            .read_graph(node_run.graph_id, Some(node_run.version))
            .await?;
        let consumer = graph
            .node(node_run.node_id)
            .ok_or(AppError::NodeNotFound(node_run.node_id))?;
        let (input, member_suffix) = consumer
            .data
            .inputs
            .iter()
            .filter_map(|input| {
                let suffix = filename.strip_prefix(input.file_name.as_str())?;
                if suffix.is_empty() {
                    Some((input, None))
                } else {
                    suffix.strip_prefix('/').map(|member| (input, Some(member)))
                }
            })
            .max_by_key(|(input, _)| input.file_name.len())
            .ok_or_else(|| AppError::ArtifactNotFound(filename.clone()))?;
        let (producer, output) = resolve_input_source(&graph, &input.source)?;
        let producer_run = NodeRun::filter_by_run_id(job.run_id)
            .filter(NodeRun::fields().node_id().eq(producer))
            .filter(NodeRun::fields().revision().le(node_run.revision))
            .filter(NodeRun::fields().ineffective().eq(0))
            .order_by(NodeRun::fields().revision().desc())
            .first()
            .exec(&mut db)
            .await?
            .ok_or(AppError::NodeNotFound(producer))?;
        if producer_run.status != "Succeeded" {
            return Err(AppError::ArtifactNotFound(filename));
        }
        let filename = match member_suffix {
            Some(suffix) if is_directory_output(output) => {
                format!("{}/{suffix}", output.file_name).into()
            }
            Some(_) => return Err(AppError::ArtifactNotFound(filename)),
            None => output_catalog_filename(output),
        };
        let file = db
            .get_file_by_node_run_filename(producer_run.uuid, &filename)
            .await?
            .ok_or_else(|| AppError::ArtifactNotFound(filename.to_string()))?;
        let catalog = file
            .catalog
            .ok_or_else(|| AppError::ArtifactNotFound(filename.into_owned()))?;
        if !catalog.uploaded {
            return Err(AppError::ArtifactNotFound(catalog.uuid.to_string()));
        }
        let config = self
            .config
            .storage
            .as_ref()
            .ok_or(AppError::StorageUnavailable)?;
        let storage = config.object_storage(self.db.clone())?;
        let presigned = storage
            .presign_catalog(&catalog, PresignOperation::Download, config.presign_ttl)
            .map_err(|error| AppError::Storage(Box::new(error)))?;
        Ok(crate::api::run::Transfer {
            catalog,
            presigned,
            multipart: false,
        })
    }

    pub async fn list_jobs(
        &self,
        page: PageQuery,
        status: Option<scheduler::actuator::Status>,
        agent_id: Option<Uuid>,
        run_id: Option<Uuid>,
    ) -> Result<Page<Job>, AppError> {
        let limit = page.limit()?;
        let mut select = Job::all();
        if let Some(before) = page.before {
            select = select.filter(Job::fields().id().lt(before));
        }
        if let Some(status) = status {
            select = select.filter(Job::fields().status().eq(status));
        }
        if let Some(agent_id) = agent_id {
            select = select.filter(Job::fields().agent_id().eq(Some(agent_id)));
        }
        if let Some(run_id) = run_id {
            select = select.filter(Job::fields().run_id().eq(run_id));
        }
        let items = select
            .order_by(Job::fields().id().desc())
            .limit(limit + 1)
            .exec(&mut self.db.clone())
            .await?;
        Ok(Page::new(items, limit, |row| row.id))
    }

    /// Claims fitting jobs and includes their configuration and original node revision.
    pub async fn poll(&self, agent: &mut AgentRef, maximum: usize) -> Result<Vec<Job>, AppError> {
        let mut db = self.db.clone();
        let mut tx = db.transaction().await?;
        self.observe_agent(&mut tx, agent).await?;
        let mut polling = agent.clone();
        let mut jobs: Vec<_> = polling.poll(&mut tx, maximum).await?.collect();
        for job in &mut jobs {
            if job.run.is_unloaded() {
                job.run = Run::filter_by_uuid(job.run_id).get(&mut tx).await?.into();
            }
            if job.node_run.is_unloaded() {
                job.node_run = NodeRun::filter_by_uuid(job.node_run_id)
                    .get(&mut tx)
                    .await?
                    .into();
            }
            if job.node_config.is_unloaded() || job.node_config.get().is_none() {
                job.node_config = NodeConfig::filter_by_uuid(job.node_config_id)
                    .first()
                    .exec(&mut tx)
                    .await?
                    .into();
            }
            job.job_lease()?;
        }
        tx.commit().await?;
        *agent = polling;
        Ok(jobs)
    }

    pub async fn cancel_job(&self, job_id: Uuid, agent_id: Uuid) -> Result<(), AppError> {
        let mut db = self.db.clone();
        cancel_job(&mut db, job_id, agent_id).await?;
        Ok(())
    }

    pub async fn yield_job(&self, job_id: Uuid, agent_id: Uuid) -> Result<(), AppError> {
        let mut db = self.db.clone();
        yield_job(&mut db, job_id, agent_id).await?;
        Ok(())
    }

    pub async fn succeed_job(&self, job_id: Uuid, agent_id: Uuid) -> Result<(), AppError> {
        let mut db = self.db.clone();
        succeed_job(&mut db, job_id, agent_id).await?;
        Ok(())
    }

    pub async fn fail_job(&self, job_id: Uuid, agent_id: Uuid) -> Result<(), AppError> {
        let mut db = self.db.clone();
        fail_job(&mut db, job_id, agent_id).await?;
        Ok(())
    }

    pub async fn terminate_job(&self, job_id: Uuid, agent_id: Uuid) -> Result<(), AppError> {
        let mut db = self.db.clone();
        terminate_job(&mut db, job_id, agent_id).await?;
        Ok(())
    }
}
