//! Run state, commands, and file downloads.

//! Read-side state of a run: node runs joined with their graph node.

use graph::Graph;
use graph::GraphStore;
use graph::types::DAG;
use runtime::command::{NodeRun, Run};
use serde::Serialize;
use uuid::Uuid;

use crate::api::run::{FileDirection, RunFileDownloadRequest, Transfer};
use crate::app::job::{output_catalog_filename, resolve_input_source};
use crate::app::{App, AppError, Page, PageQuery, RunApp};
use catalog::{FileSystem, ObjectStorage, PresignOperation};
use event::task::Task;
use event::task_event::{TaskEvent, TaskEventEntry};
use event::{Emit as TaskEventWrite, Read as TaskRead};

#[derive(Debug, Serialize)]
pub struct RunView {
    pub run: Run,
    pub graph: DAG,
    pub node_runs: Vec<(NodeRun, graph::types::Node)>,
}

impl<S> App<S> {
    /// Emits an event for a task after resolving its run and node references.
    pub async fn emit_task_event(
        &self,
        task_id: Uuid,
        event: TaskEvent,
    ) -> Result<TaskEventEntry, AppError> {
        let mut db = self.db.clone();
        let task = Task::read(&mut db, task_id)
            .await
            .map_err(|error| match error {
                event::Error::TaskNotFound(task_id) => AppError::TaskNotFound(task_id),
                error => AppError::from(error),
            })?;
        let entry = TaskEventEntry::new(&task, event);
        entry.emit(&mut db).await?;
        Ok(entry)
    }

    pub async fn download_run_file(
        &self,
        run_id: Uuid,
        request: RunFileDownloadRequest,
    ) -> Result<Transfer, AppError>
    where
        S: GraphStore<Uuid, G = DAG> + Clone,
        S::Err: Send + Sync + 'static,
    {
        let run = self.read_run_at(run_id, request.revision).await?;
        let graph = self.read_graph(run.graph_id, Some(run.version)).await?;
        let node = graph
            .node(request.node_id)
            .ok_or(AppError::NodeNotFound(request.node_id))?;
        let filename = request.file_name.as_str();
        let (producer, output) = match request.direction {
            FileDirection::Output => {
                let output = node
                    .data
                    .outputs
                    .iter()
                    .find(|output| output.file_name == filename)
                    .ok_or_else(|| {
                        AppError::InvalidRequest("file is not a declared output".into())
                    })?;
                (request.node_id, output)
            }
            FileDirection::Input => {
                let input = node
                    .data
                    .inputs
                    .iter()
                    .find(|input| input.file_name == filename)
                    .ok_or_else(|| {
                        AppError::InvalidRequest("file is not a declared input".into())
                    })?;
                resolve_input_source(&graph, &input.source)?
            }
        };
        let filename = output_catalog_filename(output);
        let mut db = self.db.clone();
        let node_run = node_run_at(&mut db, run.uuid, producer, run.revision)
            .await?
            .ok_or(AppError::NodeNotFound(producer))?;
        if node_run.status != "Succeeded" {
            return Err(AppError::InvalidRequest(
                "file producer has not succeeded".into(),
            ));
        }
        let file = db
            .get_file_by_node_run_filename(node_run.uuid, &filename)
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
        Ok(Transfer {
            catalog,
            presigned,
            multipart: config.multipart_enabled && storage.supports_multipart(),
        })
    }

    pub async fn list_runs(
        &self,
        page: PageQuery,
        graph_id: Option<Uuid>,
    ) -> Result<Page<Run>, AppError> {
        let limit = page.limit()?;
        let mut select = Run::all();
        if let Some(before) = page.before {
            select = select.filter(Run::fields().id().lt(before));
        }
        if let Some(graph_id) = graph_id {
            select = select.filter(Run::fields().graph_id().eq(graph_id));
        }
        let items = select
            .order_by(Run::fields().id().desc())
            .limit(limit + 1)
            .exec(&mut self.db.clone())
            .await?;
        Ok(Page::new(items, limit, |row| row.id))
    }

    pub(super) async fn read_run_at(
        &self,
        id: Uuid,
        revision: Option<u64>,
    ) -> Result<Run, AppError> {
        let mut run = Run::filter_by_uuid(id)
            .first()
            .exec(&mut self.db.clone())
            .await?
            .ok_or(AppError::RunIdNotFound(id))?;
        if let Some(revision) = revision {
            if revision > run.revision {
                return Err(AppError::InvalidRequest(
                    "revision exceeds the current run revision".into(),
                ));
            }
            run.revision = revision;
        }
        Ok(run)
    }

    pub async fn inspect_run(&self, id: Uuid) -> Result<RunView, AppError>
    where
        S: GraphStore<Uuid, G = DAG> + Clone,
        S::Err: Send + Sync + 'static,
    {
        self.inspect_run_at(id, None).await
    }

    /// Inspect effective node runs up to a revision, retaining each producer's graph version.
    pub async fn inspect_run_at(&self, id: Uuid, revision: Option<u64>) -> Result<RunView, AppError>
    where
        S: GraphStore<Uuid, G = DAG> + Clone,
        S::Err: Send + Sync + 'static,
    {
        let run = self.read_run_at(id, revision).await?;
        // Run is unique by (data_id, graph_id); upgrades and reruns preserve its UUID.
        // Keep `current` in the response for compatibility without reloading the same row.
        let graph = self.read_graph(run.graph_id, Some(run.version)).await?;
        let node_runs = self
            .run_with(run.data_id, run.graph_id)
            .graph_state_at(run.clone(), &graph)
            .await?;
        Ok(RunView {
            run,
            graph,
            node_runs,
        })
    }
}

impl<S> RunApp<S>
where
    S: GraphStore<Uuid, G = DAG> + Clone,
    <S as GraphStore<Uuid>>::Err: Send + Sync + 'static,
{
    /// Returns the state of `node_id`: its node run combined with the graph node.
    pub async fn node_state(
        &self,
        node_id: Uuid,
    ) -> Result<(NodeRun, graph::types::Node), AppError> {
        let run = self.run().await?;
        // Load the graph before the node run so no node run row is held across an
        // await (it keeps this future `Send`, so it can be used as a handler).
        let graph = self.graph(run.version).await?;
        let mut db = self.db.clone();
        // A node may have a node run per revision; its live state is the row
        // with the greatest revision at or before the run's current revision.
        let node_run = node_run_at(&mut db, run.uuid, node_id, run.revision)
            .await?
            .ok_or(AppError::NodeRunNotFound(node_id))?;

        let node_info = graph
            .node(node_id)
            .map(|node| node.data.clone())
            .ok_or(AppError::NodeNotFound(node_id))?;

        Ok((node_run, node_info))
    }

    /// Returns effective node states at or before the run's current revision.
    pub async fn graph_state(&self) -> Result<Vec<(NodeRun, graph::types::Node)>, AppError> {
        let run = self.run().await?;
        // Load the graph before the node runs so no node run row is held across
        // an await (it keeps this future `Send`, so it can be used as a handler).
        let graph = self.graph(run.version).await?;
        self.graph_state_at(run, &graph).await
    }

    async fn graph_state_at(
        &self,
        run: Run,
        graph: &DAG,
    ) -> Result<Vec<(NodeRun, graph::types::Node)>, AppError> {
        let mut db = self.db.clone();
        let rows = NodeRun::filter_by_run_id(run.uuid)
            .filter(NodeRun::fields().revision().le(run.revision))
            .filter(NodeRun::fields().ineffective().eq(0))
            .exec(&mut db)
            .await?;

        // A node may have a node run per revision; its live state is the row
        // with the greatest revision at or before the run's current revision.
        let mut live: std::collections::HashMap<Uuid, NodeRun> = std::collections::HashMap::new();
        for row in rows {
            let replace = live
                .get(&row.node_id)
                .is_none_or(|current| row.revision > current.revision);
            if replace {
                live.insert(row.node_id, row);
            }
        }

        let nodes = live
            .into_values()
            .filter_map(|node_run| {
                graph
                    .node(node_run.node_id)
                    .map(|node| (node_run, node.data.clone()))
            })
            .collect();

        Ok(nodes)
    }

    /// Loads this run, or fails with [`AppError::RunNotFound`].
    async fn run(&self) -> Result<Run, AppError> {
        let mut db = self.db.clone();
        Run::filter_by_data_id_and_graph_id(self.data_id, self.graph_id)
            .first()
            .exec(&mut db)
            .await?
            .ok_or(AppError::RunNotFound {
                data_id: self.data_id,
                graph_id: self.graph_id,
            })
    }

    /// Loads the graph at `version` from the graph store.
    async fn graph(&self, version: u64) -> Result<DAG, AppError> {
        self.graph_store
            .clone()
            .load(self.graph_id, Some(version))
            .await
            .map_err(|error| AppError::Graph(Box::new(error)))
    }
}

/// Latest effective state at a revision; shared by node inspection and file resolution.
pub(super) async fn node_run_at(
    db: &mut toasty::Db,
    run_id: Uuid,
    node_id: Uuid,
    revision: u64,
) -> Result<Option<NodeRun>, AppError> {
    Ok(NodeRun::filter_by_run_id(run_id)
        .filter(NodeRun::fields().node_id().eq(node_id))
        .filter(NodeRun::fields().revision().le(revision))
        .filter(NodeRun::fields().ineffective().eq(0))
        .order_by(NodeRun::fields().revision().desc())
        .first()
        .exec(db)
        .await?)
}
