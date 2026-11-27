//! Published graph read operations.

use std::collections::HashMap;

use graph::diff::DagDiff;
use graph::types::DAG;
use graph::{Diff, DraftGraphStore, ForkPoint, GraphStore};
use scheduler::actuator::NodeConfig;
use serde::Serialize;
use uuid::Uuid;

use super::{App, AppError, Page, PageQuery};

#[derive(Debug, Serialize)]
pub(crate) struct GraphComparison {
    changes: Vec<DagDiff>,
    before_node_names: HashMap<Uuid, String>,
    after_node_names: HashMap<Uuid, String>,
}

impl<S> App<S> {
    pub async fn shared_graph_nodes(
        &self,
        graph_id: Uuid,
        from_version: u64,
        version: u64,
    ) -> Result<Vec<Uuid>, AppError>
    where
        S: GraphStore<Uuid, G = DAG> + Clone,
        S::Err: Send + Sync + 'static,
    {
        if version <= from_version {
            return Err(AppError::InvalidRequest(
                "the target version must be newer than the current version".into(),
            ));
        }
        let previous = self.read_graph(graph_id, Some(from_version)).await?;
        let target = self.read_graph(graph_id, Some(version)).await?;
        Ok(target.shared_nodes(&previous).map(|node| node.id).collect())
    }

    /// Compares two exact published versions from the baseline to the target.
    pub(crate) async fn compare_graphs(
        &self,
        from_graph_id: Uuid,
        from_version: u64,
        graph_id: Uuid,
        version: u64,
    ) -> Result<GraphComparison, AppError>
    where
        S: GraphStore<Uuid, G = DAG> + Clone,
        S::Err: Send + Sync + 'static,
    {
        let before = self.read_graph(from_graph_id, Some(from_version)).await?;
        let after = self.read_graph(graph_id, Some(version)).await?;
        Ok(GraphComparison {
            changes: before.diff(&after).unwrap_or_default(),
            before_node_names: before
                .nodes
                .into_iter()
                .map(|node| (node.id, node.name))
                .collect(),
            after_node_names: after
                .nodes
                .into_iter()
                .map(|node| (node.id, node.name))
                .collect(),
        })
    }

    pub async fn list_graphs(
        &self,
        page: PageQuery,
        graph_id: Option<Uuid>,
    ) -> Result<Page<DAG>, AppError>
    where
        S: GraphStore<Uuid, G = DAG> + Clone,
        S::Err: Send + Sync + 'static,
    {
        let page = page.request()?;
        self.graph_store
            .clone()
            .graphs(page, graph_id)
            .await
            .map_err(|error| AppError::Graph(Box::new(error)))
    }

    /// Loads a published graph through the configured graph store.
    pub async fn read_graph(&self, graph_id: Uuid, version: Option<u64>) -> Result<DAG, AppError>
    where
        S: GraphStore<Uuid, G = DAG> + Clone,
        S::Err: Send + Sync + 'static,
    {
        self.graph_store
            .clone()
            .load(graph_id, version)
            .await
            .map_err(|error| AppError::Graph(Box::new(error)))
    }

    pub async fn list_drafts(&self, page: PageQuery) -> Result<Page<DAG>, AppError> {
        let page = page.request()?;
        Ok(self.db.clone().list(page).await?)
    }

    pub async fn read_graph_draft(&self, graph_id: Uuid) -> Result<DAG, AppError> {
        Ok(self.db.clone().read(graph_id).await?)
    }

    pub async fn write_graph_draft(
        &self,
        graph_id: Uuid,
        graph: DAG,
        actor: &str,
    ) -> Result<DAG, AppError> {
        if graph.id != graph_id {
            return Err(AppError::GraphIdMismatch {
                expected: graph_id,
                actual: graph.id,
            });
        }
        let mut db = self.db.clone();
        let mut tx = db.transaction().await?;
        tx.write(&graph, actor).await?;
        let graph = tx.read(graph_id).await?;
        tx.commit().await?;
        Ok(graph)
    }

    pub async fn publish_graph_draft(&self, graph_id: Uuid, actor: &str) -> Result<DAG, AppError> {
        let mut db = self.db.clone();
        let mut tx = db.transaction().await?;
        tx.publish(graph_id, actor).await?;
        let graph = tx.load(graph_id, None).await?;
        super::asset::validate_graph_assets(&mut tx, &graph).await?;
        for node in &graph.nodes {
            for settings in node.settings.named_settings() {
                if graph.updated_at.checked_add(settings.timeout).is_err()
                    || graph.updated_at.checked_sub(settings.timeout).is_err()
                {
                    return Err(AppError::InvalidRequest(format!(
                        "{}: timeout is outside timestamp range",
                        node.name
                    )));
                }
                NodeConfig::create()
                    .uuid(Uuid::new_v4())
                    .node_id(node.id)
                    .version(graph.version)
                    .name(settings.name)
                    .annotation_match(settings.annotation_match)
                    .config(settings.config.clone())
                    .lease(settings.timeout_ms())
                    .required_cpu_cores(settings.resource.cpu_cores)
                    .required_memory_bytes(settings.resource.memory_bytes)
                    .required_gpu_memory_bytes(
                        settings.resource.gpu_memory_bytes.unwrap_or_default(),
                    )
                    .created_at(graph.created_at)
                    .updated_at(graph.updated_at)
                    .created_by(actor.to_owned())
                    .updated_by(actor.to_owned())
                    .exec(&mut tx)
                    .await?;
            }
        }
        tx.commit().await?;
        Ok(graph)
    }
}
