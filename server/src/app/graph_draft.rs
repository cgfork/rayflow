//! Graph draft persistence operations.

use graph::types::DAG;
use graph::{DraftGraphStore, GraphStore};
use scheduler::actuator::NodeConfig;
use uuid::Uuid;

use super::{App, AppError, Page, PageQuery};

impl<S> App<S> {
    pub async fn list_drafts(&self, page: PageQuery) -> Result<Page<DAG>, AppError> {
        let page = page.request()?;
        Ok(self.db.clone().list(page).await?)
    }

    /// Reads the editable draft for `graph_id`.
    pub async fn read_graph_draft(&self, graph_id: Uuid) -> Result<DAG, AppError> {
        Ok(self.db.clone().read(graph_id).await?)
    }

    /// Creates or updates the editable draft for `graph_id`.
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

    /// Validates and publishes the current draft as the next graph version.
    pub async fn publish_graph_draft(&self, graph_id: Uuid, actor: &str) -> Result<DAG, AppError> {
        let mut db = self.db.clone();
        let mut tx = db.transaction().await?;
        tx.publish(graph_id, actor).await?;
        let graph = tx.load(graph_id, None).await?;
        for node in &graph.nodes {
            if graph.updated_at.checked_add(node.settings.timeout).is_err()
                || graph.updated_at.checked_sub(node.settings.timeout).is_err()
            {
                return Err(AppError::InvalidRequest(format!(
                    "{}: timeout is outside timestamp range",
                    node.name
                )));
            }
            NodeConfig::create()
                .node_id(node.id)
                .version(graph.version)
                .annotation_match(String::new())
                .config(node.settings.config.clone())
                .lease(node.settings.timeout_ms())
                .required_cpu_cores(node.settings.resource.cpu_cores)
                .required_memory_bytes(node.settings.resource.memory_bytes)
                .created_at(graph.created_at)
                .updated_at(graph.updated_at)
                .created_by(actor.to_owned())
                .updated_by(actor.to_owned())
                .exec(&mut tx)
                .await?;
        }
        tx.commit().await?;
        Ok(graph)
    }
}
