//! Published graph read operations.

use graph::types::DAG;
use graph::{ForkPoint, GraphStore};
use uuid::Uuid;

use super::{App, AppError, Page, PageQuery};

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
}
