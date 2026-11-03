//! Published graph read operations.

use graph::GraphStore;
use graph::types::DAG;
use uuid::Uuid;

use super::{App, AppError, Page, PageQuery};

impl<S> App<S> {
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
