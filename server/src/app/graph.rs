//! Published graph read operations.

use graph::GraphStore;
use graph::types::DAG;
use uuid::Uuid;

use super::{App, AppError};

impl<S> App<S> {
    /// Loads a published graph through the configured graph store.
    pub async fn read_graph(&self, graph_id: Uuid, version: Option<u64>) -> Result<DAG, AppError>
    where
        S: GraphStore<Uuid, G = DAG>,
        S::Err: Send + Sync + 'static,
    {
        self.graph_store
            .load(graph_id, version)
            .await
            .map_err(|error| AppError::Graph(Box::new(error)))
    }
}
