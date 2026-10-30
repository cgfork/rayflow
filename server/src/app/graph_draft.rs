//! Graph draft persistence operations.

use graph::store::{GraphDraftRecord, GraphRecord, MySqlGraphStore};
use graph::types::DAG;
use uuid::Uuid;

use super::{App, AppError};

impl<S> App<S> {
    /// Reads the editable draft for `graph_id`.
    pub async fn read_graph_draft(&self, graph_id: Uuid) -> Result<GraphDraftRecord, AppError> {
        MySqlGraphStore::new(self.db.clone())
            .read_draft(graph_id)
            .await
            .map_err(Into::into)
    }

    /// Creates or updates the editable draft for `graph_id`.
    pub async fn write_graph_draft(
        &self,
        graph_id: Uuid,
        graph: DAG,
        actor: &str,
    ) -> Result<GraphDraftRecord, AppError> {
        if graph.id != graph_id {
            return Err(AppError::GraphIdMismatch {
                expected: graph_id,
                actual: graph.id,
            });
        }
        MySqlGraphStore::new(self.db.clone())
            .write_draft(&graph, actor)
            .await
            .map_err(Into::into)
    }

    /// Validates and publishes the current draft as the next graph version.
    pub async fn publish_graph_draft(
        &self,
        graph_id: Uuid,
        actor: &str,
    ) -> Result<GraphRecord, AppError> {
        MySqlGraphStore::new(self.db.clone())
            .publish_draft(graph_id, actor)
            .await
            .map_err(Into::into)
    }
}
