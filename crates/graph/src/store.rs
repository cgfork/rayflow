//! MySQL persistence for versioned graph definitions.
//!
//! Register [`models`] when constructing a database handle. [`Db`](toasty::Db)
//! and [`Transaction`](toasty::Transaction) implement the graph store traits
//! directly through [`Executor`]. The table layout matches `rayflow.sql`.

use jiff::Timestamp;
use toasty::{Executor, Json};
use uuid::Uuid;

use crate::{DraftGraphStore, GraphStore, Page, PageRequest, types::DAG, validator};

/// One version of a graph, including audit metadata.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, toasty::Model)]
#[table = "graph"]
#[unique(uuid, version)]
pub struct GraphRecord {
    #[key]
    #[auto]
    pub id: u64,
    #[index]
    pub uuid: Uuid,
    #[column(type = varchar(255))]
    pub name: String,
    pub version: u64,
    #[column(type = json)]
    pub graph: Json<DAG>,
    pub updated_at: Timestamp,
    #[column(type = varchar(64))]
    pub updated_by: String,
    pub created_at: Timestamp,
    #[column(type = varchar(64))]
    pub created_by: String,
}

/// The editable draft of a graph, including audit metadata.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, toasty::Model)]
#[table = "graph_draft"]
#[unique(uuid)]
pub struct GraphDraftRecord {
    #[key]
    #[auto]
    pub id: u64,
    pub uuid: Uuid,
    #[column(type = varchar(255))]
    pub name: String,
    #[column(type = json)]
    pub graph: Json<DAG>,
    pub updated_at: Timestamp,
    #[column(type = varchar(64))]
    pub updated_by: String,
    pub created_at: Timestamp,
    #[column(type = varchar(64))]
    pub created_by: String,
}

/// Models required by the graph store.
pub fn models() -> toasty::ModelSet {
    toasty::models!(GraphRecord, GraphDraftRecord)
}

/// Error returned by the persistent graph store.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Toasty(#[from] toasty::Error),
    #[error("graph not found: {graph_id}, requested version: {version:?}")]
    GraphNotFound {
        graph_id: Uuid,
        version: Option<u64>,
    },
    #[error("graph JSON does not match row metadata: {graph_id}, version: {version}")]
    MetadataMismatch { graph_id: Uuid, version: u64 },
    #[error("graph draft not found: {0}")]
    DraftNotFound(Uuid),
    #[error("graph draft JSON does not match row metadata: {0}")]
    DraftMetadataMismatch(Uuid),
    #[error("invalid graph draft: {0}")]
    InvalidDraft(#[source] validator::WorkflowValidationError),
    #[error("graph version overflow: {0}")]
    VersionOverflow(Uuid),
}

impl TryFrom<GraphRecord> for DAG {
    type Error = Error;

    fn try_from(record: GraphRecord) -> Result<Self, Self::Error> {
        let mut graph = record.graph.0;
        if graph.id != record.uuid || graph.name != record.name || graph.version != record.version {
            return Err(Error::MetadataMismatch {
                graph_id: record.uuid,
                version: record.version,
            });
        }
        graph.node_count = graph.nodes.len();
        graph.updated_at = record.updated_at;
        graph.updated_by = record.updated_by;
        graph.created_at = record.created_at;
        graph.created_by = record.created_by;
        Ok(graph)
    }
}

impl TryFrom<GraphDraftRecord> for DAG {
    type Error = Error;

    fn try_from(record: GraphDraftRecord) -> Result<Self, Self::Error> {
        let mut graph = record.graph.0;
        if graph.id != record.uuid || graph.name != record.name {
            return Err(Error::DraftMetadataMismatch(record.uuid));
        }
        graph.node_count = graph.nodes.len();
        graph.updated_at = record.updated_at;
        graph.updated_by = record.updated_by;
        graph.created_at = record.created_at;
        graph.created_by = record.created_by;
        Ok(graph)
    }
}

impl<B: Executor> DraftGraphStore<Uuid> for B {
    async fn list(&mut self, page: PageRequest) -> Result<Page<Self::G>, Error> {
        let mut query = GraphDraftRecord::all();
        if let Some(before) = page.before() {
            query = query.filter(GraphDraftRecord::fields().id().lt(before));
        }
        let rows = query
            .order_by(GraphDraftRecord::fields().id().desc())
            .limit(page.limit() + 1)
            .exec(self)
            .await?;
        let page = Page::new(rows, page.limit(), |row| row.id);
        Ok(Page {
            items: page
                .items
                .into_iter()
                .map(DAG::try_from)
                .collect::<Result<_, _>>()?,
            next_cursor: page.next_cursor,
        })
    }

    async fn read(&mut self, graph_id: Uuid) -> Result<DAG, Error> {
        GraphDraftRecord::filter_by_uuid(graph_id)
            .first()
            .exec(self)
            .await?
            .ok_or(Error::DraftNotFound(graph_id))?
            .try_into()
    }

    async fn write(&mut self, graph: &DAG, actor: &str) -> Result<(), Error> {
        let mut graph = graph.clone();
        graph.node_count = graph.nodes.len();
        let now = Timestamp::now();
        let existing = GraphDraftRecord::filter_by_uuid(graph.id)
            .first()
            .exec(&mut *self)
            .await?;

        if let Some(record) = existing {
            GraphDraftRecord::filter_by_id(record.id)
                .update()
                .name(graph.name.clone())
                .graph(Json(graph.clone()))
                .updated_at(now)
                .updated_by(actor.to_owned())
                .exec(self)
                .await?;
        } else {
            GraphDraftRecord::create()
                .uuid(graph.id)
                .name(graph.name.clone())
                .graph(Json(graph.clone()))
                .updated_at(now)
                .updated_by(actor.to_owned())
                .created_at(now)
                .created_by(actor.to_owned())
                .exec(self)
                .await?;
        }
        Ok(())
    }

    /// Starts a transaction or savepoint, leaving any outer transaction to the caller.
    async fn publish(&mut self, graph_id: Uuid, actor: &str) -> Result<(), Error> {
        let mut tx = self.transaction().await?;
        let mut graph = tx.read(graph_id).await?;
        validator::validate_workflow(&graph).map_err(Error::InvalidDraft)?;

        let latest = GraphRecord::filter_by_uuid(graph_id)
            .order_by(GraphRecord::fields().version().desc())
            .first()
            .exec(&mut tx)
            .await?;
        graph.version = latest
            .map_or(0, |record| record.version)
            .checked_add(1)
            .ok_or(Error::VersionOverflow(graph_id))?;

        let now = Timestamp::now();
        GraphRecord::create()
            .uuid(graph.id)
            .name(graph.name.clone())
            .version(graph.version)
            .graph(Json(graph))
            .updated_at(now)
            .updated_by(actor.to_owned())
            .created_at(now)
            .created_by(actor.to_owned())
            .exec(&mut tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }
}

impl<B: Executor> GraphStore<Uuid> for B {
    type G = DAG;
    type Err = Error;

    async fn graphs(
        &mut self,
        page: PageRequest,
        graph_id: Option<Uuid>,
    ) -> Result<Page<Self::G>, Error> {
        let mut query = GraphRecord::all();
        if let Some(before) = page.before() {
            query = query.filter(GraphRecord::fields().id().lt(before));
        }
        if let Some(graph_id) = graph_id {
            query = query.filter(GraphRecord::fields().uuid().eq(graph_id));
        }
        let rows = query
            .order_by(GraphRecord::fields().id().desc())
            .limit(page.limit() + 1)
            .exec(self)
            .await?;
        let page = Page::new(rows, page.limit(), |row| row.id);
        Ok(Page {
            items: page
                .items
                .into_iter()
                .map(DAG::try_from)
                .collect::<Result<_, _>>()?,
            next_cursor: page.next_cursor,
        })
    }

    /// Loads an exact version, or the highest version when none is specified.
    async fn load(&mut self, graph_id: Uuid, version: Option<u64>) -> Result<DAG, Error> {
        let mut query = GraphRecord::filter_by_uuid(graph_id);
        if let Some(version) = version {
            query = query.filter(GraphRecord::fields().version().eq(version));
        }
        let record = query
            .order_by(GraphRecord::fields().version().desc())
            .first()
            .exec(self)
            .await?
            .ok_or(Error::GraphNotFound { graph_id, version })?;
        record.try_into()
    }
}
