//! MySQL persistence for versioned graph definitions.
//!
//! Register [`models`] when constructing a shared database handle, or use
//! [`MySqlGraphStore::connect`]. The table layout matches `rayflow.sql`.

use jiff::Timestamp;
use toasty::{Db, Json};
use uuid::Uuid;

use crate::{GraphStore, types::DAG, validator};

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
    InvalidDraft(String),
    #[error("graph version overflow: {0}")]
    VersionOverflow(Uuid),
}

impl TryFrom<GraphRecord> for DAG {
    type Error = Error;

    fn try_from(record: GraphRecord) -> Result<Self, Self::Error> {
        let graph = record.graph.0;
        if graph.id != record.uuid || graph.name != record.name || graph.version != record.version {
            return Err(Error::MetadataMismatch {
                graph_id: record.uuid,
                version: record.version,
            });
        }
        Ok(graph)
    }
}

impl TryFrom<GraphDraftRecord> for DAG {
    type Error = Error;

    fn try_from(record: GraphDraftRecord) -> Result<Self, Self::Error> {
        let graph = record.graph.0;
        if graph.id != record.uuid || graph.name != record.name {
            return Err(Error::DraftMetadataMismatch(record.uuid));
        }
        Ok(graph)
    }
}

/// A graph store backed by the MySQL `graph` table.
#[derive(Clone)]
pub struct MySqlGraphStore {
    db: Db,
}

impl MySqlGraphStore {
    /// Uses an existing database handle with [`GraphRecord`] registered.
    pub fn new(db: Db) -> Self {
        Self { db }
    }

    /// Connects to MySQL and registers the graph model without creating tables.
    pub async fn connect(url: &str) -> Result<Self, Error> {
        let mut builder = Db::builder();
        builder.models(models());
        Ok(Self::new(builder.connect(url).await?))
    }

    /// Returns the database handle for graph metadata queries and updates.
    pub fn db(&self) -> &Db {
        &self.db
    }

    /// Inserts a graph version with audit metadata.
    ///
    /// Existing `(uuid, version)` pairs are rejected rather than overwritten.
    pub async fn insert(&self, graph: &DAG, actor: &str) -> Result<GraphRecord, Error> {
        let now = Timestamp::now();
        let mut db = self.db.clone();
        Ok(GraphRecord::create()
            .uuid(graph.id)
            .name(graph.name.clone())
            .version(graph.version)
            .graph(Json(graph.clone()))
            .updated_at(now)
            .updated_by(actor.to_owned())
            .created_at(now)
            .created_by(actor.to_owned())
            .exec(&mut db)
            .await?)
    }

    /// Reads the editable draft for a graph.
    pub async fn read_draft(&self, graph_id: Uuid) -> Result<GraphDraftRecord, Error> {
        let mut db = self.db.clone();
        let record = GraphDraftRecord::filter_by_uuid(graph_id)
            .first()
            .exec(&mut db)
            .await?
            .ok_or(Error::DraftNotFound(graph_id))?;
        let _: DAG = record.clone().try_into()?;
        Ok(record)
    }

    /// Loads the graph JSON from its editable draft.
    pub async fn load_draft(&self, graph_id: Uuid) -> Result<DAG, Error> {
        self.read_draft(graph_id).await?.try_into()
    }

    /// Creates or updates the editable draft for a graph.
    pub async fn write_draft(&self, graph: &DAG, actor: &str) -> Result<GraphDraftRecord, Error> {
        let now = Timestamp::now();
        let mut db = self.db.clone();
        let existing = GraphDraftRecord::filter_by_uuid(graph.id)
            .first()
            .exec(&mut db)
            .await?;

        if let Some(mut record) = existing {
            GraphDraftRecord::filter_by_id(record.id)
                .update()
                .name(graph.name.clone())
                .graph(Json(graph.clone()))
                .updated_at(now)
                .updated_by(actor.to_owned())
                .exec(&mut db)
                .await?;
            record.name = graph.name.clone();
            record.graph = Json(graph.clone());
            record.updated_at = now;
            record.updated_by = actor.to_owned();
            Ok(record)
        } else {
            Ok(GraphDraftRecord::create()
                .uuid(graph.id)
                .name(graph.name.clone())
                .graph(Json(graph.clone()))
                .updated_at(now)
                .updated_by(actor.to_owned())
                .created_at(now)
                .created_by(actor.to_owned())
                .exec(&mut db)
                .await?)
        }
    }

    /// Validates and publishes a draft as the next graph version.
    pub async fn publish_draft(&self, graph_id: Uuid, actor: &str) -> Result<GraphRecord, Error> {
        let mut db = self.db.clone();
        let mut tx = db.transaction().await?;
        let draft = GraphDraftRecord::filter_by_uuid(graph_id)
            .first()
            .exec(&mut tx)
            .await?
            .ok_or(Error::DraftNotFound(graph_id))?;
        let mut graph: DAG = draft.try_into()?;
        validator::validate(&graph).map_err(|error| Error::InvalidDraft(error.to_string()))?;

        let latest = GraphRecord::filter_by_uuid(graph_id)
            .order_by(GraphRecord::fields().version().desc())
            .first()
            .exec(&mut tx)
            .await?;
        let version = match latest {
            Some(record) => record
                .version
                .checked_add(1)
                .ok_or(Error::VersionOverflow(graph_id))?,
            None => 0,
        };
        graph.version = version;

        let now = Timestamp::now();
        let record = GraphRecord::create()
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
        Ok(record)
    }
}

impl GraphStore<Uuid> for MySqlGraphStore {
    type G = DAG;
    type Err = Error;

    /// Loads an exact version, or the highest version when none is specified.
    async fn load(&self, graph_id: Uuid, version: Option<u64>) -> Result<DAG, Error> {
        let mut db = self.db.clone();
        let mut query = GraphRecord::filter_by_uuid(graph_id);
        if let Some(version) = version {
            query = query.filter(GraphRecord::fields().version().eq(version));
        }
        let record = query
            .order_by(GraphRecord::fields().version().desc())
            .first()
            .exec(&mut db)
            .await?
            .ok_or(Error::GraphNotFound { graph_id, version })?;
        record.try_into()
    }
}
