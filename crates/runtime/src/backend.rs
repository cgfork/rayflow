//! MySQL execution backend, state, and configuration for command application.

use crate::{Error, models};
use toasty::Db;

/// The node status.
/// 1. Ready -> Running -> Succeeded/Failed/Terminated
/// 2. Ready -> Running -> Pending -> Succeeded/Failed/Terminated
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Status {
    Ready,
    Running,
    Pending,
    Succeeded,
    Failed,
    Terminated,
}

impl std::fmt::Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Status::Ready => "Ready",
            Status::Running => "Running",
            Status::Pending => "Pending",
            Status::Succeeded => "Succeeded",
            Status::Failed => "Failed",
            Status::Terminated => "Terminated",
        })
    }
}

impl std::str::FromStr for Status {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Ready" => Ok(Status::Ready),
            "Running" => Ok(Status::Running),
            "Pending" => Ok(Status::Pending),
            "Succeeded" => Ok(Status::Succeeded),
            "Failed" => Ok(Status::Failed),
            "Terminated" => Ok(Status::Terminated),
            _ => Err(()),
        }
    }
}

/// A MySQL-backed task backend.
#[derive(Clone)]
pub struct Backend<S> {
    pub(crate) db: Db,
    pub(crate) graph_store: S,
}

impl<S> Backend<S> {
    /// Creates a backend from an existing database handle and the graph store
    /// used to resolve graph structure.
    pub fn new(db: Db, graph_store: S) -> Self {
        Self { db, graph_store }
    }

    /// Connects to `url`, registering the runtime's Toasty models.
    ///
    /// This is the recommended constructor: the models must be registered on the
    /// [`Db`] for the query builders the backend uses to work. Combine with
    /// [`models`] when constructing the [`Db`] yourself.
    pub async fn connect(url: &str, graph_store: S) -> Result<Self, Error> {
        let mut builder = toasty::Db::builder();
        builder.models(models());
        let db = builder.connect(url).await?;
        Ok(Self::new(db, graph_store))
    }

    /// Returns the underlying database handle.
    pub fn db(&self) -> &Db {
        &self.db
    }

    /// Returns a mutable reference to the underlying database handle.
    pub fn db_mut(&mut self) -> &mut Db {
        &mut self.db
    }
}
