mod agent;
mod error;
mod graph;
mod job;
mod operation;
mod run;
mod user;

pub use agent::{AgentInstance, AgentView};
pub use error::{AppError, ErrorBody};
pub use run::RunView;

use std::sync::Arc;

use ::graph::PageRequest;
use serde::Deserialize;
use tokio::sync::watch;

#[derive(Debug, Default, Deserialize)]
pub struct PageQuery {
    pub before: Option<u64>,
    pub limit: Option<usize>,
}

impl PageQuery {
    pub(crate) fn limit(&self) -> Result<usize, AppError> {
        Ok(self.request()?.limit())
    }

    pub(crate) fn request(&self) -> Result<PageRequest, AppError> {
        PageRequest::new(self.before, self.limit.unwrap_or(50))
            .map_err(|error| AppError::InvalidRequest(error.to_string()))
    }
}

pub use ::graph::Page;

/// The consumed command offset, shared between the app and its waiters.
#[derive(Clone)]
pub struct Offsets {
    sender: Arc<watch::Sender<u64>>,
}

impl Default for Offsets {
    fn default() -> Self {
        Self::new()
    }
}

impl Offsets {
    pub fn new() -> Self {
        Self {
            sender: Arc::new(watch::channel(0).0),
        }
    }

    pub fn get(&self) -> u64 {
        *self.sender.borrow()
    }

    pub fn set(&self, offset: u64) {
        self.sender.send_if_modified(|current| {
            if offset > *current {
                *current = offset;
                true
            } else {
                false
            }
        });
    }

    pub async fn wait(&self, offset: u64) {
        let mut receiver = self.sender.subscribe();
        let _ = receiver.wait_for(|current| *current >= offset).await;
    }
}

use toasty::Db;
use uuid::Uuid;

use crate::Config;

/// Application over a MySQL backend and a graph store.
///
/// `App` owns the shared command offset and hands out a [`RunApp`] for a
/// specific `(data_id, graph_id)` run through [`run_with`](Self::run_with). It
/// is the state the HTTP API is served from.
#[derive(Clone)]
pub struct App<S> {
    db: Db,
    pub(crate) config: Config,
    pub(crate) auth: Option<crate::auth::Auth>,
    offsets: Offsets,
    graph_store: S,
}

/// A single run: the commands that drive it and the state it produces.
pub struct RunApp<S> {
    db: Db,
    data_id: Uuid,
    graph_id: Uuid,
    offsets: Offsets,
    graph_store: S,
}

impl<S> App<S> {
    /// Creates an app over `db`, tracking the consumed command offset and
    /// serving graphs from `graph_store`.
    pub fn new(db: Db, config: Config, graph_store: S) -> Self {
        let offsets = Offsets::new();
        Self {
            db,
            config,
            auth: None,
            offsets,
            graph_store,
        }
    }

    /// Installs shared login providers and session storage for the HTTP router.
    pub fn with_auth(mut self, auth: crate::auth::Auth) -> Self {
        self.config.auth = auth.config().clone();
        self.auth = Some(auth);
        self
    }

    /// Connects to `url`, registering runtime and scheduling models, and builds an
    /// app over `db` (see [`new`](Self::new)).
    pub async fn connect(url: &str, config: Config, graph_store: S) -> Result<Self, toasty::Error> {
        let mut builder = Db::builder();
        builder.models(crate::models());
        let db = builder.connect(url).await?;
        Ok(Self::new(db, config, graph_store))
    }

    /// Returns the shared command offset.
    ///
    /// A consumer that advances the offset calls [`Offsets::set`], which wakes
    /// every waiter.
    pub fn offsets(&self) -> &Offsets {
        &self.offsets
    }

    /// Builds the [`RunApp`] for the run identified by `data_id` and `graph_id`.
    pub fn run_with(&self, data_id: Uuid, graph_id: Uuid) -> RunApp<S>
    where
        S: Clone,
    {
        RunApp {
            db: self.db.clone(),
            data_id,
            graph_id,
            offsets: self.offsets.clone(),
            graph_store: self.graph_store.clone(),
        }
    }
}

/*

*/

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_zero_page_limits() {
        let query = PageQuery {
            before: None,
            limit: Some(0),
        };
        assert!(query.request().is_err());
    }

    #[test]
    fn applies_default_page_limit() {
        let query = PageQuery::default();
        assert_eq!(query.request().unwrap().limit(), 50);
    }

    #[tokio::test]
    async fn offsets_only_move_forward() {
        let offsets = Offsets::new();
        offsets.set(10);
        offsets.set(5);
        assert_eq!(offsets.get(), 10);
        offsets.wait(10).await;
        assert_eq!(offsets.get(), 10);
    }
}
