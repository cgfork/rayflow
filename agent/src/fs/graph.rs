//! Durable cache for versioned graph definitions.
//!
//! Graph data is shared by job executions, so it lives outside each
//! [`crate::fs::job::JobFs`] directory. Each graph version is stored as:
//!
//! ```text
//! {workdir}/graph/{graph_id}/{version}/graph.json
//! ```

use std::{io, path::PathBuf};

use graph::{GraphStore, types::DAG};
use tokio::fs;
use uuid::Uuid;

use crate::{errors::Error, read_json, settings::Settings, write_json};

/// A filesystem-backed cache implementing [`graph::GraphStore`].
#[derive(Clone, Debug)]
pub struct GraphFs {
    settings: Settings,
}

impl GraphFs {
    /// Opens the graph cache below the configured workdir.
    ///
    /// A graph version is stored at
    /// `workdir/graph/{graph_id}/{version}/graph.json`.
    pub fn new(settings: Settings) -> Self {
        Self { settings }
    }

    /// Returns the root directory containing all cached graphs.
    pub fn dir(&self) -> PathBuf {
        self.settings.workdir.join("graph")
    }

    fn graph_dir(&self, graph_id: Uuid) -> PathBuf {
        self.dir().join(graph_id.to_string())
    }

    fn version_dir(&self, graph_id: Uuid, version: u64) -> PathBuf {
        self.graph_dir(graph_id).join(version.to_string())
    }

    fn graph_path(&self, graph_id: Uuid, version: u64) -> PathBuf {
        self.version_dir(graph_id, version).join("graph.json")
    }

    async fn latest_version(&self, graph_id: Uuid) -> Result<u64, Error> {
        let mut entries = fs::read_dir(self.graph_dir(graph_id)).await?;
        let mut latest = None;
        while let Some(entry) = entries.next_entry().await? {
            if !entry.file_type().await?.is_dir() {
                continue;
            }
            let Ok(version) = entry.file_name().to_string_lossy().parse::<u64>() else {
                continue;
            };
            latest = Some(latest.map_or(version, |current: u64| current.max(version)));
        }
        latest.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("graph has no cached versions: {graph_id}"),
            )
            .into()
        })
    }

    async fn load_version(&self, graph_id: Uuid, version: Option<u64>) -> Result<DAG, Error> {
        let version = match version {
            Some(version) => version,
            None => self.latest_version(graph_id).await?,
        };
        let mut graph: DAG = read_json(&self.graph_path(graph_id, version)).await?;
        graph.node_count = graph.nodes.len();
        if graph.id != graph_id || graph.version != version {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("graph metadata does not match its cache path: {graph_id}/{version}"),
            )
            .into());
        }
        Ok(graph)
    }

    /// Writes a complete graph version to the cache atomically.
    pub async fn write_graph(&self, mut graph: DAG) -> Result<(), Error> {
        graph.node_count = graph.nodes.len();
        write_json(&self.graph_path(graph.id, graph.version), &graph).await
    }
}

impl GraphStore<Uuid> for GraphFs {
    type G = DAG;
    type Err = Error;

    async fn graphs(
        &mut self,
        page: graph::PageRequest,
        graph_id: Option<Uuid>,
    ) -> Result<graph::Page<DAG>, Error> {
        let mut entries = match fs::read_dir(self.dir()).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(graph::Page {
                    items: vec![],
                    next_cursor: None,
                });
            }
            Err(error) => return Err(error.into()),
        };
        let mut versions = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            if !entry.file_type().await?.is_dir() {
                continue;
            }
            let Ok(id) = entry.file_name().to_string_lossy().parse::<Uuid>() else {
                continue;
            };
            if graph_id.is_some_and(|graph_id| graph_id != id) {
                continue;
            }
            let mut children = fs::read_dir(entry.path()).await?;
            while let Some(child) = children.next_entry().await? {
                if child.file_type().await?.is_dir()
                    && let Ok(version) = child.file_name().to_string_lossy().parse::<u64>()
                {
                    versions.push((id, version));
                }
            }
        }
        versions.sort_unstable();
        let selected = page.select(
            versions
                .into_iter()
                .enumerate()
                .rev()
                .map(|(index, version)| (index as u64 + 1, version)),
        );
        let mut items = Vec::with_capacity(selected.items.len());
        for (id, version) in selected.items {
            items.push(self.load(id, Some(version)).await?);
        }
        Ok(graph::Page {
            items,
            next_cursor: selected.next_cursor,
        })
    }

    /// Loads an exact version, or the highest cached version when omitted.
    async fn load(&mut self, graph_id: Uuid, version: Option<u64>) -> Result<DAG, Error> {
        self.load_version(graph_id, version).await
    }
}
