//! A simple in-memory [`GraphStore`] for the server binary.
//!
//! The server is graph-source agnostic: it serves whatever graphs a
//! [`GraphStore`] returns. This registry is the built-in store the binary uses;
//! register graphs with [`Graphs::insert`] before serving a run.

use std::collections::HashMap;
use std::convert::Infallible;

use graph::types::DAG;
use graph::{GraphStore, Page, PageRequest};
use uuid::Uuid;

/// Serves graphs from an in-memory registry keyed by graph id.
#[derive(Clone, Default)]
pub struct Graphs {
    graphs: HashMap<Uuid, DAG>,
}

impl Graphs {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `graph`, replacing any graph with the same id.
    pub fn insert(&mut self, mut graph: DAG) {
        graph.node_count = graph.nodes.len();
        self.graphs.insert(graph.id, graph);
    }
}

impl GraphStore<Uuid> for Graphs {
    type G = DAG;
    type Err = Infallible;

    async fn graphs(
        &mut self,
        page: PageRequest,
        graph_id: Option<Uuid>,
    ) -> Result<Page<DAG>, Infallible> {
        Ok(list_graphs(&self.graphs, page, graph_id))
    }

    async fn load(&mut self, graph_id: Uuid, _version: Option<u64>) -> Result<DAG, Infallible> {
        // An unknown graph resolves to an empty one, so a run's node runs still
        // read back (with no graph metadata) instead of failing the request.
        Ok(self.graphs.get(&graph_id).cloned().unwrap_or(DAG {
            id: graph_id,
            name: String::new(),
            version: 0,
            nodes: Vec::new(),
            edges: Vec::new(),
            ..DAG::default()
        }))
    }
}

/// The ten graphs in the repository's `graphs/` directory, embedded at compile
/// time. Each is named after a word (`alpha`..`juliet`).
const EMBEDDED: [&str; 10] = [
    include_str!("../../graphs/alpha.json"),
    include_str!("../../graphs/bravo.json"),
    include_str!("../../graphs/charlie.json"),
    include_str!("../../graphs/delta.json"),
    include_str!("../../graphs/echo.json"),
    include_str!("../../graphs/foxtrot.json"),
    include_str!("../../graphs/golf.json"),
    include_str!("../../graphs/hotel.json"),
    include_str!("../../graphs/india.json"),
    include_str!("../../graphs/juliet.json"),
];

/// Error returned by [`StaticGraphStore`] when a graph is not embedded.
#[derive(Debug, thiserror::Error)]
#[error("graph not found: {0}")]
pub struct GraphNotFound(pub Uuid);

/// A [`GraphStore`] serving the graphs embedded at compile time.
///
/// The ten graphs in the repository's `graphs/` directory (word-named
/// `alpha.json`..`juliet.json`, 10–20 nodes each) are parsed once, so this store
/// serves realistic graphs with no external source — handy for tests and local
/// runs.
#[derive(Clone, Default)]
pub struct StaticGraphStore {
    graphs: HashMap<Uuid, DAG>,
}

impl StaticGraphStore {
    /// Parses the embedded graphs.
    ///
    /// The embedded files are valid (checked by the `embedded_graphs_parse`
    /// test), so this never fails in practice.
    pub fn new() -> Self {
        let graphs = EMBEDDED
            .iter()
            .map(|json| {
                let mut graph: DAG =
                    serde_json::from_str(json).expect("embedded graph is valid JSON");
                graph.node_count = graph.nodes.len();
                (graph.id, graph)
            })
            .collect();
        Self { graphs }
    }

    /// Returns the embedded graph with `graph_id`, if there is one.
    pub fn graph(&self, graph_id: Uuid) -> Option<&DAG> {
        self.graphs.get(&graph_id)
    }
}

impl GraphStore<Uuid> for StaticGraphStore {
    type G = DAG;
    type Err = GraphNotFound;

    async fn graphs(
        &mut self,
        page: PageRequest,
        graph_id: Option<Uuid>,
    ) -> Result<Page<DAG>, GraphNotFound> {
        Ok(list_graphs(&self.graphs, page, graph_id))
    }

    async fn load(&mut self, graph_id: Uuid, _version: Option<u64>) -> Result<DAG, GraphNotFound> {
        self.graphs
            .get(&graph_id)
            .cloned()
            .ok_or(GraphNotFound(graph_id))
    }
}

fn list_graphs(
    graphs: &HashMap<Uuid, DAG>,
    page: PageRequest,
    graph_id: Option<Uuid>,
) -> Page<DAG> {
    let mut graphs: Vec<_> = graphs
        .values()
        .filter(|graph| graph_id.is_none_or(|id| graph.id == id))
        .cloned()
        .collect();
    graphs.sort_by_key(|graph| graph.id);
    page.select(
        graphs
            .into_iter()
            .enumerate()
            .rev()
            .map(|(index, graph)| (index as u64 + 1, graph)),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn embedded_graphs_parse() {
        let store = StaticGraphStore::new();
        assert_eq!(store.graphs.len(), 10);

        for graph in store.graphs.values() {
            let nodes = graph.nodes.len();
            assert!(
                (10..=20).contains(&nodes),
                "graph {} has {nodes} nodes",
                graph.name
            );

            let ids: HashSet<Uuid> = graph.nodes.iter().map(|node| node.id).collect();
            assert_eq!(
                ids.len(),
                nodes,
                "graph {} has duplicate node ids",
                graph.name
            );

            for &(from, to) in &graph.edges {
                assert!(ids.contains(&from) && ids.contains(&to));
            }
        }
    }

    #[test]
    fn loads_embedded_graphs() {
        let store = StaticGraphStore::new();
        let graph_0 = Uuid::parse_str("00000001-0000-0000-0000-000000000000").unwrap();
        let graph = store.graph(graph_0).expect("alpha is embedded");
        assert_eq!(graph.name, "alpha");

        let missing = Uuid::parse_str("000000ff-0000-0000-0000-000000000000").unwrap();
        assert!(store.graph(missing).is_none());
    }
}
