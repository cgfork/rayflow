use jiff::Timestamp;
use std::time::Duration;
use uuid::Uuid;

use crate::{Diff, Graph, Id, NodeMut, NodeRef};

impl Id for Uuid {}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Kind {
    File,
    Directory,
}

impl Diff for Kind {
    type Item = (Self, Self);

    fn diff(&self, other: &Self) -> Option<Self::Item> {
        (self != other).then(|| (self.clone(), other.clone()))
    }
}

/// An output produced by a node.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Output {
    /// File name of the output.
    pub file_name: String,
    /// Whether the output is a file or a directory.
    pub kind: Kind,
    /// Whether the output may expand to multiple files.
    pub multiple: bool,
}

impl Diff for Output {
    type Item = (Self, Self);

    fn diff(&self, other: &Self) -> Option<Self::Item> {
        (self != other).then(|| (self.clone(), other.clone()))
    }
}

/// An input consumed by a node.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Input {
    /// File name of the input.
    pub file_name: String,
    /// Source output reference in `node_name.file_name` form.
    pub source: String,
    /// Whether the input is mandatory.
    pub required: bool,
    /// Expected format, if any.
    pub format: Option<String>,
}

impl Diff for Input {
    type Item = (Self, Self);

    fn diff(&self, other: &Self) -> Option<Self::Item> {
        (self != other).then(|| (self.clone(), other.clone()))
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Resource {
    pub cpu_cores: u32,
    pub memory_bytes: u64,
}

impl Diff for Resource {
    type Item = (Self, Self);

    fn diff(&self, other: &Self) -> Option<Self::Item> {
        (self != other).then(|| (self.clone(), other.clone()))
    }
}

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct NodeSettings {
    pub config: serde_json::Value,
    pub resource: Resource,
    #[serde(with = "jiff::fmt::serde::unsigned_duration::friendly::compact::required")]
    pub timeout: Duration,
}

impl NodeSettings {
    pub fn timeout_ms(&self) -> Option<u64> {
        let milliseconds = u64::try_from(self.timeout.as_millis()).ok()?;
        (milliseconds > 0
            && self.timeout.subsec_nanos().is_multiple_of(1_000_000)
            && milliseconds <= (i64::MAX / 1000) as u64)
            .then_some(milliseconds)
    }
}

impl Diff for NodeSettings {
    type Item = (Self, Self);

    fn diff(&self, other: &Self) -> Option<Self::Item> {
        (self != other).then(|| (self.clone(), other.clone()))
    }
}

/// A single node (task) in a graph.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Node {
    /// Unique node identifier.
    pub id: Uuid,
    /// Human-readable node name.
    pub name: String,
    /// Node type / task kind.
    pub node_type: String,
    #[serde(default)]
    pub settings: NodeSettings,
    /// Inputs consumed by the node.
    pub inputs: Vec<Input>,
    /// Outputs produced by the node.
    pub outputs: Vec<Output>,
}

impl Diff for Node {
    type Item = (Self, Self);

    fn diff(&self, other: &Self) -> Option<Self::Item> {
        (self != other).then(|| (self.clone(), other.clone()))
    }
}

impl std::fmt::Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({})", self.name, self.id)
    }
}

/// A directed acyclic graph of nodes connected by edges.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DAG {
    /// Unique graph identifier.
    pub id: Uuid,
    /// Human-readable graph name.
    pub name: String,
    /// Graph version.
    pub version: u64,
    #[serde(default)]
    pub node_count: usize,
    #[serde(skip)]
    pub updated_at: Timestamp,
    #[serde(skip)]
    pub updated_by: String,
    #[serde(skip)]
    pub created_at: Timestamp,
    #[serde(skip)]
    pub created_by: String,
    /// Nodes in the graph.
    pub nodes: Vec<Node>,
    /// Directed edges as `(from, to)` node-id pairs.
    pub edges: Vec<(Uuid, Uuid)>,
}

impl Graph for DAG {
    type GraphId = Uuid;

    type NodeId = Uuid;

    type NodeData<'graph> = Node;

    type NodeDataRef<'graph> = &'graph Node;

    type NodeDataMut<'graph> = &'graph mut Node;

    fn graph_id(&self) -> Self::GraphId {
        self.id
    }

    fn version(&self) -> u64 {
        self.version
    }

    fn nodes(&self) -> impl Iterator<Item = NodeRef<'_, Self>> {
        self.nodes.iter().map(|node| crate::Node {
            id: node.id,
            data: node,
        })
    }

    fn nodes_mut(&mut self) -> impl Iterator<Item = NodeMut<'_, Self>> {
        self.nodes.iter_mut().map(|node| crate::Node {
            id: node.id,
            data: node,
        })
    }

    fn edges(&self) -> impl Iterator<Item = (Self::NodeId, Self::NodeId)> {
        self.edges.iter().copied()
    }

    fn delete_edge(&mut self, from: Self::NodeId, to: Self::NodeId) {
        self.edges.retain(|&(f, t)| !(f == from && t == to));
    }

    fn add_edge(&mut self, from: Self::NodeId, to: Self::NodeId) {
        self.edges.push((from, to));
    }

    fn delete_node(&mut self, node_id: Self::NodeId) {
        self.nodes.retain(|node| node.id != node_id);
        self.node_count = self.nodes.len();
        self.edges
            .retain(|&(from, to)| from != node_id && to != node_id);
    }

    fn add_node(&mut self, node: Self::NodeData<'_>) {
        self.nodes.push(node);
        self.node_count = self.nodes.len();
    }
}
