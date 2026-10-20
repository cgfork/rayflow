use uuid::Uuid;

use crate::{Graph, Id, NodeMut, NodeRef};

impl Id for Uuid {}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Kind {
    File,
    Directory,
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

/// An input consumed by a node.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Input {
    /// File name of the input.
    pub file_name: String,
    /// Source (output reference) this input is read from.
    pub source: String,
    /// Whether the input is mandatory.
    pub required: bool,
    /// Expected format, if any.
    pub format: Option<String>,
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
    /// Inputs consumed by the node.
    pub inputs: Vec<Input>,
    /// Outputs produced by the node.
    pub outputs: Vec<Output>,
}

impl std::fmt::Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({})", self.name, self.id)
    }
}

/// A directed acyclic graph of nodes connected by edges.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DAG {
    /// Unique graph identifier.
    pub id: Uuid,
    /// Human-readable graph name.
    pub name: String,
    /// Graph version.
    pub version: u64,
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
        self.edges
            .retain(|&(from, to)| from != node_id && to != node_id);
    }

    fn add_node(&mut self, node: Self::NodeData<'_>) {
        self.nodes.push(node);
    }
}
