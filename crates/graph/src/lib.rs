pub mod types;

use std::{
    borrow::{Borrow, BorrowMut},
    fmt::{Debug, Display},
};

pub trait Id: Copy + PartialEq + Debug + Display {}

pub struct Node<I, N> {
    pub id: I,
    pub data: N,
}

pub type NodeRef<'g, G> = Node<<G as Graph>::NodeId, <G as Graph>::NodeDataRef<'g>>;
pub type NodeMut<'g, G> = Node<<G as Graph>::NodeId, <G as Graph>::NodeDataMut<'g>>;

/// A directed acyclic graph.
pub trait Graph {
    /// Graph identifier.
    type GraphId: Id;

    /// Node identifier.
    type NodeId: Id;

    /// Node payload.
    type NodeData<'graph>
    where
        Self: 'graph;

    /// Immutable reference to a node's payload.
    type NodeDataRef<'graph>: Borrow<Self::NodeData<'graph>>
    where
        Self: 'graph;

    /// Mutable reference to a node's payload.
    type NodeDataMut<'graph>: BorrowMut<Self::NodeData<'graph>>
    where
        Self: 'graph;

    /// Returns the unique identifier of this graph.
    fn graph_id(&self) -> Self::GraphId;
    /// Returns the version of this graph.
    fn version(&self) -> u64;

    /// Iterates over all nodes.
    fn nodes(&self) -> impl Iterator<Item = NodeRef<'_, Self>>;
    /// Iterates over all nodes mutably.
    fn nodes_mut(&mut self) -> impl Iterator<Item = NodeMut<'_, Self>>;

    /// Iterates over all edges as `(from, to)` node-id pairs.
    fn edges(&self) -> impl Iterator<Item = (Self::NodeId, Self::NodeId)>;

    /// Removes the edge `from -> to`.
    fn delete_edge(&mut self, from: Self::NodeId, to: Self::NodeId);
    /// Adds the edge `from -> to`.
    fn add_edge(&mut self, from: Self::NodeId, to: Self::NodeId);

    /// Removes a node and its incident edges.
    fn delete_node(&mut self, node_id: Self::NodeId);
    /// Adds a node.
    fn add_node(&mut self, node: Self::NodeData<'_>);

    /// Returns the node with the given id, if present.
    fn node(&self, node_id: Self::NodeId) -> Option<NodeRef<'_, Self>> {
        self.nodes().find(|node| node.id == node_id)
    }

    /// Iterates over nodes with no incoming edges.
    fn root_nodes(&self) -> impl Iterator<Item = NodeRef<'_, Self>> {
        self.nodes().filter_map(move |node| {
            if self.in_degree(node.id) == 0 {
                Some(node)
            } else {
                None
            }
        })
    }

    /// Iterates over the predecessors of a node.
    fn predecessors(&self, node_id: Self::NodeId) -> impl Iterator<Item = NodeRef<'_, Self>> {
        self.edges().filter_map(
            move |(from, to)| {
                if to == node_id { self.node(from) } else { None }
            },
        )
    }
    /// Iterates over the successors of a node.
    fn successors(&self, node_id: Self::NodeId) -> impl Iterator<Item = NodeRef<'_, Self>> {
        self.edges().filter_map(
            move |(from, to)| {
                if from == node_id { self.node(to) } else { None }
            },
        )
    }

    /// Iterates over the ids of a node's predecessors (its incoming edges).
    fn incoming_edges(&self, node_id: Self::NodeId) -> impl Iterator<Item = Self::NodeId> {
        self.edges().filter_map(
            move |(from, to)| {
                if to == node_id { Some(from) } else { None }
            },
        )
    }
    /// Iterates over the ids of a node's successors (its outgoing edges).
    fn outgoing_edges(&self, node_id: Self::NodeId) -> impl Iterator<Item = Self::NodeId> {
        self.edges().filter_map(
            move |(from, to)| {
                if from == node_id { Some(to) } else { None }
            },
        )
    }

    /// Number of incoming edges.
    fn in_degree(&self, node_id: Self::NodeId) -> usize {
        self.incoming_edges(node_id).count()
    }

    /// Number of outgoing edges.
    fn out_degree(&self, node_id: Self::NodeId) -> usize {
        self.outgoing_edges(node_id).count()
    }
}

/// Loads graphs by id.
///
/// `load` returns a `Send` future so a graph store can be awaited from an async
/// context that must itself be `Send` (a request handler, say).
pub trait GraphStore<I: Id> {
    /// The graph type this store loads.
    type G: Graph<GraphId = I>;
    /// Error returned when loading a graph.
    type Err: std::error::Error;

    /// Loads the graph with the given id, at `version` when one is given.
    fn load(
        &self,
        graph_id: I,
        version: Option<u64>,
    ) -> impl std::future::Future<Output = Result<Self::G, Self::Err>> + Send;
}
