pub mod diff;
mod fork_point;
pub mod pagination;
pub mod store;
pub mod types;
pub mod validator;

pub use pagination::{Page, PageRequest};
pub use store::models;

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

/// Loads and lists graphs using native async methods.
#[allow(async_fn_in_trait)]
pub trait GraphStore<I: Id> {
    /// The graph type this store loads.
    type G: Graph<GraphId = I>;
    /// Error returned when loading a graph.
    type Err: std::error::Error;
    /// Lists published versions, optionally restricted to one graph.
    /// Cursors are local to the store and must be reused with the same filter.
    async fn graphs(
        &mut self,
        page: PageRequest,
        graph_id: Option<I>,
    ) -> Result<Page<Self::G>, Self::Err>;

    /// Loads the graph with the given id, at `version` when one is given.
    async fn load(&mut self, graph_id: I, version: Option<u64>) -> Result<Self::G, Self::Err>;
}

/// Edits graph drafts and publishes immutable graph versions.
#[allow(async_fn_in_trait)]
pub trait DraftGraphStore<I: Id>: GraphStore<I> {
    /// Lists editable drafts in descending cursor order.
    async fn list(&mut self, page: PageRequest) -> Result<Page<Self::G>, Self::Err>;

    /// Reads the editable draft for a graph.
    async fn read(&mut self, graph_id: I) -> Result<Self::G, Self::Err>;

    /// Creates or replaces a draft, recording the actor responsible.
    async fn write(&mut self, graph: &Self::G, actor: &str) -> Result<(), Self::Err>;

    /// Validates the draft and publishes it as the next graph version.
    async fn publish(&mut self, graph_id: I, actor: &str) -> Result<(), Self::Err>;
}

/// Compares values, returning `None` when there are no differences.
pub trait Diff {
    type Item;
    fn diff(&self, other: &Self) -> Option<Self::Item>;
}

/// Finds where execution of this graph first differs from another version.
pub trait ForkPoint: Graph {
    /// Returns the unique earliest changed node in `self`.
    /// Returns `None` when no nodes need execution or changes start on separate branches.
    fn fork_point(&self, other: &Self) -> Option<Self::NodeId>;

    /// Returns every ancestor of the fork point once, in this graph's node order.
    fn shared_nodes(&self, other: &Self) -> impl Iterator<Item = NodeRef<'_, Self>> {
        let fork_point = self.fork_point(other);
        let mut pending: Vec<_> = fork_point.into_iter().collect();
        let mut ancestors = Vec::new();
        while let Some(node) = pending.pop() {
            for predecessor in self.incoming_edges(node) {
                if Some(predecessor) != fork_point && !ancestors.contains(&predecessor) {
                    ancestors.push(predecessor);
                    pending.push(predecessor);
                }
            }
        }
        self.nodes()
            .filter(move |node| ancestors.contains(&node.id))
    }
}
