use std::borrow::Borrow;

use crate::{Diff, Graph};

/// Structural differences from `self` to `other`, ignoring graph metadata.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphDiff<NodeDiff, NodeId> {
    /// Payload differences for nodes with matching identifiers.
    pub node_diffs: Vec<NodeDiff>,
    /// Nodes present in `other` but missing from `self`.
    pub node_miss: Vec<NodeId>,
    /// Nodes present in `self` but absent from `other`.
    pub node_over: Vec<NodeId>,
    /// Edges present in `other` but missing from `self`.
    pub edge_miss: Vec<(NodeId, NodeId)>,
    /// Edges present in `self` but absent from `other`.
    pub edge_over: Vec<(NodeId, NodeId)>,
}

impl<G, NodeDiff> Diff for G
where
    G: Graph,
    for<'a> G::NodeData<'a>: Diff<Item = NodeDiff>,
{
    type Item = GraphDiff<NodeDiff, G::NodeId>;

    fn diff(&self, other: &Self) -> Option<Self::Item> {
        let nodes: Vec<_> = self.nodes().collect();
        let other_nodes: Vec<_> = other.nodes().collect();
        let edges: Vec<_> = self.edges().collect();
        let other_edges: Vec<_> = other.edges().collect();
        let mut diff = GraphDiff {
            node_diffs: Vec::new(),
            node_miss: Vec::new(),
            node_over: Vec::new(),
            edge_miss: Vec::new(),
            edge_over: Vec::new(),
        };

        for node in &nodes {
            if let Some(other_node) = other_nodes
                .iter()
                .find(|other_node| other_node.id == node.id)
            {
                if let Some(node_diff) = node.data.borrow().diff(other_node.data.borrow()) {
                    diff.node_diffs.push(node_diff);
                }
            } else {
                diff.node_over.push(node.id);
            }
        }

        diff.node_miss.extend(
            other_nodes
                .iter()
                .filter(|other_node| !nodes.iter().any(|node| node.id == other_node.id))
                .map(|node| node.id),
        );
        diff.edge_miss.extend(
            other_edges
                .iter()
                .copied()
                .filter(|edge| !edges.contains(edge)),
        );
        diff.edge_over.extend(
            edges
                .iter()
                .copied()
                .filter(|edge| !other_edges.contains(edge)),
        );

        if diff.node_diffs.is_empty()
            && diff.node_miss.is_empty()
            && diff.node_over.is_empty()
            && diff.edge_miss.is_empty()
            && diff.edge_over.is_empty()
        {
            None
        } else {
            Some(diff)
        }
    }
}
