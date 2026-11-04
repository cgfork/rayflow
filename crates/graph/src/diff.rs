use std::{
    borrow::Borrow,
    collections::{HashMap, HashSet},
};

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
        let mut other_node_indices = HashMap::with_capacity(other_nodes.len());
        for (index, node) in other_nodes.iter().enumerate() {
            other_node_indices.entry(node.id).or_insert(index);
        }
        let node_ids: HashSet<_> = nodes.iter().map(|node| node.id).collect();
        let edge_ids: HashSet<_> = edges.iter().copied().collect();
        let other_edge_ids: HashSet<_> = other_edges.iter().copied().collect();
        let mut diff = GraphDiff {
            node_diffs: Vec::with_capacity(nodes.len().min(other_nodes.len())),
            node_miss: Vec::with_capacity(other_nodes.len()),
            node_over: Vec::with_capacity(nodes.len()),
            edge_miss: Vec::with_capacity(other_edges.len()),
            edge_over: Vec::with_capacity(edges.len()),
        };

        for node in &nodes {
            if let Some(&index) = other_node_indices.get(&node.id) {
                let other_node = &other_nodes[index];
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
                .filter(|other_node| !node_ids.contains(&other_node.id))
                .map(|node| node.id),
        );
        diff.edge_miss.extend(
            other_edges
                .iter()
                .copied()
                .filter(|edge| !edge_ids.contains(edge)),
        );
        diff.edge_over.extend(
            edges
                .iter()
                .copied()
                .filter(|edge| !other_edge_ids.contains(edge)),
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
