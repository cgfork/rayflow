use std::{
    borrow::Borrow,
    collections::{HashMap, HashSet},
};

use jiff::Timestamp;
use uuid::Uuid;

use crate::{
    Diff, Graph, Id,
    types::{DAG, NodeDiff},
};

pub(crate) fn value_diff<T: Clone + PartialEq>(before: &T, after: &T) -> Option<(T, T)> {
    (before != after).then(|| (before.clone(), after.clone()))
}

pub(crate) fn field_diffs<T, const N: usize>(fields: [Option<T>; N]) -> Option<Vec<T>> {
    let changes: Vec<_> = fields.into_iter().flatten().collect();
    (!changes.is_empty()).then_some(changes)
}

impl Diff for Uuid {
    type Item = (Self, Self);

    fn diff(&self, other: &Self) -> Option<Self::Item> {
        value_diff(self, other)
    }
}

impl Diff for String {
    type Item = (Self, Self);

    fn diff(&self, other: &Self) -> Option<Self::Item> {
        value_diff(self, other)
    }
}

/// An item change in an ordered vector. Items are compared at matching indices.
#[derive(Clone, Debug, PartialEq)]
pub enum VecDiff<T, D> {
    Changed { index: usize, changes: D },
    Added { index: usize, value: T },
    Removed { index: usize, value: T },
}

impl<T: Clone + Diff> Diff for Vec<T> {
    type Item = Vec<VecDiff<T, T::Item>>;

    fn diff(&self, other: &Self) -> Option<Self::Item> {
        let mut changes: Vec<_> = self
            .iter()
            .zip(other)
            .enumerate()
            .filter_map(|(index, (before, after))| {
                before
                    .diff(after)
                    .map(|changes| VecDiff::Changed { index, changes })
            })
            .collect();
        let shared_len = self.len().min(other.len());
        changes.extend(
            self.iter()
                .enumerate()
                .skip(shared_len)
                .map(|(index, value)| VecDiff::Removed {
                    index,
                    value: value.clone(),
                }),
        );
        changes.extend(
            other
                .iter()
                .enumerate()
                .skip(shared_len)
                .map(|(index, value)| VecDiff::Added {
                    index,
                    value: value.clone(),
                }),
        );
        (!changes.is_empty()).then_some(changes)
    }
}

/// Structural differences from `self` to `other`, ignoring graph metadata.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphDiff<NodeDiff, NodeId> {
    /// The stable node ID and payload differences for each modified node.
    pub node_diffs: Vec<(NodeId, NodeDiff)>,
    /// Nodes present in `other` but missing from `self`.
    pub node_miss: Vec<NodeId>,
    /// Nodes present in `self` but absent from `other`.
    pub node_over: Vec<NodeId>,
    /// Edges present in `other` but missing from `self`.
    pub edge_miss: Vec<(NodeId, NodeId)>,
    /// Edges present in `self` but absent from `other`.
    pub edge_over: Vec<(NodeId, NodeId)>,
}

impl<NodeDiff, NodeId: Id> GraphDiff<NodeDiff, NodeId> {
    /// Compares nodes by ID and edges by endpoints.
    pub fn between<'a, G>(before: &'a G, after: &'a G) -> Option<Self>
    where
        G: Graph<NodeId = NodeId>,
        G::NodeData<'a>: Diff<Item = NodeDiff>,
    {
        let nodes: Vec<_> = before.nodes().collect();
        let other_nodes: Vec<_> = after.nodes().collect();
        let edges: Vec<_> = before.edges().collect();
        let other_edges: Vec<_> = after.edges().collect();
        let mut other_nodes_by_id = HashMap::with_capacity(other_nodes.len());
        for node in &other_nodes {
            other_nodes_by_id.entry(node.id).or_insert(node);
        }
        let node_ids: HashSet<_> = nodes.iter().map(|node| node.id).collect();
        let edge_ids: HashSet<_> = edges.iter().copied().collect();
        let other_edge_ids: HashSet<_> = other_edges.iter().copied().collect();
        let mut diff = Self {
            node_diffs: Vec::new(),
            node_miss: Vec::new(),
            node_over: Vec::new(),
            edge_miss: Vec::new(),
            edge_over: Vec::new(),
        };

        for node in &nodes {
            if let Some(other_node) = other_nodes_by_id.get(&node.id) {
                if let Some(node_diff) = node.data.borrow().diff(other_node.data.borrow()) {
                    diff.node_diffs.push((node.id, node_diff));
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

/// A changed field or structural change in a DAG, from `self` to `other`.
#[derive(Clone, Debug, PartialEq)]
pub enum DagDiff {
    Id((Uuid, Uuid)),
    Name((String, String)),
    Version((u64, u64)),
    NodeCount((usize, usize)),
    UpdatedAt((Timestamp, Timestamp)),
    UpdatedBy((String, String)),
    CreatedAt((Timestamp, Timestamp)),
    CreatedBy((String, String)),
    Structure(GraphDiff<Vec<NodeDiff>, Uuid>),
}

impl Diff for DAG {
    type Item = Vec<DagDiff>;

    fn diff(&self, other: &Self) -> Option<Self::Item> {
        field_diffs([
            self.id.diff(&other.id).map(DagDiff::Id),
            self.name.diff(&other.name).map(DagDiff::Name),
            value_diff(&self.version, &other.version).map(DagDiff::Version),
            value_diff(&self.node_count, &other.node_count).map(DagDiff::NodeCount),
            value_diff(&self.updated_at, &other.updated_at).map(DagDiff::UpdatedAt),
            self.updated_by
                .diff(&other.updated_by)
                .map(DagDiff::UpdatedBy),
            value_diff(&self.created_at, &other.created_at).map(DagDiff::CreatedAt),
            self.created_by
                .diff(&other.created_by)
                .map(DagDiff::CreatedBy),
            GraphDiff::between(self, other).map(DagDiff::Structure),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Node;

    #[test]
    fn dag_diff_reports_metadata_and_all_structural_changes() {
        let [matched, removed, added, extra] = [1, 2, 3, 4].map(Uuid::from_u128);
        let make_node = |id: Uuid| Node {
            id,
            name: format!("node-{}", id.as_u128()),
            node_type: "docker".into(),
            settings: Default::default(),
            inputs: vec![],
            outputs: vec![],
        };
        let before = DAG {
            id: Uuid::from_u128(100),
            name: "previous".into(),
            version: 2,
            node_count: 2,
            updated_at: Timestamp::from_second(2).unwrap(),
            updated_by: "previous editor".into(),
            created_at: Timestamp::from_second(1).unwrap(),
            created_by: "previous author".into(),
            nodes: vec![make_node(matched), make_node(removed)],
            edges: vec![(matched, removed)],
        };
        let mut after = DAG {
            id: Uuid::from_u128(101),
            name: "current".into(),
            version: 3,
            node_count: 3,
            updated_at: Timestamp::from_second(4).unwrap(),
            updated_by: "current editor".into(),
            created_at: Timestamp::from_second(3).unwrap(),
            created_by: "current author".into(),
            nodes: vec![make_node(added), make_node(matched), make_node(extra)],
            edges: vec![(matched, added), (added, extra)],
        };
        after.nodes[1].name = "renamed".into();

        assert_eq!(before.diff(&before), None);
        assert_eq!(
            before.diff(&after),
            Some(vec![
                DagDiff::Id((before.id, after.id)),
                DagDiff::Name(("previous".into(), "current".into())),
                DagDiff::Version((2, 3)),
                DagDiff::NodeCount((2, 3)),
                DagDiff::UpdatedAt((before.updated_at, after.updated_at)),
                DagDiff::UpdatedBy(("previous editor".into(), "current editor".into())),
                DagDiff::CreatedAt((before.created_at, after.created_at)),
                DagDiff::CreatedBy(("previous author".into(), "current author".into())),
                DagDiff::Structure(GraphDiff {
                    node_diffs: vec![(
                        matched,
                        vec![NodeDiff::Name(("node-1".into(), "renamed".into()))],
                    )],
                    node_miss: vec![added, extra],
                    node_over: vec![removed],
                    edge_miss: vec![(matched, added), (added, extra)],
                    edge_over: vec![(matched, removed)],
                }),
            ])
        );
    }
}
