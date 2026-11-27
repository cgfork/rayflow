use std::collections::{HashMap, HashSet};

use crate::{
    ForkPoint, Graph,
    diff::GraphDiff,
    types::{DAG, Node},
};

// Nodes with the same identity and ports can restart with new settings after an upgrade.
fn same_node_structure(before: &Node, after: &Node) -> bool {
    before.id == after.id
        && before.name == after.name
        && before.node_type == after.node_type
        && before.inputs == after.inputs
        && before.outputs == after.outputs
        && before.assets == after.assets
}

impl ForkPoint for DAG {
    fn same_structure(&self, other: &Self) -> bool {
        GraphDiff::between_by(self, other, |before, after| {
            (!same_node_structure(before, after)).then_some(())
        })
        .is_none()
    }

    fn fork_point(&self, other: &Self) -> Option<Self::NodeId> {
        let mut other_nodes = HashMap::with_capacity(other.nodes.len());
        for node in &other.nodes {
            other_nodes.entry(node.id).or_insert(node);
        }
        let nodes: HashSet<_> = self.nodes.iter().map(|node| node.id).collect();
        let mut changed = HashSet::new();
        for node in &self.nodes {
            if other_nodes
                .get(&node.id)
                .is_none_or(|other_node| !same_node_structure(node, other_node))
            {
                changed.insert(node.id);
            }
        }

        let edges: HashSet<_> = self.edges.iter().copied().collect();
        let other_edges: HashSet<_> = other.edges.iter().copied().collect();
        changed.extend(
            self.edges
                .iter()
                .chain(other.edges.iter())
                .filter(|edge| edges.contains(edge) != other_edges.contains(edge))
                .map(|(_, target)| *target)
                .filter(|node_id| nodes.contains(node_id)),
        );
        if changed.is_empty() {
            return None;
        }

        let mut successors: HashMap<_, Vec<_>> = HashMap::new();
        for (source, target) in self.edges() {
            successors.entry(source).or_default().push(target);
        }
        let mut pending: Vec<_> = changed
            .iter()
            .filter_map(|node_id| successors.get(node_id))
            .flatten()
            .copied()
            .collect();
        let mut downstream = HashSet::new();
        while let Some(node_id) = pending.pop() {
            if downstream.insert(node_id)
                && let Some(targets) = successors.get(&node_id)
            {
                pending.extend(targets);
            }
        }
        let mut first = changed.difference(&downstream).copied();
        let fork_point = first.next()?;
        first.next().is_none().then_some(fork_point)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::AssetRef;
    use uuid::Uuid;

    #[test]
    fn shared_nodes_distinguish_matching_structure_from_missing_forks() {
        let ids = [1, 2, 3].map(Uuid::from_u128);
        let [root, left, right] = ids;
        let previous = DAG {
            version: 2,
            node_count: ids.len(),
            nodes: ids
                .into_iter()
                .map(|id| Node {
                    id,
                    name: format!("node-{}", id.as_u128()),
                    node_type: "docker".into(),
                    assets: vec![],
                    settings: Default::default(),
                    inputs: vec![],
                    outputs: vec![],
                })
                .collect(),
            edges: vec![(root, left), (root, right)],
            ..DAG::default()
        };
        let mut target = DAG {
            version: 3,
            ..previous.clone()
        };
        assert_eq!(target.fork_point(&previous), None);
        assert_eq!(
            target
                .shared_nodes(&previous)
                .map(|node| node.id)
                .collect::<Vec<_>>(),
            ids
        );

        target.nodes[1].name = "changed-left".into();
        assert_eq!(target.fork_point(&previous), Some(left));
        assert_eq!(
            target
                .shared_nodes(&previous)
                .map(|node| node.id)
                .collect::<Vec<_>>(),
            [root]
        );

        target.nodes[2].name = "changed-right".into();
        assert_eq!(target.fork_point(&previous), None);
        assert!(target.shared_nodes(&previous).next().is_none());

        let mut removed_tail = previous.clone();
        removed_tail.delete_node(right);
        assert_eq!(removed_tail.fork_point(&previous), None);
        assert!(removed_tail.shared_nodes(&previous).next().is_none());

        let mut changed_asset = previous.clone();
        changed_asset.nodes[1].assets.push(AssetRef {
            file_name: "scripts/run.lua".into(),
            asset_id: Uuid::from_u128(33),
        });
        assert_eq!(changed_asset.fork_point(&previous), Some(left));
        assert_eq!(
            changed_asset
                .shared_nodes(&previous)
                .map(|node| node.id)
                .collect::<Vec<_>>(),
            [root],
        );
    }
}
