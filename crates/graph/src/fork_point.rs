use std::collections::{HashMap, HashSet};

use crate::{ForkPoint, Graph, types::DAG};

impl ForkPoint for DAG {
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
                .is_none_or(|other_node| node != *other_node)
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
