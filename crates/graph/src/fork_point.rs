use std::collections::{HashMap, HashSet};

use crate::{Diff, ForkPoint, Graph, types::DAG};

impl ForkPoint for DAG {
    fn fork_point(&self, other: &Self) -> Option<Self::NodeId> {
        let diff = self.diff(other)?;
        let nodes: HashSet<_> = self.nodes().map(|node| node.id).collect();
        let changed: HashSet<_> = diff
            .node_diffs
            .into_iter()
            .map(|(before, _)| before.id)
            .chain(diff.node_over)
            .chain(diff.edge_miss.into_iter().map(|(_, target)| target))
            .chain(diff.edge_over.into_iter().map(|(_, target)| target))
            .filter(|node_id| nodes.contains(node_id))
            .collect();
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
