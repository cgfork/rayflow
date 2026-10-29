use std::collections::HashSet;
use std::fmt;

use uuid::Uuid;

use crate::types::{DAG, Output};
use crate::{Graph, Id};

/// A structural error in a graph.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ValidationError<NodeId> {
    /// The graph contains a directed cycle.
    Cycle,
    /// The graph does not have exactly one node with no incoming edges.
    InvalidRootCount { count: usize },
    /// An edge references a node that is absent from the graph.
    UnknownNode(NodeId),
}

impl<NodeId: Id> fmt::Display for ValidationError<NodeId> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cycle => formatter.write_str("graph contains a cycle"),
            Self::InvalidRootCount { count } => {
                write!(
                    formatter,
                    "graph must have exactly one root node, found {count}"
                )
            }
            Self::UnknownNode(node_id) => {
                write!(formatter, "edge references unknown node {node_id}")
            }
        }
    }
}

impl<NodeId: Id> std::error::Error for ValidationError<NodeId> {}

/// Requires an acyclic graph with exactly one root and valid edge endpoints.
///
/// All components are checked for cycles before checking the root count.
/// Unreferenced outputs are allowed and can be inspected separately using
/// [`unreferenced_outputs`].
pub fn validate<G: Graph>(graph: &G) -> Result<(), ValidationError<G::NodeId>> {
    let nodes: Vec<_> = graph.nodes().map(|node| node.id).collect();
    let mut successors = vec![Vec::new(); nodes.len()];
    let mut incoming = vec![0usize; nodes.len()];

    for (from, to) in graph.edges() {
        let from_index = nodes
            .iter()
            .position(|node_id| *node_id == from)
            .ok_or(ValidationError::UnknownNode(from))?;
        let to_index = nodes
            .iter()
            .position(|node_id| *node_id == to)
            .ok_or(ValidationError::UnknownNode(to))?;
        successors[from_index].push(to_index);
        incoming[to_index] += 1;
    }

    let mut ready: Vec<_> = incoming
        .iter()
        .enumerate()
        .filter_map(|(index, count)| (*count == 0).then_some(index))
        .collect();
    let root_count = ready.len();
    let mut visited = 0;

    while let Some(index) = ready.pop() {
        visited += 1;
        for &successor in &successors[index] {
            incoming[successor] -= 1;
            if incoming[successor] == 0 {
                ready.push(successor);
            }
        }
    }

    if visited != nodes.len() {
        return Err(ValidationError::Cycle);
    }
    if root_count != 1 {
        return Err(ValidationError::InvalidRootCount { count: root_count });
    }
    Ok(())
}

/// An output that is not referenced by any input in the graph.
#[derive(Clone, Debug, PartialEq)]
pub struct UnreferencedOutput<'graph> {
    /// Identifier of the node producing the output.
    pub node_id: Uuid,
    /// The unused output declaration.
    pub output: &'graph Output,
}

/// Returns unused outputs in node and output declaration order.
///
/// Input sources are matched exactly against `node_name.file_name`. An input's
/// local file name may differ, and optional inputs also count as references.
/// This helper does not require a valid graph and does not report errors.
pub fn unreferenced_outputs(graph: &DAG) -> Vec<UnreferencedOutput<'_>> {
    let sources: HashSet<_> = graph
        .nodes
        .iter()
        .flat_map(|node| &node.inputs)
        .map(|input| input.source.as_str())
        .collect();
    let mut outputs = Vec::new();

    for node in &graph.nodes {
        for output in &node.outputs {
            let reference = format!("{}.{}", node.name, output.file_name);
            if !sources.contains(reference.as_str()) {
                outputs.push(UnreferencedOutput {
                    node_id: node.id,
                    output,
                });
            }
        }
    }
    outputs
}
