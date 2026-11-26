use jiff::Timestamp;
use std::{collections::HashMap, time::Duration};
use uuid::Uuid;

use crate::{Diff, Graph, Id, NodeMut, NodeRef};

impl Id for Uuid {}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Kind {
    File,
    Directory,
}

impl Diff for Kind {
    type Item = (Self, Self);

    fn diff(&self, other: &Self) -> Option<Self::Item> {
        (self != other).then(|| (self.clone(), other.clone()))
    }
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

impl Diff for Output {
    type Item = (Self, Self);

    fn diff(&self, other: &Self) -> Option<Self::Item> {
        (self != other).then(|| (self.clone(), other.clone()))
    }
}

/// An input consumed by a node.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Input {
    /// File name of the input.
    pub file_name: String,
    /// Source output reference in `node_name.file_name` form.
    pub source: String,
    /// Whether the input is mandatory.
    pub required: bool,
    /// Expected format, if any.
    pub format: Option<String>,
}

impl Diff for Input {
    type Item = (Self, Self);

    fn diff(&self, other: &Self) -> Option<Self::Item> {
        (self != other).then(|| (self.clone(), other.clone()))
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Resource {
    pub cpu_cores: u32,
    pub memory_bytes: u64,
}

impl Diff for Resource {
    type Item = (Self, Self);

    fn diff(&self, other: &Self) -> Option<Self::Item> {
        (self != other).then(|| (self.clone(), other.clone()))
    }
}

pub const DEFAULT_NODE_CONFIG_NAME: &str = "default";

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct NodeSettings {
    pub config: serde_json::Value,
    pub resource: Resource,
    #[serde(with = "jiff::fmt::serde::unsigned_duration::friendly::compact::required")]
    pub timeout: Duration,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub overrides: HashMap<String, AnnotationMatchNodeSettings>,
}

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AnnotationMatchNodeSettings {
    pub annotation_match: Option<String>,
    pub config: Option<serde_json::Value>,
    pub resource: Option<Resource>,
    #[serde(
        default,
        with = "jiff::fmt::serde::unsigned_duration::friendly::compact::optional"
    )]
    pub timeout: Option<Duration>,
}

/// Effective settings for one named node configuration.
pub struct NamedNodeSettings<'a> {
    pub name: &'a str,
    pub annotation_match: &'a str,
    pub config: &'a serde_json::Value,
    pub resource: &'a Resource,
    pub timeout: Duration,
}

impl NodeSettings {
    pub fn timeout_ms(&self) -> Option<u64> {
        timeout_ms(self.timeout)
    }

    /// Returns the base settings and each override with omitted fields filled in.
    pub fn named_settings(&self) -> impl Iterator<Item = NamedNodeSettings<'_>> {
        std::iter::once(NamedNodeSettings {
            name: DEFAULT_NODE_CONFIG_NAME,
            annotation_match: "",
            config: &self.config,
            resource: &self.resource,
            timeout: self.timeout,
        })
        .chain(
            self.overrides
                .iter()
                .map(|(name, settings)| NamedNodeSettings {
                    name,
                    annotation_match: settings.annotation_match.as_deref().unwrap_or_default(),
                    config: settings.config.as_ref().unwrap_or(&self.config),
                    resource: settings.resource.as_ref().unwrap_or(&self.resource),
                    timeout: settings.timeout.unwrap_or(self.timeout),
                }),
        )
    }
}

impl NamedNodeSettings<'_> {
    pub fn timeout_ms(&self) -> Option<u64> {
        timeout_ms(self.timeout)
    }
}

fn timeout_ms(timeout: Duration) -> Option<u64> {
    let milliseconds = u64::try_from(timeout.as_millis()).ok()?;
    (milliseconds > 0
        && timeout.subsec_nanos().is_multiple_of(1_000_000)
        && milliseconds <= (i64::MAX / 1000) as u64)
        .then_some(milliseconds)
}

impl Diff for NodeSettings {
    type Item = (Self, Self);

    fn diff(&self, _other: &Self) -> Option<Self::Item> {
        None
    }
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
    #[serde(default)]
    pub settings: NodeSettings,
    /// Inputs consumed by the node.
    pub inputs: Vec<Input>,
    /// Outputs produced by the node.
    pub outputs: Vec<Output>,
}

impl Diff for Node {
    type Item = (Self, Self);

    fn diff(&self, other: &Self) -> Option<Self::Item> {
        let changes = [
            self.id.diff(&other.id).is_some(),
            self.name.diff(&other.name).is_some(),
            self.node_type.diff(&other.node_type).is_some(),
            self.settings.diff(&other.settings).is_some(),
            self.inputs.diff(&other.inputs).is_some(),
            self.outputs.diff(&other.outputs).is_some(),
        ];
        changes
            .into_iter()
            .any(|changed| changed)
            .then(|| (self.clone(), other.clone()))
    }
}

impl std::fmt::Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({})", self.name, self.id)
    }
}

/// A directed acyclic graph of nodes connected by edges.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DAG {
    /// Unique graph identifier.
    pub id: Uuid,
    /// Human-readable graph name.
    pub name: String,
    /// Graph version.
    pub version: u64,
    #[serde(default)]
    pub node_count: usize,
    #[serde(default)]
    pub updated_at: Timestamp,
    #[serde(default)]
    pub updated_by: String,
    #[serde(default)]
    pub created_at: Timestamp,
    #[serde(default)]
    pub created_by: String,
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
        self.node_count = self.nodes.len();
        self.edges
            .retain(|&(from, to)| from != node_id && to != node_id);
    }

    fn add_node(&mut self, node: Self::NodeData<'_>) {
        self.nodes.push(node);
        self.node_count = self.nodes.len();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ForkPoint;

    #[test]
    fn node_diff_compares_each_field_and_ignores_settings_changes() {
        let node = Node {
            id: Uuid::new_v4(),
            name: "task".into(),
            node_type: "docker".into(),
            settings: NodeSettings::default(),
            inputs: vec![Input {
                file_name: "input.txt".into(),
                source: "build.result.txt".into(),
                required: true,
                format: None,
            }],
            outputs: vec![Output {
                file_name: "result.txt".into(),
                kind: Kind::File,
                multiple: false,
            }],
        };
        let previous = DAG {
            nodes: vec![node.clone()],
            ..DAG::default()
        };
        let mut current = previous.clone();
        current.nodes[0].settings = serde_json::from_value(serde_json::json!({
            "config": {"image": "alpine"},
            "resource": {"cpu_cores": 1, "memory_bytes": 512},
            "timeout": "2s",
            "overrides": {"large": {"timeout": "4s"}}
        }))
        .unwrap();

        assert_ne!(node, current.nodes[0]);
        assert_eq!(node.settings.diff(&current.nodes[0].settings), None);
        assert_eq!(node.diff(&current.nodes[0]), None);
        assert_eq!(current.diff(&previous), None);
        assert_eq!(current.fork_point(&previous), None);

        let mut input_change = node.clone();
        input_change.inputs[0].required = false;
        let mut output_change = node.clone();
        output_change.outputs[0].multiple = true;
        for changed in [
            Node {
                id: Uuid::new_v4(),
                ..node.clone()
            },
            Node {
                name: "renamed".into(),
                ..node.clone()
            },
            Node {
                node_type: "other".into(),
                ..node.clone()
            },
            Node {
                outputs: vec![],
                ..node.clone()
            },
            input_change,
            output_change,
        ] {
            assert_eq!(node.diff(&changed), Some((node.clone(), changed)));
        }

        current.nodes[0].outputs[0].multiple = true;
        assert!(current.diff(&previous).is_some());
        assert_eq!(current.fork_point(&previous), Some(node.id));
    }
}
