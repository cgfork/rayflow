use jiff::Timestamp;
use std::{collections::HashMap, convert::Infallible, time::Duration};
use uuid::Uuid;

use crate::{
    Diff, Graph, Id, NodeMut, NodeRef,
    diff::{field_diffs, value_diff},
};

impl Id for Uuid {}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Kind {
    File,
    Directory,
}

impl Diff for Kind {
    type Item = (Self, Self);

    fn diff(&self, other: &Self) -> Option<Self::Item> {
        value_diff(self, other)
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

/// A changed output field and its before and after values.
#[derive(Clone, Debug, PartialEq)]
pub enum OutputDiff {
    FileName((String, String)),
    Kind((Kind, Kind)),
    Multiple((bool, bool)),
}

impl Diff for Output {
    type Item = Vec<OutputDiff>;

    fn diff(&self, other: &Self) -> Option<Self::Item> {
        field_diffs([
            self.file_name
                .diff(&other.file_name)
                .map(OutputDiff::FileName),
            self.kind.diff(&other.kind).map(OutputDiff::Kind),
            value_diff(&self.multiple, &other.multiple).map(OutputDiff::Multiple),
        ])
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

/// A changed input field and its before and after values.
#[derive(Clone, Debug, PartialEq)]
pub enum InputDiff {
    FileName((String, String)),
    Source((String, String)),
    Required((bool, bool)),
    Format((Option<String>, Option<String>)),
}

impl Diff for Input {
    type Item = Vec<InputDiff>;

    fn diff(&self, other: &Self) -> Option<Self::Item> {
        field_diffs([
            self.file_name
                .diff(&other.file_name)
                .map(InputDiff::FileName),
            self.source.diff(&other.source).map(InputDiff::Source),
            value_diff(&self.required, &other.required).map(InputDiff::Required),
            value_diff(&self.format, &other.format).map(InputDiff::Format),
        ])
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Resource {
    pub cpu_cores: u32,
    pub memory_bytes: u64,
}

/// A changed resource field and its before and after values.
#[derive(Clone, Debug, PartialEq)]
pub enum ResourceDiff {
    CpuCores((u32, u32)),
    MemoryBytes((u64, u64)),
}

impl Diff for Resource {
    type Item = Vec<ResourceDiff>;

    fn diff(&self, other: &Self) -> Option<Self::Item> {
        field_diffs([
            value_diff(&self.cpu_cores, &other.cpu_cores).map(ResourceDiff::CpuCores),
            value_diff(&self.memory_bytes, &other.memory_bytes).map(ResourceDiff::MemoryBytes),
        ])
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
    type Item = Infallible;

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

/// A changed node field, including item-level changes for inputs and outputs.
#[derive(Clone, Debug, PartialEq)]
pub enum NodeDiff {
    Id((Uuid, Uuid)),
    Name((String, String)),
    NodeType((String, String)),
    Settings(<NodeSettings as Diff>::Item),
    Inputs(<Vec<Input> as Diff>::Item),
    Outputs(<Vec<Output> as Diff>::Item),
}

impl Diff for Node {
    type Item = Vec<NodeDiff>;

    fn diff(&self, other: &Self) -> Option<Self::Item> {
        field_diffs([
            self.id.diff(&other.id).map(NodeDiff::Id),
            self.name.diff(&other.name).map(NodeDiff::Name),
            self.node_type
                .diff(&other.node_type)
                .map(NodeDiff::NodeType),
            self.settings.diff(&other.settings).map(NodeDiff::Settings),
            self.inputs.diff(&other.inputs).map(NodeDiff::Inputs),
            self.outputs.diff(&other.outputs).map(NodeDiff::Outputs),
        ])
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
    use crate::{ForkPoint, diff::VecDiff};

    #[test]
    fn settings_only_changes_do_not_affect_structural_diffs() {
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

        current.nodes[0].outputs[0].multiple = true;
        assert!(current.diff(&previous).is_some());
        assert_eq!(current.fork_point(&previous), Some(node.id));
    }

    #[test]
    fn node_diff_reports_all_changed_fields_and_port_items() {
        let input = Input {
            file_name: "input.txt".into(),
            source: "build.result.txt".into(),
            required: true,
            format: None,
        };
        let added_input = Input {
            file_name: "extra.txt".into(),
            ..input.clone()
        };
        let output = Output {
            file_name: "result.txt".into(),
            kind: Kind::File,
            multiple: false,
        };
        let removed_output = Output {
            file_name: "unused.txt".into(),
            ..output.clone()
        };
        let before = Node {
            id: Uuid::from_u128(1),
            name: "task".into(),
            node_type: "docker".into(),
            settings: NodeSettings::default(),
            inputs: vec![
                input.clone(),
                Input {
                    file_name: "other.txt".into(),
                    ..input
                },
            ],
            outputs: vec![output, removed_output.clone()],
        };
        let mut after = before.clone();
        after.id = Uuid::from_u128(2);
        after.name = "renamed".into();
        after.node_type = "other".into();
        after.inputs[0] = Input {
            file_name: "input.csv".into(),
            source: "build.result.csv".into(),
            required: false,
            format: Some("csv".into()),
        };
        after.inputs[1].required = false;
        after.inputs.push(added_input.clone());
        after.outputs[0] = Output {
            file_name: "results".into(),
            kind: Kind::Directory,
            multiple: true,
        };
        after.outputs.pop();

        assert_eq!(
            before.diff(&after),
            Some(vec![
                NodeDiff::Id((before.id, after.id)),
                NodeDiff::Name(("task".into(), "renamed".into())),
                NodeDiff::NodeType(("docker".into(), "other".into())),
                NodeDiff::Inputs(vec![
                    VecDiff::Changed {
                        index: 0,
                        changes: vec![
                            InputDiff::FileName(("input.txt".into(), "input.csv".into())),
                            InputDiff::Source((
                                "build.result.txt".into(),
                                "build.result.csv".into()
                            )),
                            InputDiff::Required((true, false)),
                            InputDiff::Format((None, Some("csv".into()))),
                        ],
                    },
                    VecDiff::Changed {
                        index: 1,
                        changes: vec![InputDiff::Required((true, false))],
                    },
                    VecDiff::Added {
                        index: 2,
                        value: added_input,
                    },
                ]),
                NodeDiff::Outputs(vec![
                    VecDiff::Changed {
                        index: 0,
                        changes: vec![
                            OutputDiff::FileName(("result.txt".into(), "results".into())),
                            OutputDiff::Kind((Kind::File, Kind::Directory)),
                            OutputDiff::Multiple((false, true)),
                        ],
                    },
                    VecDiff::Removed {
                        index: 1,
                        value: removed_output,
                    },
                ]),
            ])
        );
    }

    #[test]
    fn resource_diff_reports_both_changed_limits() {
        let before = Resource {
            cpu_cores: 1,
            memory_bytes: 512,
        };
        let after = Resource {
            cpu_cores: 2,
            memory_bytes: 1024,
        };
        assert_eq!(
            before.diff(&after),
            Some(vec![
                ResourceDiff::CpuCores((1, 2)),
                ResourceDiff::MemoryBytes((512, 1024)),
            ])
        );
    }
}
