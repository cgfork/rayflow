use jiff::Timestamp;
use std::{
    collections::{BTreeSet, HashMap},
    time::Duration,
};
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
#[serde(from = "OutputData")]
pub struct Output {
    /// File name of the output.
    pub file_name: String,
    /// Whether the output is a file or a directory.
    pub kind: Kind,
}

#[derive(serde::Deserialize)]
struct OutputData {
    file_name: String,
    kind: Kind,
    #[serde(default)]
    multiple: bool,
}

impl From<OutputData> for Output {
    fn from(data: OutputData) -> Self {
        Self {
            file_name: data.file_name,
            // Legacy multi-file outputs used the directory artifact format.
            kind: if data.multiple {
                Kind::Directory
            } else {
                data.kind
            },
        }
    }
}

/// A changed output field and its before and after values.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
#[serde(tag = "field", content = "change", rename_all = "snake_case")]
pub enum OutputDiff {
    FileName((String, String)),
    Kind((Kind, Kind)),
}

impl Diff for Output {
    type Item = Vec<OutputDiff>;

    fn diff(&self, other: &Self) -> Option<Self::Item> {
        field_diffs([
            self.file_name
                .diff(&other.file_name)
                .map(OutputDiff::FileName),
            self.kind.diff(&other.kind).map(OutputDiff::Kind),
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
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
#[serde(tag = "field", content = "change", rename_all = "snake_case")]
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

/// A pinned shared asset mounted under the node's requested file name.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AssetRef {
    pub file_name: String,
    pub asset_id: Uuid,
}

/// A changed shared asset declaration.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "field", content = "change", rename_all = "snake_case")]
pub enum AssetRefDiff {
    FileName((String, String)),
    AssetId((Uuid, Uuid)),
}

impl Diff for AssetRef {
    type Item = Vec<AssetRefDiff>;

    fn diff(&self, other: &Self) -> Option<Self::Item> {
        field_diffs([
            self.file_name
                .diff(&other.file_name)
                .map(AssetRefDiff::FileName),
            self.asset_id
                .diff(&other.asset_id)
                .map(AssetRefDiff::AssetId),
        ])
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Resource {
    pub cpu_cores: u32,
    pub memory_bytes: u64,
    /// Minimum GPU memory in bytes, when the job requests a GPU.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_memory_bytes: Option<u64>,
    /// Exact qualified CDI device required by the job.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_cdi_device: Option<String>,
}

/// A changed resource field and its before and after values.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
#[serde(tag = "field", content = "change", rename_all = "snake_case")]
pub enum ResourceDiff {
    CpuCores((u32, u32)),
    MemoryBytes((u64, u64)),
    GpuMemoryBytes((Option<u64>, Option<u64>)),
    GpuCdiDevice((Option<String>, Option<String>)),
}

impl Diff for Resource {
    type Item = Vec<ResourceDiff>;

    fn diff(&self, other: &Self) -> Option<Self::Item> {
        field_diffs([
            value_diff(&self.cpu_cores, &other.cpu_cores).map(ResourceDiff::CpuCores),
            value_diff(&self.memory_bytes, &other.memory_bytes).map(ResourceDiff::MemoryBytes),
            value_diff(&self.gpu_memory_bytes, &other.gpu_memory_bytes)
                .map(ResourceDiff::GpuMemoryBytes),
            value_diff(&self.gpu_cdi_device, &other.gpu_cdi_device).map(ResourceDiff::GpuCdiDevice),
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

/// A named override added, removed, or updated between two settings.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct OverrideDiff {
    pub name: String,
    pub before: Option<AnnotationMatchNodeSettings>,
    pub after: Option<AnnotationMatchNodeSettings>,
}

/// Changes to a node's base settings and named annotation overrides.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
#[serde(tag = "field", content = "change", rename_all = "snake_case")]
pub enum NodeSettingsDiff {
    Config((serde_json::Value, serde_json::Value)),
    Resource(<Resource as Diff>::Item),
    Timeout(
        #[serde(with = "jiff::fmt::serde::unsigned_duration::friendly::compact::required")]
        Duration,
        #[serde(with = "jiff::fmt::serde::unsigned_duration::friendly::compact::required")]
        Duration,
    ),
    Overrides(Vec<OverrideDiff>),
}

impl Diff for NodeSettings {
    type Item = Vec<NodeSettingsDiff>;

    fn diff(&self, other: &Self) -> Option<Self::Item> {
        let names: BTreeSet<_> = self
            .overrides
            .keys()
            .chain(other.overrides.keys())
            .collect();
        let overrides: Vec<_> = names
            .into_iter()
            .filter_map(|name| {
                let before = self.overrides.get(name);
                let after = other.overrides.get(name);
                (before != after).then(|| OverrideDiff {
                    name: name.clone(),
                    before: before.cloned(),
                    after: after.cloned(),
                })
            })
            .collect();
        field_diffs([
            value_diff(&self.config, &other.config).map(NodeSettingsDiff::Config),
            self.resource
                .diff(&other.resource)
                .map(NodeSettingsDiff::Resource),
            value_diff(&self.timeout, &other.timeout)
                .map(|(before, after)| NodeSettingsDiff::Timeout(before, after)),
            (!overrides.is_empty()).then_some(NodeSettingsDiff::Overrides(overrides)),
        ])
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
    /// Shared files available before this node starts.
    #[serde(default)]
    pub assets: Vec<AssetRef>,
}

/// A changed node field, including settings and individual artifact declarations.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
#[serde(tag = "field", content = "change", rename_all = "snake_case")]
pub enum NodeDiff {
    Id((Uuid, Uuid)),
    Name((String, String)),
    NodeType((String, String)),
    Settings(<NodeSettings as Diff>::Item),
    Inputs(<Vec<Input> as Diff>::Item),
    Outputs(<Vec<Output> as Diff>::Item),
    Assets(<Vec<AssetRef> as Diff>::Item),
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
            self.assets.diff(&other.assets).map(NodeDiff::Assets),
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
    use crate::{
        ForkPoint,
        diff::{DagDiff, VecDiff},
    };
    use serde_json::json;

    #[test]
    fn output_reads_legacy_flags_and_serializes_only_the_kind() {
        for (input, expected_kind) in [
            (json!({"file_name": "result", "kind": "File"}), Kind::File),
            (
                json!({"file_name": "result", "kind": "File", "multiple": false}),
                Kind::File,
            ),
            (
                json!({"file_name": "result", "kind": "File", "multiple": true}),
                Kind::Directory,
            ),
            (
                json!({"file_name": "result", "kind": "Directory", "multiple": false}),
                Kind::Directory,
            ),
        ] {
            let output: Output = serde_json::from_value(input).unwrap();
            assert_eq!(output.kind, expected_kind);
            assert_eq!(
                serde_json::to_value(output).unwrap(),
                json!({"file_name": "result", "kind": expected_kind}),
            );
        }
    }

    #[test]
    fn settings_only_changes_appear_in_diffs_and_keep_nodes_shared() {
        let timeout_override = |seconds| AnnotationMatchNodeSettings {
            timeout: Some(Duration::from_secs(seconds)),
            ..Default::default()
        };
        let node = Node {
            id: Uuid::new_v4(),
            name: "task".into(),
            node_type: "docker".into(),
            assets: vec![],
            settings: NodeSettings {
                overrides: HashMap::from([
                    ("large".into(), timeout_override(3)),
                    ("old".into(), timeout_override(2)),
                    ("same".into(), timeout_override(5)),
                ]),
                ..Default::default()
            },
            inputs: vec![Input {
                file_name: "input.txt".into(),
                source: "build.result.txt".into(),
                required: true,
                format: None,
            }],
            outputs: vec![Output {
                file_name: "result.txt".into(),
                kind: Kind::File,
            }],
        };
        let previous = DAG {
            nodes: vec![node.clone()],
            ..DAG::default()
        };
        let mut current = previous.clone();
        current.nodes[0].settings = serde_json::from_value(json!({
            "config": {"image": "alpine"},
            "resource": {"cpu_cores": 1, "memory_bytes": 512},
            "timeout": "2s",
            "overrides": {"extra": {"timeout": "6s"}, "large": {"timeout": "4s"}, "same": {"timeout": "5s"}}
        }))
        .unwrap();

        let expected = vec![
            NodeSettingsDiff::Config((json!(null), json!({"image": "alpine"}))),
            NodeSettingsDiff::Resource(vec![
                ResourceDiff::CpuCores((0, 1)),
                ResourceDiff::MemoryBytes((0, 512)),
            ]),
            NodeSettingsDiff::Timeout(Duration::ZERO, Duration::from_secs(2)),
            NodeSettingsDiff::Overrides(vec![
                OverrideDiff {
                    name: "extra".into(),
                    before: None,
                    after: Some(timeout_override(6)),
                },
                OverrideDiff {
                    name: "large".into(),
                    before: Some(timeout_override(3)),
                    after: Some(timeout_override(4)),
                },
                OverrideDiff {
                    name: "old".into(),
                    before: Some(timeout_override(2)),
                    after: None,
                },
            ]),
        ];
        assert_eq!(node.settings.diff(&node.settings), None);
        assert_eq!(
            node.settings.diff(&current.nodes[0].settings),
            Some(expected.clone())
        );
        let changes = previous.diff(&current).unwrap();
        let [DagDiff::Structure(changes)] = changes.as_slice() else {
            panic!("expected only node changes, got {changes:?}");
        };
        assert_eq!(
            changes.node_diffs,
            vec![(node.id, vec![NodeDiff::Settings(expected)])]
        );
        assert_eq!(current.fork_point(&previous), None);
        assert_eq!(
            current
                .shared_nodes(&previous)
                .map(|node| node.id)
                .collect::<Vec<_>>(),
            [node.id]
        );

        current.nodes[0].outputs[0].kind = Kind::Directory;
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
        };
        let removed_output = Output {
            file_name: "unused.txt".into(),
            ..output.clone()
        };
        let before = Node {
            id: Uuid::from_u128(1),
            name: "task".into(),
            node_type: "docker".into(),
            assets: vec![],
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
    fn resource_diff_reports_changed_requirements() {
        let before = Resource {
            cpu_cores: 1,
            memory_bytes: 512,
            gpu_memory_bytes: None,
            gpu_cdi_device: None,
        };
        let after = Resource {
            cpu_cores: 2,
            memory_bytes: 1024,
            gpu_memory_bytes: Some(4096),
            gpu_cdi_device: Some("nvidia.com/gpu=1".into()),
        };
        assert_eq!(
            serde_json::to_value(&before).unwrap(),
            json!({"cpu_cores": 1, "memory_bytes": 512})
        );
        assert_eq!(
            serde_json::from_value::<Resource>(json!({"cpu_cores": 1, "memory_bytes": 512}))
                .unwrap(),
            before
        );
        assert_eq!(
            before.diff(&after),
            Some(vec![
                ResourceDiff::CpuCores((1, 2)),
                ResourceDiff::MemoryBytes((512, 1024)),
                ResourceDiff::GpuMemoryBytes((None, Some(4096))),
                ResourceDiff::GpuCdiDevice((None, Some("nvidia.com/gpu=1".into()))),
            ])
        );
    }
}
