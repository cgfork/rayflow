use std::collections::HashSet;
use std::fmt;

use uuid::Uuid;

use crate::annotation_match::AnnotationMatch;
use crate::types::{AssetRef, DAG, DEFAULT_NODE_CONFIG_NAME, NamedNodeSettings, Node, Output};
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

/// A structural, configuration, or input binding error in a runnable workflow.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum WorkflowValidationError {
    #[error(transparent)]
    Structure(#[from] ValidationError<Uuid>),
    #[error("graph name must not be empty")]
    EmptyGraphName,
    #[error("node IDs and names must be unique and names must not be empty")]
    InvalidNodeIdentity,
    #[error(
        "{node}: invalid settings override name {name:?} (reserve 'default' for base settings and use up to 255 bytes without trailing spaces)"
    )]
    InvalidOverrideName { node: String, name: String },
    #[error("{node} has an invalid CEL annotation_match expression ({reason})")]
    InvalidAnnotationMatch { node: String, reason: String },
    #[error("{node}: task type and a JSON configuration object are required")]
    InvalidTaskConfig { node: String },
    #[error("{node}: CPU cores and memory must be positive")]
    InvalidResources { node: String },
    #[error("{node}: memory exceeds the supported limit")]
    MemoryLimit { node: String },
    #[error("{node}: GPU memory must be positive when specified")]
    InvalidGpuMemory { node: String },
    #[error("{node}: timeout must be positive whole milliseconds within timestamp range")]
    InvalidTimeout { node: String },
    #[error("{node}: Docker image is required")]
    MissingDockerImage { node: String },
    #[error("{node}: Docker {field} must be an array of strings")]
    InvalidDockerArray { node: String, field: &'static str },
    #[error("{node}: Docker working_dir must be a string")]
    InvalidDockerWorkingDir { node: String },
    #[error("{node}: {port} file names must be nonempty and unique")]
    InvalidFileNames { node: String, port: &'static str },
    #[error("{node}: input {input} must select an upstream output")]
    InvalidInputSource { node: String, input: String },
    #[error("{node}: invalid or overlapping asset path {path:?}")]
    InvalidAssetPath { node: String, path: String },
    #[error("{node}: asset {path:?} must select an asset version")]
    InvalidAssetId { node: String, path: String },
}

/// Validates asset declarations within the separate asset mount.
pub fn validate_assets(node: &Node) -> Result<(), WorkflowValidationError> {
    let invalid_path = |path: &str| WorkflowValidationError::InvalidAssetPath {
        node: node.name.clone(),
        path: path.to_owned(),
    };
    let mut paths = HashSet::new();
    for AssetRef {
        file_name,
        asset_id,
    } in &node.assets
    {
        if file_name.contains('\\')
            || file_name.chars().any(char::is_control)
            || file_name
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == ".." || part.contains(':'))
            || !paths.insert(file_name.as_str())
        {
            return Err(invalid_path(file_name));
        }
        if asset_id.is_nil() {
            return Err(WorkflowValidationError::InvalidAssetId {
                node: node.name.clone(),
                path: file_name.clone(),
            });
        }
    }
    for asset in &node.assets {
        let path = &asset.file_name;
        if path
            .match_indices('/')
            .any(|(index, _)| paths.contains(&path[..index]))
        {
            return Err(invalid_path(path));
        }
    }
    Ok(())
}

/// Validates runnable settings and input bindings before publishing a DAG.
pub fn validate_workflow(graph: &DAG) -> Result<(), WorkflowValidationError> {
    validate(graph)?;
    let mut ids = HashSet::new();
    let mut names = HashSet::new();
    if graph.name.trim().is_empty() {
        return Err(WorkflowValidationError::EmptyGraphName);
    }
    for node in &graph.nodes {
        if !ids.insert(node.id) || node.name.trim().is_empty() || !names.insert(&node.name) {
            return Err(WorkflowValidationError::InvalidNodeIdentity);
        }
        for name in node.settings.overrides.keys() {
            if name == DEFAULT_NODE_CONFIG_NAME || name.len() > 255 || name.ends_with(' ') {
                return Err(WorkflowValidationError::InvalidOverrideName {
                    node: node.name.clone(),
                    name: name.clone(),
                });
            }
        }
        for settings in node.settings.named_settings() {
            validate_node_settings(node, &settings)?;
        }
        validate_assets(node)?;
        for (label, ports) in [
            (
                "input",
                node.inputs
                    .iter()
                    .map(|input| input.file_name.as_str())
                    .collect::<Vec<_>>(),
            ),
            (
                "output",
                node.outputs
                    .iter()
                    .map(|output| output.file_name.as_str())
                    .collect::<Vec<_>>(),
            ),
        ] {
            let mut files = HashSet::new();
            if ports
                .iter()
                .any(|file| file.trim().is_empty() || !files.insert(file))
            {
                return Err(WorkflowValidationError::InvalidFileNames {
                    node: node.name.clone(),
                    port: label,
                });
            }
        }
        let mut upstream = HashSet::new();
        let mut pending = vec![node.id];
        while let Some(current) = pending.pop() {
            for &(source, target) in &graph.edges {
                if target == current && upstream.insert(source) {
                    pending.push(source);
                }
            }
        }
        for input in &node.inputs {
            let producers: Vec<_> = graph
                .nodes
                .iter()
                .filter(|producer| {
                    producer.outputs.iter().any(|output| {
                        input.source == format!("{}.{}", producer.name, output.file_name)
                    })
                })
                .collect();
            if producers.len() != 1 || !upstream.contains(&producers[0].id) {
                return Err(WorkflowValidationError::InvalidInputSource {
                    node: node.name.clone(),
                    input: input.file_name.clone(),
                });
            }
        }
    }
    Ok(())
}

fn validate_node_settings(
    node: &Node,
    settings: &NamedNodeSettings<'_>,
) -> Result<(), WorkflowValidationError> {
    let label = || {
        if settings.name == DEFAULT_NODE_CONFIG_NAME {
            node.name.clone()
        } else {
            format!("{} override {:?}", node.name, settings.name)
        }
    };
    AnnotationMatch::compile(settings.annotation_match).map_err(|error| {
        WorkflowValidationError::InvalidAnnotationMatch {
            node: label(),
            reason: error.to_string(),
        }
    })?;
    if node.node_type.trim().is_empty() || !settings.config.is_object() {
        return Err(WorkflowValidationError::InvalidTaskConfig { node: label() });
    }
    if settings.resource.cpu_cores == 0 || settings.resource.memory_bytes == 0 {
        return Err(WorkflowValidationError::InvalidResources { node: label() });
    }
    if settings.resource.memory_bytes > i64::MAX as u64 {
        return Err(WorkflowValidationError::MemoryLimit { node: label() });
    }
    if settings.resource.gpu_memory_bytes == Some(0) {
        return Err(WorkflowValidationError::InvalidGpuMemory { node: label() });
    }
    if settings.timeout_ms().is_none() {
        return Err(WorkflowValidationError::InvalidTimeout { node: label() });
    }
    if node.node_type == "docker" {
        if !settings
            .config
            .get("image")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|image| !image.trim().is_empty())
        {
            return Err(WorkflowValidationError::MissingDockerImage { node: label() });
        }
        for field in ["command", "env"] {
            if settings.config.get(field).is_some_and(|value| {
                !value
                    .as_array()
                    .is_some_and(|items| items.iter().all(serde_json::Value::is_string))
            }) {
                return Err(WorkflowValidationError::InvalidDockerArray {
                    node: label(),
                    field,
                });
            }
        }
        if settings
            .config
            .get("working_dir")
            .is_some_and(|value| !value.is_string())
        {
            return Err(WorkflowValidationError::InvalidDockerWorkingDir { node: label() });
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn older_nodes_serialize_an_empty_asset_array_and_mount_paths_cannot_overlap() {
        let mut node: Node = serde_json::from_value(serde_json::json!({
            "id": Uuid::new_v4(), "name": "task", "node_type": "docker",
            "inputs": [], "outputs": [],
        }))
        .unwrap();
        assert_eq!(
            serde_json::to_value(&node).unwrap()["assets"],
            serde_json::json!([])
        );

        let asset_id = Uuid::new_v4();
        node.assets = ["scripts/run.lua", "scripts-data"]
            .map(|file_name| AssetRef {
                file_name: file_name.into(),
                asset_id,
            })
            .to_vec();
        validate_assets(&node).unwrap();
        node.assets.push(AssetRef {
            file_name: "scripts".into(),
            asset_id,
        });
        assert!(matches!(
            validate_assets(&node),
            Err(WorkflowValidationError::InvalidAssetPath { .. }),
        ));
    }

    #[test]
    fn gpu_memory_must_be_positive_and_old_device_selectors_are_ignored() {
        let mut node: Node = serde_json::from_value(serde_json::json!({
            "id": Uuid::new_v4(), "name": "task", "node_type": "docker",
            "settings": {
                "config": {"image": "alpine"},
                "resource": {"cpu_cores": 1, "memory_bytes": 1024, "gpu_cdi_device": "nvidia.com/gpu=1"},
                "timeout": "30s",
            },
            "inputs": [], "outputs": [],
        }))
        .unwrap();
        let validate = |node: &Node| {
            validate_node_settings(node, &node.settings.named_settings().next().unwrap())
        };
        validate(&node).unwrap();
        node.settings.resource.gpu_memory_bytes = Some(4096);
        validate(&node).unwrap();
        assert_eq!(
            serde_json::to_value(&node).unwrap()["settings"]["resource"],
            serde_json::json!({"cpu_cores": 1, "memory_bytes": 1024, "gpu_memory_bytes": 4096})
        );
        node.settings.resource.gpu_memory_bytes = Some(0);
        assert!(matches!(
            validate(&node),
            Err(WorkflowValidationError::InvalidGpuMemory { .. }),
        ));
    }
}
