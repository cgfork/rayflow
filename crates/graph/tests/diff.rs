use graph::{
    Diff,
    types::{DAG, Input, Kind, Node, Output},
};
use uuid::Uuid;

fn input() -> Input {
    Input {
        file_name: "input.txt".into(),
        source: "previous.output".into(),
        required: true,
        format: Some("text".into()),
    }
}

fn output() -> Output {
    Output {
        file_name: "output.txt".into(),
        kind: Kind::File,
        multiple: false,
    }
}

fn node(id: u128) -> Node {
    Node {
        id: Uuid::from_u128(id),
        name: format!("node-{id}"),
        node_type: "task".into(),
        settings: Default::default(),
        inputs: vec![input()],
        outputs: vec![output()],
    }
}

fn graph() -> DAG {
    DAG {
        id: Uuid::from_u128(100),
        name: "graph".into(),
        version: 1,
        node_count: 3,
        nodes: vec![node(1), node(2), node(3)],
        edges: vec![
            (Uuid::from_u128(1), Uuid::from_u128(2)),
            (Uuid::from_u128(2), Uuid::from_u128(3)),
        ],
        ..DAG::default()
    }
}

fn assert_value_diff<Value>(before: &Value, after: &Value)
where
    Value: Diff<Item = (Value, Value)> + Clone + PartialEq + std::fmt::Debug,
{
    assert_eq!(before.diff(after).unwrap(), (before.clone(), after.clone()),);
    assert_eq!(after.diff(before).unwrap(), (after.clone(), before.clone()),);
}

#[test]
fn equal_values_have_no_diff() {
    for kind in [Kind::File, Kind::Directory] {
        assert!(kind.diff(&kind).is_none());
    }
    let input = input();
    assert!(input.diff(&input.clone()).is_none());
    let output = output();
    assert!(output.diff(&output.clone()).is_none());
    let node = node(1);
    assert!(node.diff(&node.clone()).is_none());
    let graph = graph();
    assert!(graph.diff(&graph.clone()).is_none());
}

#[test]
fn kind_diff_retains_both_values() {
    assert_value_diff(&Kind::File, &Kind::Directory);
}

#[test]
fn input_diff_detects_each_field() {
    let before = input();
    for after in [
        Input {
            file_name: "other.txt".into(),
            ..before.clone()
        },
        Input {
            source: "other.output".into(),
            ..before.clone()
        },
        Input {
            required: false,
            ..before.clone()
        },
        Input {
            format: None,
            ..before.clone()
        },
        Input {
            format: Some("json".into()),
            ..before.clone()
        },
    ] {
        assert_value_diff(&before, &after);
    }
}

#[test]
fn output_diff_detects_each_field() {
    let before = output();
    for after in [
        Output {
            file_name: "other.txt".into(),
            ..before.clone()
        },
        Output {
            kind: Kind::Directory,
            ..before.clone()
        },
        Output {
            multiple: true,
            ..before.clone()
        },
    ] {
        assert_value_diff(&before, &after);
    }
}

#[test]
fn node_diff_detects_each_field_and_nested_values() {
    let before = node(1);
    for after in [
        Node {
            id: Uuid::from_u128(2),
            ..before.clone()
        },
        Node {
            name: "renamed".into(),
            ..before.clone()
        },
        Node {
            node_type: "other".into(),
            ..before.clone()
        },
        Node {
            settings: graph::types::NodeSettings {
                config: serde_json::json!({"image": "alpine:3.21"}),
                resource: graph::types::Resource {
                    cpu_cores: 1,
                    memory_bytes: 1024,
                },
                timeout: std::time::Duration::from_secs(10),
            },
            ..before.clone()
        },
        Node {
            inputs: vec![],
            ..before.clone()
        },
        Node {
            inputs: vec![Input {
                required: false,
                ..input()
            }],
            ..before.clone()
        },
        Node {
            outputs: vec![],
            ..before.clone()
        },
        Node {
            outputs: vec![Output {
                kind: Kind::Directory,
                ..output()
            }],
            ..before.clone()
        },
    ] {
        assert_value_diff(&before, &after);
    }
}

#[test]
fn graph_diff_ignores_iteration_order_and_graph_metadata() {
    let before = graph();
    let mut after = before.clone();
    after.id = Uuid::from_u128(101);
    after.name = "renamed".into();
    after.version += 1;
    after.nodes.reverse();
    after.edges.reverse();

    assert!(before.diff(&after).is_none());
    assert!(after.diff(&before).is_none());
}

#[test]
fn graph_diff_reports_changed_missing_and_extra_nodes_and_edges() {
    let before = graph();
    let mut after = before.clone();
    after.nodes[1].name = "renamed".into();
    after.nodes[2] = node(4);
    after.edges[1] = (Uuid::from_u128(2), Uuid::from_u128(4));

    let diff = before.diff(&after).unwrap();
    assert_eq!(
        diff.node_diffs,
        vec![(before.nodes[1].clone(), after.nodes[1].clone())]
    );
    assert_eq!(diff.node_miss, vec![Uuid::from_u128(4)]);
    assert_eq!(diff.node_over, vec![Uuid::from_u128(3)]);
    assert_eq!(diff.edge_miss, vec![after.edges[1]]);
    assert_eq!(diff.edge_over, vec![before.edges[1]]);

    let reverse = after.diff(&before).unwrap();
    assert_eq!(
        reverse.node_diffs,
        vec![(after.nodes[1].clone(), before.nodes[1].clone())]
    );
    assert_eq!(reverse.node_miss, diff.node_over);
    assert_eq!(reverse.node_over, diff.node_miss);
    assert_eq!(reverse.edge_miss, diff.edge_over);
    assert_eq!(reverse.edge_over, diff.edge_miss);
}

#[test]
fn graph_diff_handles_empty_graphs() {
    let populated = graph();
    let empty = DAG {
        node_count: 0,
        nodes: vec![],
        edges: vec![],
        ..populated.clone()
    };
    assert!(empty.diff(&empty).is_none());

    let diff = empty.diff(&populated).unwrap();
    assert!(diff.node_diffs.is_empty());
    assert_eq!(
        diff.node_miss,
        populated
            .nodes
            .iter()
            .map(|node| node.id)
            .collect::<Vec<_>>()
    );
    assert!(diff.node_over.is_empty());
    assert_eq!(diff.edge_miss, populated.edges);
    assert!(diff.edge_over.is_empty());

    let reverse = populated.diff(&empty).unwrap();
    assert!(reverse.node_diffs.is_empty());
    assert!(reverse.node_miss.is_empty());
    assert_eq!(reverse.node_over, diff.node_miss);
    assert!(reverse.edge_miss.is_empty());
    assert_eq!(reverse.edge_over, diff.edge_miss);
}

#[test]
fn graph_diff_detects_edge_only_changes_and_direction() {
    let before = graph();
    let mut after = before.clone();
    after.edges = vec![(Uuid::from_u128(2), Uuid::from_u128(1))];

    let diff = before.diff(&after).unwrap();
    assert!(diff.node_diffs.is_empty());
    assert!(diff.node_miss.is_empty());
    assert!(diff.node_over.is_empty());
    assert_eq!(diff.edge_miss, after.edges);
    assert_eq!(diff.edge_over, before.edges);
}

#[test]
fn graph_diff_detects_node_payload_only_changes() {
    let before = graph();
    let mut after = before.clone();
    after.nodes[0].inputs[0].format = None;
    after.nodes[1].outputs[0].multiple = true;

    let diff = before.diff(&after).unwrap();
    assert_eq!(
        diff.node_diffs,
        vec![
            (before.nodes[0].clone(), after.nodes[0].clone()),
            (before.nodes[1].clone(), after.nodes[1].clone()),
        ]
    );
    assert!(diff.node_miss.is_empty());
    assert!(diff.node_over.is_empty());
    assert!(diff.edge_miss.is_empty());
    assert!(diff.edge_over.is_empty());
}
