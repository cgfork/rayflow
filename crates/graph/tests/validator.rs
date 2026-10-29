use graph::{
    types::{DAG, Input, Kind, Node, Output},
    validator::{UnreferencedOutput, ValidationError, unreferenced_outputs, validate},
};
use uuid::Uuid;

fn graph(node_count: u128, edges: &[(u128, u128)]) -> DAG {
    DAG {
        id: Uuid::from_u128(100),
        name: "graph".into(),
        version: 1,
        nodes: (1..=node_count)
            .map(|id| Node {
                id: Uuid::from_u128(id),
                name: format!("node-{id}"),
                node_type: "task".into(),
                inputs: Vec::new(),
                outputs: Vec::new(),
            })
            .collect(),
        edges: edges
            .iter()
            .map(|&(from, to)| (Uuid::from_u128(from), Uuid::from_u128(to)))
            .collect(),
    }
}

fn output(file_name: &str) -> Output {
    Output {
        file_name: file_name.into(),
        kind: Kind::File,
        multiple: false,
    }
}

fn input(source: &str) -> Input {
    Input {
        file_name: "local.txt".into(),
        source: source.into(),
        required: true,
        format: None,
    }
}

#[test]
fn accepts_single_node_and_single_root_dags() {
    for graph in [
        graph(1, &[]),
        graph(3, &[(1, 2), (2, 3)]),
        graph(3, &[(1, 2), (1, 3)]),
        graph(4, &[(1, 2), (1, 3), (2, 4), (3, 4)]),
    ] {
        assert_eq!(validate(&graph), Ok(()));
    }
}

#[test]
fn empty_graph_has_no_root() {
    assert_eq!(
        validate(&graph(0, &[])),
        Err(ValidationError::InvalidRootCount { count: 0 })
    );
}

#[test]
fn rejects_multiple_roots_in_connected_and_disconnected_graphs() {
    for graph in [graph(2, &[]), graph(3, &[(1, 3), (2, 3)])] {
        assert_eq!(
            validate(&graph),
            Err(ValidationError::InvalidRootCount { count: 2 })
        );
    }
}

#[test]
fn rejects_self_loop_and_cycles_without_roots() {
    for graph in [graph(1, &[(1, 1)]), graph(3, &[(1, 2), (2, 3), (3, 1)])] {
        assert_eq!(validate(&graph), Err(ValidationError::Cycle));
    }
}

#[test]
fn rejects_cycle_reachable_from_a_single_root() {
    let graph = graph(4, &[(1, 2), (2, 3), (3, 2), (3, 4)]);

    assert_eq!(validate(&graph), Err(ValidationError::Cycle));
}

#[test]
fn rejects_disconnected_cycle_beside_a_valid_root() {
    let graph = graph(4, &[(1, 2), (3, 4), (4, 3)]);

    assert_eq!(validate(&graph), Err(ValidationError::Cycle));
}

#[test]
fn rejects_missing_edge_endpoints() {
    for graph in [graph(1, &[(2, 1)]), graph(1, &[(1, 2)])] {
        assert_eq!(
            validate(&graph),
            Err(ValidationError::UnknownNode(Uuid::from_u128(2)))
        );
    }
}

#[test]
fn repeated_edges_do_not_create_a_cycle() {
    let graph = graph(2, &[(1, 2), (1, 2)]);

    assert_eq!(validate(&graph), Ok(()));
}

#[test]
fn validation_ignores_node_and_edge_iteration_order() {
    let mut graph = graph(4, &[(1, 2), (1, 3), (2, 4), (3, 4)]);
    graph.nodes.reverse();
    graph.edges.reverse();

    assert_eq!(validate(&graph), Ok(()));
}

#[test]
fn unreferenced_outputs_are_not_validation_errors() {
    let mut graph = graph(1, &[]);
    graph.nodes[0].outputs = vec![output("result.txt"), output("log.txt")];

    assert_eq!(validate(&graph), Ok(()));
    assert_eq!(
        unreferenced_outputs(&graph),
        vec![
            UnreferencedOutput {
                node_id: graph.nodes[0].id,
                output: &graph.nodes[0].outputs[0],
            },
            UnreferencedOutput {
                node_id: graph.nodes[0].id,
                output: &graph.nodes[0].outputs[1],
            },
        ]
    );
}

#[test]
fn references_use_source_and_distinguish_producers_with_the_same_file_name() {
    let mut graph = graph(3, &[(1, 2), (1, 3)]);
    graph.nodes[0].outputs = vec![output("result.txt"), output("log.txt")];
    graph.nodes[1].outputs = vec![output("result.txt")];
    graph.nodes[2].inputs = vec![input("node-1.result.txt")];

    assert_eq!(
        unreferenced_outputs(&graph),
        vec![
            UnreferencedOutput {
                node_id: graph.nodes[0].id,
                output: &graph.nodes[0].outputs[1],
            },
            UnreferencedOutput {
                node_id: graph.nodes[1].id,
                output: &graph.nodes[1].outputs[0],
            },
        ]
    );
}

#[test]
fn optional_inputs_and_repeated_references_mark_outputs_as_used() {
    let mut graph = graph(2, &[(1, 2)]);
    graph.nodes[0].outputs = vec![Output {
        file_name: "results".into(),
        kind: Kind::Directory,
        multiple: true,
    }];
    graph.nodes[1].inputs = vec![
        Input {
            required: false,
            ..input("node-1.results")
        },
        Input {
            required: false,
            ..input("node-1.results")
        },
    ];

    assert!(unreferenced_outputs(&graph).is_empty());
}

#[test]
fn partial_source_matches_do_not_reference_outputs() {
    let mut graph = graph(2, &[(1, 2)]);
    graph.nodes[0].outputs = vec![output("result.txt")];
    graph.nodes[1].inputs = vec![
        input("result.txt"),
        input("node-1.result"),
        input("node-1.result.txt.extra"),
    ];

    assert_eq!(
        unreferenced_outputs(&graph),
        vec![UnreferencedOutput {
            node_id: graph.nodes[0].id,
            output: &graph.nodes[0].outputs[0],
        }]
    );
}

#[test]
fn output_reporting_does_not_require_a_valid_graph() {
    assert!(unreferenced_outputs(&graph(0, &[])).is_empty());
    let mut graph = graph(1, &[(1, 1)]);
    graph.nodes[0].outputs.push(output("result.txt"));

    assert_eq!(
        unreferenced_outputs(&graph),
        vec![UnreferencedOutput {
            node_id: graph.nodes[0].id,
            output: &graph.nodes[0].outputs[0],
        }]
    );
}
