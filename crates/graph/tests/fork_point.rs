use std::time::Duration;

use graph::{
    ForkPoint, Graph,
    types::{DAG, Node},
};
use uuid::Uuid;

fn dag(node_ids: &[u128], edges: &[(u128, u128)]) -> DAG {
    DAG {
        id: Uuid::from_u128(100),
        name: "workflow".into(),
        version: 1,
        node_count: node_ids.len(),
        nodes: node_ids
            .iter()
            .map(|&id| Node {
                id: Uuid::from_u128(id),
                name: format!("node-{id}"),
                node_type: "docker".into(),
                settings: Default::default(),
                inputs: Vec::new(),
                outputs: Vec::new(),
            })
            .collect(),
        edges: edges
            .iter()
            .map(|&(source, target)| (Uuid::from_u128(source), Uuid::from_u128(target)))
            .collect(),
        ..DAG::default()
    }
}

fn shared_nodes(current: &DAG, previous: &DAG) -> Vec<u128> {
    current
        .shared_nodes(previous)
        .map(|node| node.id.as_u128())
        .collect()
}

#[test]
fn identical_graphs_have_no_fork_point_or_shared_predecessors() {
    for graph in [DAG::default(), dag(&[1], &[]), dag(&[1, 2], &[(1, 2)])] {
        assert_eq!(graph.fork_point(&graph), None);
        assert!(graph.shared_nodes(&graph).next().is_none());
    }
}

#[test]
fn metadata_and_iteration_order_do_not_create_a_fork() {
    let previous = dag(&[1, 2, 3], &[(1, 2), (2, 3)]);
    let mut current = previous.clone();
    current.id = Uuid::from_u128(200);
    current.name = "renamed".into();
    current.version = 2;
    current.node_count = 0;
    current.created_at = jiff::Timestamp::now();
    current.updated_at = current.created_at;
    current.created_by = "creator".into();
    current.updated_by = "editor".into();
    current.nodes.reverse();
    current.edges.reverse();
    assert_eq!(current.fork_point(&previous), None);
    assert!(shared_nodes(&current, &previous).is_empty());
}

#[test]
fn payload_change_forks_at_the_changed_node() {
    let previous = dag(&[1, 2, 3, 4], &[(1, 2), (2, 3), (3, 4)]);
    let mut current = previous.clone();
    current.nodes[2].name = "changed".into();
    assert_eq!(current.fork_point(&previous), Some(Uuid::from_u128(3)));
    assert_eq!(previous.fork_point(&current), Some(Uuid::from_u128(3)));
    assert_eq!(shared_nodes(&current, &previous), vec![1, 2]);
    let shared = current.shared_nodes(&previous).next().unwrap();
    assert!(std::ptr::eq(shared.data, &current.nodes[0]));
}

#[test]
fn runnable_settings_changes_create_a_fork() {
    let previous = dag(&[1, 2, 3], &[(1, 2), (2, 3)]);
    let mut config = previous.clone();
    config.nodes[1].settings.config = serde_json::json!({ "image": "alpine:3.21" });
    let mut resource = previous.clone();
    resource.nodes[1].settings.resource.cpu_cores = 2;
    let mut timeout = previous.clone();
    timeout.nodes[1].settings.timeout = Duration::from_secs(10);
    for current in [config, resource, timeout] {
        assert_eq!(current.fork_point(&previous), Some(Uuid::from_u128(2)));
        assert_eq!(shared_nodes(&current, &previous), vec![1]);
    }
}

#[test]
fn adding_a_tail_node_forks_in_the_new_graph_only() {
    let previous = dag(&[1, 2], &[(1, 2)]);
    let current = dag(&[1, 2, 3], &[(1, 2), (2, 3)]);
    assert_eq!(current.fork_point(&previous), Some(Uuid::from_u128(3)));
    assert_eq!(shared_nodes(&current, &previous), vec![1, 2]);
    assert_eq!(previous.fork_point(&current), None);
}

#[test]
fn removing_an_intermediate_node_forks_at_its_surviving_successor() {
    let previous = dag(&[1, 2, 3, 4], &[(1, 2), (2, 3), (3, 4)]);
    let current = dag(&[1, 2, 4], &[(1, 2), (2, 4)]);
    assert_eq!(current.fork_point(&previous), Some(Uuid::from_u128(4)));
    assert_eq!(shared_nodes(&current, &previous), vec![1, 2]);
}

#[test]
fn adding_or_removing_an_edge_forks_at_its_target() {
    let previous = dag(&[1, 2, 3], &[(1, 2), (2, 3)]);
    let mut current = previous.clone();
    current.add_edge(Uuid::from_u128(1), Uuid::from_u128(3));
    assert_eq!(current.fork_point(&previous), Some(Uuid::from_u128(3)));
    assert_eq!(previous.fork_point(&current), Some(Uuid::from_u128(3)));
    assert_eq!(shared_nodes(&current, &previous), vec![1, 2]);
    assert_eq!(shared_nodes(&previous, &current), vec![1, 2]);
}

#[test]
fn root_changes_have_no_shared_predecessors() {
    let previous = dag(&[1, 2], &[(1, 2)]);
    let mut changed = previous.clone();
    changed.nodes[0].name = "changed".into();
    let replaced = dag(&[3, 2], &[(3, 2)]);
    for (current, root) in [(changed, 1), (replaced, 3)] {
        assert_eq!(current.fork_point(&previous), Some(Uuid::from_u128(root)));
        assert!(shared_nodes(&current, &previous).is_empty());
    }
}

#[test]
fn earliest_change_wins_over_changed_descendants_regardless_of_node_order() {
    let previous = dag(&[1, 2, 3, 4], &[(1, 2), (2, 3), (3, 4)]);
    let mut current = previous.clone();
    current.nodes[1].name = "earliest".into();
    current.nodes[3].name = "downstream".into();
    current.nodes.reverse();
    current.edges.reverse();
    assert_eq!(current.fork_point(&previous), Some(Uuid::from_u128(2)));
    assert_eq!(shared_nodes(&current, &previous), vec![1]);
}

#[test]
fn join_change_shares_all_ancestors_without_duplicates() {
    let previous = dag(&[1, 2, 3, 4], &[(1, 2), (1, 3), (2, 4), (3, 4)]);
    let mut current = previous.clone();
    current.nodes[3].name = "changed".into();
    assert_eq!(current.fork_point(&previous), Some(Uuid::from_u128(4)));
    assert_eq!(shared_nodes(&current, &previous), vec![1, 2, 3]);
    current.nodes.reverse();
    current.edges.reverse();
    assert_eq!(shared_nodes(&current, &previous), vec![3, 2, 1]);
}

#[test]
fn shared_ancestors_exclude_unrelated_branches_and_changed_descendants() {
    let previous = dag(
        &[1, 2, 3, 4, 5, 6, 7],
        &[(1, 2), (2, 3), (3, 4), (2, 5), (4, 6), (5, 7)],
    );
    let mut current = previous.clone();
    current.nodes[3].name = "fork".into();
    current.nodes[5].name = "downstream".into();
    assert_eq!(current.fork_point(&previous), Some(Uuid::from_u128(4)));
    assert_eq!(shared_nodes(&current, &previous), vec![1, 2, 3]);
}

#[test]
fn changes_on_independent_branches_have_no_unique_fork_point() {
    let previous = dag(&[1, 2, 3, 4], &[(1, 2), (1, 3), (2, 4), (3, 4)]);
    let mut current = previous.clone();
    current.nodes[1].name = "left".into();
    current.nodes[2].name = "right".into();
    current.nodes[3].name = "joined".into();
    assert_eq!(current.fork_point(&previous), None);
    assert!(shared_nodes(&current, &previous).is_empty());
    current.nodes[0].name = "root".into();
    assert_eq!(current.fork_point(&previous), Some(Uuid::from_u128(1)));
}

#[test]
fn a_new_graph_forks_at_its_root_and_an_empty_graph_does_not() {
    let current = dag(&[1, 2, 3], &[(1, 2), (2, 3)]);
    assert_eq!(
        current.fork_point(&DAG::default()),
        Some(Uuid::from_u128(1))
    );
    assert_eq!(DAG::default().fork_point(&current), None);
    let unrelated = dag(&[4, 5], &[(4, 5)]);
    assert_eq!(current.fork_point(&unrelated), Some(Uuid::from_u128(1)));
    assert_eq!(unrelated.fork_point(&current), Some(Uuid::from_u128(4)));
}
