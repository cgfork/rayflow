use graph::{
    DraftGraphStore,
    types::{DAG, Node, NodeSettings, Resource},
};
use scheduler::actuator::NodeConfig;
use std::time::Duration;
use toasty::Db;
use uuid::Uuid;

pub fn graph_template() -> DAG {
    let base = 1_u128 << 96;
    DAG {
        id: Uuid::from_u128(base),
        name: "test-graph".into(),
        version: 1,
        node_count: 10,
        nodes: (1..=10)
            .map(|id| Node {
                id: Uuid::from_u128(base + id),
                name: format!("node-{id}"),
                node_type: "docker".into(),
                settings: NodeSettings {
                    config: serde_json::json!({"image":"test-image"}),
                    resource: Resource {
                        cpu_cores: 1,
                        memory_bytes: 1024,
                    },
                    timeout: Duration::from_secs(30),
                },
                inputs: vec![],
                outputs: vec![],
            })
            .collect(),
        edges: (1..10)
            .map(|id| (id, id + 1))
            .chain([(1, 3), (4, 6), (7, 9)])
            .map(|(from, to)| (Uuid::from_u128(base + from), Uuid::from_u128(base + to)))
            .collect(),
        ..DAG::default()
    }
}

pub async fn configure_graph(db: &mut Db, graph_id: Uuid) {
    let mut graph = graph_template();
    graph.id = graph_id;
    db.write(&graph, "test").await.unwrap();
    db.publish(graph_id, "test").await.unwrap();
    for node in &graph.nodes {
        let now = jiff::Timestamp::now();
        NodeConfig::create()
            .node_id(node.id)
            .version(1)
            .annotation_match(String::new())
            .config(serde_json::json!({}))
            .lease(Some(30_000))
            .required_cpu_cores(0)
            .required_memory_bytes(0)
            .created_at(now)
            .updated_at(now)
            .created_by(String::new())
            .updated_by(String::new())
            .exec(&mut *db)
            .await
            .unwrap();
    }
}
