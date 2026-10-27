use scheduler::actuator::NodeConfig;
use server::StaticGraphStore;
use toasty::Db;
use uuid::Uuid;

pub async fn configure_graph(db: &mut Db, graph_id: Uuid) {
    let store = StaticGraphStore::new();
    for node in &store.graph(graph_id).unwrap().nodes {
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
