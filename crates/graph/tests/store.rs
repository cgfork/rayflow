use graph::{
    DraftGraphStore, Graph,
    store::{Error, GraphDraftRecord, GraphRecord},
    types::{DAG, Input, Kind, Node, Output},
};
use jiff::Timestamp;
use toasty::Json;
use uuid::Uuid;

fn record() -> GraphRecord {
    let graph = DAG {
        id: Uuid::from_u128(1),
        name: "graph".into(),
        version: 3,
        node_count: 1,
        nodes: vec![Node {
            id: Uuid::from_u128(2),
            name: "task".into(),
            node_type: "script".into(),
            settings: Default::default(),
            inputs: vec![Input {
                file_name: "input.txt".into(),
                source: "source.output.txt".into(),
                required: false,
                format: Some("text".into()),
            }],
            outputs: vec![Output {
                file_name: "output.txt".into(),
                kind: Kind::File,
                multiple: true,
            }],
        }],
        edges: Vec::new(),
        ..DAG::default()
    };
    let now = Timestamp::now();
    GraphRecord {
        id: 1,
        uuid: graph.id,
        name: graph.name.clone(),
        version: graph.version,
        graph: Json(graph),
        updated_at: now,
        updated_by: "editor".into(),
        created_at: now,
        created_by: "author".into(),
    }
}

#[test]
fn record_round_trips_graph_json_and_audit_fields() {
    let record = record();
    let value = serde_json::to_value(&record).unwrap();
    let fields = value.as_object().unwrap();
    let expected_fields = [
        "id",
        "uuid",
        "name",
        "version",
        "graph",
        "updated_at",
        "updated_by",
        "created_at",
        "created_by",
    ];
    assert_eq!(fields.len(), expected_fields.len());
    for field in expected_fields {
        assert!(fields.contains_key(field));
    }
    assert!(value["graph"].is_object());
    assert_eq!(value["graph"]["version"], record.version);
    assert_eq!(
        serde_json::from_value::<GraphRecord>(value).unwrap(),
        record
    );
}

#[test]
fn consistent_record_returns_the_complete_graph() {
    let mut record = record();
    let mut expected = record.graph.0.clone();
    expected.updated_at = record.updated_at;
    expected.updated_by = record.updated_by.clone();
    expected.created_at = record.created_at;
    expected.created_by = record.created_by.clone();
    record.graph.node_count = 999;
    record.graph.updated_by = "untrusted".into();

    assert_eq!(DAG::try_from(record).unwrap(), expected);
}

#[test]
fn draft_record_populates_audit_fields_and_node_count() {
    let record = record();
    let expected = DAG::try_from(record.clone()).unwrap();
    let draft = GraphDraftRecord {
        id: record.id,
        uuid: record.uuid,
        name: record.name,
        graph: record.graph,
        updated_at: record.updated_at,
        updated_by: record.updated_by,
        created_at: record.created_at,
        created_by: record.created_by,
    };
    assert_eq!(DAG::try_from(draft).unwrap(), expected);
}

#[test]
fn dag_json_excludes_and_ignores_audit_fields() {
    let graph = DAG::try_from(record()).unwrap();
    let mut value = serde_json::to_value(&graph).unwrap();
    assert_eq!(value["node_count"], graph.nodes.len());
    for field in ["updated_at", "updated_by", "created_at", "created_by"] {
        assert!(value.get(field).is_none(), "{field}");
        value[field] = serde_json::json!({"untrusted": true});
    }
    let decoded: DAG = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(decoded.updated_at, Timestamp::UNIX_EPOCH);
    assert_eq!(decoded.created_at, Timestamp::UNIX_EPOCH);
    assert!(decoded.updated_by.is_empty());
    assert!(decoded.created_by.is_empty());
    assert_eq!(decoded.nodes, graph.nodes);

    value.as_object_mut().unwrap().remove("node_count");
    let mut legacy = record();
    legacy.graph = Json(serde_json::from_value(value).unwrap());
    assert_eq!(DAG::try_from(legacy).unwrap().node_count, 1);
}

#[test]
fn node_count_tracks_graph_mutations() {
    let mut graph = DAG::try_from(record()).unwrap();
    let mut node = graph.nodes[0].clone();
    node.id = Uuid::new_v4();
    graph.add_node(node.clone());
    assert_eq!(graph.node_count, 2);
    graph.delete_node(node.id);
    assert_eq!(graph.node_count, 1);
    graph.delete_node(node.id);
    assert_eq!(graph.node_count, 1);
}

#[test]
fn inconsistent_graph_identity_name_or_version_is_rejected() {
    let original = record();
    let mut wrong_id = original.clone();
    wrong_id.graph.id = Uuid::from_u128(100);
    let mut wrong_name = original.clone();
    wrong_name.graph.name = "another graph".into();
    let mut wrong_version = original.clone();
    wrong_version.graph.version += 1;

    for record in [wrong_id, wrong_name, wrong_version] {
        assert!(matches!(
            DAG::try_from(record),
            Err(Error::MetadataMismatch { graph_id, version })
                if graph_id == original.uuid && version == original.version
        ));
    }
}

#[test]
fn database_and_transaction_satisfy_graph_store_bounds() {
    fn assert_store<Store: DraftGraphStore<Uuid, G = DAG, Err = Error> + Send + Sync>() {}
    fn assert_error<StoreError: std::error::Error + Send + Sync + 'static>() {}

    assert_store::<toasty::Db>();
    assert_store::<toasty::Transaction<'_>>();
    assert_error::<Error>();
}
