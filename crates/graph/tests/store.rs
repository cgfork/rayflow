use graph::{
    GraphStore,
    store::{Error, GraphRecord, MySqlGraphStore},
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
        nodes: vec![Node {
            id: Uuid::from_u128(2),
            name: "task".into(),
            node_type: "script".into(),
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
    let record = record();
    let expected = record.graph.0.clone();

    assert_eq!(DAG::try_from(record).unwrap(), expected);
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
fn store_satisfies_graph_store_and_shared_state_bounds() {
    fn assert_store<Store: GraphStore<Uuid, G = DAG> + Clone + Send + Sync>() {}
    fn assert_error<StoreError: std::error::Error + Send + Sync + 'static>() {}

    assert_store::<MySqlGraphStore>();
    assert_error::<Error>();
}
