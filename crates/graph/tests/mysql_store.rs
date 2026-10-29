//! Set DATABASE_URL to run against MySQL. Only a uniquely prefixed test table
//! is created and dropped; existing tables are never reset.

use graph::{
    GraphStore,
    store::{Error, GraphRecord, MySqlGraphStore},
    types::{DAG, Input, Kind, Node, Output},
};
use jiff::Timestamp;
use toasty::Json;
use uuid::Uuid;

fn graph() -> DAG {
    let source = Node {
        id: Uuid::new_v4(),
        name: "source".into(),
        node_type: "script".into(),
        inputs: Vec::new(),
        outputs: vec![Output {
            file_name: "output.txt".into(),
            kind: Kind::File,
            multiple: false,
        }],
    };
    let consumer = Node {
        id: Uuid::new_v4(),
        name: "consumer".into(),
        node_type: "script".into(),
        inputs: vec![Input {
            file_name: "local.txt".into(),
            source: "source.output.txt".into(),
            required: true,
            format: Some("text".into()),
        }],
        outputs: Vec::new(),
    };
    DAG {
        id: Uuid::new_v4(),
        name: "versioned graph".into(),
        version: 0,
        edges: vec![(source.id, consumer.id)],
        nodes: vec![source, consumer],
    }
}

/// Raw DDL checks the deployed SQL schema against the ORM and removes only
/// this test's isolated table, which Toasty cannot drop individually.
#[tokio::test]
async fn mysql_store_persists_versioned_graphs_and_metadata() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping MySQL graph store test: DATABASE_URL is not set");
        return;
    };
    let prefix = format!("graph_test_{}_", Uuid::new_v4().simple());
    let mut builder = toasty::Db::builder();
    builder.models(graph::models()).table_name_prefix(&prefix);
    let mut db = builder.connect(&url).await.unwrap();
    let table = format!("{prefix}graph");
    let schema = include_str!("../../../rayflow.sql")
        .split("\n\n")
        .find(|statement| statement.starts_with("CREATE TABLE IF NOT EXISTS graph ("))
        .expect("graph table is defined in rayflow.sql")
        .replacen(
            "CREATE TABLE IF NOT EXISTS graph (",
            &format!("CREATE TABLE IF NOT EXISTS `{table}` ("),
            1,
        );
    toasty::sql::statement(schema).exec(&mut db).await.unwrap();
    let store = MySqlGraphStore::new(db.clone());
    let initial = graph();

    for version in [None, Some(0), Some(100)] {
        assert!(matches!(
            store.load(initial.id, version).await,
            Err(Error::GraphNotFound { graph_id, version: requested })
                if graph_id == initial.id && requested == version
        ));
    }

    let stored = store.insert(&initial, "author").await.unwrap();
    assert!(stored.id > 0);
    assert_eq!(stored.uuid, initial.id);
    assert_eq!(stored.name, initial.name);
    assert_eq!(stored.version, initial.version);
    assert_eq!(stored.graph.0, initial);
    assert_eq!(stored.created_by, "author");
    assert_eq!(stored.updated_by, "author");
    assert_eq!(stored.created_at, stored.updated_at);
    let persisted = GraphRecord::filter_by_id(stored.id)
        .get(&mut db)
        .await
        .unwrap();
    assert_eq!(persisted.uuid, stored.uuid);
    assert_eq!(persisted.name, stored.name);
    assert_eq!(persisted.version, stored.version);
    assert_eq!(persisted.graph, stored.graph);
    assert_eq!(persisted.created_by, stored.created_by);
    assert_eq!(persisted.updated_by, stored.updated_by);
    assert_eq!(persisted.created_at, persisted.updated_at);
    assert!(
        persisted
            .created_at
            .as_nanosecond()
            .abs_diff(stored.created_at.as_nanosecond())
            < 1000
    );
    assert_eq!(store.load(initial.id, Some(0)).await.unwrap(), initial);
    assert_eq!(store.clone().load(initial.id, None).await.unwrap(), initial);

    let mut latest = initial.clone();
    latest.version = 9;
    latest.name = "new name".into();
    latest.nodes[1].inputs[0].required = false;
    let latest_record = store.insert(&latest, "editor").await.unwrap();
    let latest_record = GraphRecord::filter_by_id(latest_record.id)
        .get(&mut db)
        .await
        .unwrap();
    let mut intermediate = initial.clone();
    intermediate.version = 2;
    store.insert(&intermediate, "author").await.unwrap();
    let mut other = graph();
    other.version = 100;
    store.insert(&other, "other author").await.unwrap();

    assert_eq!(store.load(initial.id, None).await.unwrap(), latest);
    for expected in [&initial, &intermediate, &latest, &other] {
        assert_eq!(
            store
                .load(expected.id, Some(expected.version))
                .await
                .unwrap(),
            *expected
        );
    }
    assert_eq!(store.load(other.id, None).await.unwrap(), other);
    assert!(matches!(
        store.load(initial.id, Some(100)).await,
        Err(Error::GraphNotFound { graph_id, version: Some(100) }) if graph_id == initial.id
    ));
    let mut duplicate = initial.clone();
    duplicate.name = "must not replace the original".into();
    assert!(matches!(
        store.insert(&duplicate, "editor").await,
        Err(Error::Toasty(_))
    ));
    assert_eq!(store.load(initial.id, Some(0)).await.unwrap(), initial);

    GraphRecord::filter_by_id(latest_record.id)
        .update()
        .updated_at(Timestamp::now())
        .updated_by("reviewer".to_owned())
        .exec(&mut db)
        .await
        .unwrap();
    let updated = GraphRecord::filter_by_id(latest_record.id)
        .get(&mut db)
        .await
        .unwrap();
    assert_eq!(updated.updated_by, "reviewer");
    assert_eq!(updated.created_at, latest_record.created_at);
    assert_eq!(updated.created_by, latest_record.created_by);
    assert_eq!(store.load(initial.id, None).await.unwrap(), latest);

    let mut mismatched = latest.clone();
    mismatched.version = 10;
    GraphRecord::filter_by_id(latest_record.id)
        .update()
        .graph(Json(mismatched))
        .exec(&mut db)
        .await
        .unwrap();
    assert!(matches!(
        store.load(initial.id, None).await,
        Err(Error::MetadataMismatch { graph_id, version: 9 }) if graph_id == initial.id
    ));

    toasty::sql::statement(format!("DROP TABLE `{table}`"))
        .exec(&mut db)
        .await
        .unwrap();
    assert!(matches!(
        store.load(initial.id, None).await,
        Err(Error::Toasty(_))
    ));
}
