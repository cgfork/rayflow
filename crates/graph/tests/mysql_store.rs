//! Set DATABASE_URL to run against MySQL. Only a uniquely prefixed test table
//! is created and dropped; existing tables are never reset.

use graph::{
    DraftGraphStore, GraphStore, PageRequest,
    store::{Error, GraphDraftRecord, GraphRecord},
    types::{DAG, Input, Kind, Node, Output},
};
use jiff::Timestamp;
use toasty::{Db, Json};
use uuid::Uuid;

fn graph() -> DAG {
    let source = Node {
        id: Uuid::new_v4(),
        name: "source".into(),
        node_type: "docker".into(),
        settings: settings(),
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
        node_type: "docker".into(),
        settings: settings(),
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
        node_count: 2,
        ..DAG::default()
    }
}

fn settings() -> graph::types::NodeSettings {
    graph::types::NodeSettings {
        config: serde_json::json!({"image": "alpine:3.21"}),
        resource: graph::types::Resource {
            cpu_cores: 1,
            memory_bytes: 1024,
        },
        timeout: std::time::Duration::from_secs(30),
    }
}

fn assert_definition(actual: &DAG, expected: &DAG) {
    assert_eq!(
        serde_json::to_value(actual).unwrap(),
        serde_json::to_value(expected).unwrap()
    );
    assert_eq!(actual.node_count, actual.nodes.len());
}

async fn seed_record(db: &mut Db, graph: &DAG, actor: &str) -> toasty::Result<GraphRecord> {
    let now = Timestamp::now();
    GraphRecord::create()
        .uuid(graph.id)
        .name(graph.name.clone())
        .version(graph.version)
        .graph(Json(graph.clone()))
        .updated_at(now)
        .updated_by(actor.to_owned())
        .created_at(now)
        .created_by(actor.to_owned())
        .exec(db)
        .await
}

async fn write_and_read_draft<S: DraftGraphStore<Uuid, G = DAG>>(
    store: &mut S,
    graph: &DAG,
    actor: &str,
) -> Result<DAG, S::Err> {
    store.write(graph, actor).await?;
    store.read(graph.id).await
}

async fn publish_and_load<S: DraftGraphStore<Uuid>>(
    store: &mut S,
    graph_id: Uuid,
    actor: &str,
) -> Result<S::G, S::Err> {
    store.publish(graph_id, actor).await?;
    store.load(graph_id, None).await
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
    let mut store = db.clone();
    let initial = graph();

    for version in [None, Some(0), Some(100)] {
        assert!(matches!(
            store.load(initial.id, version).await,
            Err(Error::GraphNotFound { graph_id, version: requested })
                if graph_id == initial.id && requested == version
        ));
    }

    let stored = seed_record(&mut db, &initial, "author").await.unwrap();
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
    assert_definition(&store.load(initial.id, Some(0)).await.unwrap(), &initial);
    assert_definition(
        &store.clone().load(initial.id, None).await.unwrap(),
        &initial,
    );

    let mut latest = initial.clone();
    latest.version = 9;
    latest.name = "new name".into();
    latest.nodes[1].inputs[0].required = false;
    let latest_record = seed_record(&mut db, &latest, "editor").await.unwrap();
    let latest_record = GraphRecord::filter_by_id(latest_record.id)
        .get(&mut db)
        .await
        .unwrap();
    let mut intermediate = initial.clone();
    intermediate.version = 2;
    seed_record(&mut db, &intermediate, "author").await.unwrap();
    let mut other = graph();
    other.version = 100;
    seed_record(&mut db, &other, "other author").await.unwrap();

    assert_definition(&store.load(initial.id, None).await.unwrap(), &latest);
    for expected in [&initial, &intermediate, &latest, &other] {
        assert_definition(
            &store
                .load(expected.id, Some(expected.version))
                .await
                .unwrap(),
            expected,
        );
    }
    assert_definition(&store.load(other.id, None).await.unwrap(), &other);
    assert!(matches!(
        store.load(initial.id, Some(100)).await,
        Err(Error::GraphNotFound { graph_id, version: Some(100) }) if graph_id == initial.id
    ));
    let mut duplicate = initial.clone();
    duplicate.name = "must not replace the original".into();
    assert!(seed_record(&mut db, &duplicate, "editor").await.is_err());
    assert_definition(&store.load(initial.id, Some(0)).await.unwrap(), &initial);

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
    let loaded = store.load(initial.id, None).await.unwrap();
    assert_definition(&loaded, &latest);
    assert_eq!(loaded.updated_at, updated.updated_at);
    assert_eq!(loaded.updated_by, "reviewer");
    assert_eq!(loaded.created_at, latest_record.created_at);
    assert_eq!(loaded.created_by, "editor");

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

/// Raw DDL uses the deployed schema and isolates each test without resetting existing tables.
async fn draft_database() -> Option<(Db, String)> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping MySQL graph draft test: DATABASE_URL is not set");
        return None;
    };
    let prefix = format!("graph_draft_test_{}_", Uuid::new_v4().simple());
    let mut builder = toasty::Db::builder();
    builder.models(graph::models()).table_name_prefix(&prefix);
    let mut db = builder.connect(&url).await.unwrap();

    for table_name in ["graph", "graph_draft"] {
        let statement = include_str!("../../../rayflow.sql")
            .split("\n\n")
            .find(|statement| {
                statement.starts_with(&format!("CREATE TABLE IF NOT EXISTS {table_name} ("))
            })
            .expect("table is defined in rayflow.sql")
            .replacen(
                &format!("CREATE TABLE IF NOT EXISTS {table_name} ("),
                &format!("CREATE TABLE IF NOT EXISTS `{prefix}{table_name}` ("),
                1,
            );
        toasty::sql::statement(statement)
            .exec(&mut db)
            .await
            .unwrap();
    }
    Some((db, prefix))
}

#[tokio::test]
async fn publishing_increments_the_maximum_published_version_ignoring_the_draft_version() {
    let Some((mut db, prefix)) = draft_database().await else {
        return;
    };
    let mut draft = graph();
    draft.version = 1000;
    let mut unrelated = graph();
    unrelated.version = 100;
    seed_record(&mut db, &unrelated, "importer").await.unwrap();

    db.write(&draft, "editor").await.unwrap();
    let first = publish_and_load(&mut db, draft.id, "publisher")
        .await
        .unwrap();
    assert_eq!(first.version, 1);
    assert_eq!(db.read(draft.id).await.unwrap().version, 1000);

    for version in [9, 2, 7] {
        let imported = DAG {
            version,
            ..draft.clone()
        };
        seed_record(&mut db, &imported, "importer").await.unwrap();
    }
    for (draft_version, expected_version) in [(0, 10), (1000, 11)] {
        draft.version = draft_version;
        db.write(&draft, "editor").await.unwrap();
        let published = publish_and_load(&mut db, draft.id, "publisher")
            .await
            .unwrap();
        assert_eq!(published.version, expected_version);
        let expected = DAG {
            version: expected_version,
            ..draft.clone()
        };
        assert_definition(&published, &expected);
        assert_definition(&db.read(draft.id).await.unwrap(), &draft);
        assert_definition(&db.load(draft.id, Some(1)).await.unwrap(), &first);
    }
    assert_definition(&db.load(unrelated.id, None).await.unwrap(), &unrelated);

    for table_name in ["graph", "graph_draft"] {
        toasty::sql::statement(format!("DROP TABLE `{prefix}{table_name}`"))
            .exec(&mut db)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn mysql_store_reads_writes_and_publishes_graph_drafts() {
    let Some((mut db, prefix)) = draft_database().await else {
        return;
    };

    let mut store = db.clone();
    let initial = graph();
    assert!(matches!(
        store.read(initial.id).await,
        Err(Error::DraftNotFound(graph_id)) if graph_id == initial.id
    ));

    let author = String::from("author");
    let saved = write_and_read_draft(&mut store, &initial, &author)
        .await
        .unwrap();
    assert_definition(&saved, &initial);
    let draft = GraphDraftRecord::filter_by_uuid(initial.id)
        .get(&mut db)
        .await
        .unwrap();
    assert_eq!(draft.uuid, initial.id);
    assert_eq!(draft.graph.0, initial);
    assert_eq!(draft.created_by, "author");
    assert_eq!(draft.updated_by, "author");
    assert_eq!(saved.updated_at, draft.updated_at);
    assert_eq!(saved.updated_by, draft.updated_by);
    assert_eq!(saved.created_at, draft.created_at);
    assert_eq!(saved.created_by, draft.created_by);
    assert_eq!(store.read(initial.id).await.unwrap(), saved);

    let mut edited = initial.clone();
    edited.name = "edited graph".into();
    edited.nodes[1].inputs[0].required = false;
    store.write(&edited, "editor").await.unwrap();
    let updated = GraphDraftRecord::filter_by_uuid(edited.id)
        .get(&mut db)
        .await
        .unwrap();
    assert_eq!(updated.id, draft.id);
    assert_eq!(updated.name, edited.name);
    assert_eq!(updated.graph.0, edited);
    assert_eq!(updated.created_by, "author");
    assert_eq!(updated.updated_by, "editor");
    let loaded = store.read(edited.id).await.unwrap();
    assert_definition(&loaded, &edited);
    assert_eq!(loaded.updated_at, updated.updated_at);
    assert_eq!(loaded.updated_by, "editor");
    assert_eq!(loaded.created_at, draft.created_at);
    assert_eq!(loaded.created_by, "author");
    let persisted = GraphDraftRecord::filter_by_id(updated.id)
        .get(&mut db)
        .await
        .unwrap();
    assert_eq!(persisted.graph, updated.graph);

    store.publish(initial.id, "publisher").await.unwrap();
    let published = GraphRecord::filter_by_uuid(initial.id)
        .get(&mut db)
        .await
        .unwrap();
    assert_eq!(published.uuid, initial.id);
    assert_eq!(published.version, 1);
    assert_eq!(published.name, edited.name);
    assert_eq!(published.graph.0.version, 1);
    assert_eq!(published.created_by, "publisher");
    assert_eq!(published.updated_by, "publisher");
    assert_eq!(published.created_at, published.updated_at);
    assert_eq!(
        store.load(initial.id, Some(1)).await.unwrap(),
        DAG::try_from(published.clone()).unwrap()
    );

    let publisher = String::from("publisher");
    let published_again = publish_and_load(&mut store, initial.id, &publisher)
        .await
        .unwrap();
    assert_eq!(published_again.version, 2);
    assert_eq!(store.load(initial.id, None).await.unwrap(), published_again);
    assert_eq!(
        store.load(initial.id, Some(1)).await.unwrap(),
        DAG::try_from(published.clone()).unwrap()
    );

    let mut invalid = edited;
    invalid
        .edges
        .push((invalid.nodes[1].id, invalid.nodes[0].id));
    store.write(&invalid, "editor").await.unwrap();
    assert!(matches!(
        store.publish(initial.id, "publisher").await,
        Err(Error::InvalidDraft(_))
    ));

    for table_name in ["graph", "graph_draft"] {
        toasty::sql::statement(format!("DROP TABLE `{prefix}{table_name}`"))
            .exec(&mut db)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn mysql_transaction_rollback_discards_draft_and_published_versions() {
    let Some((mut db, prefix)) = draft_database().await else {
        return;
    };
    let mut observer = db.clone();
    let mut tx = db.transaction().await.unwrap();
    let initial = DAG {
        version: 1,
        ..graph()
    };
    assert_definition(
        &write_and_read_draft(&mut tx, &initial, "author")
            .await
            .unwrap(),
        &initial,
    );
    assert_definition(
        &publish_and_load(&mut tx, initial.id, "publisher")
            .await
            .unwrap(),
        &initial,
    );

    let mut edited = initial.clone();
    edited.name = "edited in transaction".into();
    tx.write(&edited, "editor").await.unwrap();
    let published = publish_and_load(&mut tx, initial.id, "publisher")
        .await
        .unwrap();
    let mut expected = edited.clone();
    expected.version = 2;
    assert_definition(&published, &expected);
    assert_definition(&tx.read(initial.id).await.unwrap(), &edited);
    assert_definition(&tx.load(initial.id, Some(1)).await.unwrap(), &initial);
    assert_definition(&tx.load(initial.id, Some(2)).await.unwrap(), &expected);
    assert!(matches!(
        observer.read(initial.id).await,
        Err(Error::DraftNotFound(graph_id)) if graph_id == initial.id
    ));
    assert!(matches!(
        observer.load(initial.id, None).await,
        Err(Error::GraphNotFound { graph_id, .. }) if graph_id == initial.id
    ));

    tx.rollback().await.unwrap();
    assert!(matches!(
        db.read(initial.id).await,
        Err(Error::DraftNotFound(graph_id)) if graph_id == initial.id
    ));
    for version in [None, Some(1), Some(2)] {
        assert!(matches!(
            db.load(initial.id, version).await,
            Err(Error::GraphNotFound { graph_id, .. }) if graph_id == initial.id
        ));
    }

    for table_name in ["graph", "graph_draft"] {
        toasty::sql::statement(format!("DROP TABLE `{prefix}{table_name}`"))
            .exec(&mut db)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn mysql_transaction_can_commit_after_invalid_publish() {
    let Some((mut db, prefix)) = draft_database().await else {
        return;
    };
    let mut tx = db.transaction().await.unwrap();
    let valid = DAG {
        version: 1,
        ..graph()
    };
    let mut invalid = valid.clone();
    invalid
        .edges
        .push((invalid.nodes[1].id, invalid.nodes[0].id));
    tx.write(&invalid, "author").await.unwrap();
    assert!(matches!(
        tx.publish(invalid.id, "publisher").await,
        Err(Error::InvalidDraft(_))
    ));
    assert_definition(&tx.read(valid.id).await.unwrap(), &invalid);
    assert!(matches!(
        tx.load(valid.id, None).await,
        Err(Error::GraphNotFound { graph_id, .. }) if graph_id == valid.id
    ));

    tx.write(&valid, "editor").await.unwrap();
    tx.publish(valid.id, "publisher").await.unwrap();
    tx.commit().await.unwrap();

    assert_definition(&db.read(valid.id).await.unwrap(), &valid);
    assert_definition(&db.load(valid.id, Some(1)).await.unwrap(), &valid);
    let draft = GraphDraftRecord::filter_by_uuid(valid.id)
        .get(&mut db)
        .await
        .unwrap();
    assert_eq!(draft.created_by, "author");
    assert_eq!(draft.updated_by, "editor");
    let published = GraphRecord::filter_by_uuid(valid.id)
        .get(&mut db)
        .await
        .unwrap();
    assert_eq!(published.created_by, "publisher");
    assert_eq!(published.updated_by, "publisher");
    assert_eq!(published.created_at, published.updated_at);

    for table_name in ["graph", "graph_draft"] {
        toasty::sql::statement(format!("DROP TABLE `{prefix}{table_name}`"))
            .exec(&mut db)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn mysql_lists_complete_graph_versions_and_drafts() {
    let Some((mut db, prefix)) = draft_database().await else {
        return;
    };
    assert!(
        GraphStore::graphs(&mut db, PageRequest::default(), None)
            .await
            .unwrap()
            .items
            .is_empty()
    );
    assert!(
        DraftGraphStore::list(&mut db, PageRequest::default())
            .await
            .unwrap()
            .items
            .is_empty()
    );

    let first = DAG {
        version: 1,
        ..graph()
    };
    let mut second = DAG {
        version: 1,
        ..graph()
    };
    db.write(&first, "author").await.unwrap();
    db.publish(first.id, "publisher").await.unwrap();
    db.publish(first.id, "publisher").await.unwrap();
    db.write(&second, "author").await.unwrap();
    db.publish(second.id, "publisher").await.unwrap();

    let page = db
        .graphs(PageRequest::new(None, 2).unwrap(), None)
        .await
        .unwrap();
    assert_eq!(page.items.len(), 2);
    let mut published = first.clone();
    published.version = 2;
    assert_definition(&page.items[0], &second);
    assert_definition(&page.items[1], &published);
    let record = GraphRecord::filter_by_uuid(first.id)
        .filter(GraphRecord::fields().version().eq(2))
        .get(&mut db)
        .await
        .unwrap();
    assert_eq!(page.next_cursor, Some(record.id));
    assert_eq!(page.items[1].updated_at, record.updated_at);
    assert_eq!(page.items[1].created_at, record.created_at);
    assert_eq!(page.items[1].updated_by, "publisher");
    assert_eq!(page.items[1].created_by, "publisher");
    let next = GraphStore::graphs(
        &mut db,
        PageRequest::new(page.next_cursor, 2).unwrap(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(next.items.len(), 1);
    assert_eq!(next.items[0].version, 1);
    assert_definition(&next.items[0], &first);
    assert_eq!(next.next_cursor, None);

    let filtered = GraphStore::graphs(&mut db, PageRequest::new(None, 1).unwrap(), Some(first.id))
        .await
        .unwrap();
    assert_definition(&filtered.items[0], &published);
    let older = GraphStore::graphs(
        &mut db,
        PageRequest::new(filtered.next_cursor, 1).unwrap(),
        Some(first.id),
    )
    .await
    .unwrap();
    assert_definition(&older.items[0], &first);
    assert_eq!(older.next_cursor, None);
    assert!(
        GraphStore::graphs(&mut db, PageRequest::default(), Some(Uuid::new_v4()))
            .await
            .unwrap()
            .items
            .is_empty()
    );
    let empty = GraphStore::graphs(&mut db, PageRequest::new(Some(0), 1).unwrap(), None)
        .await
        .unwrap();
    assert!(empty.items.is_empty());
    assert_eq!(empty.next_cursor, None);

    second.name = "edited".into();
    db.write(&second, "editor").await.unwrap();
    let drafts = db.list(PageRequest::new(None, 1).unwrap()).await.unwrap();
    assert_definition(&drafts.items[0], &second);
    let record = GraphDraftRecord::filter_by_uuid(second.id)
        .get(&mut db)
        .await
        .unwrap();
    assert_eq!(drafts.next_cursor, Some(record.id));
    assert_eq!(drafts.items[0].updated_at, record.updated_at);
    assert_eq!(drafts.items[0].created_at, record.created_at);
    assert_eq!(drafts.items[0].updated_by, "editor");
    assert_eq!(drafts.items[0].created_by, "author");
    let third = graph();
    db.write(&third, "author").await.unwrap();
    let next = DraftGraphStore::list(&mut db, PageRequest::new(drafts.next_cursor, 1).unwrap())
        .await
        .unwrap();
    assert_eq!(next.items.len(), 1);
    assert_definition(&next.items[0], &first);
    assert_eq!(next.next_cursor, None);
    let empty = DraftGraphStore::list(&mut db, PageRequest::new(Some(0), 1).unwrap())
        .await
        .unwrap();
    assert!(empty.items.is_empty());
    assert_eq!(empty.next_cursor, None);

    for table_name in ["graph", "graph_draft"] {
        toasty::sql::statement(format!("DROP TABLE `{prefix}{table_name}`"))
            .exec(&mut db)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn mysql_graph_pages_reject_mismatched_record_metadata() {
    let Some((mut db, prefix)) = draft_database().await else {
        return;
    };
    let definition = graph();
    db.write(&definition, "author").await.unwrap();
    db.publish(definition.id, "publisher").await.unwrap();
    GraphRecord::filter_by_uuid(definition.id)
        .update()
        .name("inconsistent".to_owned())
        .exec(&mut db)
        .await
        .unwrap();
    assert!(matches!(db.graphs(PageRequest::default(), None).await,
        Err(Error::MetadataMismatch { graph_id, version: 1 }) if graph_id == definition.id));
    GraphDraftRecord::filter_by_uuid(definition.id)
        .update()
        .name("inconsistent".to_owned())
        .exec(&mut db)
        .await
        .unwrap();
    assert!(matches!(db.list(PageRequest::default()).await,
        Err(Error::DraftMetadataMismatch(graph_id)) if graph_id == definition.id));

    for table_name in ["graph", "graph_draft"] {
        toasty::sql::statement(format!("DROP TABLE `{prefix}{table_name}`"))
            .exec(&mut db)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn mysql_transaction_lists_uncommitted_rows_without_publishing_them_to_other_connections() {
    let Some((mut db, prefix)) = draft_database().await else {
        return;
    };
    let mut observer = db.clone();
    let mut tx = db.transaction().await.unwrap();
    let definition = DAG {
        version: 1,
        ..graph()
    };
    tx.write(&definition, "author").await.unwrap();
    tx.publish(definition.id, "publisher").await.unwrap();
    assert_definition(
        &tx.graphs(PageRequest::default(), None).await.unwrap().items[0],
        &definition,
    );
    assert_definition(
        &tx.list(PageRequest::default()).await.unwrap().items[0],
        &definition,
    );
    assert!(
        GraphStore::graphs(&mut observer, PageRequest::default(), None)
            .await
            .unwrap()
            .items
            .is_empty()
    );
    assert!(
        DraftGraphStore::list(&mut observer, PageRequest::default())
            .await
            .unwrap()
            .items
            .is_empty()
    );
    tx.rollback().await.unwrap();
    assert!(
        GraphStore::graphs(&mut db, PageRequest::default(), None)
            .await
            .unwrap()
            .items
            .is_empty()
    );
    assert!(
        DraftGraphStore::list(&mut db, PageRequest::default())
            .await
            .unwrap()
            .items
            .is_empty()
    );

    for table_name in ["graph", "graph_draft"] {
        toasty::sql::statement(format!("DROP TABLE `{prefix}{table_name}`"))
            .exec(&mut db)
            .await
            .unwrap();
    }
}
