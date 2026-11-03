use agent::graphfs::GraphFs;
use graph::{GraphStore, PageRequest, types::DAG};
use uuid::Uuid;

#[tokio::test]
async fn cached_graphs_list_numeric_versions_and_filter_by_id() {
    let root = tempfile::tempdir().unwrap();
    let mut store = GraphFs::from_workdir(root.path());
    let empty = store.graphs(PageRequest::default(), None).await.unwrap();
    assert!(empty.items.is_empty());
    assert_eq!(empty.next_cursor, None);
    let id = Uuid::from_u128(1);
    for (id, version) in [(id, 2), (id, 10), (Uuid::from_u128(2), 0)] {
        store
            .write_graph(DAG {
                id,
                name: "cached".into(),
                version,
                nodes: vec![],
                edges: vec![],
                ..DAG::default()
            })
            .await
            .unwrap();
    }
    let first = store
        .graphs(PageRequest::new(None, 1).unwrap(), Some(id))
        .await
        .unwrap();
    assert_eq!(first.items[0].version, 10);
    assert!(first.next_cursor.is_some());
    let next = store
        .graphs(PageRequest::new(first.next_cursor, 1).unwrap(), Some(id))
        .await
        .unwrap();
    assert_eq!(next.items[0].id, id);
    assert_eq!(next.items[0].version, 2);
    assert_eq!(next.next_cursor, None);
    assert_eq!(
        store
            .graphs(PageRequest::default(), None)
            .await
            .unwrap()
            .items
            .len(),
        3
    );
    assert!(
        store
            .graphs(PageRequest::default(), Some(Uuid::new_v4()))
            .await
            .unwrap()
            .items
            .is_empty()
    );
}
