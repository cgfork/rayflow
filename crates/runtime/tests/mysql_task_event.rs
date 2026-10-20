//! Integration test for the MySQL-backed task event log.
//!
//! It needs a MySQL instance. Set `DATABASE_URL` (for example
//! `mysql://root:password@127.0.0.1:3306/rayflow`) to run it; the test resets
//! the database and pushes the schema from the Toasty models itself. Without
//! `DATABASE_URL` the test is skipped.
#![cfg(feature = "mysql")]

use runtime::{TaskEvent, TaskEventEntry, TaskEventWrite};
use toasty::Db;
use uuid::Uuid;

/// Connects and rebuilds the schema from the Toasty models, or returns `None`
/// when no `DATABASE_URL` is configured.
async fn setup() -> Option<Db> {
    let url = std::env::var("DATABASE_URL").ok()?;

    let mut builder = Db::builder();
    builder.models(runtime::mysql::models());
    let db = builder
        .connect(&url)
        .await
        .expect("connect to DATABASE_URL");

    db.reset_db().await.expect("reset database");
    db.push_schema().await.expect("push schema");

    Some(db)
}

fn entry(node: u128, shard: u32, event: TaskEvent) -> TaskEventEntry {
    TaskEventEntry {
        event_offset: 0,
        id: Uuid::new_v4(),
        task_id: Uuid::new_v4(),
        data_id: Uuid::from_u128(0xda7a),
        graph_id: Uuid::from_u128(0x9a1),
        node_id: Uuid::from_u128(node),
        node_run_id: 1,
        shard,
        event,
        created_at: jiff::Timestamp::now(),
    }
}

#[tokio::test]
async fn writes_polls_and_acks_task_events() {
    let Some(mut db) = setup().await else {
        eprintln!("skipping: set DATABASE_URL to run the MySQL task event test");
        return;
    };

    entry(1, 0, TaskEvent::Succeed)
        .write(&mut db)
        .await
        .expect("write succeed");
    entry(2, 0, TaskEvent::Fail)
        .write(&mut db)
        .await
        .expect("write fail");

    let polled: Vec<TaskEventEntry> = TaskEventEntry::poll(&mut db, 16, &[])
        .await
        .expect("poll events")
        .collect();
    assert_eq!(polled.len(), 2);
    assert_eq!(polled[0].node_id, Uuid::from_u128(1));
    assert_eq!(polled[0].event, TaskEvent::Succeed);
    assert_eq!(polled[1].event, TaskEvent::Fail);
    // The log assigns a monotonically increasing offset on write.
    assert!(polled[0].event_offset < polled[1].event_offset);

    for event in &polled {
        event.ack(&mut db).await.expect("ack event");
    }

    // Acknowledged events are not polled again.
    let again: Vec<TaskEventEntry> = TaskEventEntry::poll(&mut db, 16, &[])
        .await
        .expect("poll events again")
        .collect();
    assert!(again.is_empty());
}

#[tokio::test]
async fn poll_scopes_by_shards() {
    let Some(mut db) = setup().await else {
        eprintln!("skipping: set DATABASE_URL to run the MySQL task event test");
        return;
    };

    entry(1, 0, TaskEvent::Succeed)
        .write(&mut db)
        .await
        .expect("write shard 0");
    entry(2, 1, TaskEvent::Fail)
        .write(&mut db)
        .await
        .expect("write shard 1");

    let shard0: Vec<TaskEventEntry> = TaskEventEntry::poll(&mut db, 16, &[0])
        .await
        .expect("poll shard 0")
        .collect();
    assert_eq!(shard0.len(), 1);
    assert_eq!(shard0[0].node_id, Uuid::from_u128(1));

    let selected: Vec<TaskEventEntry> = TaskEventEntry::poll(&mut db, 16, &[0, 1])
        .await
        .expect("poll shards 0 and 1")
        .collect();
    assert_eq!(selected.len(), 2);
    assert_eq!(selected[0].node_id, Uuid::from_u128(1));
    assert_eq!(selected[1].node_id, Uuid::from_u128(2));
}
