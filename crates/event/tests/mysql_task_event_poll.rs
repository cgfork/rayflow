//! Integration tests for the MySQL-backed task event log.
//!
//! Set `DATABASE_URL` to run these tests. Without it they skip.

use event::Poll;
use std::time::Duration;

use event::Emit as TaskEventWrite;
use event::poll::PollOffset as PollOffsetModel;
use event::task_event::TaskEvent;
use event::task_event::TaskEventEntry as TaskEventModel;
use event::task_event::TaskEventEntry;
use jiff::Timestamp;
use toasty::Db;
use uuid::Uuid;

async fn setup() -> Option<Db> {
    let url = std::env::var("DATABASE_URL").ok()?;

    let mut builder = Db::builder();
    builder.models(event::models());
    let db = builder
        .connect(&url)
        .await
        .expect("connect to DATABASE_URL");

    db.reset_db().await.expect("reset database");
    db.push_schema().await.expect("push schema");

    Some(db)
}

fn entry(node: u128, event: TaskEvent) -> TaskEventEntry {
    TaskEventEntry {
        event_offset: 0,
        id: Uuid::new_v4(),
        task_id: Uuid::new_v4(),
        data_id: Uuid::from_u128(0xda7a),
        graph_id: Uuid::from_u128(0x9a1),
        node_id: Uuid::from_u128(node),
        node_run_id: 1,
        event,
        created_at: Timestamp::now(),
    }
}

#[tokio::test]
async fn writes_polls_and_acks_contiguous_task_events() {
    let Some(mut db) = setup().await else {
        eprintln!("skipping: set DATABASE_URL to run the MySQL task event test");
        return;
    };

    entry(1, TaskEvent::Succeed)
        .emit(&mut db)
        .await
        .expect("write succeed");
    entry(2, TaskEvent::Fail)
        .emit(&mut db)
        .await
        .expect("write fail");

    let polled: Vec<TaskEventEntry> = TaskEventEntry::poll(&mut db, 16)
        .await
        .expect("poll events")
        .collect();
    assert_eq!(polled.len(), 2);
    assert_eq!(polled[0].node_id, Uuid::from_u128(1));
    assert_eq!(polled[0].event, TaskEvent::Succeed);
    assert_eq!(polled[1].event, TaskEvent::Fail);
    assert!(polled[0].event_offset < polled[1].event_offset);

    for event in &polled {
        event.ack(&mut db).await.expect("ack event");
    }

    assert!(
        TaskEventEntry::poll(&mut db, 16)
            .await
            .expect("poll events again")
            .next()
            .is_none()
    );
}

#[tokio::test]
async fn waits_for_a_fresh_task_event_gap() {
    let Some(mut db) = setup().await else {
        eprintln!("skipping: set DATABASE_URL to run the MySQL task event test");
        return;
    };

    entry(1, TaskEvent::Pend)
        .emit(&mut db)
        .await
        .expect("write first");
    let gap = entry(2, TaskEvent::Fail);
    gap.emit(&mut db).await.expect("write gap row");
    entry(3, TaskEvent::Succeed)
        .emit(&mut db)
        .await
        .expect("write after gap");
    let gap_id = TaskEventModel::filter_by_id(gap.id)
        .first()
        .exec(&mut db)
        .await
        .expect("read gap row")
        .expect("gap row")
        .event_offset;
    toasty::sql::statement("DELETE FROM task_event WHERE id = ?")
        .bind(gap_id)
        .exec(&mut db)
        .await
        .expect("delete gap row");

    let first: Vec<TaskEventEntry> = TaskEventEntry::poll(&mut db, 16)
        .await
        .expect("poll fresh gap")
        .collect();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].node_id, Uuid::from_u128(1));
    first[0].ack(&mut db).await.expect("ack first");

    assert!(
        TaskEventEntry::poll(&mut db, 16)
            .await
            .expect("fresh gap blocks later events")
            .next()
            .is_none()
    );

    let now = Timestamp::now();
    PollOffsetModel::filter_by_name("task_event".to_string())
        .update()
        .gap_since(Some(now - Duration::from_secs(11)))
        .updated_at(now)
        .exec(&mut db)
        .await
        .expect("age task event gap");

    let after: Vec<TaskEventEntry> = TaskEventEntry::poll(&mut db, 16)
        .await
        .expect("skip aged gap")
        .collect();
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].node_id, Uuid::from_u128(3));
}
