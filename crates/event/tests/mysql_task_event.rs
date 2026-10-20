//! Set DATABASE_URL to run against an isolated MySQL database.
use event::command::{Command, CommandEntry};
use event::task::Task;
use event::task_event::{TaskEvent, TaskEventEntry};
use event::{Emit, Error, Event, Poll, Read};
use uuid::Uuid;

#[tokio::test]
async fn task_events_have_independent_identity_and_append_only_history() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        return;
    };
    let mut builder = toasty::Db::builder();
    builder.models(event::models());
    let mut db = builder.connect(&url).await.unwrap();
    db.reset_db().await.unwrap();
    db.push_schema().await.unwrap();
    let task = Task::new(Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4(), 1);
    task.emit(&mut db).await.unwrap();
    CommandEntry::new(
        task.data_id,
        task.graph_id,
        Command::Start { nodes: vec![] },
    )
    .emit(&mut db)
    .await
    .unwrap();
    let mut ids = Vec::new();
    for kind in [
        TaskEvent::Pend,
        TaskEvent::Succeed,
        TaskEvent::Fail,
        TaskEvent::Terminate,
        TaskEvent::Cancel,
    ] {
        let entry = TaskEventEntry::new(&task, kind);
        assert_ne!(entry.id(), task.id());
        assert!(!ids.contains(&entry.id()));
        ids.push(entry.id());
        let offset = entry.emit(&mut db).await.unwrap();
        let stored = TaskEventEntry::read(&mut db, entry.id()).await.unwrap();
        assert_eq!(stored.event, kind);
        assert_eq!(stored.offset(), offset);
        assert_eq!(stored.task_id, task.id());
    }
    assert!(matches!(
        TaskEventEntry::read(&mut db, Uuid::new_v4()).await,
        Err(Error::TaskEventNotFound(_))
    ));
    assert_eq!(TaskEventEntry::poll(&mut db, 0).await.unwrap().count(), 0);
    let first = TaskEventEntry::poll(&mut db, 1)
        .await
        .unwrap()
        .next()
        .unwrap();
    assert_eq!(first.id(), ids[0]);
    let mut tx = db.transaction().await.unwrap();
    first.ack(&mut tx).await.unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(
        TaskEventEntry::poll(&mut db, 1)
            .await
            .unwrap()
            .next()
            .unwrap()
            .id(),
        first.id()
    );
    let entries: Vec<_> = TaskEventEntry::poll(&mut db, 10).await.unwrap().collect();
    assert_eq!(entries.iter().map(Event::id).collect::<Vec<_>>(), ids);
    let mut tx = db.transaction().await.unwrap();
    for entry in entries {
        entry.ack(&mut tx).await.unwrap();
    }
    tx.commit().await.unwrap();
    assert_eq!(TaskEventEntry::poll(&mut db, 10).await.unwrap().count(), 0);
    let mut duplicate = first.clone();
    duplicate.event = TaskEvent::Fail;
    assert!(duplicate.emit(&mut db).await.is_err());
    assert_eq!(
        TaskEventEntry::read(&mut db, first.id()).await.unwrap(),
        first
    );
    assert_eq!(Task::poll(&mut db, 10).await.unwrap().count(), 1);
    assert_eq!(CommandEntry::poll(&mut db, 10).await.unwrap().count(), 1);
}
