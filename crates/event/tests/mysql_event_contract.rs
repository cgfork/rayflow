//! Shared append-only log contract, exercised against all runtime record types.

use event::command::Command;
use event::command::CommandEntry;
use event::task::Task;
use event::task_event::TaskEvent;
use event::task_event::TaskEventEntry;
use event::{Emit, Event, Poll, Read};
use toasty::Db;
use uuid::Uuid;

async fn verify<T>(db: &mut Db, entry: T, id: Uuid)
where
    T: Event + Emit<Db> + Read<Uuid, Db> + Poll<Db>,
    <T as Emit<Db>>::Err: std::fmt::Debug,
    <T as Read<Uuid, Db>>::Err: std::fmt::Debug,
    <T as Poll<Db>>::Err: std::fmt::Debug,
{
    assert_eq!(entry.id(), id);
    let offset = entry.emit(db).await.unwrap();
    assert!(offset > 0);
    assert!(T::read(db, Uuid::new_v4()).await.is_err());
    let mut rows = T::poll(db, 1).await.unwrap();
    let first = rows.next().expect("emitted record");
    assert!(rows.next().is_none());
    drop(rows);
    assert_eq!(first.offset(), offset);
    assert_eq!(first.id(), id);
    let read = T::read(db, id).await.unwrap();
    assert_eq!(read.offset(), offset);
    assert_eq!(read.id(), id);
    assert_eq!(T::poll(db, 1).await.unwrap().count(), 1);
    first.ack(db).await.unwrap();
    assert_eq!(T::poll(db, 1).await.unwrap().count(), 0);
    T::read(db, id)
        .await
        .expect("ack does not remove the record");
    assert!(
        entry.emit(db).await.is_err(),
        "duplicate IDs must not overwrite"
    );
    T::read(db, id)
        .await
        .expect("duplicate leaves original record readable");
}

#[tokio::test]
async fn all_logs_support_emit_read_poll_and_ack() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        return;
    };
    let mut builder = Db::builder();
    builder.models(event::models());
    let mut db = builder.connect(&url).await.unwrap();
    db.reset_db().await.unwrap();
    db.push_schema().await.unwrap();
    let task = Task {
        task_offset: 0,
        id: Uuid::new_v4(),
        data_id: Uuid::new_v4(),
        graph_id: Uuid::new_v4(),
        node_id: Uuid::new_v4(),
        node_run_id: 1,
        created_at: jiff::Timestamp::now(),
    };
    let command = CommandEntry::new(
        task.data_id,
        task.graph_id,
        Command::Start { nodes: vec![] },
    );
    let event = TaskEventEntry::new(&task, TaskEvent::Succeed);
    verify(&mut db, command.clone(), command.id).await;
    verify(&mut db, task.clone(), task.id).await;
    verify(&mut db, event.clone(), event.id).await;
    assert_eq!(
        Task::read(&mut db, task.id).await.unwrap().node_id,
        task.node_id
    );
    assert_eq!(
        CommandEntry::read(&mut db, command.id)
            .await
            .unwrap()
            .command,
        command.command
    );
    let stored = TaskEventEntry::read(&mut db, event.id).await.unwrap();
    assert_eq!(stored.task_id, task.id);
    assert_eq!(stored.event, event.event);
    assert_ne!(stored.id, task.id);
}
