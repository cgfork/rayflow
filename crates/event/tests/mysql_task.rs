//! Set DATABASE_URL to run against an isolated MySQL database.

use event::command::{Command, CommandEntry};
use event::task::Task;
use event::{Emit, Error, Event, Poll, Read};
use uuid::Uuid;

#[tokio::test]
async fn task_log_is_append_only_and_transactional() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        return;
    };
    let mut builder = toasty::Db::builder();
    builder.models(event::models());
    let mut db = builder.connect(&url).await.unwrap();
    db.reset_db().await.unwrap();
    db.push_schema().await.unwrap();

    let first = Task::new(Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4(), 1);
    let second = Task::new(first.data_id, first.graph_id, Uuid::new_v4(), 2);
    let command = CommandEntry::new(
        first.data_id,
        first.graph_id,
        Command::Start { nodes: vec![] },
    );
    command.emit(&mut db).await.unwrap();
    let offset = first.emit(&mut db).await.unwrap();
    assert!(second.emit(&mut db).await.unwrap() > offset);
    let stored = Task::read(&mut db, first.id()).await.unwrap();
    assert_eq!(stored.offset(), offset);
    assert_eq!(stored.node_id, first.node_id);
    assert_eq!(stored.node_run_id, first.node_run_id);
    assert!(matches!(
        Task::read(&mut db, Uuid::new_v4()).await,
        Err(Error::TaskNotFound(_))
    ));
    assert_eq!(Task::poll(&mut db, 0).await.unwrap().count(), 0);
    let pending: Vec<_> = Task::poll(&mut db, 1).await.unwrap().collect();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id(), first.id());

    let mut tx = db.transaction().await.unwrap();
    pending[0].ack(&mut tx).await.unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(
        Task::poll(&mut db, 1).await.unwrap().next().unwrap().id(),
        first.id()
    );
    let mut tx = db.transaction().await.unwrap();
    pending[0].ack(&mut tx).await.unwrap();
    tx.commit().await.unwrap();
    let remaining: Vec<_> = Task::poll(&mut db, 10).await.unwrap().collect();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].id(), second.id());
    remaining[0].ack(&mut db).await.unwrap();
    assert_eq!(Task::poll(&mut db, 10).await.unwrap().count(), 0);

    let mut duplicate = first.clone();
    duplicate.node_id = Uuid::new_v4();
    assert!(duplicate.emit(&mut db).await.is_err());
    assert_eq!(Task::read(&mut db, first.id()).await.unwrap(), stored);
    // Acknowledging tasks does not advance the command log's cursor.
    assert_eq!(CommandEntry::poll(&mut db, 10).await.unwrap().count(), 1);
}
