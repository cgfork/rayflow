//! Set DATABASE_URL to run against an isolated MySQL database.

use event::command::{self, Command, CommandEntry};
use event::{Emit, Error, Event, Poll, Read};
use uuid::Uuid;

#[tokio::test]
async fn command_log_is_append_only_and_transactional() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        return;
    };
    let mut builder = toasty::Db::builder();
    builder.models(command::models());
    let mut db = builder.connect(&url).await.unwrap();
    db.reset_db().await.unwrap();
    db.push_schema().await.unwrap();

    let first = CommandEntry::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        Command::Start { nodes: vec![] },
    );
    let second = CommandEntry::new(
        first.data_id,
        first.graph_id,
        Command::Run {
            node: Uuid::new_v4(),
        },
    );
    let offset = first.emit(&mut db).await.unwrap();
    assert!(second.emit(&mut db).await.unwrap() > offset);
    let stored = CommandEntry::read(&mut db, first.id()).await.unwrap();
    assert_eq!(stored.offset(), offset);
    assert_eq!(stored.command, first.command);
    assert!(matches!(
        CommandEntry::read(&mut db, Uuid::new_v4()).await,
        Err(Error::CommandNotFound(_))
    ));
    assert_eq!(CommandEntry::poll(&mut db, 0).await.unwrap().count(), 0);
    let pending: Vec<_> = CommandEntry::poll(&mut db, 1).await.unwrap().collect();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id(), first.id());

    let mut tx = db.transaction().await.unwrap();
    pending[0].ack(&mut tx).await.unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(
        CommandEntry::poll(&mut db, 1)
            .await
            .unwrap()
            .next()
            .unwrap()
            .id(),
        first.id()
    );
    let mut tx = db.transaction().await.unwrap();
    pending[0].ack(&mut tx).await.unwrap();
    tx.commit().await.unwrap();
    let remaining: Vec<_> = CommandEntry::poll(&mut db, 10).await.unwrap().collect();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].id(), second.id());
    remaining[0].ack(&mut db).await.unwrap();
    assert_eq!(CommandEntry::poll(&mut db, 10).await.unwrap().count(), 0);

    let mut duplicate = first.clone();
    duplicate.command = toasty::Json(Command::Fail {
        node: Uuid::new_v4(),
    });
    assert!(duplicate.emit(&mut db).await.is_err());
    assert_eq!(
        CommandEntry::read(&mut db, first.id()).await.unwrap(),
        stored
    );
}
