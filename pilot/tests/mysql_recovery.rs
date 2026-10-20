//! Atomic recovery of command effects, acknowledgements, and automatic events.
use std::time::Duration;

use coordinator::mysql::MySqlLeaderElection;
use event::Emit as CommandEmit;
use event::Poll as CommandPoll;
use event::command::Command;
use event::command::CommandEntry;
use event::task::Task;
use event::task_event::TaskEventEntry;
use pilot::Pilot;
use runtime::{backend::Backend, command::Run};
use server::StaticGraphStore;
use uuid::Uuid;

#[tokio::test]
async fn interrupted_command_application_can_be_retried() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        return;
    };
    let mut backend = Backend::connect(&url, StaticGraphStore::new())
        .await
        .unwrap();
    let mut db = backend.db().clone();
    db.reset_db().await.unwrap();
    db.push_schema().await.unwrap();
    let data_id = Uuid::new_v4();
    let graph_id = Uuid::parse_str("00000001-0000-0000-0000-000000000000").unwrap();
    CommandEntry::new(data_id, graph_id, Command::Start { nodes: vec![] })
        .emit(&mut db)
        .await
        .unwrap();

    let mut tx = db.transaction().await.unwrap();
    let entry = CommandEntry::poll(&mut tx, 1)
        .await
        .unwrap()
        .next()
        .unwrap();
    backend.apply_in(&mut tx, &entry).await.unwrap();
    entry.ack(&mut tx).await.unwrap();
    // Simulate losing the connection before the commit becomes durable.
    tx.rollback().await.unwrap();
    assert!(Run::all().exec(&mut db).await.unwrap().is_empty());
    assert_eq!(
        CommandEntry::poll(&mut db, 1)
            .await
            .unwrap()
            .next()
            .unwrap()
            .id,
        entry.id
    );

    let mut tx = db.transaction().await.unwrap();
    backend.apply_in(&mut tx, &entry).await.unwrap();
    entry.ack(&mut tx).await.unwrap();
    tx.commit().await.unwrap();
    assert!(
        CommandEntry::poll(&mut db, 1)
            .await
            .unwrap()
            .next()
            .is_none()
    );
    assert_eq!(Run::all().exec(&mut db).await.unwrap().len(), 1);

    // The rolled-back insert leaves an auto-increment gap. Let the normal
    // persisted gap timeout expire before expecting the replacement task.
    let task = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(task) = Task::poll(&mut db, 1).await.unwrap().next() {
                break task;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("task becomes visible after the rolled-back offset gap");
    // A replacement pilot consumes the pending task and commits its success
    // together with the task and command acknowledgements.
    let election = MySqlLeaderElection::new(
        db.clone(),
        "recovery",
        "replacement",
        Duration::from_secs(30),
    );
    let mut pilot = Pilot::new(election, backend, 64, Duration::from_millis(10));
    let step = pilot.step().await.unwrap();
    assert!(step.tasks > 0);
    assert!(step.commands > 0);

    // The completion event has an identity independent of the task and command.
    let events = event::task_event::TaskEventEntry::filter_by_task_id(task.id)
        .exec(&mut db)
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_ne!(events[0].id, task.id);
    assert_ne!(events[0].id, entry.id);
    // Distinct events for one task are still distinct, immutable log records.
    let next = TaskEventEntry::new(&task, event::task_event::TaskEvent::Pend);
    event::Emit::emit(&next, &mut db).await.unwrap();
    let events_after = event::task_event::TaskEventEntry::filter_by_task_id(task.id)
        .exec(&mut db)
        .await
        .unwrap();
    assert_eq!(events_after.len(), 2);
    assert!(events_after.iter().any(|event| event.id == events[0].id));
    assert_ne!(next.id, events[0].id);
}
