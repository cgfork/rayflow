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
use scheduler::{AgentRef, JobPoll, JobRead, ResourceCapacity};
use server::StaticGraphStore;
use uuid::Uuid;

mod support;

const AGENT: Uuid = Uuid::from_u128(1);

#[tokio::test]
async fn interrupted_command_application_can_be_retried() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        return;
    };
    let mut builder = toasty::Db::builder();
    builder.models(scheduler::models());
    let db = builder.connect(&url).await.unwrap();
    let mut backend = Backend::new(db, StaticGraphStore::new());
    let mut db = backend.db().clone();
    db.reset_db().await.unwrap();
    db.push_schema().await.unwrap();
    let data_id = Uuid::new_v4();
    let graph_id = Uuid::parse_str("00000001-0000-0000-0000-000000000000").unwrap();
    support::configure_graph(&mut db, graph_id).await;
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
    let election = MySqlLeaderElection::new(
        db.clone(),
        "recovery",
        "replacement",
        Duration::from_secs(30),
    );
    let mut pilot = Pilot::new(election, backend, 64, Duration::from_millis(10));
    let step = pilot.step().await.unwrap();
    assert!(step.tasks > 0);
    assert_eq!(step.commands, 0);

    // Actuation commits an available job, without fabricating completion.
    let scheduled = scheduler::Job::read(&mut db, task.id).await.unwrap();
    assert_eq!(scheduled.status, scheduler::actuator::Status::Scheduled);
    assert!(scheduled.agent_id.is_none());
    assert!(
        TaskEventEntry::filter_by_task_id(task.id)
            .exec(&mut db)
            .await
            .unwrap()
            .is_empty()
    );
    let claimed: Vec<_> = AgentRef::new(
        AGENT,
        ResourceCapacity {
            cpu_cores: 1,
            memory_bytes: 1,
        },
        Vec::new(),
    )
    .poll(&mut db, 64)
    .await
    .unwrap()
    .collect();
    assert!(claimed.iter().any(|row| row.task_id == task.id));
    let event = TaskEventEntry::new(&task, event::task_event::TaskEvent::Succeed);
    event.emit(&mut db).await.unwrap();
    pilot.step().await.unwrap();
    let settled = scheduler::Job::read(&mut db, task.id).await.unwrap();
    assert_eq!(settled.status, scheduler::actuator::Status::Succeeded);
    assert_ne!(event.id, task.id);
}
