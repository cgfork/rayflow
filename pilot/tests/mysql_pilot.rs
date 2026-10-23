//! Integration test for pilot consumption against the MySQL backend.
//!
//! Set `DATABASE_URL` to run this test. Without it the test skips.

use std::env;
use std::time::Duration;

use coordinator::mysql::MySqlLeaderElection;
use event::Emit as CommandEmit;
use event::Poll as CommandPoll;
use event::command::Command;
use event::command::CommandEntry;
use event::task::Task;
use event::task_event::TaskEventEntry;
use pilot::{Pilot, Step};
use runtime::backend::Backend;
use server::StaticGraphStore;
use toasty::Db;
use uuid::Uuid;

async fn setup() -> Option<(Db, Backend<StaticGraphStore>)> {
    let url = env::var("DATABASE_URL").ok()?;
    let mut builder = toasty::Db::builder();
    builder.models(scheduler::models());
    let db = builder.connect(&url).await.unwrap();
    let backend = Backend::new(db, StaticGraphStore::new());
    let db = backend.db().clone();
    db.reset_db().await.expect("reset database");
    db.push_schema().await.expect("push schema");
    Some((db, backend))
}

#[tokio::test]
async fn only_the_leader_consumes_commands_tasks_and_events() {
    let Some((mut db, backend)) = setup().await else {
        eprintln!("skipping: set DATABASE_URL to run the MySQL pilot test");
        return;
    };

    let role = format!("pilot-test-{}", Uuid::new_v4());
    let first =
        MySqlLeaderElection::new(db.clone(), role.clone(), "first", Duration::from_secs(30));
    let second = MySqlLeaderElection::new(db.clone(), role, "second", Duration::from_secs(30));
    let mut leader = Pilot::new(first, backend.clone(), 64, Duration::from_millis(10));
    let mut standby = Pilot::new(second, backend, 64, Duration::from_millis(10));

    let graph_id = Uuid::parse_str("00000001-0000-0000-0000-000000000000").unwrap();
    CommandEntry::new(
        Uuid::new_v4(),
        graph_id,
        Command::Start { nodes: Vec::new() },
    )
    .emit(&mut db)
    .await
    .expect("emit Start");

    let mut total = Step::default();
    let mut idle = 0;
    for _ in 0..100 {
        let step = leader.step().await.expect("leader pass");
        if step == Step::default() {
            idle += 1;
            if idle == 2 {
                break;
            }
        } else {
            idle = 0;
            total.commands += step.commands;
            total.tasks += step.tasks;
        }
    }

    assert!(total.commands > 0, "expected commands to be consumed");
    assert!(total.tasks > 0, "expected tasks to be consumed");
    assert!(leader.is_leader().await.expect("leader state"));
    assert!(!standby.is_leader().await.expect("standby state"));

    // A command emitted while the first pilot holds the lease is invisible to
    // the standby and remains pending until the leader polls again.
    CommandEntry::new(
        Uuid::new_v4(),
        graph_id,
        Command::Start { nodes: Vec::new() },
    )
    .emit(&mut db)
    .await
    .expect("emit pending Start");
    assert_eq!(standby.step().await.expect("standby pass"), Step::default());
    assert!(
        CommandEntry::poll(&mut db, 100)
            .await
            .expect("poll pending command")
            .next()
            .is_some(),
        "the standby must not consume commands"
    );

    let mut resumed = Step::default();
    let mut idle = 0;
    for _ in 0..100 {
        let step = leader.step().await.expect("leader resume pass");
        if step == Step::default() {
            idle += 1;
            if idle == 2 {
                break;
            }
        } else {
            idle = 0;
            resumed.commands += step.commands;
            resumed.tasks += step.tasks;
        }
    }
    assert!(
        resumed.commands > 0,
        "the leader should resume consuming pending commands"
    );

    assert!(
        CommandEntry::poll(&mut db, 100)
            .await
            .expect("poll commands")
            .next()
            .is_none(),
        "commands remained unconsumed after the final leader pass"
    );
    assert!(
        Task::poll(&mut db, 100)
            .await
            .expect("poll tasks")
            .next()
            .is_none(),
        "tasks remained unconsumed"
    );
    assert!(
        TaskEventEntry::poll(&mut db, 100)
            .await
            .expect("poll task events")
            .next()
            .is_none(),
        "task events remained unconsumed"
    );
}
