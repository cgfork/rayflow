//! Integration tests for contiguous command polling.
//!
//! Set `DATABASE_URL` to run these tests. Without it they skip, matching the
//! other MySQL integration tests in the workspace.

use std::time::Duration;

use event::Emit as CommandEmit;
use event::Poll as CommandPoll;
use event::command::Command;
use event::command::CommandEntry;
use event::poll::PollOffset as PollOffsetModel;
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

fn entry(node: u128) -> CommandEntry {
    CommandEntry::new(
        Uuid::from_u128(0xda7a),
        Uuid::from_u128(0x9a1),
        Command::Run {
            node: Uuid::from_u128(node),
        },
    )
}

async fn age_gap(db: &mut Db) {
    let now = Timestamp::now();
    PollOffsetModel::filter_by_name("command_entry".to_string())
        .update()
        .gap_since(Some(now - Duration::from_secs(11)))
        .updated_at(now)
        .exec(db)
        .await
        .expect("age command gap");
}

#[tokio::test]
async fn polls_and_acknowledges_contiguous_commands() {
    let Some(mut db) = setup().await else {
        eprintln!("skipping: set DATABASE_URL to run the MySQL command poll test");
        return;
    };

    for node in [1, 2, 3] {
        entry(node).emit(&mut db).await.expect("emit command");
    }

    let polled: Vec<CommandEntry> = CommandEntry::poll(&mut db, 16)
        .await
        .expect("poll commands")
        .collect();
    assert_eq!(polled.len(), 3);
    assert_eq!(
        polled[0].command.0,
        Command::Run {
            node: Uuid::from_u128(1)
        }
    );
    assert_eq!(
        polled[2].command.0,
        Command::Run {
            node: Uuid::from_u128(3)
        }
    );

    for entry in &polled {
        entry.ack(&mut db).await.expect("ack command");
    }

    assert!(
        CommandEntry::poll(&mut db, 16)
            .await
            .expect("poll acknowledged commands")
            .next()
            .is_none()
    );
}

#[tokio::test]
async fn waits_for_a_fresh_gap_then_skips_it_after_ten_seconds() {
    let Some(mut db) = setup().await else {
        eprintln!("skipping: set DATABASE_URL to run the MySQL command poll test");
        return;
    };

    let first = entry(1).emit(&mut db).await.expect("emit first");
    let missing = entry(2).emit(&mut db).await.expect("emit gap row");
    entry(3).emit(&mut db).await.expect("emit after gap");
    CommandEntry::filter_by_command_offset(missing)
        .delete()
        .exec(&mut db)
        .await
        .expect("delete gap row");

    let polled: Vec<CommandEntry> = CommandEntry::poll(&mut db, 16)
        .await
        .expect("poll fresh gap")
        .collect();
    assert_eq!(polled.len(), 1);
    assert_eq!(polled[0].command_offset, first);
    polled[0].ack(&mut db).await.expect("ack first");

    assert!(
        CommandEntry::poll(&mut db, 16)
            .await
            .expect("fresh gap blocks later rows")
            .next()
            .is_none()
    );

    age_gap(&mut db).await;
    let after: Vec<CommandEntry> = CommandEntry::poll(&mut db, 16)
        .await
        .expect("skip aged gap")
        .collect();
    assert_eq!(after.len(), 1);
    assert_eq!(
        after[0].command.0,
        Command::Run {
            node: Uuid::from_u128(3)
        }
    );
    after[0].ack(&mut db).await.expect("ack after gap");

    assert!(
        CommandEntry::poll(&mut db, 16)
            .await
            .expect("poll after ack")
            .next()
            .is_none()
    );
}
