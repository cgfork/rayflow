//! Integration test for multi-shard command polling.
//!
//! It needs a MySQL instance. Set `DATABASE_URL` (for example
//! `mysql://root:password@127.0.0.1:3306/rayflow`) to run it; the test resets
//! the database and pushes the schema from the Toasty models itself. Without
//! `DATABASE_URL` the test is skipped.
#![cfg(feature = "mysql")]

use runtime::{Command, CommandEmit, CommandEntry, CommandPoll};
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

fn entry(shard: u32, node: u128) -> CommandEntry {
    CommandEntry {
        command_offset: 0,
        id: Uuid::new_v4(),
        data_id: Uuid::from_u128(0xda7a),
        graph_id: Uuid::from_u128(0x9a1),
        shard,
        command: Command::Run {
            node: Uuid::from_u128(node),
        },
        created_at: jiff::Timestamp::now(),
    }
}

#[tokio::test]
async fn polls_selected_shards_and_acks_independently() {
    let Some(mut db) = setup().await else {
        eprintln!("skipping: set DATABASE_URL to run the MySQL command poll test");
        return;
    };

    for (node, shard) in [(1, 0), (2, 1), (3, 2)] {
        entry(shard, node)
            .emit(&mut db)
            .await
            .expect("emit command");
    }

    let selected: Vec<CommandEntry> = CommandEntry::poll(&mut db, 16, &[0, 2])
        .await
        .expect("poll shards 0 and 2")
        .collect();
    assert_eq!(
        selected
            .iter()
            .map(|entry| entry.command.clone())
            .collect::<Vec<_>>(),
        vec![
            Command::Run {
                node: Uuid::from_u128(1)
            },
            Command::Run {
                node: Uuid::from_u128(3)
            },
        ]
    );

    for entry in &selected {
        entry.ack(&mut db).await.expect("ack command");
    }

    let selected_again: Vec<CommandEntry> = CommandEntry::poll(&mut db, 16, &[0, 2])
        .await
        .expect("poll acknowledged shards")
        .collect();
    assert!(selected_again.is_empty());

    let remaining: Vec<CommandEntry> = CommandEntry::poll(&mut db, 16, &[])
        .await
        .expect("poll all shards")
        .collect();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].shard, 1);
}
