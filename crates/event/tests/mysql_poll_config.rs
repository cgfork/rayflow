//! Configurable gap handling for each MySQL log. Uses an isolated DATABASE_URL.

use std::time::Duration;

use event::{
    Emit,
    command::{Command, CommandEntry},
    poll::{Config, PollOffset},
    task::Task,
    task_event::{TaskEvent, TaskEventEntry},
};
use jiff::Timestamp;
use toasty::Db;
use uuid::Uuid;

#[tokio::test]
async fn configured_gap_timeout_controls_polling_and_acknowledgement() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        return;
    };
    let mut db = Db::builder()
        .models(event::models())
        .connect(&url)
        .await
        .unwrap();
    db.reset_db().await.unwrap();
    db.push_schema().await.unwrap();
    let short = Config {
        gap_timeout: Duration::from_millis(500),
    };
    let long = Config {
        gap_timeout: Duration::from_secs(60),
    };

    macro_rules! check_log {
        ($model:ident, $name:literal, $entry:expr) => {{
            let mut entry = $entry;
            let mut tx = db.transaction().await.unwrap();
            assert_eq!(entry.emit(&mut tx).await.unwrap(), 1);
            tx.rollback().await.unwrap();
            assert_eq!(entry.emit(&mut db).await.unwrap(), 2);
            PollOffset::create()
                .name($name)
                .offset(0)
                .gap_since(Some(Timestamp::now() - Duration::from_secs(1)))
                .updated_at(Timestamp::now())
                .exec(&mut db)
                .await
                .unwrap();
            assert_eq!(
                $model::poll_with_config(&mut db, 10, long)
                    .await
                    .unwrap()
                    .count(),
                0
            );
            let rows: Vec<_> = $model::poll_with_config(&mut db, 10, short)
                .await
                .unwrap()
                .collect();
            assert_eq!(rows.len(), 1);
            entry = rows.into_iter().next().unwrap();
            // Restore the cursor before the gap to check acknowledgement's
            // timeout guard independently from polling's gap advancement.
            PollOffset::filter_by_name($name)
                .update()
                .offset(0)
                .gap_since(Some(Timestamp::now() - Duration::from_secs(1)))
                .exec(&mut db)
                .await
                .unwrap();
            entry.ack_with_config(&mut db, long).await.unwrap();
            assert_eq!(
                PollOffset::filter_by_name($name)
                    .get(&mut db)
                    .await
                    .unwrap()
                    .offset,
                0
            );
            entry.ack_with_config(&mut db, short).await.unwrap();
            assert_eq!(
                PollOffset::filter_by_name($name)
                    .get(&mut db)
                    .await
                    .unwrap()
                    .offset,
                2
            );
        }};
    }
    check_log!(
        CommandEntry,
        "command_entry",
        CommandEntry::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            Command::Start { nodes: vec![] }
        )
    );
    check_log!(
        Task,
        "task",
        Task::new(Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4(), 0)
    );
    check_log!(
        TaskEventEntry,
        "task_event",
        TaskEventEntry::new(
            &Task::new(Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4(), 0),
            TaskEvent::Succeed
        )
    );
}
