//! Integration tests for contiguous task polling.
//!
//! Set `DATABASE_URL` to run these tests. Without it they skip.

use std::time::Duration;

use event::Emit as TaskWrite;
use event::Poll as TaskPoll;
use event::poll::PollOffset as PollOffsetModel;
use event::task::Task as TaskModel;
use event::task::Task;
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

fn task(node: u128) -> Task {
    Task {
        task_offset: 0,
        id: Uuid::new_v4(),
        data_id: Uuid::from_u128(0xda7a),
        graph_id: Uuid::from_u128(0x9a1),
        node_id: Uuid::from_u128(node),
        node_run_id: 1,
        created_at: Timestamp::now(),
    }
}

#[tokio::test]
async fn polls_and_acknowledges_contiguous_tasks() {
    let Some(mut db) = setup().await else {
        eprintln!("skipping: set DATABASE_URL to run the MySQL task poll test");
        return;
    };

    for node in [1, 2, 3] {
        task(node).emit(&mut db).await.expect("write task");
    }

    let polled: Vec<Task> = Task::poll(&mut db, 16).await.expect("poll tasks").collect();
    assert_eq!(
        polled.iter().map(|task| task.node_id).collect::<Vec<_>>(),
        vec![Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(3)]
    );

    for task in &polled {
        task.ack(&mut db).await.expect("ack task");
    }

    assert!(
        Task::poll(&mut db, 16)
            .await
            .expect("poll acknowledged tasks")
            .next()
            .is_none()
    );
}

#[tokio::test]
async fn skips_a_task_gap_only_after_the_timeout() {
    let Some(mut db) = setup().await else {
        eprintln!("skipping: set DATABASE_URL to run the MySQL task poll test");
        return;
    };

    task(1).emit(&mut db).await.expect("write first");
    let gap = task(2);
    gap.emit(&mut db).await.expect("write gap row");
    task(3).emit(&mut db).await.expect("write after gap");
    let gap_id = TaskModel::filter_by_id(gap.id)
        .first()
        .exec(&mut db)
        .await
        .expect("read gap row")
        .expect("gap row")
        .task_offset;
    toasty::sql::statement("DELETE FROM task WHERE id = ?")
        .bind(gap_id)
        .exec(&mut db)
        .await
        .expect("delete gap row");

    let first: Vec<Task> = Task::poll(&mut db, 16)
        .await
        .expect("poll fresh gap")
        .collect();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].node_id, Uuid::from_u128(1));
    first[0].ack(&mut db).await.expect("ack first");

    assert!(
        Task::poll(&mut db, 16)
            .await
            .expect("fresh gap blocks later tasks")
            .next()
            .is_none()
    );

    let now = Timestamp::now();
    PollOffsetModel::filter_by_name("task".to_string())
        .update()
        .gap_since(Some(now - Duration::from_secs(11)))
        .updated_at(now)
        .exec(&mut db)
        .await
        .expect("age task gap");

    let after: Vec<Task> = Task::poll(&mut db, 16)
        .await
        .expect("skip aged gap")
        .collect();
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].node_id, Uuid::from_u128(3));
}
