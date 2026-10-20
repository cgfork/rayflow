//! Integration test for the MySQL-backed task dispatcher.
//!
//! It needs a MySQL instance. Set `DATABASE_URL` (for example
//! `mysql://root:password@127.0.0.1:3306/rayflow`) to run it; the test resets
//! the database and pushes the schema from the Toasty models itself. Without
//! `DATABASE_URL` the test is skipped.
#![cfg(feature = "mysql")]

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Mutex;

use runtime::mysql::model::{
    NodeConfigModel, NodeRunModel, RunConfigModel, RunModel, ScheduledTaskModel, TaskModel,
};
use runtime::{Task, TaskEventPoll};
use scheduler::dispatcher::Dispatcher;
use scheduler::{TaskEvent, TaskEventEntry, TaskRunner};
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

/// A [`TaskRunner`] that only records the tasks it was asked to run.
#[derive(Default)]
struct MockRunner {
    ran: Mutex<Vec<Uuid>>,
}

impl TaskRunner for MockRunner {
    type Err = Infallible;

    async fn run(&self, task: &Task) -> Result<(), Self::Err> {
        self.ran.lock().unwrap().push(task.id);
        Ok(())
    }
}

impl TaskEventPoll for MockRunner {
    type Err = Infallible;

    async fn poll(&self, _max: usize) -> Result<Vec<TaskEventEntry>, Self::Err> {
        Ok(Vec::new())
    }

    async fn ack(&self, _event: &TaskEventEntry) -> Result<(), Self::Err> {
        Ok(())
    }
}

#[tokio::test]
async fn dispatcher_records_scheduled_task_with_resolved_references() {
    let Some(mut db) = setup().await else {
        eprintln!("skipping: set DATABASE_URL to run the MySQL dispatcher test");
        return;
    };

    let now = jiff::Timestamp::now();
    let data_id = Uuid::from_u128(0xda7a);
    let graph_id = Uuid::from_u128(0x9a1);
    let node_id = Uuid::from_u128(1);

    let run = RunModel::create()
        .uuid(Uuid::new_v4())
        .data_id(data_id)
        .graph_id(graph_id)
        .version(1)
        .revision(1)
        .created_at(now)
        .updated_at(now)
        .exec(&mut db)
        .await
        .expect("create run");

    let node_run = NodeRunModel::create()
        .run_id(run.uuid)
        .data_id(data_id)
        .graph_id(graph_id)
        .node_id(node_id)
        .revision(1)
        .status(runtime::Status::Running.to_string())
        .created_at(now)
        .updated_at(now)
        .exec(&mut db)
        .await
        .expect("create node run");

    // Two node config versions; the latest (version 2) is in effect.
    let mut node_config_ids = Vec::new();
    for version in [1u64, 2] {
        let row = NodeConfigModel::create()
            .node_id(node_id)
            .version(version)
            .annotation_match(String::new())
            .config(serde_json::json!({ "version": version }))
            .created_at(now)
            .updated_at(now)
            .created_by(String::new())
            .updated_by(String::new())
            .exec(&mut db)
            .await
            .expect("create node config");
        node_config_ids.push(row.id);
    }

    // Two run config revisions; only revision 0 is at or before the run's
    // revision 1, so it is the one in effect.
    let annotation: HashMap<String, String> = HashMap::new();
    let mut run_config_ids = Vec::new();
    for revision in [0u64, 3] {
        let row = RunConfigModel::create()
            .run_id(run.uuid)
            .revision(revision)
            .annotation(annotation.clone())
            .created_at(now)
            .updated_at(now)
            .created_by(String::new())
            .updated_by(String::new())
            .exec(&mut db)
            .await
            .expect("create run config");
        run_config_ids.push(row.id);
    }

    let task = Task {
        task_offset: 0,
        id: Uuid::new_v4(),
        data_id,
        graph_id,
        node_id,
        node_run_id: node_run.id,
        created_at: now,
    };
    TaskModel::create()
        .uuid(task.id)
        .data_id(data_id)
        .graph_id(graph_id)
        .node_id(node_id)
        .node_run_id(node_run.id)
        .created_at(now)
        .exec(&mut db)
        .await
        .expect("create task");

    let dispatcher = Dispatcher::new(db.clone(), MockRunner::default());
    dispatcher.run(&task).await.expect("dispatch");

    // The wrapped runner still ran the task.
    assert_eq!(*dispatcher.runner().ran.lock().unwrap(), vec![task.id]);

    let scheduled = ScheduledTaskModel::filter_by_task_id(task.id)
        .first()
        .exec(&mut db)
        .await
        .expect("read scheduled task")
        .expect("scheduled task written");

    assert_eq!(scheduled.task_id, task.id);
    assert_eq!(scheduled.run_id, run.uuid);
    assert_eq!(scheduled.node_run_id, node_run.id);
    assert_eq!(scheduled.node_config_id, node_config_ids[1]);
    assert_eq!(scheduled.run_config_id, run_config_ids[0]);
    assert_eq!(scheduled.status, runtime::Status::Running.to_string());

    // Acknowledging the task's terminal event settles the recorded status.
    let event = TaskEventEntry::new(&task, TaskEvent::Succeed);
    dispatcher.ack(&event).await.expect("ack event");
    let settled = ScheduledTaskModel::filter_by_task_id(task.id)
        .first()
        .exec(&mut db)
        .await
        .expect("read scheduled task")
        .expect("scheduled task still exists");
    assert_eq!(settled.status, runtime::Status::Succeeded.to_string());
}
