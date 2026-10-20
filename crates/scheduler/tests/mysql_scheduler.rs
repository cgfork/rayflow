//! Integration test for the MySQL-backed scheduler with a mocked task runner.
//!
//! The test drives a whole run end to end. The [`Scheduler`] is a command
//! producer: it consumes the tasks a run enqueues and *emits* the driving
//! commands into the command log. A small consumer then polls that log and
//! applies each command, which advances node status and enqueues the next tasks;
//! the two are interleaved until the run is quiescent. A [`MockRunner`] plays the
//! role of the task executor: it runs each scheduled task and records the event
//! the run produced, which the scheduler polls back to drive the node.
//!
//! It needs a MySQL instance. Set `DATABASE_URL` (for example
//! `mysql://root:password@127.0.0.1:3306/rayflow`) to run it; the test resets the
//! database and pushes the schema from the Toasty models itself. Without
//! `DATABASE_URL` the test is skipped, so it is safe to run in environments
//! without a database.
#![cfg(feature = "mysql")]

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Mutex;

use dag::types::{DAG, Node};
use runtime::mysql::Backend;
use runtime::mysql::model::{NodeRunModel, RunModel};
use runtime::{
    Command, CommandApply, CommandEmit, CommandEntry, CommandPoll, GraphStore, Status, Task,
    TaskEventPoll,
};
use scheduler::{Scheduler, TaskEvent, TaskEventEntry, TaskRunner};
use toasty::Db;
use uuid::Uuid;

/// A [`GraphStore`] that serves a single in-memory graph.
#[derive(Clone)]
struct TestGraphStore(DAG);

impl GraphStore<Uuid> for TestGraphStore {
    type G = DAG;
    type Err = Infallible;

    async fn load(&self, _graph_id: Uuid, _version: Option<u64>) -> Result<Self::G, Self::Err> {
        Ok(self.0.clone())
    }
}

/// A [`TaskRunner`] mock that records the event each run produces.
struct MockRunner {
    /// Nodes this runner fails; every other node succeeds.
    failing: Vec<Uuid>,
    events: Mutex<Vec<TaskEventEntry>>,
}

impl MockRunner {
    /// A runner that succeeds every task.
    fn succeed() -> Self {
        Self {
            failing: Vec::new(),
            events: Mutex::new(Vec::new()),
        }
    }

    /// A runner that fails the listed nodes and succeeds the rest.
    fn fail(nodes: Vec<Uuid>) -> Self {
        Self {
            failing: nodes,
            events: Mutex::new(Vec::new()),
        }
    }
}

impl TaskRunner for MockRunner {
    type Err = Infallible;

    async fn run(&self, task: &Task) -> Result<(), Self::Err> {
        let event = if self.failing.contains(&task.node_id) {
            TaskEvent::Fail
        } else {
            TaskEvent::Succeed
        };
        self.events
            .lock()
            .unwrap()
            .push(TaskEventEntry::new(task, event));
        Ok(())
    }
}

impl TaskEventPoll for MockRunner {
    type Err = Infallible;

    async fn poll(&self, max: usize, shards: &[u32]) -> Result<Vec<TaskEventEntry>, Self::Err> {
        let mut events = self.events.lock().unwrap();
        let mut taken = Vec::new();
        events.retain(|entry| {
            if taken.len() < max && (shards.is_empty() || shards.contains(&entry.shard)) {
                taken.push(entry.clone());
                false
            } else {
                true
            }
        });
        Ok(taken)
    }

    async fn ack(&self, _event: &TaskEventEntry) -> Result<(), Self::Err> {
        Ok(())
    }
}

fn node(name: &str) -> Node {
    Node {
        id: Uuid::new_v4(),
        name: name.to_string(),
        node_type: "test".to_string(),
        inputs: Vec::new(),
        outputs: Vec::new(),
    }
}

/// Connects and rebuilds the schema from the Toasty models, or returns `None`
/// when no `DATABASE_URL` is configured.
async fn setup() -> Option<(Db, DAG, Uuid)> {
    let url = std::env::var("DATABASE_URL").ok()?;

    let mut builder = Db::builder();
    builder.models(runtime::mysql::models());
    let db = builder
        .connect(&url)
        .await
        .expect("connect to DATABASE_URL");

    db.reset_db().await.expect("reset database");
    db.push_schema().await.expect("push schema");

    let a = node("a");
    let b = node("b");
    let c = node("c");
    let d = node("d");
    let e = node("e");
    let dag = DAG {
        id: Uuid::new_v4(),
        name: "diamond".to_string(),
        version: 1,
        nodes: vec![a.clone(), b.clone(), c.clone(), d.clone(), e.clone()],
        edges: vec![
            (a.id, b.id),
            (a.id, c.id),
            (b.id, d.id),
            (c.id, d.id),
            (d.id, e.id),
        ],
    };
    Some((db, dag, Uuid::new_v4()))
}

/// Writes `Start` to the command log, then consumes and applies it.
async fn start_run(db: &mut Db, backend: &Backend<TestGraphStore>, data_id: Uuid, graph_id: Uuid) {
    let entry = CommandEntry::routed(data_id, graph_id, Command::Start { nodes: Vec::new() }, 1);
    entry.emit(db).await.expect("emit Start");
    consume_commands(db, backend).await;
}

/// Polls the command log and applies every unacknowledged command, in offset
/// order. Returns how many commands were applied.
async fn consume_commands(db: &mut Db, backend: &Backend<TestGraphStore>) -> usize {
    let entries: Vec<CommandEntry> = CommandEntry::poll(db, 256, &[])
        .await
        .expect("poll commands")
        .collect();
    let count = entries.len();
    for entry in entries {
        entry.apply(backend.clone()).await.expect("apply command");
        entry.ack(db).await.expect("ack command");
    }
    count
}

/// Alternates between the scheduler emitting commands and the consumer applying
/// them, until neither makes progress. Returns the number of tasks scheduled.
async fn run_to_quiescence(
    db: &mut Db,
    scheduler: &mut Scheduler<Backend<TestGraphStore>, MockRunner>,
    consumer: &Backend<TestGraphStore>,
) -> usize {
    let mut scheduled = 0;
    loop {
        let processed = scheduler.tick().await.expect("tick");
        let applied = consume_commands(db, consumer).await;
        scheduled += processed;
        if processed == 0 && applied == 0 {
            return scheduled;
        }
    }
}

/// Returns the latest node run per `(run_id, node_id)`.
fn latest_node_runs(rows: &[NodeRunModel]) -> Vec<&NodeRunModel> {
    let mut latest: HashMap<(Uuid, Uuid), &NodeRunModel> = HashMap::new();
    for row in rows {
        match latest.get(&(row.run_id, row.node_id)) {
            Some(current) if current.revision >= row.revision => {}
            _ => {
                latest.insert((row.run_id, row.node_id), row);
            }
        }
    }
    latest.into_values().collect()
}

/// Counts the current node runs (the latest revision per node) with `status`.
async fn count_status(db: &mut Db, status: Status) -> i64 {
    let rows = NodeRunModel::all()
        .exec(&mut *db)
        .await
        .expect("load node runs");
    let expected = status.to_string();
    latest_node_runs(&rows)
        .into_iter()
        .filter(|row| row.status == expected)
        .count() as i64
}

async fn status_counts(db: &mut Db) -> (i64, i64) {
    (
        count_status(db, Status::Succeeded).await,
        count_status(db, Status::Failed).await,
    )
}

/// Reads the current revision of the run for `(data_id, graph_id)`.
async fn run_revision(db: &mut Db, data_id: Uuid, graph_id: Uuid) -> u64 {
    RunModel::filter_by_data_id_and_graph_id(data_id, graph_id)
        .first()
        .exec(&mut *db)
        .await
        .expect("load run")
        .expect("run exists")
        .revision
}

/// Reads the `(revision, status)` of `node`'s current node run.
async fn node_run(db: &mut Db, node: Uuid) -> (u64, String) {
    let row = NodeRunModel::all()
        .filter(NodeRunModel::fields().node_id().eq(node))
        .order_by(NodeRunModel::fields().revision().desc())
        .first()
        .exec(&mut *db)
        .await
        .expect("load node run")
        .expect("node run exists");
    (row.revision, row.status)
}

/// Reads the status of `node`'s node run at `revision`, if any.
async fn node_run_at(db: &mut Db, node: Uuid, revision: u64) -> Option<String> {
    NodeRunModel::all()
        .filter(NodeRunModel::fields().node_id().eq(node))
        .filter(NodeRunModel::fields().revision().eq(revision))
        .first()
        .exec(&mut *db)
        .await
        .expect("load node run")
        .map(|row| row.status)
}

/// Counts every node run row, including superseded revisions.
async fn node_run_rows(db: &mut Db) -> i64 {
    NodeRunModel::all()
        .exec(&mut *db)
        .await
        .expect("load node runs")
        .len() as i64
}

#[tokio::test]
async fn mysql_scheduler_runs_a_chain_to_completion() {
    let Some((mut db, dag, data_id)) = setup().await else {
        eprintln!("skipping: set DATABASE_URL to run the MySQL scheduler test");
        return;
    };
    let graph_id = dag.id;

    let producer = Backend::new(db.clone(), 1, TestGraphStore(dag.clone()));
    let consumer = Backend::new(db.clone(), 1, TestGraphStore(dag));
    let mut scheduler = Scheduler::new(producer, MockRunner::succeed(), 16);

    start_run(&mut db, &consumer, data_id, graph_id).await;

    // The whole graph runs: a, then b and c, then d, then e.
    let scheduled = run_to_quiescence(&mut db, &mut scheduler, &consumer).await;
    assert_eq!(scheduled, 5);

    let (succeeded, failed) = status_counts(&mut db).await;
    assert_eq!(succeeded, 5);
    assert_eq!(failed, 0);

    // Every emitted command was consumed.
    let pending: Vec<CommandEntry> = CommandEntry::poll(&mut db, 256, &[])
        .await
        .expect("poll commands")
        .collect();
    assert!(pending.is_empty(), "every emitted command was consumed");
}

#[tokio::test]
async fn mysql_scheduler_stops_downstream_after_failure() {
    let Some((mut db, dag, data_id)) = setup().await else {
        eprintln!("skipping: set DATABASE_URL to run the MySQL scheduler test");
        return;
    };
    let graph_id = dag.id;
    let middle = dag.nodes[1].id;

    let producer = Backend::new(db.clone(), 1, TestGraphStore(dag.clone()));
    let consumer = Backend::new(db.clone(), 1, TestGraphStore(dag));
    let mut scheduler = Scheduler::new(producer, MockRunner::fail(vec![middle]), 16);

    start_run(&mut db, &consumer, data_id, graph_id).await;

    // `a` succeeds, then its children `b` (fails) and `c` (succeeds) run; `d`
    // needs both `b` and `c` and so is never scheduled, nor is `e`.
    let scheduled = run_to_quiescence(&mut db, &mut scheduler, &consumer).await;
    assert_eq!(scheduled, 3);

    let (succeeded, failed) = status_counts(&mut db).await;
    assert_eq!(succeeded, 2);
    assert_eq!(failed, 1);

    assert_eq!(node_run_rows(&mut db).await, 3);
}

#[tokio::test]
async fn mysql_scheduler_reruns_a_succeeded_node() {
    let Some((mut db, dag, data_id)) = setup().await else {
        eprintln!("skipping: set DATABASE_URL to run the MySQL scheduler test");
        return;
    };
    let graph_id = dag.id;
    // `c` (index 2), rerun once the whole graph has succeeded.
    let middle = dag.nodes[2].id;

    let producer = Backend::new(db.clone(), 1, TestGraphStore(dag.clone()));
    let consumer = Backend::new(db.clone(), 1, TestGraphStore(dag));
    let mut scheduler = Scheduler::new(producer, MockRunner::succeed(), 16);

    start_run(&mut db, &consumer, data_id, graph_id).await;
    assert_eq!(
        run_to_quiescence(&mut db, &mut scheduler, &consumer).await,
        5
    );

    // Rerun `c`: it is reset to `Ready` at a new revision and enqueued again.
    let entry = CommandEntry::routed(data_id, graph_id, Command::Rerun { node: middle }, 1);
    entry.emit(&mut db).await.expect("emit Rerun");
    consume_commands(&mut db, &consumer).await;

    // `c` succeeds, which re-readies its successor `d`; `d` succeeding re-readies
    // `e`. So the rerun cascades through the tail of the graph.
    assert_eq!(
        run_to_quiescence(&mut db, &mut scheduler, &consumer).await,
        3
    );

    let (succeeded, failed) = status_counts(&mut db).await;
    assert_eq!(succeeded, 5);
    assert_eq!(failed, 0);

    // The rerun advanced the run revision and wrote fresh node runs at it, keeping
    // the superseded revision: `c`'s original run is still there, and `c`, `d`,
    // and `e` each gained a new revision on top of the original five node runs.
    assert_eq!(run_revision(&mut db, data_id, graph_id).await, 1);
    assert_eq!(
        node_run_at(&mut db, middle, 0).await,
        Some(Status::Succeeded.to_string())
    );
    assert_eq!(
        node_run(&mut db, middle).await,
        (1, Status::Succeeded.to_string())
    );
    assert_eq!(node_run_rows(&mut db).await, 8);
}
