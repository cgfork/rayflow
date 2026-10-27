//! Integration test for the MySQL-backed scheduler with a mocked task runner.
//!
//! Actuator consumes logs and drives graph state, agents poll work directly, and a
//! simulated agent claims jobs and emits completion events. The loop
//! checks success, failure propagation, and rerunning completed graph nodes.
//!
//! It needs a MySQL instance. Set `DATABASE_URL` (for example
//! `mysql://root:password@127.0.0.1:3306/rayflow`) to run it; the test resets the
//! database and pushes the schema from the Toasty models itself. Without
//! `DATABASE_URL` the test is skipped, so it is safe to run in environments
//! without a database.

use std::collections::HashMap;
use std::convert::Infallible;

use event::Emit as CommandEmit;
use event::Poll as CommandPoll;
use event::command::Command;
use event::command::CommandEntry;
use event::task::Task;
use graph::GraphStore;
use graph::types::{DAG, Node};
use runtime::CommandApply;
use runtime::backend::Backend;
use runtime::backend::Status;
use runtime::command::{NodeRun, Run};
use scheduler::{
    Actuator, AgentRef, JobPoll, ResourceCapacity, TaskEvent, TaskEventEntry, actuator::NodeConfig,
};
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

/// Agent simulator: selected nodes fail, others succeed.
struct MockRunner {
    failing: Vec<Uuid>,
}
impl MockRunner {
    fn succeed() -> Self {
        Self { failing: vec![] }
    }
    fn fail(failing: Vec<Uuid>) -> Self {
        Self { failing }
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
    builder.models(scheduler::models());
    let mut db = builder
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
    for node in &dag.nodes {
        let now = jiff::Timestamp::now();
        NodeConfig::create()
            .node_id(node.id)
            .version(1)
            .annotation_match(String::new())
            .config(serde_json::json!({}))
            .lease(Some(30_000))
            .required_cpu_cores(0)
            .required_memory_bytes(0)
            .created_at(now)
            .updated_at(now)
            .created_by(String::new())
            .updated_by(String::new())
            .exec(&mut db)
            .await
            .unwrap();
    }
    Some((db, dag, Uuid::new_v4()))
}

#[tokio::test]
async fn yielded_jobs_end_and_resume_creates_new_tasks_in_the_same_node_run() {
    use scheduler::{Job, actuator::Status as JobStatus, reclaim_expired_in, yield_job};

    let Some((mut db, dag, data_id)) = setup().await else {
        return;
    };
    let graph_id = dag.id;
    let node = dag.nodes[0].id;
    let backend = Backend::new(db.clone(), TestGraphStore(dag));
    let mut actuator = Actuator::new(backend.clone(), 64);
    CommandEntry::new(data_id, graph_id, Command::Start { nodes: vec![node] })
        .emit(&mut db)
        .await
        .unwrap();
    assert_eq!(actuator.tick().await.unwrap().tasks, 1);
    let agent_id = Uuid::new_v4();
    let capacity = ResourceCapacity {
        cpu_cores: 1,
        memory_bytes: 1,
    };
    let mut job = AgentRef::new(agent_id, capacity, Vec::new())
        .poll(&mut db, 1)
        .await
        .unwrap()
        .next()
        .unwrap();
    let node_run_id = job.node_run_id;
    let revision = job.node_run.get().revision;
    for use_status_report in [true, false] {
        let mut active = AgentRef::new(agent_id, capacity, vec![job.uuid]);
        assert!(active.poll(&mut db, 0).await.unwrap().next().is_none());
        assert_eq!(active.renewed_jobs(), &[job.uuid]);
        assert_eq!(
            node_run(&mut db, node).await,
            (revision, "Running".to_owned())
        );
        if use_status_report {
            yield_job(&mut db, job.uuid, agent_id).await.unwrap();
            yield_job(&mut db, job.uuid, agent_id).await.unwrap();
        } else {
            TaskEventEntry::new(job.task.get(), TaskEvent::Yield)
                .emit(&mut db)
                .await
                .unwrap();
        }
        assert_eq!(actuator.tick().await.unwrap().events, 1);
        assert_eq!(
            node_run(&mut db, node).await,
            (revision, "Pending".to_owned())
        );
        let yielded = Job::filter_by_id(job.id).get(&mut db).await.unwrap();
        assert_eq!(yielded.status, JobStatus::Yielded);
        assert!(yielded.status.is_terminal());
        assert!(
            yielded.agent_id.is_none()
                && yielded.polled_at.is_none()
                && yielded.expired_at.is_none()
        );
        assert_eq!(reclaim_expired_in(&mut db).await.unwrap(), 0);
        assert!(
            AgentRef::new(agent_id, capacity, vec![job.uuid])
                .poll(&mut db, 1)
                .await
                .unwrap()
                .next()
                .is_none()
        );
        assert!(active.poll(&mut db, 0).await.unwrap().next().is_none());
        assert!(active.renewed_jobs().is_empty());
        assert!(
            scheduler::succeed_job(&mut db, job.uuid, agent_id)
                .await
                .is_err()
        );
        let task_count = Task::all().exec(&mut db).await.unwrap().len();
        let job_count = Job::all().exec(&mut db).await.unwrap().len();
        CommandEntry::new(data_id, graph_id, Command::Resume { node })
            .emit(&mut db)
            .await
            .unwrap();
        assert_eq!(consume_commands(&mut db, &backend).await, 1);
        assert_eq!(
            node_run(&mut db, node).await,
            (revision, "Ready".to_owned())
        );
        assert_eq!(
            Task::all().exec(&mut db).await.unwrap().len(),
            task_count + 1
        );
        let mut tx = db.transaction().await.unwrap();
        let step = actuator.tick_in(&mut tx).await.unwrap();
        assert_eq!(step.tasks, 1);
        assert_eq!(step.commands, 0);
        tx.rollback().await.unwrap();
        assert_eq!(
            node_run(&mut db, node).await,
            (revision, "Ready".to_owned())
        );
        assert_eq!(Job::all().exec(&mut db).await.unwrap().len(), job_count);
        let step = actuator.tick().await.unwrap();
        assert_eq!(step.tasks, 1);
        assert_eq!(step.commands, 0);
        assert_eq!(
            Task::all().exec(&mut db).await.unwrap().len(),
            task_count + 1
        );
        assert_eq!(Job::all().exec(&mut db).await.unwrap().len(), job_count + 1);
        assert!(Task::poll(&mut db, 1).await.unwrap().next().is_none());
        assert_eq!(
            node_run(&mut db, node).await,
            (revision, "Running".to_owned())
        );
        let resumed = AgentRef::new(agent_id, capacity, Vec::new())
            .poll(&mut db, 1)
            .await
            .unwrap()
            .next()
            .unwrap();
        assert_ne!(resumed.uuid, job.uuid);
        assert_ne!(resumed.task_id, job.task_id);
        assert_eq!(resumed.node_run_id, node_run_id);
        yield_job(&mut db, job.uuid, agent_id).await.unwrap();
        assert_eq!(actuator.tick().await.unwrap().events, 0);
        assert_eq!(
            Job::filter_by_id(job.id).get(&mut db).await.unwrap().status,
            JobStatus::Yielded
        );
        job = resumed;
    }
    scheduler::succeed_job(&mut db, job.uuid, agent_id)
        .await
        .unwrap();
    actuator.tick().await.unwrap();
    assert_eq!(
        node_run(&mut db, node).await,
        (revision, "Succeeded".to_owned())
    );
}

/// Writes `Start` to the command log, then consumes and applies it.
async fn start_run(db: &mut Db, backend: &Backend<TestGraphStore>, data_id: Uuid, graph_id: Uuid) {
    let entry = CommandEntry::new(data_id, graph_id, Command::Start { nodes: Vec::new() });
    entry.emit(db).await.expect("emit Start");
    consume_commands(db, backend).await;
}

/// Polls the command log and applies every unacknowledged command, in offset
/// order. Returns how many commands were applied.
async fn consume_commands(db: &mut Db, backend: &Backend<TestGraphStore>) -> usize {
    let entries: Vec<CommandEntry> = CommandEntry::poll(db, 256)
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
    runner: &mut MockRunner,
    consumer: &Backend<TestGraphStore>,
) -> usize {
    let mut actuator = Actuator::new(consumer.clone(), 16);
    let mut total = 0;
    loop {
        let step = actuator.tick().await.expect("actuate");
        let tasks: Vec<_> = AgentRef::new(
            Uuid::from_u128(1),
            ResourceCapacity {
                cpu_cores: 1,
                memory_bytes: 1,
            },
            Vec::new(),
        )
        .poll(db, 16)
        .await
        .expect("poll jobs")
        .collect();
        total += tasks.len();
        for record in &tasks {
            let task = Task::filter_by_id(record.task_id).get(db).await.unwrap();
            let event = if runner.failing.contains(&task.node_id) {
                TaskEvent::Fail
            } else {
                TaskEvent::Succeed
            };
            TaskEventEntry::new(&task, event).emit(db).await.unwrap();
        }
        if tasks.is_empty() && step == scheduler::Step::default() {
            return total;
        }
    }
}

/// Returns the latest node run per `(run_id, node_id)`.
fn latest_node_runs(rows: &[NodeRun]) -> Vec<&NodeRun> {
    let mut latest: HashMap<(Uuid, Uuid), &NodeRun> = HashMap::new();
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
    let rows = NodeRun::all().exec(&mut *db).await.expect("load node runs");
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
    Run::filter_by_data_id_and_graph_id(data_id, graph_id)
        .first()
        .exec(&mut *db)
        .await
        .expect("load run")
        .expect("run exists")
        .revision
}

/// Reads the `(revision, status)` of `node`'s current node run.
async fn node_run(db: &mut Db, node: Uuid) -> (u64, String) {
    let row = NodeRun::all()
        .filter(NodeRun::fields().node_id().eq(node))
        .order_by(NodeRun::fields().revision().desc())
        .first()
        .exec(&mut *db)
        .await
        .expect("load node run")
        .expect("node run exists");
    (row.revision, row.status)
}

/// Reads the status of `node`'s node run at `revision`, if any.
async fn node_run_at(db: &mut Db, node: Uuid, revision: u64) -> Option<String> {
    NodeRun::all()
        .filter(NodeRun::fields().node_id().eq(node))
        .filter(NodeRun::fields().revision().eq(revision))
        .first()
        .exec(&mut *db)
        .await
        .expect("load node run")
        .map(|row| row.status)
}

/// Counts every node run row, including superseded revisions.
async fn node_run_rows(db: &mut Db) -> i64 {
    NodeRun::all()
        .exec(&mut *db)
        .await
        .expect("load node runs")
        .len() as i64
}

#[tokio::test]
async fn actuator_rollback_preserves_tasks_and_graph_state() {
    let Some((mut db, dag, data_id)) = setup().await else {
        return;
    };
    let graph_id = dag.id;
    let backend = Backend::new(db.clone(), TestGraphStore(dag));
    start_run(&mut db, &backend, data_id, graph_id).await;
    let pending: Vec<_> = Task::poll(&mut db, 16).await.unwrap().collect();
    assert!(!pending.is_empty());

    let mut actuator = Actuator::new(backend, 16);
    let mut tx = db.transaction().await.unwrap();
    let step = actuator.tick_in(&mut tx).await.unwrap();
    assert_eq!(step.commands, 0);
    assert_eq!(step.tasks, pending.len());
    assert_eq!(step.events, 0);
    assert_eq!(
        scheduler::Job::all().exec(&mut tx).await.unwrap().len(),
        pending.len()
    );
    assert!(Task::poll(&mut tx, 16).await.unwrap().next().is_none());
    tx.rollback().await.unwrap();

    assert!(
        scheduler::Job::all()
            .exec(&mut db)
            .await
            .unwrap()
            .is_empty()
    );
    let replay: Vec<_> = Task::poll(&mut db, 16).await.unwrap().collect();
    assert_eq!(
        replay.iter().map(|task| task.id).collect::<Vec<_>>(),
        pending.iter().map(|task| task.id).collect::<Vec<_>>()
    );
    for task in &replay {
        let row = NodeRun::filter_by_id(task.node_run_id)
            .get(&mut db)
            .await
            .unwrap();
        assert_eq!(row.status, Status::Ready.to_string());
    }
}

#[tokio::test]
async fn mysql_scheduler_runs_a_chain_to_completion() {
    let Some((mut db, dag, data_id)) = setup().await else {
        eprintln!("skipping: set DATABASE_URL to run the MySQL scheduler test");
        return;
    };
    let graph_id = dag.id;

    let consumer = Backend::new(db.clone(), TestGraphStore(dag));
    let mut scheduler = MockRunner::succeed();

    start_run(&mut db, &consumer, data_id, graph_id).await;

    // The whole graph runs: a, then b and c, then d, then e.
    let scheduled = run_to_quiescence(&mut db, &mut scheduler, &consumer).await;
    assert_eq!(scheduled, 5);

    let (succeeded, failed) = status_counts(&mut db).await;
    assert_eq!(succeeded, 5);
    assert_eq!(failed, 0);

    // Every emitted command was consumed.
    let pending: Vec<CommandEntry> = CommandEntry::poll(&mut db, 256)
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

    let consumer = Backend::new(db.clone(), TestGraphStore(dag));
    let mut scheduler = MockRunner::fail(vec![middle]);

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

    let consumer = Backend::new(db.clone(), TestGraphStore(dag));
    let mut scheduler = MockRunner::succeed();

    start_run(&mut db, &consumer, data_id, graph_id).await;
    assert_eq!(
        run_to_quiescence(&mut db, &mut scheduler, &consumer).await,
        5
    );

    // Rerun `c`: it is reset to `Ready` at a new revision and enqueued again.
    let entry = CommandEntry::new(data_id, graph_id, Command::Rerun { node: middle });
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
