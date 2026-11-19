use std::{convert::Infallible, io, time::Duration};

use event::{Emit, command::Command, command::CommandEntry, task::Task};
use graph::{GraphStore, Page, PageRequest, types::DAG};
use jiff::Timestamp;
use pilot::Pilot;
use runtime::{
    backend::{Backend, Status as NodeStatus},
    command::{NodeRun, Run},
};
use scheduler::{
    AgentRef, Job, JobPoll, ResourceCapacity, Scheduler, actuator::NodeConfig, fail_job,
    succeed_job, yield_job,
};
use uuid::Uuid;

#[path = "../../server/tests/support/mod.rs"]
mod support;

#[derive(Clone)]
struct TestGraphStore {
    graph: DAG,
}

impl GraphStore<Uuid> for TestGraphStore {
    type G = DAG;
    type Err = Infallible;

    async fn graphs(
        &mut self,
        _page: PageRequest,
        _graph_id: Option<Uuid>,
    ) -> Result<Page<DAG>, Infallible> {
        Ok(Page {
            items: vec![self.graph.clone()],
            next_cursor: None,
        })
    }

    async fn load(&mut self, graph_id: Uuid, _version: Option<u64>) -> Result<DAG, Infallible> {
        assert_eq!(self.graph.id, graph_id);
        Ok(self.graph.clone())
    }
}

fn graph() -> (TestGraphStore, Uuid, [Uuid; 3]) {
    let graph_id = Uuid::new_v4();
    let nodes = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
    let [first, second, third] = nodes;
    let graph = DAG {
        id: graph_id,
        name: "workflow-test".into(),
        version: 1,
        node_count: nodes.len(),
        nodes: nodes
            .into_iter()
            .enumerate()
            .map(|(position, id)| graph::types::Node {
                id,
                name: format!("node-{position}"),
                node_type: "test".into(),
                settings: Default::default(),
                inputs: vec![],
                outputs: vec![],
            })
            .collect(),
        edges: vec![(first, second), (second, third)],
        ..DAG::default()
    };
    (TestGraphStore { graph }, graph_id, nodes)
}

async fn insert_node_configs(db: &mut toasty::Db, node_ids: [Uuid; 3]) {
    let now = Timestamp::now();
    for node_id in node_ids {
        NodeConfig::create()
            .uuid(Uuid::new_v4())
            .node_id(node_id)
            .version(1)
            .annotation_match(String::new())
            .config(serde_json::json!({}))
            .lease(Some(60_000))
            .required_cpu_cores(1)
            .required_memory_bytes(1)
            .created_at(now)
            .updated_at(now)
            .created_by("workflow-test")
            .updated_by("workflow-test")
            .exec(db)
            .await
            .expect("insert node config");
    }
}

async fn start_run(
    pilot: &mut Pilot<
        coordinator::mysql::MySqlLeaderElection,
        scheduler::AgentScheduler,
        TestGraphStore,
    >,
    db: &mut toasty::Db,
    data_id: Uuid,
    graph_id: Uuid,
) {
    CommandEntry::new(data_id, graph_id, Command::Start { nodes: vec![] })
        .emit(db)
        .await
        .expect("emit start command");
    let step = pilot.step().await.expect("consume start and first task");
    assert_eq!((step.commands, step.tasks, step.events), (1, 1, 0));
}

async fn run_job(db: &mut toasty::Db, run_id: Uuid, agent_id: Uuid) -> Job {
    let mut agent = AgentRef::new(
        agent_id,
        ResourceCapacity {
            cpu_cores: 4,
            memory_bytes: 4,
        },
        vec![],
    );
    let jobs: Vec<_> = agent
        .poll(db, 10)
        .await
        .expect("poll scheduled job")
        .collect();
    assert_eq!(jobs.len(), 1);
    let job = jobs.into_iter().next().unwrap();
    assert_eq!(job.run_id, run_id);
    job
}

#[tokio::test]
async fn pilot_materializes_successor_and_stops_after_failure() {
    support::with_test_database(success_then_failure).await;
}

async fn success_then_failure(mut db: toasty::Db) {
    let (store, graph_id, nodes) = graph();
    let [first, second, third] = nodes;
    insert_node_configs(&mut db, nodes).await;
    let data_id = Uuid::new_v4();
    let election = coordinator::mysql::MySqlLeaderElection::new(
        db.clone(),
        format!("workflow-{data_id}"),
        format!("pilot-{data_id}"),
        Duration::from_secs(30),
    );
    let backend = Backend::new(db.clone(), store);
    let mut pilot = Pilot::new(election, backend, 100, Duration::from_millis(1));

    start_run(&mut pilot, &mut db, data_id, graph_id).await;
    let run = Run::filter_by_data_id_and_graph_id(data_id, graph_id)
        .first()
        .exec(&mut db)
        .await
        .unwrap()
        .unwrap();
    let first_job = run_job(&mut db, run.uuid, Uuid::new_v4()).await;
    assert_eq!(first_job.node_run.get().node_id, first);
    assert_eq!(first_job.node_run_id, first_job.node_run.get().uuid);
    assert_eq!(
        first_job.node_config_id,
        first_job.node_config.get().as_ref().unwrap().uuid
    );
    succeed_job(&mut db, first_job.uuid, first_job.agent_id.unwrap())
        .await
        .expect("succeed first job");
    let step = pilot.step().await.expect("consume first success");
    assert_eq!((step.commands, step.tasks, step.events), (1, 0, 1));

    let ready = NodeRun::filter_by_run_id_and_node_id_and_revision(run.uuid, second, run.revision)
        .get(&mut db)
        .await
        .unwrap();
    assert_eq!(ready.status, NodeStatus::Ready.to_string());

    let step = pilot.step().await.expect("materialize successor");
    assert_eq!((step.commands, step.tasks, step.events), (0, 1, 0));
    let second_run = NodeRun::filter_by_id(ready.id).get(&mut db).await.unwrap();
    assert_eq!(second_run.status, NodeStatus::Running.to_string());
    let second_job = run_job(&mut db, run.uuid, Uuid::new_v4()).await;
    assert_eq!(second_job.node_run_id, second_run.uuid);
    fail_job(&mut db, second_job.uuid, second_job.agent_id.unwrap())
        .await
        .expect("fail successor job");
    let step = pilot.step().await.expect("consume successor failure");
    assert_eq!((step.commands, step.tasks, step.events), (1, 0, 1));

    let failed = NodeRun::filter_by_id(second_run.id)
        .get(&mut db)
        .await
        .unwrap();
    assert_eq!(failed.status, NodeStatus::Failed.to_string());
    let blocked = NodeRun::filter_by_run_id_and_node_id_and_revision(run.uuid, third, run.revision)
        .first()
        .exec(&mut db)
        .await
        .unwrap();
    assert!(blocked.is_none());
    let tasks: Vec<_> = Task::all()
        .filter(Task::fields().data_id().eq(data_id))
        .exec(&mut db)
        .await
        .unwrap();
    assert_eq!(tasks.len(), 2);
}

#[tokio::test]
async fn pilot_yield_and_resume_create_a_fresh_task() {
    support::with_test_database(yield_and_resume).await;
}

async fn yield_and_resume(mut db: toasty::Db) {
    let (store, graph_id, nodes) = graph();
    let [first, ..] = nodes;
    insert_node_configs(&mut db, nodes).await;
    let data_id = Uuid::new_v4();
    let election = coordinator::mysql::MySqlLeaderElection::new(
        db.clone(),
        format!("yield-{data_id}"),
        format!("pilot-{data_id}"),
        Duration::from_secs(30),
    );
    let backend = Backend::new(db.clone(), store);
    let mut pilot = Pilot::new(election, backend, 100, Duration::from_millis(1));

    start_run(&mut pilot, &mut db, data_id, graph_id).await;
    let run = Run::filter_by_data_id_and_graph_id(data_id, graph_id)
        .first()
        .exec(&mut db)
        .await
        .unwrap()
        .unwrap();
    let first_job = run_job(&mut db, run.uuid, Uuid::new_v4()).await;
    let agent_id = first_job.agent_id.unwrap();
    yield_job(&mut db, first_job.uuid, agent_id)
        .await
        .expect("yield first job");
    let step = pilot.step().await.expect("consume yield");
    assert_eq!((step.commands, step.tasks, step.events), (1, 0, 1));

    let pending = NodeRun::filter_by_id(first_job.node_run.get().id)
        .get(&mut db)
        .await
        .unwrap();
    assert_eq!(pending.status, NodeStatus::Pending.to_string());
    let yielded = Job::filter_by_uuid(first_job.uuid)
        .get(&mut db)
        .await
        .unwrap();
    assert_eq!(yielded.status, scheduler::actuator::Status::Yielded);
    assert_eq!(yielded.agent_id, None);

    CommandEntry::new(data_id, graph_id, Command::Resume { node: first })
        .emit(&mut db)
        .await
        .expect("emit resume command");
    let step = pilot.step().await.expect("materialize resumed task");
    assert_eq!((step.commands, step.tasks, step.events), (1, 1, 0));

    let resumed = run_job(&mut db, run.uuid, agent_id).await;
    assert_ne!(resumed.uuid, yielded.uuid);
    assert_ne!(resumed.task_id, yielded.task_id);
    assert_eq!(resumed.node_run_id, yielded.node_run_id);
    assert_eq!(resumed.node_run.get().revision, pending.revision);
    let running = NodeRun::filter_by_id(pending.id)
        .get(&mut db)
        .await
        .unwrap();
    assert_eq!(running.status, NodeStatus::Running.to_string());
}

struct RejectScheduling;

impl<B> Scheduler<B> for RejectScheduling {
    type Err = io::Error;

    async fn schedule(&self, _backend: &mut B) -> Result<usize, Self::Err> {
        Err(io::Error::other("injected scheduling failure"))
    }
}

#[tokio::test]
async fn scheduling_failure_rolls_back_the_lease_and_job_before_retry() {
    support::with_test_database(rollback_then_retry).await;
}

async fn rollback_then_retry(mut db: toasty::Db) {
    let (store, graph_id, nodes) = graph();
    insert_node_configs(&mut db, nodes).await;
    let data_id = Uuid::new_v4();
    let election = |owner| {
        coordinator::mysql::MySqlLeaderElection::new(
            db.clone(),
            "workflow-test",
            owner,
            Duration::from_secs(30),
        )
    };
    let backend = Backend::new(db.clone(), store);
    let mut rejecting = Pilot::new(
        election("rejecting"),
        backend.clone(),
        16,
        Duration::from_millis(10),
    )
    .with_scheduler(RejectScheduling);
    let mut retry = Pilot::new(election("retry"), backend, 16, Duration::from_millis(10))
        .with_gap_timeout(Duration::from_millis(10));
    CommandEntry::new(data_id, graph_id, Command::Start { nodes: vec![] })
        .emit(&mut db)
        .await
        .unwrap();

    assert!(matches!(
        rejecting.step().await,
        Err(pilot::Error::Scheduler(_))
    ));
    assert!(!rejecting.is_leader().await.unwrap());
    assert!(Run::all().exec(&mut db).await.unwrap().is_empty());
    assert!(Task::all().exec(&mut db).await.unwrap().is_empty());
    assert!(Job::all().exec(&mut db).await.unwrap().is_empty());

    let first_pass = retry.step().await.unwrap();
    assert_eq!(first_pass.commands, 1);
    // Rolling back an insert may leave a gap in the task table's auto-increment IDs.
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut tasks = first_pass.tasks;
        while tasks == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            tasks += retry.step().await.unwrap().tasks;
        }
        assert_eq!(tasks, 1);
    })
    .await
    .expect("retry applies the previously unacknowledged start command");
    assert!(retry.is_leader().await.unwrap());
    let run = Run::filter_by_data_id_and_graph_id(data_id, graph_id)
        .get(&mut db)
        .await
        .unwrap();
    let job = run_job(&mut db, run.uuid, Uuid::new_v4()).await;
    assert_eq!(job.node_run.get().node_id, nodes[0]);
}
