//! Integration test for guarded command transitions.
//!
//! It needs a MySQL instance. Set `DATABASE_URL` (for example
//! `mysql://root:password@127.0.0.1:3306/rayflow`) to run it; the test resets
//! the database and pushes the schema from the Toasty models itself. Without
//! `DATABASE_URL` the test is skipped.

use std::convert::Infallible;

use event::command::Command;
use event::command::CommandEntry;
use graph::GraphStore;
use graph::types::{DAG, Node};
use runtime::CommandApply;
use runtime::backend::Status;
use runtime::command::{NodeRun, Run};
use runtime::{Error, backend::Backend};
use toasty::Db;
use uuid::Uuid;

#[derive(Clone)]
struct TestGraphStore(DAG);

impl GraphStore<Uuid> for TestGraphStore {
    type G = DAG;
    type Err = Infallible;

    async fn graphs(
        &mut self,
        page: graph::PageRequest,
        graph_id: Option<Uuid>,
    ) -> Result<graph::Page<DAG>, Infallible> {
        Ok(page.select(
            std::iter::once((1, self.0.clone()))
                .filter(|(_, graph)| graph_id.is_none_or(|id| graph.id == id)),
        ))
    }

    async fn load(&mut self, _graph_id: Uuid, _version: Option<u64>) -> Result<Self::G, Self::Err> {
        Ok(self.0.clone())
    }
}

/// Connects and rebuilds the schema from the Toasty models, or returns `None`
/// when no `DATABASE_URL` is configured.
async fn setup() -> Option<(Db, DAG, Uuid, Uuid)> {
    let url = std::env::var("DATABASE_URL").ok()?;

    let mut builder = Db::builder();
    builder.models(runtime::models());
    let db = builder
        .connect(&url)
        .await
        .expect("connect to DATABASE_URL");

    db.reset_db().await.expect("reset database");
    db.push_schema().await.expect("push schema");

    let node = Node {
        id: Uuid::new_v4(),
        name: "root".to_string(),
        node_type: "test".to_string(),
        settings: Default::default(),
        inputs: Vec::new(),
        outputs: Vec::new(),
    };
    let dag = DAG {
        id: Uuid::new_v4(),
        name: "single".to_string(),
        version: 1,
        nodes: vec![node],
        edges: Vec::new(),
        node_count: 1,
        ..DAG::default()
    };
    let graph_id = dag.id;
    Some((db, dag, Uuid::new_v4(), graph_id))
}

#[tokio::test]
async fn start_rejects_an_existing_data_graph_pair_across_versions() {
    let Some((mut db, mut dag, data_id, graph_id)) = setup().await else {
        return;
    };
    let command = CommandEntry::new(data_id, graph_id, Command::Start { nodes: Vec::new() });
    command
        .apply(Backend::new(db.clone(), TestGraphStore(dag.clone())))
        .await
        .unwrap();
    let initial = Run::filter_by_data_id_and_graph_id(data_id, graph_id)
        .get(&mut db)
        .await
        .unwrap();

    for version in [dag.version, dag.version + 1] {
        dag.version = version;
        assert!(matches!(
            command
                .apply(Backend::new(db.clone(), TestGraphStore(dag.clone())))
                .await,
            Err(Error::AlreadyStarted)
        ));
    }
    let runs = Run::all().exec(&mut db).await.unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].uuid, initial.uuid);
    assert_eq!(runs[0].version, initial.version);
    assert_eq!(NodeRun::all().exec(&mut db).await.unwrap().len(), 1);
    assert_eq!(
        event::task::Task::all().exec(&mut db).await.unwrap().len(),
        1
    );

    CommandEntry::new(
        Uuid::new_v4(),
        graph_id,
        Command::Start { nodes: Vec::new() },
    )
    .apply(Backend::new(db.clone(), TestGraphStore(dag.clone())))
    .await
    .unwrap();
    dag.id = Uuid::new_v4();
    CommandEntry::new(data_id, dag.id, Command::Start { nodes: Vec::new() })
        .apply(Backend::new(db.clone(), TestGraphStore(dag)))
        .await
        .unwrap();
    assert_eq!(Run::all().exec(&mut db).await.unwrap().len(), 3);
}

#[tokio::test]
async fn database_rejects_duplicate_data_graph_pairs_with_different_versions() {
    let Some((mut db, dag, data_id, graph_id)) = setup().await else {
        return;
    };
    let now = jiff::Timestamp::now();
    for version in [1, 2] {
        let result = Run::create()
            .uuid(Uuid::new_v4())
            .data_id(data_id)
            .graph_id(graph_id)
            .version(version)
            .revision(0)
            .created_at(now)
            .updated_at(now)
            .exec(&mut db)
            .await;
        if version == 1 {
            result.unwrap();
        } else {
            assert!(result.is_err());
        }
    }
    let runs = Run::all().exec(&mut db).await.unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].version, dag.version);
}

#[tokio::test]
async fn resume_returns_to_ready_and_enqueues_one_task() {
    let Some((mut db, mut dag, data_id, graph_id)) = setup().await else {
        return;
    };
    let node = dag.nodes[0].id;
    let mut predecessor = dag.nodes[0].clone();
    predecessor.id = Uuid::new_v4();
    dag.edges.push((predecessor.id, node));
    dag.nodes.push(predecessor);
    let version = dag.version;
    let backend = Backend::new(db.clone(), TestGraphStore(dag));
    CommandEntry::new(data_id, graph_id, Command::Start { nodes: vec![node] })
        .apply(backend.clone())
        .await
        .unwrap();
    let initial = NodeRun::all().exec(&mut db).await.unwrap().remove(0);
    assert_eq!(initial.status, Status::Ready.to_string());
    assert_eq!(initial.version, version);
    assert_eq!(
        event::task::Task::all().exec(&mut db).await.unwrap().len(),
        1
    );
    let resume = CommandEntry::new(data_id, graph_id, Command::Resume { node });
    assert!(matches!(
        resume.apply(backend.clone()).await,
        Err(Error::InvalidTransition { .. })
    ));
    CommandEntry::new(data_id, graph_id, Command::Run { node })
        .apply(backend.clone())
        .await
        .unwrap();
    CommandEntry::new(data_id, graph_id, Command::Pend { node })
        .apply(backend.clone())
        .await
        .unwrap();
    let (first, second) =
        tokio::join!(resume.apply(backend.clone()), resume.apply(backend.clone()));
    let mut successes = 0;
    for result in [first, second] {
        match result {
            Ok(()) => successes += 1,
            Err(Error::InvalidTransition { .. } | Error::ConcurrentUpdate(_)) => {}
            Err(error) => panic!("unexpected resume error: {error}"),
        }
    }
    assert_eq!(successes, 1);
    let resumed = NodeRun::filter_by_id(initial.id)
        .get(&mut db)
        .await
        .unwrap();
    assert_eq!(resumed.status, Status::Ready.to_string());
    assert_eq!(resumed.revision, initial.revision);
    let tasks = event::task::Task::all().exec(&mut db).await.unwrap();
    assert_eq!(tasks.len(), 2);
    assert_ne!(tasks[0].id, tasks[1].id);
    assert!(tasks.iter().all(|task| task.node_run_id == initial.id));
    assert!(matches!(
        resume.apply(backend.clone()).await,
        Err(Error::InvalidTransition { .. })
    ));
    CommandEntry::new(data_id, graph_id, Command::Run { node })
        .apply(backend.clone())
        .await
        .unwrap();
    CommandEntry::new(data_id, graph_id, Command::Succeed { node })
        .apply(backend.clone())
        .await
        .unwrap();
    assert!(matches!(
        resume.apply(backend).await,
        Err(Error::InvalidTransition { .. })
    ));
    assert_eq!(
        event::task::Task::all().exec(&mut db).await.unwrap().len(),
        2
    );
}

#[tokio::test]
async fn concurrent_run_transitions_once_without_creating_tasks() {
    let Some((mut db, dag, data_id, graph_id)) = setup().await else {
        eprintln!("skipping: set DATABASE_URL to run the MySQL command transition test");
        return;
    };

    let backend = Backend::new(db.clone(), TestGraphStore(dag.clone()));
    let node = dag.nodes[0].id;

    let now = jiff::Timestamp::now();
    let run = Run::create()
        .uuid(Uuid::new_v4())
        .data_id(data_id)
        .graph_id(graph_id)
        .version(dag.version)
        .revision(0)
        .created_at(now)
        .updated_at(now)
        .exec(&mut db)
        .await
        .unwrap();
    NodeRun::create()
        .run_id(run.uuid)
        .data_id(data_id)
        .graph_id(graph_id)
        .version(run.version)
        .node_id(node)
        .revision(0)
        .status(Status::Ready.to_string())
        .created_at(now)
        .updated_at(now)
        .exec(&mut db)
        .await
        .unwrap();

    // Guarded updates allow only one consumer to apply the transition.
    let transition = CommandEntry::new(data_id, graph_id, Command::Run { node });
    let (first, second) = tokio::join!(
        transition.apply(backend.clone()),
        transition.apply(backend.clone())
    );

    let mut applied = 0;
    let mut rejected = 0;
    for result in [first, second] {
        match result {
            Ok(()) => applied += 1,
            Err(Error::InvalidTransition {
                node: rejected_node,
                ..
            }) => {
                assert_eq!(rejected_node, node);
                rejected += 1;
            }
            Err(Error::ConcurrentUpdate(rejected_node)) => {
                assert_eq!(rejected_node, node);
                rejected += 1;
            }
            Err(error) => panic!("unexpected transition error: {error}"),
        }
    }
    assert_eq!(applied, 1);
    assert_eq!(rejected, 1);

    let node_run = NodeRun::all()
        .first()
        .exec(&mut db)
        .await
        .expect("load node run")
        .expect("node run exists");
    assert_eq!(node_run.status, Status::Running.to_string());
    let tasks = event::task::Task::all().exec(&mut db).await.unwrap();
    assert!(tasks.is_empty());
}

#[tokio::test]
async fn dryrun_creates_tasks_for_ready_targets_only() {
    let Some((mut db, mut dag, data_id, graph_id)) = setup().await else {
        return;
    };
    let root = dag.nodes[0].id;
    let mut first = dag.nodes[0].clone();
    first.id = Uuid::new_v4();
    let mut second = dag.nodes[0].clone();
    second.id = Uuid::new_v4();
    let targets = vec![first.id, second.id];
    dag.edges = vec![(root, first.id), (root, second.id)];
    dag.nodes.extend([first, second]);
    let backend = Backend::new(db.clone(), TestGraphStore(dag));
    CommandEntry::new(data_id, graph_id, Command::Start { nodes: vec![] })
        .apply(backend.clone())
        .await
        .unwrap();
    CommandEntry::new(
        data_id,
        graph_id,
        Command::Dryrun {
            nodes: targets.clone(),
        },
    )
    .apply(backend.clone())
    .await
    .unwrap();

    let run = Run::filter_by_data_id_and_graph_id(data_id, graph_id)
        .first()
        .exec(&mut db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.revision, 1);
    let tasks = event::task::Task::all().exec(&mut db).await.unwrap();
    assert_eq!(tasks.len(), 3);
    let mocked = NodeRun::filter_by_run_id_and_node_id_and_revision(run.uuid, root, 1)
        .get(&mut db)
        .await
        .unwrap();
    assert_eq!(mocked.status, Status::Succeeded.to_string());
    assert!(tasks.iter().all(|task| task.node_run_id != mocked.id));
    for node in targets {
        let node_run = NodeRun::filter_by_run_id_and_node_id_and_revision(run.uuid, node, 1)
            .get(&mut db)
            .await
            .unwrap();
        assert_eq!(node_run.status, Status::Ready.to_string());
        assert_eq!(
            tasks
                .iter()
                .filter(|task| task.node_run_id == node_run.id)
                .count(),
            1
        );
        CommandEntry::new(data_id, graph_id, Command::Run { node })
            .apply(backend.clone())
            .await
            .unwrap();
        assert_eq!(
            NodeRun::filter_by_id(node_run.id)
                .get(&mut db)
                .await
                .unwrap()
                .status,
            Status::Running.to_string()
        );
        assert!(matches!(
            CommandEntry::new(data_id, graph_id, Command::Run { node })
                .apply(backend.clone())
                .await,
            Err(Error::InvalidTransition { .. })
        ));
    }
    assert_eq!(
        event::task::Task::all().exec(&mut db).await.unwrap().len(),
        3
    );
}

#[tokio::test]
async fn rerun_creates_a_ready_revision_and_one_task() {
    let Some((mut db, dag, data_id, graph_id)) = setup().await else {
        return;
    };
    let node = dag.nodes[0].id;
    let backend = Backend::new(db.clone(), TestGraphStore(dag));
    for command in [
        Command::Start { nodes: vec![] },
        Command::Run { node },
        Command::Succeed { node },
    ] {
        CommandEntry::new(data_id, graph_id, command)
            .apply(backend.clone())
            .await
            .unwrap();
    }
    let original = NodeRun::all().exec(&mut db).await.unwrap().remove(0);
    let original_task = event::task::Task::all()
        .exec(&mut db)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(original.status, Status::Succeeded.to_string());
    for revision in [1, 2] {
        CommandEntry::new(data_id, graph_id, Command::Rerun { node })
            .apply(backend.clone())
            .await
            .unwrap();
        let run = Run::filter_by_uuid(original.run_id)
            .get(&mut db)
            .await
            .unwrap();
        assert_eq!(run.revision, revision);
        let current = NodeRun::filter_by_run_id_and_node_id_and_revision(run.uuid, node, revision)
            .get(&mut db)
            .await
            .unwrap();
        assert_eq!(current.status, Status::Ready.to_string());
        assert_ne!(current.id, original.id);
        let tasks = event::task::Task::all().exec(&mut db).await.unwrap();
        assert_eq!(tasks.len(), revision as usize + 1);
        let current_tasks: Vec<_> = tasks
            .iter()
            .filter(|task| task.node_run_id == current.id)
            .collect();
        assert_eq!(current_tasks.len(), 1);
        assert_ne!(current_tasks[0].id, original_task.id);
    }
    assert_eq!(
        NodeRun::filter_by_id(original.id)
            .get(&mut db)
            .await
            .unwrap()
            .status,
        Status::Succeeded.to_string()
    );
}

#[tokio::test]
async fn succeed_enqueues_only_newly_ready_successors() {
    let Some((mut db, mut dag, data_id, graph_id)) = setup().await else {
        return;
    };
    let first = dag.nodes[0].id;
    let mut second = dag.nodes[0].clone();
    second.id = Uuid::new_v4();
    let mut waiting = dag.nodes[0].clone();
    waiting.id = Uuid::new_v4();
    let mut ready = dag.nodes[0].clone();
    ready.id = Uuid::new_v4();
    let second_id = second.id;
    let waiting_id = waiting.id;
    let ready_id = ready.id;
    dag.edges = vec![
        (first, waiting_id),
        (second_id, waiting_id),
        (first, ready_id),
        (second_id, ready_id),
    ];
    dag.nodes.extend([second, waiting, ready]);
    let backend = Backend::new(db.clone(), TestGraphStore(dag));
    CommandEntry::new(
        data_id,
        graph_id,
        Command::Start {
            nodes: vec![first, second_id, ready_id],
        },
    )
    .apply(backend.clone())
    .await
    .unwrap();
    for node in [first, second_id] {
        CommandEntry::new(data_id, graph_id, Command::Run { node })
            .apply(backend.clone())
            .await
            .unwrap();
        CommandEntry::new(data_id, graph_id, Command::Succeed { node })
            .apply(backend.clone())
            .await
            .unwrap();
        let tasks = event::task::Task::all().exec(&mut db).await.unwrap();
        assert_eq!(
            tasks.iter().filter(|task| task.node_id == ready_id).count(),
            1
        );
        if node == first {
            assert_eq!(tasks.len(), 3);
            assert!(
                NodeRun::all()
                    .filter(NodeRun::fields().node_id().eq(waiting_id))
                    .exec(&mut db)
                    .await
                    .unwrap()
                    .is_empty()
            );
        } else {
            assert_eq!(tasks.len(), 4);
            let task = tasks
                .iter()
                .find(|task| task.node_id == waiting_id)
                .unwrap();
            let node_run = NodeRun::filter_by_id(task.node_run_id)
                .get(&mut db)
                .await
                .unwrap();
            assert_eq!(node_run.status, Status::Ready.to_string());
            assert_eq!(node_run.revision, 0);
        }
    }
}
