//! Integration test for the distributed transition lock.
//!
//! It needs a MySQL instance. Set `DATABASE_URL` (for example
//! `mysql://root:password@127.0.0.1:3306/rayflow`) to run it; the test resets
//! the database and pushes the schema from the Toasty models itself. Without
//! `DATABASE_URL` the test is skipped.
#![cfg(feature = "mysql")]

use std::convert::Infallible;

use dag::types::{DAG, Node};
use runtime::mysql::model::NodeRunModel;
use runtime::mysql::{Backend, Error};
use runtime::{Command, CommandApply, CommandEntry, GraphStore, Status};
use toasty::Db;
use uuid::Uuid;

#[derive(Clone)]
struct TestGraphStore(DAG);

impl GraphStore<Uuid> for TestGraphStore {
    type G = DAG;
    type Err = Infallible;

    async fn load(&self, _graph_id: Uuid, _version: Option<u64>) -> Result<Self::G, Self::Err> {
        Ok(self.0.clone())
    }
}

/// Connects and rebuilds the schema from the Toasty models, or returns `None`
/// when no `DATABASE_URL` is configured.
async fn setup() -> Option<(Db, DAG, Uuid, Uuid)> {
    let url = std::env::var("DATABASE_URL").ok()?;

    let mut builder = Db::builder();
    builder.models(runtime::mysql::models());
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
        inputs: Vec::new(),
        outputs: Vec::new(),
    };
    let dag = DAG {
        id: Uuid::new_v4(),
        name: "single".to_string(),
        version: 1,
        nodes: vec![node],
        edges: Vec::new(),
    };
    let graph_id = dag.id;
    Some((db, dag, Uuid::new_v4(), graph_id))
}

#[tokio::test]
async fn concurrent_applications_visit_a_transition_once() {
    let Some((mut db, dag, data_id, graph_id)) = setup().await else {
        eprintln!("skipping: set DATABASE_URL to run the MySQL transition lock test");
        return;
    };

    let backend = Backend::new(db.clone(), 1, TestGraphStore(dag.clone()));
    let node = dag.nodes[0].id;

    CommandEntry::routed(data_id, graph_id, Command::Start { nodes: Vec::new() }, 1)
        .apply(backend.clone())
        .await
        .expect("start run");

    // Two consumers race to apply the same transition. The transaction-scoped
    // lock makes the loser wait, after which it observes Running and rejects the
    // duplicate transition instead of visiting the node's state twice.
    let transition = CommandEntry::routed(data_id, graph_id, Command::Run { node }, 1);
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
            Err(error) => panic!("unexpected transition error: {error}"),
        }
    }
    assert_eq!(applied, 1);
    assert_eq!(rejected, 1);

    let node_run = NodeRunModel::all()
        .first()
        .exec(&mut db)
        .await
        .expect("load node run")
        .expect("node run exists");
    assert_eq!(node_run.status, Status::Running.to_string());
}
