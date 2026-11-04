//! Read-side state of a run: node runs joined with their graph node.

use std::collections::HashMap;

use graph::Graph;
use graph::GraphStore;
use graph::types::{DAG, Input, Node, Output};
use jiff::Timestamp;
use runtime::command::{NodeRun, Run};
use serde::Serialize;
use uuid::Uuid;

use crate::app::{App, AppError, Page, PageQuery, RunApp};

#[derive(Debug, Serialize)]
pub struct RunView {
    pub state: GraphState,
    pub graph: DAG,
    pub current: bool,
}

impl<S> App<S> {
    pub async fn list_runs(
        &self,
        page: PageQuery,
        graph_id: Option<Uuid>,
    ) -> Result<Page<Run>, AppError> {
        let limit = page.limit()?;
        let mut select = Run::all();
        if let Some(before) = page.before {
            select = select.filter(Run::fields().id().lt(before));
        }
        if let Some(graph_id) = graph_id {
            select = select.filter(Run::fields().graph_id().eq(graph_id));
        }
        let items = select
            .order_by(Run::fields().id().desc())
            .limit(limit + 1)
            .exec(&mut self.db.clone())
            .await?;
        Ok(Page::new(items, limit, |row| row.id))
    }

    pub async fn inspect_run(&self, id: Uuid) -> Result<RunView, AppError>
    where
        S: GraphStore<Uuid, G = DAG> + Clone,
        S::Err: Send + Sync + 'static,
    {
        let mut db = self.db.clone();
        let run = Run::filter_by_uuid(id)
            .first()
            .exec(&mut db)
            .await?
            .ok_or(AppError::RunIdNotFound(id))?;
        let latest = Run::filter_by_data_id_and_graph_id(run.data_id, run.graph_id)
            .order_by(Run::fields().version().desc())
            .first()
            .exec(&mut db)
            .await?;
        let current = latest.is_some_and(|latest| latest.uuid == run.uuid);
        let graph = self.read_graph(run.graph_id, Some(run.version)).await?;
        let state = self
            .run_with(run.data_id, run.graph_id)
            .graph_state_at(run, &graph)
            .await?;
        Ok(RunView {
            state,
            graph,
            current,
        })
    }
}

/// The state of a single node, combining its node run and its graph definition.
///
/// The fields of the node run row and of the graph's [`Node`] are embedded flat,
/// so a state read is one object rather than two nested ones. The node run's
/// internal primary key and run id are omitted; `node_id` is the graph node's id.
#[derive(Debug, Clone, Serialize)]
pub struct NodeState {
    /// Data identifier of the run.
    pub data_id: Uuid,
    /// Graph identifier of the run.
    pub graph_id: Uuid,
    /// Node identifier from the node run.
    pub node_id: Uuid,
    /// Revision the node run was read at.
    pub revision: u64,
    /// Node status, e.g. `Succeeded`.
    pub status: String,
    pub ineffective: u32,
    /// When the node run row was created.
    pub created_at: Timestamp,
    /// When the node run row was last updated.
    pub updated_at: Timestamp,
    /// Node name from the graph.
    pub name: String,
    /// Node type from the graph.
    pub node_type: String,
    /// Node inputs from the graph.
    pub inputs: Vec<Input>,
    /// Node outputs from the graph.
    pub outputs: Vec<Output>,
}

impl NodeState {
    /// Combines a node run row with the graph's node.
    fn join(node_run: NodeRun, node: Node) -> Self {
        Self {
            data_id: node_run.data_id,
            graph_id: node_run.graph_id,
            node_id: node_run.node_id,
            revision: node_run.revision,
            status: node_run.status,
            ineffective: node_run.ineffective,
            created_at: node_run.created_at,
            updated_at: node_run.updated_at,
            name: node.name,
            node_type: node.node_type,
            inputs: node.inputs,
            outputs: node.outputs,
        }
    }
}

/// The effective node states of a run at its current revision.
#[derive(Debug, Clone, Serialize)]
pub struct GraphState {
    /// Unique identifier of the run.
    pub uuid: Uuid,
    /// Data identifier of the run.
    pub data_id: Uuid,
    /// Graph identifier of the run.
    pub graph_id: Uuid,
    /// The graph version the run executes.
    pub version: u64,
    /// The revision the states were read at.
    pub revision: u64,
    /// Directed edges of the graph as `(from, to)` node-id pairs.
    pub edges: Vec<(Uuid, Uuid)>,
    /// The node states at `revision`.
    pub nodes: Vec<NodeState>,
}

impl<S> RunApp<S>
where
    S: GraphStore<Uuid, G = DAG> + Clone,
    <S as GraphStore<Uuid>>::Err: Send + Sync + 'static,
{
    /// Returns the state of `node_id`: its node run combined with the graph node.
    pub async fn node_state(&self, node_id: Uuid) -> Result<NodeState, AppError> {
        let run = self.run().await?;
        // Load the graph before the node run so no node run row is held across an
        // await (it keeps this future `Send`, so it can be used as a handler).
        let graph = self.graph(run.version).await?;
        let mut db = self.db.clone();
        // A node may have a node run per revision; its live state is the row
        // with the greatest revision at or before the run's current revision.
        let node_run = NodeRun::filter_by_run_id(run.uuid)
            .filter(NodeRun::fields().node_id().eq(node_id))
            .filter(NodeRun::fields().revision().le(run.revision))
            .order_by(NodeRun::fields().revision().desc())
            .first()
            .exec(&mut db)
            .await?
            .ok_or(AppError::NodeRunNotFound(node_id))?;

        let node_info = graph
            .node(node_id)
            .map(|node| node.data.clone())
            .ok_or(AppError::NodeNotFound(node_id))?;

        Ok(NodeState::join(node_run, node_info))
    }

    /// Returns effective node states at or before the run's current revision.
    pub async fn graph_state(&self) -> Result<GraphState, AppError> {
        let run = self.run().await?;
        // Load the graph before the node runs so no node run row is held across
        // an await (it keeps this future `Send`, so it can be used as a handler).
        let graph = self.graph(run.version).await?;
        self.graph_state_at(run, &graph).await
    }

    async fn graph_state_at(&self, run: Run, graph: &DAG) -> Result<GraphState, AppError> {
        let mut db = self.db.clone();
        let rows = NodeRun::filter_by_run_id(run.uuid)
            .filter(NodeRun::fields().revision().le(run.revision))
            .filter(NodeRun::fields().ineffective().eq(0))
            .exec(&mut db)
            .await?;

        // A node may have a node run per revision; its live state is the row
        // with the greatest revision at or before the run's current revision.
        let mut live: HashMap<Uuid, NodeRun> = HashMap::new();
        for row in rows {
            let replace = live
                .get(&row.node_id)
                .is_none_or(|current| row.revision > current.revision);
            if replace {
                live.insert(row.node_id, row);
            }
        }

        let nodes = live
            .into_values()
            .filter_map(|node_run| {
                graph
                    .node(node_run.node_id)
                    .map(|node| NodeState::join(node_run, node.data.clone()))
            })
            .collect();

        Ok(GraphState {
            uuid: run.uuid,
            data_id: self.data_id,
            graph_id: self.graph_id,
            version: run.version,
            revision: run.revision,
            edges: graph.edges().collect(),
            nodes,
        })
    }

    /// Loads this run, or fails with [`AppError::RunNotFound`].
    async fn run(&self) -> Result<Run, AppError> {
        let mut db = self.db.clone();
        Run::filter_by_data_id_and_graph_id(self.data_id, self.graph_id)
            .order_by(Run::fields().version().desc())
            .first()
            .exec(&mut db)
            .await?
            .ok_or(AppError::RunNotFound {
                data_id: self.data_id,
                graph_id: self.graph_id,
            })
    }

    /// Loads the graph at `version` from the graph store.
    async fn graph(&self, version: u64) -> Result<DAG, AppError> {
        self.graph_store
            .clone()
            .load(self.graph_id, Some(version))
            .await
            .map_err(|error| AppError::Graph(Box::new(error)))
    }
}

#[cfg(test)]
mod tests {
    use runtime::backend::Status;
    use uuid::Uuid;

    use super::*;
    use crate::app::App;
    use crate::app::test::{TestGraphStore, setup};
    use crate::config::Config;

    fn node(id: Uuid) -> Node {
        Node {
            id,
            name: format!("node-{}", id.as_u128()),
            node_type: "script".to_string(),
            settings: Default::default(),
            inputs: Vec::new(),
            outputs: Vec::new(),
        }
    }

    #[tokio::test]
    async fn node_state_embeds_node_run_and_graph_node() {
        let Some(mut db) = setup().await else {
            eprintln!("skipping: set DATABASE_URL to run the MySQL app test");
            return;
        };

        let data_id = Uuid::from_u128(0xda7a);
        let graph_id = Uuid::from_u128(0x9a1);
        let node_id = Uuid::from_u128(1);
        let now = jiff::Timestamp::now();

        let graph = DAG {
            id: graph_id,
            name: "g".to_string(),
            version: 7,
            nodes: vec![node(node_id)],
            edges: Vec::new(),
            node_count: 1,
            ..DAG::default()
        };

        let run = Run::create()
            .uuid(Uuid::new_v4())
            .data_id(data_id)
            .graph_id(graph_id)
            .version(7)
            .revision(0)
            .created_at(now)
            .updated_at(now)
            .exec(&mut db)
            .await
            .expect("create run");

        NodeRun::create()
            .run_id(run.uuid)
            .data_id(data_id)
            .graph_id(graph_id)
            .version(run.version)
            .node_id(node_id)
            .revision(0)
            .status(Status::Running.to_string())
            .created_at(now)
            .updated_at(now)
            .exec(&mut db)
            .await
            .expect("create node run");

        let config = Config {
            addr: "0.0.0.0:0".parse().unwrap(),
            database_url: String::new(),
            ..Config::default()
        };
        let app = App::new(db, config, TestGraphStore(graph));
        let run_app = app.run_with(data_id, graph_id);

        let state = run_app.node_state(node_id).await.expect("node_state");
        assert_eq!(state.data_id, data_id);
        assert_eq!(state.graph_id, graph_id);
        assert_eq!(state.node_id, node_id);
        assert_eq!(state.revision, 0);
        assert_eq!(state.status, Status::Running.to_string());
        assert_eq!(state.name, node(node_id).name);
        assert_eq!(state.node_type, "script");

        let state = run_app.graph_state().await.expect("graph_state");
        assert_eq!(state.data_id, data_id);
        assert_eq!(state.graph_id, graph_id);
        assert_eq!(state.revision, 0);
        assert_eq!(state.nodes.len(), 1);
        assert_eq!(state.nodes[0].node_id, node_id);
        assert_eq!(state.nodes[0].name, node(node_id).name);
    }

    #[tokio::test]
    async fn graph_state_takes_latest_revision_at_or_before_run() {
        let Some(mut db) = setup().await else {
            eprintln!("skipping: set DATABASE_URL to run the MySQL app test");
            return;
        };

        let data_id = Uuid::from_u128(0xda7a);
        let graph_id = Uuid::from_u128(0x9a1);
        let first = Uuid::from_u128(1);
        let second = Uuid::from_u128(2);
        let now = jiff::Timestamp::now();

        let graph = DAG {
            id: graph_id,
            name: "g".to_string(),
            version: 7,
            nodes: vec![node(first), node(second)],
            edges: vec![(first, second)],
            node_count: 2,
            ..DAG::default()
        };

        let run = Run::create()
            .uuid(Uuid::new_v4())
            .data_id(data_id)
            .graph_id(graph_id)
            .version(7)
            .revision(1)
            .created_at(now)
            .updated_at(now)
            .exec(&mut db)
            .await
            .expect("create run");

        // `first` was not rerun, so it still lives at revision 0; a later row is
        // beyond the run's revision and must be ignored.
        for (node_id, revision, status) in [
            (first, 0, Status::Succeeded),
            (second, 1, Status::Ready),
            (first, 2, Status::Failed),
        ] {
            NodeRun::create()
                .run_id(run.uuid)
                .data_id(data_id)
                .graph_id(graph_id)
                .version(run.version)
                .node_id(node_id)
                .revision(revision)
                .status(status.to_string())
                .created_at(now)
                .updated_at(now)
                .exec(&mut db)
                .await
                .expect("create node run");
        }

        let config = Config {
            addr: "0.0.0.0:0".parse().unwrap(),
            database_url: String::new(),
            ..Config::default()
        };
        let app = App::new(db, config, TestGraphStore(graph));
        let run_app = app.run_with(data_id, graph_id);

        let state = run_app.graph_state().await.expect("graph_state");
        assert_eq!(state.uuid, run.uuid);
        assert_eq!(state.version, 7);
        assert_eq!(state.revision, 1);
        assert_eq!(state.edges, vec![(first, second)]);
        assert_eq!(state.nodes.len(), 2);

        let status_of = |node_id: Uuid| {
            state
                .nodes
                .iter()
                .find(|state| state.node_id == node_id)
                .map(|state| (state.revision, state.status.clone()))
        };
        assert_eq!(status_of(first), Some((0, Status::Succeeded.to_string())));
        assert_eq!(status_of(second), Some((1, Status::Ready.to_string())));
    }

    #[tokio::test]
    async fn graph_state_without_run_is_not_found() {
        let Some(db) = setup().await else {
            eprintln!("skipping: set DATABASE_URL to run the MySQL app test");
            return;
        };

        let config = Config {
            addr: "0.0.0.0:0".parse().unwrap(),
            database_url: String::new(),
            ..Config::default()
        };
        let app = App::new(db, config, TestGraphStore::default());
        let run_app = app.run_with(Uuid::from_u128(1), Uuid::from_u128(2));

        let error = run_app.graph_state().await.expect_err("no run");
        assert!(matches!(error, AppError::RunNotFound { .. }));
    }
}
