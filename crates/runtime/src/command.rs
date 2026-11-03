//! [`CommandApply`] implementation driving the node state of a run.

use graph::Graph;
use std::collections::BTreeSet;

use jiff::Timestamp;
use toasty::Executor;
use uuid::Uuid;

use crate::{CommandApply, backend::Status};
use crate::{Error, backend::Backend};
use event::{
    Emit,
    command::{Command, CommandEntry},
    task::Task,
};
use graph::GraphStore;

/// A run of a graph version for a `(data_id, graph_id)`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, toasty::Model)]
#[table = "run"]
#[unique(data_id, graph_id)]
pub struct Run {
    #[key]
    #[auto]
    pub id: u64,
    #[unique]
    pub uuid: Uuid,
    pub data_id: Uuid,
    pub graph_id: Uuid,
    pub version: u64,
    pub revision: u64,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

/// A node's status within a run, one row per revision.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, toasty::Model)]
#[table = "node_run"]
#[unique(run_id, node_id, revision)]
#[index(data_id, graph_id)]
pub struct NodeRun {
    #[key]
    #[auto]
    pub id: u64,
    pub run_id: Uuid,
    pub data_id: Uuid,
    pub graph_id: Uuid,
    pub version: u64,
    pub node_id: Uuid,
    pub revision: u64,
    #[column(type = varchar(16))]
    pub status: String,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    #[belongs_to(key = run_id, references = uuid)]
    #[serde(skip)]
    pub run: toasty::Deferred<Run>,
}

/// Registers command state and canonical event log models.
pub fn models() -> toasty::ModelSet {
    toasty::models!(
        Run,
        NodeRun,
        event::command::CommandEntry,
        event::task::Task,
        event::task_event::TaskEventEntry,
        event::poll::PollOffset,
        coordinator::mysql::LeaderElectionModel
    )
}

impl<S> CommandApply<Backend<S>> for CommandEntry
where
    S: GraphStore<Uuid>,
    <S as GraphStore<Uuid>>::Err: Send + Sync + 'static,
    <S as GraphStore<Uuid>>::G: Graph<NodeId = Uuid>,
{
    type Err = Error;

    async fn apply(&self, mut backend: Backend<S>) -> Result<(), Self::Err> {
        // Clone the handle so the transaction does not borrow `backend`, which is
        // passed mutably to the transition methods below.
        let mut db = backend.db.clone();
        let mut tx = db.transaction().await?;

        backend.apply_in(&mut tx, self).await?;

        tx.commit().await?;
        Ok(())
    }
}

impl<S> Backend<S> {
    /// Applies a command inside a caller-owned transaction. Commit its log
    /// acknowledgement in the same transaction to make recovery atomic.
    pub async fn apply_in(
        &mut self,
        tx: &mut toasty::Transaction<'_>,
        entry: &CommandEntry,
    ) -> Result<(), Error>
    where
        S: GraphStore<Uuid>,
        <S as GraphStore<Uuid>>::Err: Send + Sync + 'static,
        <S as GraphStore<Uuid>>::G: Graph<NodeId = Uuid>,
    {
        match &entry.command.0 {
            Command::Start { nodes } => {
                self.start(&mut *tx, entry.data_id, entry.graph_id, nodes.clone())
                    .await?;
            }
            Command::Rerun { node } => {
                self.rerun(&mut *tx, entry.data_id, entry.graph_id, *node)
                    .await?;
            }
            Command::Dryrun { nodes } => {
                self.dryrun(&mut *tx, entry.data_id, entry.graph_id, nodes.clone())
                    .await?;
            }
            Command::Run { node } => {
                self.run(&mut *tx, entry.data_id, entry.graph_id, *node)
                    .await?;
            }
            Command::Pend { node } => {
                self.pend(&mut *tx, entry.data_id, entry.graph_id, *node)
                    .await?;
            }
            Command::Resume { node } => {
                self.resume(&mut *tx, entry.data_id, entry.graph_id, *node)
                    .await?;
            }
            Command::Succeed { node } => {
                self.succeed(&mut *tx, entry.data_id, entry.graph_id, *node)
                    .await?;
            }
            Command::Fail { node } => {
                self.fail(&mut *tx, entry.data_id, entry.graph_id, *node)
                    .await?;
            }
            Command::Cancel { node } => {
                self.cancel(&mut *tx, entry.data_id, entry.graph_id, *node)
                    .await?;
            }
            Command::Terminate { node } => {
                self.terminate(&mut *tx, entry.data_id, entry.graph_id, *node)
                    .await?;
            }
        };

        Ok(())
    }

    /// Starts `nodes`, marking them `Ready` and adding a task for each.
    ///
    /// The graph is loaded from the backend's graph store. When `nodes` is empty
    /// every root node of the graph is started; otherwise the listed nodes are
    /// started. A fresh `run` and a `Ready` node run per started node are created,
    /// and a task is enqueued for each.
    ///
    /// Starting is a one-time operation per `(data_id, graph_id)`; an existing
    /// run must be rerun instead, even when a newer graph version is published.
    async fn start<E>(
        &mut self,
        tx: &mut E,
        data_id: Uuid,
        graph_id: Uuid,
        nodes: Vec<Uuid>,
    ) -> Result<Uuid, Error>
    where
        S: GraphStore<Uuid>,
        <S as GraphStore<Uuid>>::Err: Send + Sync + 'static,
        <S as GraphStore<Uuid>>::G: Graph<NodeId = Uuid>,
        E: Executor,
    {
        let graph = self
            .graph_store
            .load(graph_id, None)
            .await
            .map_err(|error| Error::Graph(Box::new(error)))?;
        let version = graph.version();

        // An empty node list means "start the graph", which begins at its roots.
        let targets: Vec<Uuid> = if nodes.is_empty() {
            graph.root_nodes().map(|node| node.id).collect()
        } else {
            nodes
        };
        for node_id in &targets {
            if graph.node(*node_id).is_none() {
                return Err(Error::NodeNotFound(*node_id));
            }
        }

        let started = Run::filter_by_data_id_and_graph_id(data_id, graph_id)
            .first()
            .exec(&mut *tx)
            .await?;
        if started.is_some() {
            return Err(Error::AlreadyStarted);
        }

        let now = Timestamp::now();
        let run = Run::create()
            .uuid(Uuid::new_v4())
            .data_id(data_id)
            .graph_id(graph_id)
            .version(version)
            .revision(0)
            .created_at(now)
            .updated_at(now)
            .exec(&mut *tx)
            .await?;
        let run_id = run.uuid;

        for node_id in &targets {
            let node_run = NodeRun::create()
                .run_id(run_id)
                .data_id(data_id)
                .graph_id(graph_id)
                .version(version)
                .node_id(*node_id)
                .revision(0)
                .status(Status::Ready.to_string())
                .created_at(now)
                .updated_at(now)
                .exec(&mut *tx)
                .await?;
            Task::new(data_id, graph_id, *node_id, node_run.id)
                .emit(&mut *tx)
                .await?;
        }

        Ok(run_id)
    }

    /// Re-runs `node`, advancing the revision and marking it `Ready`.
    ///
    /// The run must exist and the node must have a node run in the current
    /// revision. The node run must be in a status that can be rerun (`Ready`,
    /// `Succeeded`, `Failed` or `Terminated`); a node that is still `Running` or
    /// `Pending` is rejected with [`Error::InvalidTransition`].
    ///
    /// The run's revision is advanced and a fresh node run is written `Ready` at
    /// the new revision; the node run of the prior revision is kept, so a rerun
    /// preserves history rather than overwriting it. A fresh task is enqueued
    /// for the new `Ready` node run. Returns the run id.
    async fn rerun<E>(
        &mut self,
        tx: &mut E,
        data_id: Uuid,
        graph_id: Uuid,
        node: Uuid,
    ) -> Result<Uuid, Error>
    where
        E: Executor,
    {
        let run = Run::filter_by_data_id_and_graph_id(data_id, graph_id)
            .order_by(Run::fields().version().desc())
            .first()
            .exec(&mut *tx)
            .await?
            .ok_or(Error::RunNotFound { data_id, graph_id })?;
        let revision = run.revision;

        // The node run must exist and be in a status that can be rerun; a node
        // that is still running or pending cannot be restarted.
        let node_run = NodeRun::filter_by_run_id_and_node_id_and_revision(run.uuid, node, revision)
            .first()
            .exec(&mut *tx)
            .await?
            .ok_or(Error::NodeRunNotFound(node))?;

        let allowed: Vec<String> = [
            Status::Ready,
            Status::Succeeded,
            Status::Failed,
            Status::Terminated,
        ]
        .iter()
        .map(Status::to_string)
        .collect();
        if !allowed.contains(&node_run.status) {
            return Err(Error::InvalidTransition {
                node,
                expected: allowed.join(" or "),
                status: node_run.status,
            });
        }

        // Advance the run revision; a fresh node run is written `Ready` at the new
        // revision. The run is versioned by its revision, so the update only
        // applies when no concurrent transaction has advanced it first.
        let revision = revision + 1;
        let now = Timestamp::now();
        Run::filter_by_id(run.id)
            .filter(Run::fields().revision().eq(revision - 1))
            .update()
            .revision(revision)
            .updated_at(now)
            .exec(&mut *tx)
            .await?;

        let node_run = NodeRun::create()
            .run_id(run.uuid)
            .data_id(data_id)
            .graph_id(graph_id)
            .version(run.version)
            .node_id(node)
            .revision(revision)
            .status(Status::Ready.to_string())
            .created_at(now)
            .updated_at(now)
            .exec(&mut *tx)
            .await?;

        Task::new(data_id, graph_id, node, node_run.id)
            .emit(&mut *tx)
            .await?;

        Ok(run.uuid)
    }

    /// Dry-runs `nodes`, mock-succeeding their predecessors and marking the
    /// selected nodes `Ready`.
    ///
    /// The run must exist; its revision is advanced and every node run below is
    /// written at the new revision. Each target's direct predecessors are mocked
    /// as `Succeeded` so the target is runnable without executing its upstream,
    /// then the target itself is marked `Ready` and its task is enqueued. An empty
    /// node list dry-runs the graph, which begins at its roots. Returns the run
    /// id.
    async fn dryrun<E>(
        &mut self,
        tx: &mut E,
        data_id: Uuid,
        graph_id: Uuid,
        nodes: Vec<Uuid>,
    ) -> Result<Uuid, Error>
    where
        S: GraphStore<Uuid>,
        <S as GraphStore<Uuid>>::Err: Send + Sync + 'static,
        <S as GraphStore<Uuid>>::G: Graph<NodeId = Uuid>,
        E: Executor,
    {
        let graph = self
            .graph_store
            .load(graph_id, None)
            .await
            .map_err(|error| Error::Graph(Box::new(error)))?;

        let run = Run::filter_by_data_id_and_graph_id(data_id, graph_id)
            .order_by(Run::fields().version().desc())
            .first()
            .exec(&mut *tx)
            .await?
            .ok_or(Error::RunNotFound { data_id, graph_id })?;
        let revision = run.revision + 1;

        // An empty node list means "dry-run the graph", which begins at its roots.
        let targets: Vec<Uuid> = if nodes.is_empty() {
            graph.root_nodes().map(|node| node.id).collect()
        } else {
            nodes
        };
        for node_id in &targets {
            if graph.node(*node_id).is_none() {
                return Err(Error::NodeNotFound(*node_id));
            }
        }

        // Advance the run revision; the node runs are written at the new revision.
        let now = Timestamp::now();
        Run::filter_by_id(run.id)
            .filter(Run::fields().revision().eq(revision - 1))
            .update()
            .revision(revision)
            .updated_at(now)
            .exec(&mut *tx)
            .await?;

        // Mock each target's direct predecessors as `Succeeded` so the target is
        // runnable without executing its upstream. A node that is itself a target
        // is written `Ready` below, and a shared predecessor is only written once.
        let mut mocked = BTreeSet::new();
        for node in &targets {
            for predecessor in graph.incoming_edges(*node) {
                if targets.contains(&predecessor) || !mocked.insert(predecessor) {
                    continue;
                }
                NodeRun::create()
                    .run_id(run.uuid)
                    .data_id(data_id)
                    .graph_id(graph_id)
                    .version(run.version)
                    .node_id(predecessor)
                    .revision(revision)
                    .status(Status::Succeeded.to_string())
                    .created_at(now)
                    .updated_at(now)
                    .exec(&mut *tx)
                    .await?;
            }
        }

        for node in &targets {
            let node_run = NodeRun::create()
                .run_id(run.uuid)
                .data_id(data_id)
                .graph_id(graph_id)
                .version(run.version)
                .node_id(*node)
                .revision(revision)
                .status(Status::Ready.to_string())
                .created_at(now)
                .updated_at(now)
                .exec(&mut *tx)
                .await?;
            Task::new(data_id, graph_id, *node, node_run.id)
                .emit(&mut *tx)
                .await?;
        }

        Ok(run.uuid)
    }

    /// Transitions `node`'s node run from one of `allowed` to `next`.
    ///
    /// The run is loaded first; its revision scopes both the node run lookup and the
    /// update, so the transition only touches the node run of the current run
    /// revision. Returns the run that was transitioned.
    async fn transition<E>(
        &mut self,
        tx: &mut E,
        data_id: Uuid,
        graph_id: Uuid,
        node: Uuid,
        allowed: &[Status],
        next: Status,
    ) -> Result<Run, Error>
    where
        E: Executor,
    {
        let run = Run::filter_by_data_id_and_graph_id(data_id, graph_id)
            .order_by(Run::fields().version().desc())
            .first()
            .exec(&mut *tx)
            .await?
            .ok_or(Error::RunNotFound { data_id, graph_id })?;

        let node_run =
            NodeRun::filter_by_run_id_and_node_id_and_revision(run.uuid, node, run.revision)
                .first()
                .exec(&mut *tx)
                .await?
                .ok_or(Error::NodeRunNotFound(node))?;

        let allowed: Vec<String> = allowed.iter().map(Status::to_string).collect();
        if !allowed.contains(&node_run.status) {
            return Err(Error::InvalidTransition {
                node,
                expected: allowed.join(" or "),
                status: node_run.status,
            });
        }

        // Guard the update on the node run's revision and status so a concurrent
        // change cannot be lost.
        let status = node_run.status.clone();
        NodeRun::filter_by_id(node_run.id)
            .filter(NodeRun::fields().revision().eq(run.revision))
            .filter(NodeRun::fields().status().eq(status.clone()))
            .update()
            .status(next.to_string())
            .updated_at(Timestamp::now())
            .exec(&mut *tx)
            .await?;

        // The guarded update matches nothing when the node run changed under us;
        // re-read to detect it.
        let current = NodeRun::filter_by_id(node_run.id)
            .first()
            .exec(&mut *tx)
            .await?;
        if current.is_none_or(|row| row.status != next.to_string()) {
            return Err(Error::ConcurrentUpdate(node));
        }

        Ok(run)
    }

    /// Marks `node` `Running`.
    ///
    /// The node run must currently be `Ready`; any other status is rejected with
    /// [`Error::InvalidTransition`]. Returns the node run's revision without
    /// adding another task.
    async fn run<E>(
        &mut self,
        tx: &mut E,
        data_id: Uuid,
        graph_id: Uuid,
        node: Uuid,
    ) -> Result<u64, Error>
    where
        E: Executor,
    {
        let run = self
            .transition(
                &mut *tx,
                data_id,
                graph_id,
                node,
                &[Status::Ready],
                Status::Running,
            )
            .await?;
        Ok(run.revision)
    }

    /// Marks `node` `Pending`.
    ///
    /// The node run must currently be `Ready` or `Running`; any other status is
    /// rejected with [`Error::InvalidTransition`]. Returns the node run's revision.
    async fn pend<E>(
        &mut self,
        tx: &mut E,
        data_id: Uuid,
        graph_id: Uuid,
        node: Uuid,
    ) -> Result<u64, Error>
    where
        E: Executor,
    {
        let run = self
            .transition(
                &mut *tx,
                data_id,
                graph_id,
                node,
                &[Status::Ready, Status::Running],
                Status::Pending,
            )
            .await?;
        Ok(run.revision)
    }

    /// Resumes `node` by returning it to `Ready` and adding a fresh task.
    async fn resume<E>(
        &mut self,
        tx: &mut E,
        data_id: Uuid,
        graph_id: Uuid,
        node: Uuid,
    ) -> Result<u64, Error>
    where
        E: Executor,
    {
        let run = self
            .transition(
                &mut *tx,
                data_id,
                graph_id,
                node,
                &[Status::Pending],
                Status::Ready,
            )
            .await?;
        let node_run =
            NodeRun::filter_by_run_id_and_node_id_and_revision(run.uuid, node, run.revision)
                .get(&mut *tx)
                .await?;
        Task::new(data_id, graph_id, node, node_run.id)
            .emit(&mut *tx)
            .await?;
        Ok(run.revision)
    }

    /// Marks `node` `Succeeded`.
    ///
    /// The node run must currently be `Pending` or `Running`; any other status is
    /// rejected with [`Error::InvalidTransition`]. Once the node is succeeded, every
    /// successor whose predecessors have all succeeded is marked `Ready` and
    /// given a task, unless already `Ready`. Returns the node run's revision.
    async fn succeed<E>(
        &mut self,
        tx: &mut E,
        data_id: Uuid,
        graph_id: Uuid,
        node: Uuid,
    ) -> Result<u64, Error>
    where
        S: GraphStore<Uuid>,
        <S as GraphStore<Uuid>>::Err: Send + Sync + 'static,
        <S as GraphStore<Uuid>>::G: Graph<NodeId = Uuid>,
        E: Executor,
    {
        let run = self
            .transition(
                &mut *tx,
                data_id,
                graph_id,
                node,
                &[Status::Pending, Status::Running],
                Status::Succeeded,
            )
            .await?;

        // A successor becomes `Ready` once every one of its predecessors has
        // succeeded. Load the graph at the run's version so the successor set
        // matches the version the run was started with.
        let graph = self
            .graph_store
            .load(graph_id, Some(run.version))
            .await
            .map_err(|error| Error::Graph(Box::new(error)))?;
        let succeeded = Status::Succeeded.to_string();
        for successor in graph.successors(node).map(|node| node.id) {
            let mut ready = true;
            for predecessor in graph.incoming_edges(successor) {
                // A node may have a node run per revision, so take the latest one
                // at or before the current revision; that is its live status.
                let status = NodeRun::filter_by_run_id(run.uuid)
                    .filter(NodeRun::fields().node_id().eq(predecessor))
                    .filter(NodeRun::fields().revision().le(run.revision))
                    .order_by(NodeRun::fields().revision().desc())
                    .first()
                    .exec(&mut *tx)
                    .await?
                    .map(|row| row.status);
                if status.as_deref() != Some(succeeded.as_str()) {
                    ready = false;
                    break;
                }
            }
            if !ready {
                continue;
            }

            // The successor may already have a node run at this revision (for
            // example, a target prepared by a dry run), so update it in place
            // when it exists and insert it otherwise.
            let now = Timestamp::now();
            let successor_run_id = match NodeRun::filter_by_run_id_and_node_id_and_revision(
                run.uuid,
                successor,
                run.revision,
            )
            .first()
            .exec(&mut *tx)
            .await?
            {
                Some(row) if row.status == Status::Ready.to_string() => continue,
                Some(row) => {
                    NodeRun::filter_by_id(row.id)
                        .update()
                        .status(Status::Ready.to_string())
                        .updated_at(now)
                        .exec(&mut *tx)
                        .await?;
                    row.id
                }
                None => {
                    NodeRun::create()
                        .run_id(run.uuid)
                        .data_id(data_id)
                        .graph_id(graph_id)
                        .version(run.version)
                        .node_id(successor)
                        .revision(run.revision)
                        .status(Status::Ready.to_string())
                        .created_at(now)
                        .updated_at(now)
                        .exec(&mut *tx)
                        .await?
                        .id
                }
            };
            Task::new(data_id, graph_id, successor, successor_run_id)
                .emit(&mut *tx)
                .await?;
        }

        Ok(run.revision)
    }

    /// Marks `node` `Failed`.
    ///
    /// The node run must currently be `Pending` or `Running`; any other status is
    /// rejected with [`Error::InvalidTransition`]. Returns the node run's revision.
    async fn fail<E>(
        &mut self,
        tx: &mut E,
        data_id: Uuid,
        graph_id: Uuid,
        node: Uuid,
    ) -> Result<u64, Error>
    where
        E: Executor,
    {
        let run = self
            .transition(
                &mut *tx,
                data_id,
                graph_id,
                node,
                &[Status::Pending, Status::Running],
                Status::Failed,
            )
            .await?;
        Ok(run.revision)
    }

    /// Marks `node` `Terminated`.
    ///
    /// The node run must currently be `Pending` or `Running`; any other status is
    /// rejected with [`Error::InvalidTransition`]. Returns the node run's revision.
    async fn cancel<E>(
        &mut self,
        tx: &mut E,
        data_id: Uuid,
        graph_id: Uuid,
        node: Uuid,
    ) -> Result<u64, Error>
    where
        E: Executor,
    {
        let run = self
            .transition(
                &mut *tx,
                data_id,
                graph_id,
                node,
                &[Status::Pending, Status::Running],
                Status::Terminated,
            )
            .await?;
        Ok(run.revision)
    }

    /// Marks `node` `Terminated`.
    ///
    /// The node run must currently be `Pending` or `Running`; any other status is
    /// rejected with [`Error::InvalidTransition`]. Returns the node run's revision.
    async fn terminate<E>(
        &mut self,
        tx: &mut E,
        data_id: Uuid,
        graph_id: Uuid,
        node: Uuid,
    ) -> Result<u64, Error>
    where
        E: Executor,
    {
        let run = self
            .transition(
                &mut *tx,
                data_id,
                graph_id,
                node,
                &[Status::Pending, Status::Running],
                Status::Terminated,
            )
            .await?;
        Ok(run.revision)
    }
}
