use std::time::Duration;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use event::{
    Emit, Poll,
    command::{Command, CommandEntry},
    task::Task,
};
use graph::types::{DAG, Node, NodeSettings, Resource};
use runtime::{
    CommandApply,
    backend::{Backend, Status},
    command::{CommandResult, NodeRun, Run},
};
use scheduler::{Actuator, Job, TaskEvent, TaskEventEntry, actuator::NodeConfig};
use serde_json::json;
use server::{App, Config};
use toasty::Db;
use tower::ServiceExt;
use uuid::Uuid;

async fn setup() -> Option<(App<Db>, Db)> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping upgrade integration tests: DATABASE_URL is not set");
        return None;
    };
    let mut builder = Db::builder();
    builder.models(server::models());
    builder.table_name_prefix(&format!("upgrade_{}_", Uuid::new_v4().simple()));
    let db = builder.connect(&url).await.unwrap();
    db.push_schema().await.unwrap();
    Some((App::new(db.clone(), Config::default(), db.clone()), db))
}

fn graph() -> DAG {
    DAG {
        id: Uuid::new_v4(),
        name: "upgrade".into(),
        nodes: (1..=6)
            .map(|id| Node {
                id: Uuid::from_u128(id),
                name: format!("node-{id}"),
                node_type: "docker".into(),
                settings: NodeSettings {
                    config: json!({"image": "original"}),
                    resource: Resource {
                        cpu_cores: 1,
                        memory_bytes: 1024,
                    },
                    timeout: Duration::from_secs(30),
                },
                inputs: vec![],
                outputs: vec![],
            })
            .collect(),
        edges: [(1, 2), (2, 3), (2, 4), (3, 5), (4, 5), (5, 6)]
            .into_iter()
            .map(|(source, target)| (Uuid::from_u128(source), Uuid::from_u128(target)))
            .collect(),
        node_count: 6,
        ..DAG::default()
    }
}

async fn publish(app: &App<Db>, graph: &DAG) -> DAG {
    app.write_graph_draft(graph.id, graph.clone(), "test")
        .await
        .unwrap();
    app.publish_graph_draft(graph.id, "test").await.unwrap()
}

async fn apply(db: &Db, data_id: Uuid, graph_id: Uuid, command: Command) -> CommandEntry {
    let entry = CommandEntry::new(data_id, graph_id, command);
    entry
        .apply(Backend::new(db.clone(), db.clone()))
        .await
        .unwrap();
    entry
}

async fn completed_run(db: &mut Db, graph: &DAG) -> Run {
    let data_id = Uuid::new_v4();
    apply(db, data_id, graph.id, Command::Start { nodes: vec![] }).await;
    for node in &graph.nodes {
        apply(db, data_id, graph.id, Command::Run { node: node.id }).await;
        apply(db, data_id, graph.id, Command::Succeed { node: node.id }).await;
    }
    assert!(
        CommandResult::all()
            .exec(&mut *db)
            .await
            .unwrap()
            .is_empty()
    );
    let tasks: Vec<_> = Task::poll(db, 100).await.unwrap().collect();
    for task in tasks {
        task.ack(db).await.unwrap();
    }
    Run::filter_by_data_id_and_graph_id(data_id, graph.id)
        .get(db)
        .await
        .unwrap()
}

#[tokio::test]
async fn rerun_invalidates_all_descendant_revisions_without_affecting_other_runs() {
    let Some((app, mut db)) = setup().await else {
        return;
    };
    let previous = publish(&app, &graph()).await;
    let run = completed_run(&mut db, &previous).await;
    completed_run(&mut db, &previous).await;
    apply(
        &db,
        run.data_id,
        run.graph_id,
        Command::Rerun {
            node: Uuid::from_u128(6),
        },
    )
    .await;
    for (node, status) in [(5, "Pending"), (6, "Running")] {
        NodeRun::filter_by_run_id(run.uuid)
            .filter(NodeRun::fields().node_id().eq(Uuid::from_u128(node)))
            .update()
            .status(status)
            .version(99)
            .exec(&mut db)
            .await
            .unwrap();
    }
    let history = NodeRun::all().exec(&mut db).await.unwrap();
    let task_count = Task::all().exec(&mut db).await.unwrap().len();
    let mut changed = previous.clone();
    changed
        .edges
        .retain(|&(source, _)| source != Uuid::from_u128(3));
    publish(&app, &changed).await;

    let entry = CommandEntry::new(
        run.data_id,
        run.graph_id,
        Command::Rerun {
            node: Uuid::from_u128(3),
        },
    );
    let mut backend = Backend::new(db.clone(), db.clone());
    let mut tx = db.transaction().await.unwrap();
    backend.apply_in(&mut tx, &entry).await.unwrap();
    assert_eq!(
        NodeRun::filter_by_run_id(run.uuid)
            .filter(NodeRun::fields().ineffective().eq(1))
            .exec(&mut tx)
            .await
            .unwrap()
            .len(),
        4
    );
    tx.rollback().await.unwrap();
    for original in &history {
        let unchanged = NodeRun::filter_by_id(original.id)
            .get(&mut db)
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(unchanged).unwrap(),
            serde_json::to_value(original).unwrap()
        );
    }
    entry.apply(backend).await.unwrap();

    for mut original in history {
        let current = NodeRun::filter_by_id(original.id)
            .get(&mut db)
            .await
            .unwrap();
        if original.run_id == run.uuid && [3, 5, 6].map(Uuid::from_u128).contains(&original.node_id)
        {
            assert_eq!(current.ineffective, 1);
            assert!(current.updated_at >= original.updated_at);
            original.ineffective = 1;
            original.updated_at = current.updated_at;
        }
        assert_eq!(
            serde_json::to_value(current).unwrap(),
            serde_json::to_value(original).unwrap()
        );
    }
    let selected =
        NodeRun::filter_by_run_id_and_node_id_and_revision(run.uuid, Uuid::from_u128(3), 2)
            .get(&mut db)
            .await
            .unwrap();
    assert_eq!(selected.ineffective, 0);
    assert_eq!(selected.version, previous.version);
    assert_eq!(selected.status, "Ready");
    assert_eq!(
        Task::all().exec(&mut db).await.unwrap().len(),
        task_count + 1
    );
    let view = app.inspect_run(run.uuid).await.unwrap();
    assert_eq!(view.state.nodes.len(), 4);
    for node in view.state.nodes {
        assert_eq!(node.ineffective, 0);
        assert!([1, 2, 3, 4].map(Uuid::from_u128).contains(&node.node_id));
    }
    assert!(CommandResult::all().exec(&mut db).await.unwrap().is_empty());
}

#[tokio::test]
async fn ineffective_successes_do_not_unblock_a_join_until_the_branch_runs_again() {
    let Some((app, mut db)) = setup().await else {
        return;
    };
    let mut definition = graph();
    let mut branch = definition.nodes[3].clone();
    branch.id = Uuid::from_u128(7);
    branch.name = "node-7".into();
    definition.nodes.insert(4, branch);
    definition.node_count = definition.nodes.len();
    definition
        .edges
        .retain(|&edge| edge != (Uuid::from_u128(4), Uuid::from_u128(5)));
    definition.edges.extend([
        (Uuid::from_u128(4), Uuid::from_u128(7)),
        (Uuid::from_u128(7), Uuid::from_u128(5)),
    ]);
    let published = publish(&app, &definition).await;
    let run = completed_run(&mut db, &published).await;
    apply(
        &db,
        run.data_id,
        run.graph_id,
        Command::Rerun {
            node: Uuid::from_u128(2),
        },
    )
    .await;
    for node in [2, 3, 4] {
        apply(
            &db,
            run.data_id,
            run.graph_id,
            Command::Run {
                node: Uuid::from_u128(node),
            },
        )
        .await;
        apply(
            &db,
            run.data_id,
            run.graph_id,
            Command::Succeed {
                node: Uuid::from_u128(node),
            },
        )
        .await;
        assert!(
            NodeRun::filter_by_run_id_and_node_id_and_revision(run.uuid, Uuid::from_u128(5), 1)
                .first()
                .exec(&mut db)
                .await
                .unwrap()
                .is_none()
        );
    }
    for node in [7, 5] {
        let ready =
            NodeRun::filter_by_run_id_and_node_id_and_revision(run.uuid, Uuid::from_u128(node), 1)
                .get(&mut db)
                .await
                .unwrap();
        assert_eq!(ready.ineffective, 0);
        assert_eq!(ready.status, "Ready");
        apply(
            &db,
            run.data_id,
            run.graph_id,
            Command::Run {
                node: Uuid::from_u128(node),
            },
        )
        .await;
        apply(
            &db,
            run.data_id,
            run.graph_id,
            Command::Succeed {
                node: Uuid::from_u128(node),
            },
        )
        .await;
    }
    let next = NodeRun::filter_by_run_id_and_node_id_and_revision(run.uuid, Uuid::from_u128(6), 1)
        .get(&mut db)
        .await
        .unwrap();
    assert_eq!(next.ineffective, 0);
    assert_eq!(next.status, "Ready");
    assert_eq!(
        NodeRun::filter_by_run_id(run.uuid)
            .filter(NodeRun::fields().ineffective().eq(1))
            .exec(&mut db)
            .await
            .unwrap()
            .len(),
        6
    );
    assert!(CommandResult::all().exec(&mut db).await.unwrap().is_empty());
}

#[tokio::test]
async fn upgrade_invalidates_descendants_from_the_target_graph() {
    let Some((app, mut db)) = setup().await else {
        return;
    };
    let previous = publish(&app, &graph()).await;
    let run = completed_run(&mut db, &previous).await;
    let mut changed = previous.clone();
    changed
        .edges
        .retain(|&edge| edge != (Uuid::from_u128(2), Uuid::from_u128(4)));
    changed.edges.push((Uuid::from_u128(3), Uuid::from_u128(4)));
    let published = publish(&app, &changed).await;
    apply(
        &db,
        run.data_id,
        run.graph_id,
        Command::Upgrade {
            version: published.version,
            node: Uuid::from_u128(3),
        },
    )
    .await;
    let history = NodeRun::filter_by_run_id(run.uuid)
        .filter(NodeRun::fields().revision().eq(0))
        .exec(&mut db)
        .await
        .unwrap();
    for node in history {
        assert_eq!(
            node.ineffective,
            u32::from([3, 4, 5, 6].map(Uuid::from_u128).contains(&node.node_id))
        );
        assert_eq!(node.status, "Succeeded");
    }
    let selected =
        NodeRun::filter_by_run_id_and_node_id_and_revision(run.uuid, Uuid::from_u128(3), 1)
            .get(&mut db)
            .await
            .unwrap();
    assert_eq!(selected.ineffective, 0);
    assert_eq!(selected.version, published.version);
    assert!(CommandResult::all().exec(&mut db).await.unwrap().is_empty());
}

#[tokio::test]
async fn upgrade_reruns_only_the_selected_shared_node_and_schedules_the_requested_version() {
    let Some((app, mut db)) = setup().await else {
        return;
    };
    let previous = publish(&app, &graph()).await;
    let started = completed_run(&mut db, &previous).await;
    for command in [
        Command::Rerun {
            node: Uuid::from_u128(6),
        },
        Command::Run {
            node: Uuid::from_u128(6),
        },
        Command::Succeed {
            node: Uuid::from_u128(6),
        },
    ] {
        apply(&db, started.data_id, started.graph_id, command).await;
    }
    let run = Run::filter_by_uuid(started.uuid)
        .get(&mut db)
        .await
        .unwrap();
    assert_eq!(run.revision, 1);
    let history = NodeRun::all().exec(&mut db).await.unwrap();
    let old_tasks = Task::all().exec(&mut db).await.unwrap();
    let tasks: Vec<_> = Task::poll(&mut db, 100).await.unwrap().collect();
    for task in tasks {
        task.ack(&mut db).await.unwrap();
    }
    let mut changed = previous.clone();
    changed.nodes[4].settings.config = json!({"image": "upgraded"});
    let target = publish(&app, &changed).await;
    changed.nodes[0].settings.config = json!({"image": "future"});
    publish(&app, &changed).await;
    let entry = CommandEntry::new(
        run.data_id,
        run.graph_id,
        Command::Upgrade {
            version: target.version,
            node: Uuid::from_u128(3),
        },
    );
    let mut backend = Backend::new(db.clone(), db.clone());
    let mut tx = db.transaction().await.unwrap();
    assert_eq!(
        backend.apply_in(&mut tx, &entry).await.unwrap(),
        Some(run.uuid)
    );
    tx.commit().await.unwrap();

    let view = app.inspect_run(run.uuid).await.unwrap();
    assert_eq!(view.state.uuid, run.uuid);
    assert_eq!(view.state.data_id, run.data_id);
    assert_eq!(view.state.version, 2);
    assert_eq!(view.state.revision, run.revision + 1);
    assert_eq!(view.graph, target);
    assert_eq!(view.state.nodes.len(), 4);
    for (id, status) in [
        (1, "Succeeded"),
        (2, "Succeeded"),
        (3, "Ready"),
        (4, "Succeeded"),
    ] {
        let node = view
            .state
            .nodes
            .iter()
            .find(|node| node.node_id == Uuid::from_u128(id))
            .unwrap();
        assert_eq!(node.status, status);
        assert_eq!(node.ineffective, 0);
        let revision = if id == 3 {
            view.state.revision
        } else {
            history
                .iter()
                .filter(|row| row.node_id == node.node_id)
                .map(|row| row.revision)
                .max()
                .unwrap()
        };
        assert_eq!(node.revision, revision);
    }
    assert_eq!(
        NodeRun::all().exec(&mut db).await.unwrap().len(),
        history.len() + 1
    );
    for mut original in history {
        let preserved = NodeRun::filter_by_id(original.id)
            .get(&mut db)
            .await
            .unwrap();
        if [3, 5, 6].map(Uuid::from_u128).contains(&original.node_id) {
            assert_eq!(preserved.ineffective, 1);
            assert!(preserved.updated_at >= original.updated_at);
            original.ineffective = 1;
            original.updated_at = preserved.updated_at;
        }
        assert_eq!(
            serde_json::to_value(original).unwrap(),
            serde_json::to_value(preserved).unwrap()
        );
    }
    assert_eq!(
        Task::all().exec(&mut db).await.unwrap().len(),
        old_tasks.len() + 1
    );
    let mut actuator = Actuator::new(backend, 100);
    actuator.tick().await.unwrap();
    let jobs = Job::all().exec(&mut db).await.unwrap();
    assert_eq!(jobs.len(), 1);
    for job in &jobs {
        let config = NodeConfig::filter_by_id(job.node_config_id)
            .get(&mut db)
            .await
            .unwrap();
        assert_eq!(config.version, target.version);
    }
    assert_eq!(
        app.run_with(run.data_id, run.graph_id)
            .node_state(Uuid::from_u128(5))
            .await
            .unwrap()
            .status,
        "Succeeded"
    );
    apply(
        &db,
        run.data_id,
        run.graph_id,
        Command::Succeed {
            node: Uuid::from_u128(3),
        },
    )
    .await;
    actuator.tick().await.unwrap();
    let join = NodeRun::filter_by_run_id_and_node_id_and_revision(
        run.uuid,
        Uuid::from_u128(5),
        run.revision + 1,
    )
    .get(&mut db)
    .await
    .unwrap();
    assert_eq!(join.version, target.version);
    assert_eq!(join.status, "Running");
    assert_eq!(join.ineffective, 0);
    let join_task = Task::all()
        .exec(&mut db)
        .await
        .unwrap()
        .into_iter()
        .find(|task| task.node_run_id == join.id)
        .unwrap();
    let join_job = Job::filter_by_task_id(join_task.id)
        .get(&mut db)
        .await
        .unwrap();
    let config = NodeConfig::filter_by_id(join_job.node_config_id)
        .get(&mut db)
        .await
        .unwrap();
    assert_eq!(config.version, target.version);
    assert_eq!(config.config, json!({"image": "upgraded"}));

    TaskEventEntry::new(&join_task, TaskEvent::Succeed)
        .emit(&mut db)
        .await
        .unwrap();
    actuator.tick().await.unwrap();
    let successor = NodeRun::filter_by_run_id_and_node_id_and_revision(
        run.uuid,
        Uuid::from_u128(6),
        run.revision + 1,
    )
    .get(&mut db)
    .await
    .unwrap();
    assert_eq!(successor.version, target.version);
    assert_eq!(successor.status, "Ready");
    assert_eq!(successor.ineffective, 0);
    let view = app.inspect_run(run.uuid).await.unwrap();
    assert_eq!(view.state.nodes.len(), 6);
    assert!(view.state.nodes.iter().all(|node| node.ineffective == 0));
    assert!(CommandResult::all().exec(&mut db).await.unwrap().is_empty());
}

#[tokio::test]
async fn upgrade_uses_rerun_status_rules_without_requiring_other_nodes_to_finish() {
    for (selected_status, other_status) in [
        (Status::Ready, Status::Running),
        (Status::Succeeded, Status::Pending),
        (Status::Failed, Status::Ready),
        (Status::Terminated, Status::Failed),
    ] {
        let Some((app, mut db)) = setup().await else {
            return;
        };
        let previous = publish(&app, &graph()).await;
        let run = completed_run(&mut db, &previous).await;
        let mut changed = previous.clone();
        changed.nodes[4].name = "changed".into();
        publish(&app, &changed).await;
        for (node, status) in [(3, selected_status), (2, other_status)] {
            NodeRun::filter_by_run_id_and_node_id_and_revision(run.uuid, Uuid::from_u128(node), 0)
                .update()
                .status(status.to_string())
                .exec(&mut db)
                .await
                .unwrap();
        }
        let history = NodeRun::all().exec(&mut db).await.unwrap();
        apply(
            &db,
            run.data_id,
            run.graph_id,
            Command::Upgrade {
                version: 2,
                node: Uuid::from_u128(3),
            },
        )
        .await;
        let updated = Run::filter_by_uuid(run.uuid).get(&mut db).await.unwrap();
        assert_eq!(updated.version, 2);
        assert_eq!(updated.revision, 1);
        let selected =
            NodeRun::filter_by_run_id_and_node_id_and_revision(run.uuid, Uuid::from_u128(3), 1)
                .get(&mut db)
                .await
                .unwrap();
        assert_eq!(selected.status, "Ready");
        assert_eq!(selected.version, 2);
        assert_eq!(
            NodeRun::all().exec(&mut db).await.unwrap().len(),
            history.len() + 1
        );
        assert_eq!(
            Task::all().exec(&mut db).await.unwrap().len(),
            previous.nodes.len() + 1
        );
        for mut original in history {
            let preserved = NodeRun::filter_by_id(original.id)
                .get(&mut db)
                .await
                .unwrap();
            if [3, 5, 6].map(Uuid::from_u128).contains(&original.node_id) {
                assert_eq!(preserved.ineffective, 1);
                assert!(preserved.updated_at >= original.updated_at);
                original.ineffective = 1;
                original.updated_at = preserved.updated_at;
            }
            assert_eq!(
                serde_json::to_value(preserved).unwrap(),
                serde_json::to_value(original).unwrap()
            );
        }
        assert!(CommandResult::all().exec(&mut db).await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn node_run_version_does_not_gate_tasks_events_successors_or_reruns() {
    let Some((app, mut db)) = setup().await else {
        return;
    };
    let previous = publish(&app, &graph()).await;
    let run = completed_run(&mut db, &previous).await;
    let mut changed = previous.clone();
    changed.nodes[4].name = "changed".into();
    publish(&app, &changed).await;
    apply(
        &db,
        run.data_id,
        run.graph_id,
        Command::Upgrade {
            version: 2,
            node: Uuid::from_u128(3),
        },
    )
    .await;
    let selected =
        NodeRun::filter_by_run_id_and_node_id_and_revision(run.uuid, Uuid::from_u128(3), 1)
            .get(&mut db)
            .await
            .unwrap();
    NodeRun::filter_by_id(selected.id)
        .update()
        .version(1)
        .exec(&mut db)
        .await
        .unwrap();
    let mut actuator = Actuator::new(Backend::new(db.clone(), db.clone()), 100);
    assert_eq!(actuator.tick().await.unwrap().tasks, 1);
    let job = Job::all().exec(&mut db).await.unwrap().remove(0);
    let config = NodeConfig::filter_by_id(job.node_config_id)
        .get(&mut db)
        .await
        .unwrap();
    assert_eq!(config.version, 2);
    assert_eq!(
        app.run_with(run.data_id, run.graph_id)
            .node_state(selected.node_id)
            .await
            .unwrap()
            .status,
        "Running"
    );
    let task = Task::all()
        .exec(&mut db)
        .await
        .unwrap()
        .into_iter()
        .find(|task| task.node_run_id == selected.id)
        .unwrap();
    TaskEventEntry::new(&task, TaskEvent::Succeed)
        .emit(&mut db)
        .await
        .unwrap();
    assert_eq!(actuator.tick().await.unwrap().commands, 1);
    let succeeded = NodeRun::filter_by_id(selected.id)
        .get(&mut db)
        .await
        .unwrap();
    assert_eq!(succeeded.status, "Succeeded");
    assert_eq!(succeeded.version, 1);
    let successor =
        NodeRun::filter_by_run_id_and_node_id_and_revision(run.uuid, Uuid::from_u128(5), 1)
            .get(&mut db)
            .await
            .unwrap();
    assert_eq!(successor.status, "Ready");
    assert_eq!(successor.version, 2);
    apply(
        &db,
        run.data_id,
        run.graph_id,
        Command::Rerun {
            node: Uuid::from_u128(4),
        },
    )
    .await;
    let rerun = NodeRun::filter_by_run_id_and_node_id_and_revision(run.uuid, Uuid::from_u128(4), 2)
        .get(&mut db)
        .await
        .unwrap();
    assert_eq!(rerun.status, "Ready");
    assert_eq!(rerun.version, 2);
    let earlier =
        NodeRun::filter_by_run_id_and_node_id_and_revision(run.uuid, Uuid::from_u128(4), 0)
            .get(&mut db)
            .await
            .unwrap();
    assert_eq!(earlier.version, 1);
    assert_eq!(earlier.status, "Succeeded");
    assert!(CommandResult::all().exec(&mut db).await.unwrap().is_empty());
}

#[tokio::test]
async fn upgrade_rejects_invalid_versions_and_active_selected_nodes_without_changing_state() {
    let Some((app, mut db)) = setup().await else {
        return;
    };
    let previous = publish(&app, &graph()).await;
    let run = completed_run(&mut db, &previous).await;
    let mut changed = previous.clone();
    changed.nodes[4].name = "changed".into();
    publish(&app, &changed).await;
    let rows = NodeRun::all().exec(&mut db).await.unwrap();
    let task_count = Task::all().exec(&mut db).await.unwrap().len();
    for (version, code) in [
        (0, "RAYFLOW_INVALID_UPGRADE_VERSION"),
        (1, "RAYFLOW_INVALID_UPGRADE_VERSION"),
        (99, "RAYFLOW_GRAPH_ERROR"),
    ] {
        let entry = apply(
            &db,
            run.data_id,
            run.graph_id,
            Command::Upgrade {
                version,
                node: Uuid::from_u128(3),
            },
        )
        .await;
        let failure = CommandResult::filter_by_command_id(entry.id)
            .get(&mut db)
            .await
            .unwrap();
        assert_eq!(failure.error_code, code);
    }
    let selected = rows
        .iter()
        .find(|row| row.node_id == Uuid::from_u128(3))
        .unwrap();
    for status in [Status::Running, Status::Pending] {
        NodeRun::filter_by_id(selected.id)
            .update()
            .status(status.to_string())
            .exec(&mut db)
            .await
            .unwrap();
        let entry = apply(
            &db,
            run.data_id,
            run.graph_id,
            Command::Upgrade {
                version: 2,
                node: Uuid::from_u128(3),
            },
        )
        .await;
        let failure = CommandResult::filter_by_command_id(entry.id)
            .get(&mut db)
            .await
            .unwrap();
        assert_eq!(failure.error_code, "RAYFLOW_INVALID_TRANSITION");
        assert!(failure.error_message.contains(&status.to_string()));
    }
    let current = Run::filter_by_uuid(run.uuid).get(&mut db).await.unwrap();
    assert_eq!(
        serde_json::to_value(current).unwrap(),
        serde_json::to_value(run).unwrap()
    );
    assert_eq!(
        NodeRun::all().exec(&mut db).await.unwrap().len(),
        rows.len()
    );
    assert_eq!(Task::all().exec(&mut db).await.unwrap().len(), task_count);
}

#[tokio::test]
async fn upgrade_rejects_nodes_outside_the_shared_set_without_changing_execution() {
    let Some((app, mut db)) = setup().await else {
        return;
    };
    let previous = publish(&app, &graph()).await;
    let run = completed_run(&mut db, &previous).await;
    let mut changed = previous.clone();
    changed.nodes[4].name = "changed".into();
    publish(&app, &changed).await;
    let history = NodeRun::all().exec(&mut db).await.unwrap();
    let task_count = Task::all().exec(&mut db).await.unwrap().len();
    for node in [5, 6, 999].map(Uuid::from_u128) {
        let entry = apply(
            &db,
            run.data_id,
            run.graph_id,
            Command::Upgrade { version: 2, node },
        )
        .await;
        let failure = CommandResult::filter_by_command_id(entry.id)
            .get(&mut db)
            .await
            .unwrap();
        assert_eq!(failure.error_code, "RAYFLOW_INVALID_UPGRADE_NODE");
        assert!(failure.error_message.contains(&node.to_string()));
        let current = Run::filter_by_uuid(run.uuid).get(&mut db).await.unwrap();
        assert_eq!(
            serde_json::to_value(&current).unwrap(),
            serde_json::to_value(&run).unwrap()
        );
    }
    assert_eq!(Task::all().exec(&mut db).await.unwrap().len(), task_count);
    assert_eq!(
        NodeRun::all().exec(&mut db).await.unwrap().len(),
        history.len()
    );
    for original in history {
        let preserved = NodeRun::filter_by_id(original.id)
            .get(&mut db)
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(preserved).unwrap(),
            serde_json::to_value(original).unwrap()
        );
    }
}

#[tokio::test]
async fn upgrade_accepts_the_root_and_indirect_ancestors_as_shared_nodes() {
    for selected in [1, 2].map(Uuid::from_u128) {
        let Some((app, mut db)) = setup().await else {
            return;
        };
        let previous = publish(&app, &graph()).await;
        let run = completed_run(&mut db, &previous).await;
        let mut changed = previous.clone();
        changed.nodes[4].name = "changed join".into();
        publish(&app, &changed).await;
        let response = server::router(app.clone())
            .oneshot(
                Request::post("/runs/upgrade")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&json!({
                        "data_id": run.data_id, "graph_id": run.graph_id, "version": 2, "node": selected,
                    })).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let mut actuator = Actuator::new(Backend::new(db.clone(), db.clone()), 100);
        actuator.tick().await.unwrap();
        assert!(CommandResult::all().exec(&mut db).await.unwrap().is_empty());
        let current = Run::filter_by_uuid(run.uuid).get(&mut db).await.unwrap();
        assert_eq!(current.version, 2);
        assert_eq!(current.revision, 1);
        let active = NodeRun::filter_by_run_id_and_node_id_and_revision(run.uuid, selected, 1)
            .get(&mut db)
            .await
            .unwrap();
        assert_eq!(active.status, "Running");
        assert_eq!(active.ineffective, 0);
        let history = NodeRun::filter_by_run_id(run.uuid)
            .filter(NodeRun::fields().revision().eq(0))
            .exec(&mut db)
            .await
            .unwrap();
        for original in history {
            assert_eq!(original.status, "Succeeded");
            assert_eq!(
                original.ineffective,
                u32::from(original.node_id >= selected)
            );
        }
        assert_eq!(Job::all().exec(&mut db).await.unwrap().len(), 1);
    }
}

#[tokio::test]
async fn upgrade_rejects_nodes_when_no_shared_restart_point_exists() {
    for changes in [vec![0], vec![2, 3], vec![]] {
        let Some((app, mut db)) = setup().await else {
            return;
        };
        let previous = publish(&app, &graph()).await;
        let run = completed_run(&mut db, &previous).await;
        let mut changed = previous.clone();
        for index in changes {
            changed.nodes[index].settings.config = json!({"image": "new"});
        }
        publish(&app, &changed).await;
        let entry = apply(
            &db,
            run.data_id,
            run.graph_id,
            Command::Upgrade {
                version: 2,
                node: previous.nodes[0].id,
            },
        )
        .await;
        let view = app.inspect_run(run.uuid).await.unwrap();
        assert_eq!(view.state.version, run.version);
        assert_eq!(view.state.revision, run.revision);
        assert_eq!(view.state.nodes.len(), previous.nodes.len());
        assert!(
            view.state
                .nodes
                .iter()
                .all(|node| node.status == "Succeeded")
        );
        assert_eq!(
            Task::all().exec(&mut db).await.unwrap().len(),
            previous.nodes.len()
        );
        let failure = CommandResult::filter_by_command_id(entry.id)
            .get(&mut db)
            .await
            .unwrap();
        assert_eq!(failure.error_code, "RAYFLOW_INVALID_UPGRADE_NODE");
    }
}

#[tokio::test]
async fn upgrade_can_select_a_shared_node_without_rerunning_its_shared_ancestor() {
    let Some((app, mut db)) = setup().await else {
        return;
    };
    let mut definition = graph();
    definition
        .edges
        .push((Uuid::from_u128(3), Uuid::from_u128(4)));
    let previous = publish(&app, &definition).await;
    let run = completed_run(&mut db, &previous).await;
    let mut changed = previous.clone();
    changed.nodes[4].name = "changed".into();
    publish(&app, &changed).await;
    apply(
        &db,
        run.data_id,
        run.graph_id,
        Command::Upgrade {
            version: 2,
            node: Uuid::from_u128(4),
        },
    )
    .await;
    let view = app.inspect_run(run.uuid).await.unwrap();
    let ready: Vec<_> = view
        .state
        .nodes
        .iter()
        .filter(|node| node.status == "Ready")
        .collect();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].node_id, Uuid::from_u128(4));
    let ancestor = view
        .state
        .nodes
        .iter()
        .find(|node| node.node_id == Uuid::from_u128(3))
        .unwrap();
    assert_eq!(ancestor.status, "Succeeded");
    apply(
        &db,
        run.data_id,
        run.graph_id,
        Command::Run {
            node: Uuid::from_u128(4),
        },
    )
    .await;
    apply(
        &db,
        run.data_id,
        run.graph_id,
        Command::Succeed {
            node: Uuid::from_u128(4),
        },
    )
    .await;
    let next = app
        .run_with(run.data_id, run.graph_id)
        .node_state(Uuid::from_u128(5))
        .await
        .unwrap();
    assert_eq!(next.status, "Ready");
    assert_eq!(
        app.run_with(run.data_id, run.graph_id)
            .node_state(Uuid::from_u128(6))
            .await
            .unwrap()
            .status,
        "Succeeded"
    );
}

#[tokio::test]
async fn shared_nodes_api_compares_exact_published_versions() {
    let Some((app, mut db)) = setup().await else {
        return;
    };
    let previous = publish(&app, &graph()).await;
    let mut changed = previous.clone();
    changed.nodes[4].name = "changed join".into();
    publish(&app, &changed).await;
    changed.nodes[0].name = "changed root".into();
    publish(&app, &changed).await;

    for (version, expected) in [
        (
            2,
            vec![
                Uuid::from_u128(1),
                Uuid::from_u128(2),
                Uuid::from_u128(3),
                Uuid::from_u128(4),
            ],
        ),
        (3, Vec::new()),
    ] {
        let response = server::router(app.clone())
            .oneshot(
                Request::get(format!(
                    "/graphs/{}/shared-nodes?from_version=1&version={version}",
                    previous.id
                ))
                .body(Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        let mut nodes: Vec<Uuid> = serde_json::from_slice(&body).unwrap();
        nodes.sort();
        assert_eq!(nodes, expected);
    }
    for query in [
        "from_version=2&version=2",
        "from_version=2&version=1",
        "from_version=1",
        "version=2",
        "from_version=invalid&version=2",
    ] {
        let response = server::router(app.clone())
            .oneshot(
                Request::get(format!("/graphs/{}/shared-nodes?{query}", previous.id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
    assert!(CommandEntry::all().exec(&mut db).await.unwrap().is_empty());
}

#[tokio::test]
async fn upgrade_api_emits_a_separate_command_and_preserves_the_run_identity() {
    let Some((app, mut db)) = setup().await else {
        return;
    };
    let previous = publish(&app, &graph()).await;
    let run = completed_run(&mut db, &previous).await;
    let mut changed = previous.clone();
    changed.nodes[4].name = "changed".into();
    publish(&app, &changed).await;
    for invalid in [
        json!({"data_id": run.data_id, "graph_id": run.graph_id, "version": 2}),
        json!({"data_id": run.data_id, "graph_id": run.graph_id, "version": 2, "node": null}),
        json!({"data_id": run.data_id, "graph_id": run.graph_id, "version": 2, "node": "invalid"}),
    ] {
        let response = server::router(app.clone())
            .oneshot(
                Request::post("/runs/upgrade")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&invalid).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }
    assert!(CommandEntry::all().exec(&mut db).await.unwrap().is_empty());
    let response = server::router(app.clone())
        .oneshot(
            Request::post("/runs/upgrade")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(
                        &json!({"data_id": run.data_id, "graph_id": run.graph_id, "version": 2, "node": Uuid::from_u128(4)}),
                    )
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let entries = CommandEntry::all().exec(&mut db).await.unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].command.0,
        Command::Upgrade {
            version: 2,
            node: Uuid::from_u128(4)
        }
    );
    assert_eq!(
        Run::filter_by_uuid(run.uuid)
            .get(&mut db)
            .await
            .unwrap()
            .version,
        1
    );
    let mut actuator = Actuator::new(Backend::new(db.clone(), db.clone()), 100);
    actuator.tick().await.unwrap();
    let view = app.inspect_run(run.uuid).await.unwrap();
    assert_eq!(view.state.version, 2);
    assert_eq!(view.state.revision, 1);
    assert_eq!(Run::all().exec(&mut db).await.unwrap().len(), 1);
    assert_eq!(
        view.state
            .nodes
            .iter()
            .filter(|node| node.status == "Running")
            .count(),
        1
    );
    assert_eq!(
        view.state
            .nodes
            .iter()
            .find(|node| node.status == "Running")
            .unwrap()
            .node_id,
        Uuid::from_u128(4)
    );
}

#[tokio::test]
async fn upgrade_participates_in_the_callers_transaction() {
    let Some((app, mut db)) = setup().await else {
        return;
    };
    let previous = publish(&app, &graph()).await;
    let run = completed_run(&mut db, &previous).await;
    let mut changed = previous.clone();
    changed.nodes[4].name = "changed".into();
    publish(&app, &changed).await;
    let task_count = Task::all().exec(&mut db).await.unwrap().len();
    let history = NodeRun::all()
        .order_by(NodeRun::fields().id().asc())
        .exec(&mut db)
        .await
        .unwrap();
    let entry = CommandEntry::new(
        run.data_id,
        run.graph_id,
        Command::Upgrade {
            version: 2,
            node: Uuid::from_u128(3),
        },
    );
    let mut backend = Backend::new(db.clone(), db.clone());
    let mut tx = db.transaction().await.unwrap();
    assert_eq!(
        backend.apply_in(&mut tx, &entry).await.unwrap(),
        Some(run.uuid)
    );
    assert_eq!(
        Run::filter_by_uuid(run.uuid)
            .get(&mut tx)
            .await
            .unwrap()
            .version,
        2
    );
    assert_eq!(
        NodeRun::filter_by_run_id(run.uuid)
            .filter(NodeRun::fields().ineffective().eq(1))
            .exec(&mut tx)
            .await
            .unwrap()
            .len(),
        3
    );
    tx.rollback().await.unwrap();
    assert_eq!(
        serde_json::to_value(
            NodeRun::all()
                .order_by(NodeRun::fields().id().asc())
                .exec(&mut db)
                .await
                .unwrap()
        )
        .unwrap(),
        serde_json::to_value(history).unwrap()
    );
    assert_eq!(
        Run::filter_by_uuid(run.uuid)
            .get(&mut db)
            .await
            .unwrap()
            .version,
        1
    );
    assert_eq!(Task::all().exec(&mut db).await.unwrap().len(), task_count);
    assert_eq!(
        NodeRun::all().exec(&mut db).await.unwrap().len(),
        previous.nodes.len()
    );
}
