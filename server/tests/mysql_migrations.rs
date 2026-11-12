//! Migration integration tests use temporary databases, including legacy DDL
//! that the current Toasty models cannot express.

use jiff::Timestamp;
use runtime::command::{NodeRun, Run};
use server::migration::{MIGRATIONS, initialize};
use toasty::{Db, migration::MigrationSet, sql};
use url::Url;
use uuid::Uuid;

fn database_url() -> Option<String> {
    let mut url = Url::parse(&std::env::var("DATABASE_URL").ok()?).unwrap();
    url.set_path(&format!("/rayflow_migration_{}", Uuid::new_v4().simple()));
    Some(url.to_string())
}

async fn connect(url: &str) -> Db {
    Db::builder()
        .models(server::models())
        .connect(url)
        .await
        .unwrap()
}

async fn legacy_database(url: &str) -> Db {
    let mut admin_url = Url::parse(url).unwrap();
    let name = admin_url.path().strip_prefix('/').unwrap().to_owned();
    assert!(name.starts_with("rayflow_migration_"));
    admin_url.set_path("/information_schema");
    let mut admin = connect(admin_url.as_str()).await;
    sql::statement(format!("CREATE DATABASE `{name}`"))
        .exec(&mut admin)
        .await
        .unwrap();
    let mut db = connect(url).await;
    let migrations: &'static MigrationSet = &MIGRATIONS;
    MigrationSet::new(&migrations.migrations()[..1])
        .apply(&db)
        .await
        .unwrap();
    sql::statement("DROP TABLE __toasty_migrations")
        .exec(&mut db)
        .await
        .unwrap();
    db
}

async fn cleanup(url: &str, db: &mut Db) {
    let parsed = Url::parse(url).unwrap();
    let name = parsed.path().strip_prefix('/').unwrap();
    assert!(name.starts_with("rayflow_migration_"));
    sql::statement(format!("DROP DATABASE `{name}`"))
        .exec(db)
        .await
        .unwrap();
}

async fn create_run(db: &mut Db, data_id: Uuid, graph_id: Uuid, version: u64) -> Run {
    let now = jiff::Timestamp::now();
    Run::create()
        .uuid(Uuid::new_v4())
        .data_id(data_id)
        .graph_id(graph_id)
        .version(version)
        .revision(0)
        .created_at(now)
        .updated_at(now)
        .exec(db)
        .await
        .unwrap()
}

#[tokio::test]
async fn node_run_ineffective_migration_preserves_history_and_defaults_to_zero() {
    let Some(url) = database_url() else { return };
    let mut db = legacy_database(&url).await;
    let migrations: &'static MigrationSet = &MIGRATIONS;
    MigrationSet::new(&migrations.migrations()[..5])
        .apply(&db)
        .await
        .unwrap();
    let run = create_run(&mut db, Uuid::new_v4(), Uuid::new_v4(), 1).await;
    let node = Uuid::new_v4();
    sql::statement("INSERT INTO node_run (run_id, data_id, graph_id, node_id, status) VALUES (?, ?, ?, ?, 'Succeeded')")
        .bind(run.uuid).bind(run.data_id).bind(run.graph_id).bind(node)
        .exec(&mut db).await.unwrap();
    let report = initialize(&url).await.unwrap();
    assert_eq!(report.applied(), MIGRATIONS.migrations().len() - 5);
    assert_eq!(report.skipped(), 5);
    let preserved = NodeRun::filter_by_run_id_and_node_id_and_revision(run.uuid, node, 0)
        .get(&mut db)
        .await
        .unwrap();
    assert_eq!(preserved.ineffective, 0);
    assert_eq!(preserved.status, "Succeeded");
    sql::statement("INSERT INTO node_run (run_id, data_id, graph_id, node_id, revision) VALUES (?, ?, ?, ?, 1)")
        .bind(run.uuid).bind(run.data_id).bind(run.graph_id).bind(node)
        .exec(&mut db).await.unwrap();
    let current = NodeRun::filter_by_run_id_and_node_id_and_revision(run.uuid, node, 1)
        .get(&mut db)
        .await
        .unwrap();
    assert_eq!(current.ineffective, 0);
    NodeRun::filter_by_id(preserved.id)
        .update()
        .ineffective(1)
        .exec(&mut db)
        .await
        .unwrap();
    assert_eq!(initialize(&url).await.unwrap().applied(), 0);
    assert_eq!(
        NodeRun::filter_by_id(preserved.id)
            .get(&mut db)
            .await
            .unwrap()
            .ineffective,
        1
    );
    cleanup(&url, &mut db).await;
}

#[tokio::test]
async fn initialization_creates_the_database_and_tracks_migrations() {
    let Some(url) = database_url() else { return };
    let first = initialize(&url).await.unwrap();
    assert_eq!(first.applied(), MIGRATIONS.migrations().len());
    assert_eq!(first.skipped(), 0);
    let mut db = connect(&url).await;
    let run = create_run(&mut db, Uuid::new_v4(), Uuid::new_v4(), 1).await;
    let now = Timestamp::now();
    let user = server::auth::User::new("local", "alice", "Alice".into(), None);
    server::auth::UserRecord::create()
        .id(user.id.clone())
        .profile(serde_json::to_value(&user).unwrap())
        .updated_at(now)
        .exec(&mut db)
        .await
        .unwrap();
    server::auth::Session::create()
        .token_hash("a".repeat(64))
        .user_id(user.id.clone())
        .expires_at(now)
        .exec(&mut db)
        .await
        .unwrap();
    server::auth::LoginFlow::create()
        .state_hash("b".repeat(64))
        .browser_hash("c".repeat(64))
        .nonce("nonce")
        .verifier("verifier")
        .expires_at(now)
        .exec(&mut db)
        .await
        .unwrap();
    let second = initialize(&url).await.unwrap();
    assert_eq!(second.applied(), 0);
    assert_eq!(second.skipped(), MIGRATIONS.migrations().len());
    assert_eq!(Run::all().exec(&mut db).await.unwrap()[0].uuid, run.uuid);
    assert_eq!(
        server::auth::Session::all().exec(&mut db).await.unwrap()[0].user_id,
        user.id
    );
    assert_eq!(
        server::auth::UserRecord::all()
            .exec(&mut db)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        server::auth::LoginFlow::all()
            .exec(&mut db)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        sql::query("SELECT id FROM __toasty_migrations")
            .exec(&mut db)
            .await
            .unwrap()
            .len(),
        MIGRATIONS.migrations().len()
    );
    assert!(sql::query("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND COLUMN_NAME IN ('assignment_version', 'steal_count')")
        .exec(&mut db).await.unwrap().is_empty());
    assert_eq!(
        sql::query("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'command_result'")
            .exec(&mut db)
            .await
            .unwrap()
            .len(),
        1
    );
    let command_result_columns = sql::query(
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'command_result' AND COLUMN_NAME IN ('error_code', 'error_message', 'error')",
    )
    .exec(&mut db)
    .await
    .unwrap();
    assert_eq!(command_result_columns.len(), 2);
    cleanup(&url, &mut db).await;
}

#[tokio::test]
async fn legacy_job_columns_get_defaults_without_losing_values() {
    let Some(url) = database_url() else { return };
    let mut db = legacy_database(&url).await;
    sql::statement("ALTER TABLE job ADD COLUMN assignment_version BIGINT UNSIGNED NOT NULL, ADD COLUMN steal_count INT UNSIGNED NOT NULL")
        .exec(&mut db).await.unwrap();
    sql::statement(
        "INSERT INTO job (uuid, assignment_version, steal_count) VALUES ('legacy', 12, 4)",
    )
    .exec(&mut db)
    .await
    .unwrap();

    assert_eq!(
        initialize(&url).await.unwrap().applied(),
        MIGRATIONS.migrations().len()
    );
    sql::statement("INSERT INTO job (uuid) VALUES ('current')")
        .exec(&mut db)
        .await
        .unwrap();
    let values = sql::query("SELECT uuid, assignment_version, steal_count FROM job ORDER BY uuid")
        .exec(&mut db)
        .await
        .unwrap();
    let current = values[0].as_record_unwrap();
    assert_eq!(current[0], "current");
    assert_eq!(current[1], 0_u64);
    assert_eq!(current[2], 0_u32);
    let legacy = values[1].as_record_unwrap();
    assert_eq!(legacy[0], "legacy");
    assert_eq!(legacy[1], 12_u64);
    assert_eq!(legacy[2], 4_u32);
    assert_eq!(initialize(&url).await.unwrap().applied(), 0);
    cleanup(&url, &mut db).await;
}

#[tokio::test]
async fn duplicate_runs_fail_migration_without_deleting_rows_and_can_be_retried() {
    let Some(url) = database_url() else { return };
    let mut db = legacy_database(&url).await;
    sql::statement("ALTER TABLE run DROP INDEX uk_run_data_graph, ADD UNIQUE KEY legacy_run_key (data_id, graph_id, version)")
        .exec(&mut db).await.unwrap();
    let data_id = Uuid::new_v4();
    let graph_id = Uuid::new_v4();
    let first = create_run(&mut db, data_id, graph_id, 1).await;
    let second = create_run(&mut db, data_id, graph_id, 2).await;
    assert!(initialize(&url).await.is_err());
    assert_eq!(Run::all().exec(&mut db).await.unwrap().len(), 2);
    assert_eq!(
        sql::query("SELECT id FROM __toasty_migrations")
            .exec(&mut db)
            .await
            .unwrap()
            .len(),
        2
    );
    Run::filter_by_id(second.id)
        .delete()
        .exec(&mut db)
        .await
        .unwrap();
    let report = initialize(&url).await.unwrap();
    assert_eq!(report.applied(), MIGRATIONS.migrations().len() - 2);
    assert_eq!(report.skipped(), 2);
    assert_eq!(Run::all().exec(&mut db).await.unwrap()[0].uuid, first.uuid);
    assert!(
        sql::statement("INSERT INTO run (uuid, data_id, graph_id, version) VALUES (?, ?, ?, 3)")
            .bind(Uuid::new_v4().to_string())
            .bind(data_id.to_string())
            .bind(graph_id.to_string())
            .exec(&mut db)
            .await
            .is_err()
    );
    cleanup(&url, &mut db).await;
}
