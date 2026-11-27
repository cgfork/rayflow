//! Historical DDL and migration checkpoints require SQL outside current models.

use runtime::command::NodeRun;
use scheduler::{
    Job,
    actuator::{NodeConfig, RunConfig, Status},
};
use server::migration::{MIGRATIONS, initialize};
use toasty::{Db, migration::MigrationSet, sql, stmt::Value};
use url::Url;
use uuid::Uuid;

const NODE_RUN_UUID: Uuid = Uuid::from_u128(11);
const JOB_UUID: Uuid = Uuid::from_u128(44);
const ORIGINAL_MIGRATION: &str = include_str!("fixtures/0022_uuid_references_original.sql");

fn database_url() -> Option<Url> {
    let mut url = Url::parse(&std::env::var("DATABASE_URL").ok()?).unwrap();
    url.set_path(&format!(
        "/rayflow_uuid_migration_{}",
        Uuid::new_v4().simple()
    ));
    Some(url)
}

async fn connect(url: &str) -> Db {
    Db::builder()
        .models(server::models())
        .connect(url)
        .await
        .unwrap()
}

async fn legacy_database() -> Option<(String, Db)> {
    let mut url = database_url()?;
    let name = url.path().strip_prefix('/').unwrap().to_owned();
    url.set_path("/information_schema");
    let mut admin = connect(url.as_str()).await;
    sql::statement(format!("CREATE DATABASE `{name}`"))
        .exec(&mut admin)
        .await
        .unwrap();
    url.set_path(&format!("/{name}"));
    let mut db = connect(url.as_str()).await;
    let migrations: &'static MigrationSet = &MIGRATIONS;
    MigrationSet::new(&migrations.migrations()[..21])
        .apply(&db)
        .await
        .unwrap();

    sql::statement("INSERT INTO node_run (id, uuid, run_id, data_id, graph_id, node_id, created_at, updated_at) VALUES (11, ?, ?, ?, ?, ?, '2025-01-01', '2025-03-01')")
        .bind(NODE_RUN_UUID).bind(Uuid::new_v4()).bind(Uuid::new_v4()).bind(Uuid::new_v4()).bind(Uuid::new_v4())
        .exec(&mut db).await.unwrap();
    sql::statement("INSERT INTO node_config (id, node_id, config, lease, created_at, updated_at, created_by, updated_by) VALUES (22, ?, '{\"image\":\"example\"}', 30000, '2025-01-01', '2025-03-01', 'creator', 'updater')")
        .bind(Uuid::new_v4()).exec(&mut db).await.unwrap();
    sql::statement("INSERT INTO run_config (id, run_id, annotation, created_at, updated_at, created_by, updated_by) VALUES (33, ?, '{\"label\":\"original\"}', '2025-01-01', '2025-03-01', 'creator', 'updater')")
        .bind(Uuid::new_v4()).exec(&mut db).await.unwrap();
    sql::statement("INSERT INTO job (id, uuid, task_id, run_id, node_run_id, node_config_id, run_config_id, status, created_at, updated_at) VALUES (44, ?, ?, ?, 11, 22, 33, 'Succeeded', '2025-01-01', '2025-03-01')")
        .bind(JOB_UUID).bind(Uuid::new_v4()).bind(Uuid::new_v4()).exec(&mut db).await.unwrap();
    Some((url.to_string(), db))
}

async fn cleanup(url: &str, db: &mut Db) {
    let parsed = Url::parse(url).unwrap();
    let name = parsed.path().strip_prefix('/').unwrap();
    assert!(name.starts_with("rayflow_uuid_migration_"));
    sql::statement(format!("DROP DATABASE `{name}`"))
        .exec(db)
        .await
        .unwrap();
}

async fn config_uuids(db: &mut Db) -> Vec<Value> {
    let mut values = Vec::new();
    for table in ["run_config", "node_config"] {
        let columns = sql::query("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = 'uuid'")
            .bind(table).exec(&mut *db).await.unwrap();
        if !columns.is_empty() {
            values.extend(
                sql::query(format!("SELECT uuid FROM {table} WHERE uuid IS NOT NULL"))
                    .exec(&mut *db)
                    .await
                    .unwrap(),
            );
        }
    }
    values
}

async fn timestamps(db: &mut Db) -> Vec<Value> {
    sql::query("SELECT 'run_config' AS source, id, created_at, updated_at FROM run_config UNION ALL SELECT 'node_config', id, created_at, updated_at FROM node_config UNION ALL SELECT 'node_run', id, created_at, updated_at FROM node_run UNION ALL SELECT 'job', id, created_at, updated_at FROM job ORDER BY source, id")
        .exec(db).await.unwrap()
}

async fn assert_migrated(db: &mut Db) {
    let run_config = RunConfig::filter_by_id(33).get(&mut *db).await.unwrap();
    let node_config = NodeConfig::filter_by_id(22).get(&mut *db).await.unwrap();
    let node_run = NodeRun::filter_by_id(11).get(&mut *db).await.unwrap();
    let job = Job::filter_by_id(44).get(&mut *db).await.unwrap();
    assert!(!run_config.uuid.is_nil());
    assert!(!node_config.uuid.is_nil());
    assert_eq!(run_config.annotation.0["label"], "original");
    assert_eq!(node_config.config["image"], "example");
    assert_eq!(node_config.name, "default");
    assert_eq!(node_config.required_gpu_memory_bytes, 0);
    assert_eq!(node_config.required_gpu_cdi_device, None);
    assert_eq!(run_config.created_by, "creator");
    assert_eq!(run_config.updated_by, "updater");
    assert_eq!(node_config.created_by, "creator");
    assert_eq!(node_config.updated_by, "updater");
    assert_eq!(node_run.uuid, NODE_RUN_UUID);
    assert_eq!(job.uuid, JOB_UUID);
    assert_eq!(job.node_run_id, NODE_RUN_UUID);
    assert_eq!(job.node_config_id, node_config.uuid);
    assert_eq!(job.run_config_id, run_config.uuid);
    assert_eq!(job.status, Status::Succeeded);
    assert_eq!(job.required_gpu_memory_bytes, 0);
    assert_eq!(job.required_gpu_cdi_device, None);
    assert_eq!(job.assigned_gpu_cdi_device, None);
    assert!(sql::query("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'job' AND COLUMN_NAME IN ('node_run_uuid', 'node_config_uuid', 'run_config_uuid')")
        .exec(&mut *db).await.unwrap().is_empty());
    let indexes = sql::query("SELECT DISTINCT INDEX_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'job' AND INDEX_NAME IN ('idx_job_node_run_id', 'idx_job_node_config_id', 'idx_job_run_config_id')")
        .exec(db).await.unwrap();
    assert_eq!(indexes.len(), 3);
}

#[tokio::test]
async fn initialization_creates_a_fresh_database_and_skips_completed_migrations() {
    let Some(url) = database_url() else { return };
    let first = initialize(url.as_str()).await.unwrap();
    assert_eq!(first.applied(), MIGRATIONS.migrations().len());
    assert_eq!(first.skipped(), 0);
    let mut db = connect(url.as_str()).await;
    let columns = sql::query("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'job' AND DATA_TYPE = 'char' AND CHARACTER_MAXIMUM_LENGTH = 36 AND IS_NULLABLE = 'NO' AND COLUMN_NAME IN ('node_run_id', 'node_config_id', 'run_config_id')")
        .exec(&mut db).await.unwrap();
    assert_eq!(columns.len(), 3);
    let second = initialize(url.as_str()).await.unwrap();
    assert_eq!(second.applied(), 0);
    assert_eq!(second.skipped(), MIGRATIONS.migrations().len());
    cleanup(url.as_str(), &mut db).await;
}

#[tokio::test]
async fn job_artifact_migration_preserves_prepared_result_catalogs() {
    let Some((url, mut db)) = legacy_database().await else {
        return;
    };
    MigrationSet::new(&MIGRATIONS.migrations()[..23])
        .apply(&db)
        .await
        .unwrap();
    let owner = Uuid::new_v4();
    let catalog = Uuid::new_v4();
    sql::statement("INSERT INTO job_result (job_uuid, agent_id, catalog_uuid) VALUES (?, ?, ?)")
        .bind(JOB_UUID)
        .bind(owner)
        .bind(catalog)
        .exec(&mut db)
        .await
        .unwrap();

    assert_eq!(
        initialize(&url).await.unwrap().applied(),
        MIGRATIONS.migrations().len() - 23
    );
    // Simulate the ALTER committing before its migration ID is recorded.
    sql::statement("DELETE FROM __toasty_migrations WHERE id = 24")
        .exec(&mut db)
        .await
        .unwrap();
    assert_eq!(initialize(&url).await.unwrap().applied(), 1);
    let preserved = sql::query("SELECT id FROM job_artifact WHERE job_uuid = ? AND filename = 'result.json' AND agent_id = ? AND catalog_uuid = ?")
        .bind(JOB_UUID).bind(owner).bind(catalog).column_types([toasty::stmt::Type::U64])
        .exec(&mut db).await.unwrap();
    assert_eq!(preserved.len(), 1);
    cleanup(&url, &mut db).await;
}

#[tokio::test]
async fn run_config_migration_keeps_one_snapshot_per_run_and_preserves_job_references() {
    let Some((url, mut db)) = legacy_database().await else {
        return;
    };
    MigrationSet::new(&MIGRATIONS.migrations()[..24])
        .apply(&db)
        .await
        .unwrap();
    let original = RunConfig::filter_by_id(33).get(&mut db).await.unwrap();
    let run_id = original.run_id.unwrap();
    let latest_id = Uuid::new_v4();
    sql::statement("INSERT INTO run_config (uuid, run_id, annotation) VALUES (?, ?, '{}')")
        .bind(latest_id)
        .bind(run_id)
        .exec(&mut db)
        .await
        .unwrap();
    let legacy_unattached = Uuid::new_v4();
    sql::statement("INSERT INTO run_config (uuid, annotation) VALUES (?, '{}')")
        .bind(legacy_unattached)
        .exec(&mut db)
        .await
        .unwrap();
    let existing_timestamps = timestamps(&mut db).await;

    assert_eq!(
        initialize(&url).await.unwrap().applied(),
        MIGRATIONS.migrations().len() - 24
    );
    sql::statement("DELETE FROM __toasty_migrations WHERE id = 25")
        .exec(&mut db)
        .await
        .unwrap();
    assert_eq!(initialize(&url).await.unwrap().applied(), 1);
    assert_eq!(timestamps(&mut db).await, existing_timestamps);

    let latest = RunConfig::filter_by_run_id(run_id)
        .get(&mut db)
        .await
        .unwrap();
    assert_eq!(latest.uuid, latest_id);
    for uuid in [original.uuid, legacy_unattached] {
        let config = RunConfig::filter_by_uuid(uuid).get(&mut db).await.unwrap();
        assert_eq!(config.run_id, None);
    }
    assert_migrated(&mut db).await;
    assert!(
        sql::statement("INSERT INTO run_config (uuid, run_id, annotation) VALUES (?, ?, '{}')")
            .bind(Uuid::new_v4())
            .bind(run_id)
            .exec(&mut db)
            .await
            .is_err()
    );
    sql::statement("INSERT INTO run_config (uuid, annotation) VALUES (?, '{}')")
        .bind(Uuid::new_v4())
        .exec(&mut db)
        .await
        .unwrap();
    cleanup(&url, &mut db).await;
}

#[tokio::test]
async fn node_config_migration_preserves_legacy_configs_and_allows_named_versions() {
    let Some((url, mut db)) = legacy_database().await else {
        return;
    };
    MigrationSet::new(&MIGRATIONS.migrations()[..25])
        .apply(&db)
        .await
        .unwrap();
    let existing_uuids = config_uuids(&mut db).await;
    let existing_timestamps = timestamps(&mut db).await;

    assert_eq!(
        initialize(&url).await.unwrap().applied(),
        MIGRATIONS.migrations().len() - 25
    );
    assert_eq!(config_uuids(&mut db).await, existing_uuids);
    assert_eq!(timestamps(&mut db).await, existing_timestamps);
    assert_migrated(&mut db).await;
    let legacy = NodeConfig::filter_by_id(22).get(&mut db).await.unwrap();

    let insert = |node_id: Uuid, version: u64, name: &str| {
        sql::statement("INSERT INTO node_config (uuid, node_id, version, name, config) VALUES (?, ?, ?, ?, '{}')")
            .bind(Uuid::new_v4()).bind(node_id).bind(version).bind(name)
    };
    for (node_id, version, name) in [
        (legacy.node_id, legacy.version, "large"),
        (legacy.node_id, legacy.version, "Large"),
        (legacy.node_id, legacy.version + 1, "large"),
        (Uuid::new_v4(), legacy.version, "large"),
    ] {
        insert(node_id, version, name).exec(&mut db).await.unwrap();
    }
    for name in ["default", "large"] {
        assert!(
            insert(legacy.node_id, legacy.version, name)
                .exec(&mut db)
                .await
                .is_err()
        );
    }
    let defaulted_id = Uuid::new_v4();
    sql::statement("INSERT INTO node_config (uuid, node_id, config) VALUES (?, ?, '{}')")
        .bind(defaulted_id)
        .bind(Uuid::new_v4())
        .exec(&mut db)
        .await
        .unwrap();
    assert_eq!(
        NodeConfig::filter_by_uuid(defaulted_id)
            .get(&mut db)
            .await
            .unwrap()
            .name,
        "default"
    );

    // The DDL can commit before Toasty records the migration.
    sql::statement("DELETE FROM __toasty_migrations WHERE id = 26")
        .exec(&mut db)
        .await
        .unwrap();
    assert_eq!(initialize(&url).await.unwrap().applied(), 1);
    let named =
        NodeConfig::filter_by_node_id_and_version_and_name(legacy.node_id, legacy.version, "large")
            .get(&mut db)
            .await
            .unwrap();
    assert_eq!(named.name, "large");
    cleanup(&url, &mut db).await;
}

#[tokio::test]
async fn uuid_migration_resumes_every_original_statement_boundary() {
    // Include the last ALTER committing before the migration ID was recorded.
    let statements: Vec<_> = ORIGINAL_MIGRATION
        .split(';')
        .filter(|statement| !statement.trim().is_empty())
        .collect();
    for completed in 0..=statements.len() {
        let Some((url, mut db)) = legacy_database().await else {
            return;
        };
        for statement in &statements[..completed] {
            sql::statement(*statement).exec(&mut db).await.unwrap();
        }
        let existing_uuids = config_uuids(&mut db).await;
        let existing_timestamps = timestamps(&mut db).await;
        let report = initialize(&url).await.unwrap_or_else(|error| {
            panic!("failed after {completed} original statements: {error}")
        });
        assert_eq!(report.skipped(), 21);
        assert_eq!(report.applied(), MIGRATIONS.migrations().len() - 21);
        assert_migrated(&mut db).await;
        let current_uuids = config_uuids(&mut db).await;
        for uuid in existing_uuids {
            assert!(
                current_uuids.contains(&uuid),
                "an assigned UUID was replaced"
            );
        }
        assert_eq!(timestamps(&mut db).await, existing_timestamps);
        assert_eq!(initialize(&url).await.unwrap().applied(), 0);
        cleanup(&url, &mut db).await;
    }
}

#[tokio::test]
async fn uuid_migration_resumes_individually_added_shadow_columns() {
    for column in ["node_run_uuid", "node_config_uuid", "run_config_uuid"] {
        let Some((url, mut db)) = legacy_database().await else {
            return;
        };
        sql::statement(format!("ALTER TABLE job ADD COLUMN {column} CHAR(36) NULL"))
            .exec(&mut db)
            .await
            .unwrap();
        initialize(&url).await.unwrap();
        assert_migrated(&mut db).await;
        cleanup(&url, &mut db).await;
    }
}

#[tokio::test]
async fn uuid_migration_maps_absent_optional_configs_to_nil() {
    let Some((url, mut db)) = legacy_database().await else {
        return;
    };
    for (id, node_config_id, run_config_id) in [(45_u64, 22_u64, 0_u64), (46, 0, 33), (47, 0, 0)] {
        sql::statement("INSERT INTO job (id, uuid, task_id, run_id, node_run_id, node_config_id, run_config_id) VALUES (?, ?, ?, ?, 11, ?, ?)")
            .bind(id).bind(Uuid::new_v4()).bind(Uuid::new_v4()).bind(Uuid::new_v4()).bind(node_config_id).bind(run_config_id)
            .exec(&mut db).await.unwrap();
    }
    let existing_timestamps = timestamps(&mut db).await;
    initialize(&url).await.unwrap();
    assert_migrated(&mut db).await;
    let node_config = NodeConfig::filter_by_id(22).get(&mut db).await.unwrap();
    let run_config = RunConfig::filter_by_id(33).get(&mut db).await.unwrap();
    for (id, node_config_id, run_config_id) in [
        (45, node_config.uuid, Uuid::nil()),
        (46, Uuid::nil(), run_config.uuid),
        (47, Uuid::nil(), Uuid::nil()),
    ] {
        let job = Job::filter_by_id(id).get(&mut db).await.unwrap();
        assert_eq!(job.node_run_id, NODE_RUN_UUID);
        assert_eq!(job.node_config_id, node_config_id);
        assert_eq!(job.run_config_id, run_config_id);
    }
    assert_eq!(timestamps(&mut db).await, existing_timestamps);
    assert_eq!(initialize(&url).await.unwrap().applied(), 0);
    cleanup(&url, &mut db).await;
}

#[tokio::test]
async fn uuid_migration_rejects_missing_references_and_resumes_after_repair() {
    for (column, original_id) in [
        ("node_run_id", 11),
        ("node_config_id", 22),
        ("run_config_id", 33),
    ] {
        let Some((url, mut db)) = legacy_database().await else {
            return;
        };
        sql::statement(format!("UPDATE job SET {column} = 9999 WHERE id = 44"))
            .exec(&mut db)
            .await
            .unwrap();
        assert!(initialize(&url).await.is_err());
        let existing_uuids = config_uuids(&mut db).await;
        let references = sql::query(format!("SELECT {column} FROM job WHERE id = 44"))
            .exec(&mut db)
            .await
            .unwrap();
        assert_eq!(references[0].as_record_unwrap()[0], 9999_u64);
        assert!(
            sql::query("SELECT id FROM __toasty_migrations WHERE id = 22")
                .exec(&mut db)
                .await
                .unwrap()
                .is_empty()
        );

        // Only this explicit repair resolves the orphan; the migration cannot
        // guess ownership or replace a missing nonzero reference with nil.
        sql::statement(format!(
            "UPDATE job SET {column} = {original_id} WHERE id = 44"
        ))
        .exec(&mut db)
        .await
        .unwrap();
        initialize(&url).await.unwrap();
        assert_migrated(&mut db).await;
        assert_eq!(config_uuids(&mut db).await, existing_uuids);
        cleanup(&url, &mut db).await;
    }
}
