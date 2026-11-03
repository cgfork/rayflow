//! Migration integration tests use temporary databases, including legacy DDL
//! that the current Toasty models cannot express.

use runtime::command::Run;
use server::migration::{MIGRATIONS, initialize};
use toasty::{Db, sql};
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
async fn initialization_creates_the_database_and_tracks_migrations() {
    let Some(url) = database_url() else { return };
    let first = initialize(&url).await.unwrap();
    assert_eq!(first.applied(), MIGRATIONS.migrations().len());
    assert_eq!(first.skipped(), 0);
    let mut db = connect(&url).await;
    let run = create_run(&mut db, Uuid::new_v4(), Uuid::new_v4(), 1).await;
    let second = initialize(&url).await.unwrap();
    assert_eq!(second.applied(), 0);
    assert_eq!(second.skipped(), MIGRATIONS.migrations().len());
    assert_eq!(Run::all().exec(&mut db).await.unwrap()[0].uuid, run.uuid);
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
    cleanup(&url, &mut db).await;
}

#[tokio::test]
async fn legacy_job_columns_get_defaults_without_losing_values() {
    let Some(url) = database_url() else { return };
    initialize(&url).await.unwrap();
    let mut db = connect(&url).await;
    sql::statement("DROP TABLE __toasty_migrations")
        .exec(&mut db)
        .await
        .unwrap();
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
    initialize(&url).await.unwrap();
    let mut db = connect(&url).await;
    sql::statement("DROP TABLE __toasty_migrations")
        .exec(&mut db)
        .await
        .unwrap();
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
    assert_eq!(report.applied(), 1);
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
