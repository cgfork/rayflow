//! Migration integration tests use temporary databases, including legacy DDL
//! that the current Toasty models cannot express.

use catalog::{CatalogInput, CatalogSystem, FileInput, FileSystem, model::Catalog};
use event::{Emit, task::Task};
use jiff::Timestamp;
use runtime::command::{NodeRun, Run};
use scheduler::{
    Job,
    actuator::{NodeConfig, RunConfig},
};
use server::{
    auth::{LoginFlow, Session, UserRecord},
    migration::{MIGRATIONS, initialize},
};
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
async fn catalog_key_and_file_fullpath_migrations_preserve_metadata_and_references() {
    let Some(url) = database_url() else { return };
    let mut db = legacy_database(&url).await;
    let migrations: &'static MigrationSet = &MIGRATIONS;
    MigrationSet::new(&migrations.migrations()[..16])
        .apply(&db)
        .await
        .unwrap();
    let catalog_id = Uuid::new_v4();
    let file_id = Uuid::new_v4();
    sql::statement("INSERT INTO catalog (uuid, url, size, format, uploaded, created_at, updated_at) VALUES (?, 's3://bucket/existing', 42, 'csv', TRUE, '2025-01-01', '2025-03-01')")
        .bind(catalog_id).exec(&mut db).await.unwrap();
    sql::statement("INSERT INTO file (uuid, fullname, filename, catalog_uuid, created_at, updated_at) VALUES (?, '/existing', 'existing', ?, '2025-01-01', '2025-03-01')")
        .bind(file_id).bind(catalog_id).exec(&mut db).await.unwrap();
    assert_eq!(
        MigrationSet::new(&migrations.migrations()[..18])
            .apply(&db)
            .await
            .unwrap()
            .applied(),
        2
    );
    let preserved = sql::query("SELECT f.uuid, f.fullpath, f.catalog_uuid, c.`key`, c.size, c.uploaded FROM file f JOIN catalog c ON c.uuid=f.catalog_uuid WHERE f.uuid=?").bind(file_id).exec(&mut db).await.unwrap();
    let row = preserved[0].as_record_unwrap();
    assert_eq!(row[0], file_id.to_string());
    assert_eq!(row[1], "/existing");
    assert_eq!(row[2], catalog_id.to_string());
    assert_eq!(row[3], "s3://bucket/existing");
    assert_eq!(row[4], 42_u64);
    assert_eq!(row[5], 1_i64);
    assert!(sql::statement("INSERT INTO catalog (uuid, `key`, url, size, format) VALUES (?, 's3://bucket/existing', 's3://bucket/other', 0, 'bin')")
        .bind(Uuid::new_v4()).exec(&mut db).await.is_err());
    assert!(sql::statement("INSERT INTO file (uuid, fullpath, filename, catalog_uuid) VALUES (?, '/existing', 'other', ?)")
        .bind(Uuid::new_v4()).bind(catalog_id).exec(&mut db).await.is_err());
    assert!(sql::query("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'file' AND COLUMN_NAME = 'fullname'")
        .exec(&mut db).await.unwrap().is_empty());
    assert_eq!(
        MigrationSet::new(&migrations.migrations()[..18])
            .apply(&db)
            .await
            .unwrap()
            .applied(),
        0
    );
    cleanup(&url, &mut db).await;
}

#[tokio::test]
async fn catalog_url_migration_preserves_references_and_enforces_uniqueness() {
    let Some(url) = database_url() else { return };
    let mut db = legacy_database(&url).await;
    let migrations: &'static MigrationSet = &MIGRATIONS;
    MigrationSet::new(&migrations.migrations()[..10])
        .apply(&db)
        .await
        .unwrap();
    let active = Uuid::new_v4();
    let deleted = Uuid::new_v4();
    let file = Uuid::new_v4();
    sql::statement("INSERT INTO catalog (uuid, url, size, format, uploaded) VALUES (?, 's3://bucket/existing', 42, 'bin', TRUE)")
        .bind(active).exec(&mut db).await.unwrap();
    sql::statement("INSERT INTO catalog (uuid, url, size, format, deleted_at) VALUES (?, 's3://bucket/existing', 40, 'bin', '2025-01-01 00:00:00')")
        .bind(deleted).exec(&mut db).await.unwrap();
    sql::statement("INSERT INTO file (uuid, fullname, filename, catalog_uuid) VALUES (?, '/existing', 'existing', ?)")
        .bind(file).bind(active).exec(&mut db).await.unwrap();
    assert_eq!(
        MigrationSet::new(&migrations.migrations()[..11])
            .apply(&db)
            .await
            .unwrap()
            .applied(),
        1
    );
    // Inspect historical columns without selecting fields introduced later.
    let joined = sql::query("SELECT c.uuid, c.size, c.uploaded FROM file f JOIN catalog c ON c.uuid = f.catalog_uuid WHERE f.uuid = ?")
        .bind(file).exec(&mut db).await.unwrap();
    let record = joined[0].as_record_unwrap();
    assert_eq!(record[0], active.to_string());
    assert_eq!(record[1], 42_u64);
    assert_eq!(record[2], 1_i64);
    let historical = sql::query("SELECT size FROM catalog WHERE uuid = ?")
        .bind(deleted)
        .exec(&mut db)
        .await
        .unwrap();
    assert_eq!(historical[0].as_record_unwrap()[0], 40_u64);
    let columns = sql::query("SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'catalog' AND COLUMN_NAME = 'url'")
        .exec(&mut db).await.unwrap();
    let column = columns[0].as_record_unwrap();
    assert_eq!(column[0], "varchar");
    assert_eq!(column[1], 750_u64);
    // The database itself enforces uniqueness, even outside FileSystem upserts.
    for deleted_at in ["1970-01-01 00:00:00", "2025-01-01 00:00:00"] {
        assert!(sql::statement("INSERT INTO catalog (uuid, url, size, format, deleted_at) VALUES (?, 's3://bucket/existing', 1, 'bin', ?)")
            .bind(Uuid::new_v4()).bind(deleted_at).exec(&mut db).await.is_err());
    }
    assert_eq!(
        MigrationSet::new(&migrations.migrations()[..11])
            .apply(&db)
            .await
            .unwrap()
            .applied(),
        0
    );
    cleanup(&url, &mut db).await;
}

#[tokio::test]
async fn catalog_url_migration_rejects_existing_duplicates_without_rewriting_them() {
    let Some(url) = database_url() else { return };
    let mut db = legacy_database(&url).await;
    let migrations: &'static MigrationSet = &MIGRATIONS;
    MigrationSet::new(&migrations.migrations()[..10])
        .apply(&db)
        .await
        .unwrap();
    for _ in 0..2 {
        sql::statement("INSERT INTO catalog (uuid, url, size, format) VALUES (?, 's3://bucket/duplicate', 1, 'bin')")
            .bind(Uuid::new_v4()).exec(&mut db).await.unwrap();
    }
    assert!(initialize(&url).await.is_err());
    let catalogs = sql::query("SELECT uuid, url FROM catalog")
        .exec(&mut db)
        .await
        .unwrap();
    assert_eq!(catalogs.len(), 2);
    assert!(
        catalogs
            .iter()
            .all(|c| c.as_record_unwrap()[1] == "s3://bucket/duplicate")
    );
    sql::statement("UPDATE catalog SET url = 's3://bucket/resolved-duplicate' WHERE uuid = ?")
        .bind(catalogs[0].as_record_unwrap()[0].as_str().unwrap())
        .exec(&mut db)
        .await
        .unwrap();
    assert_eq!(
        initialize(&url).await.unwrap().applied(),
        MIGRATIONS.migrations().len() - 10
    );
    cleanup(&url, &mut db).await;
}

#[tokio::test]
async fn catalog_url_migration_rejects_overlong_urls_without_truncating_them() {
    let Some(url) = database_url() else { return };
    let mut db = legacy_database(&url).await;
    let migrations: &'static MigrationSet = &MIGRATIONS;
    MigrationSet::new(&migrations.migrations()[..10])
        .apply(&db)
        .await
        .unwrap();
    let uuid = Uuid::new_v4();
    let object_url = format!("s3://bucket/{}", "x".repeat(751 - "s3://bucket/".len()));
    sql::statement("INSERT INTO catalog (uuid, url, size, format) VALUES (?, ?, 1, 'bin')")
        .bind(uuid)
        .bind(object_url.clone())
        .exec(&mut db)
        .await
        .unwrap();
    assert!(initialize(&url).await.is_err());
    let stored = sql::query("SELECT url FROM catalog WHERE uuid = ?")
        .bind(uuid)
        .exec(&mut db)
        .await
        .unwrap();
    assert_eq!(stored[0].as_record_unwrap()[0], object_url);
    sql::statement("UPDATE catalog SET url = 's3://bucket/short' WHERE uuid = ?")
        .bind(uuid)
        .exec(&mut db)
        .await
        .unwrap();
    assert_eq!(
        initialize(&url).await.unwrap().applied(),
        MIGRATIONS.migrations().len() - 10
    );
    cleanup(&url, &mut db).await;
}

#[tokio::test]
async fn catalog_and_file_migrations_remove_deletion_fields_and_preserve_records() {
    let Some(url) = database_url() else { return };
    let mut db = legacy_database(&url).await;
    let migrations: &'static MigrationSet = &MIGRATIONS;
    MigrationSet::new(&migrations.migrations()[..11])
        .apply(&db)
        .await
        .unwrap();
    let catalog = Uuid::new_v4();
    let file = Uuid::new_v4();
    sql::statement("INSERT INTO catalog (uuid, url, size, format, uploaded, created_at, deleted_at, updated_at) VALUES (?, 's3://bucket/kept', 42, 'bin', TRUE, '2025-01-01', '2025-02-01', '2025-03-01')")
        .bind(catalog).exec(&mut db).await.unwrap();
    sql::statement("INSERT INTO file (uuid, fullname, filename, catalog_uuid, created_at, deleted_at, updated_at) VALUES (?, '/kept', 'kept', ?, '2025-01-01', '2025-02-01', '2025-03-01')")
        .bind(file).bind(catalog).exec(&mut db).await.unwrap();
    let original = sql::query("SELECT f.uuid, f.fullname, f.catalog_uuid, f.created_at, f.updated_at, c.uuid, c.url, c.size, c.format, c.uploaded, c.created_at, c.updated_at FROM file f JOIN catalog c ON c.uuid = f.catalog_uuid WHERE f.uuid = ?")
        .bind(file).exec(&mut db).await.unwrap();
    assert_eq!(
        MigrationSet::new(&migrations.migrations()[..18])
            .apply(&db)
            .await
            .unwrap()
            .applied(),
        18 - 11
    );
    let stored = db.get_catalog(catalog).await.unwrap().unwrap();
    let preserved = sql::query("SELECT f.uuid, f.fullpath, f.catalog_uuid, f.created_at, f.updated_at, c.uuid, c.url, c.size, c.format, c.uploaded, c.created_at, c.updated_at FROM file f JOIN catalog c ON c.uuid = f.catalog_uuid WHERE f.uuid = ?")
        .bind(file).exec(&mut db).await.unwrap();
    assert_eq!(preserved, original);
    assert!(stored.uploaded);
    assert!(sql::query("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN ('catalog', 'file') AND COLUMN_NAME = 'deleted_at'")
        .exec(&mut db).await.unwrap().is_empty());
    assert!(
        serde_json::to_value(&stored)
            .unwrap()
            .get("deleted_at")
            .is_none()
    );
    assert!(
        sql::statement(
            "INSERT INTO catalog (uuid, `key`, url, size, format) VALUES (?, '/other', 's3://bucket/kept', 1, 'bin')"
        )
        .bind(Uuid::new_v4())
        .exec(&mut db)
        .await
        .is_err()
    );
    assert!(sql::statement("INSERT INTO file (uuid, fullpath, filename, catalog_uuid) VALUES (?, '/kept', 'other', ?)")
        .bind(Uuid::new_v4()).bind(catalog).exec(&mut db).await.is_err());
    assert_eq!(
        MigrationSet::new(&migrations.migrations()[..18])
            .apply(&db)
            .await
            .unwrap()
            .applied(),
        0
    );
    cleanup(&url, &mut db).await;
}

#[tokio::test]
async fn removal_of_deleted_at_rejects_historical_conflicts_and_can_be_retried() {
    let Some(url) = database_url() else { return };
    let mut db = legacy_database(&url).await;
    let migrations: &'static MigrationSet = &MIGRATIONS;
    MigrationSet::new(&migrations.migrations()[..11])
        .apply(&db)
        .await
        .unwrap();
    let catalog = Uuid::new_v4();
    let old_catalog = Uuid::new_v4();
    let file = Uuid::new_v4();
    let old_file = Uuid::new_v4();
    for (uuid, deleted_at) in [(catalog, "1970-01-01"), (old_catalog, "2025-01-01")] {
        sql::statement("INSERT INTO catalog (uuid, url, size, format, deleted_at) VALUES (?, 's3://bucket/conflict', 1, 'bin', ?)")
            .bind(uuid).bind(deleted_at).exec(&mut db).await.unwrap();
    }
    for (uuid, deleted_at) in [(file, "1970-01-01"), (old_file, "2025-01-01")] {
        sql::statement("INSERT INTO file (uuid, fullname, filename, catalog_uuid, deleted_at) VALUES (?, '/conflict', 'conflict', ?, ?)")
            .bind(uuid).bind(catalog).bind(deleted_at).exec(&mut db).await.unwrap();
    }
    assert!(
        MigrationSet::new(&migrations.migrations()[..18])
            .apply(&db)
            .await
            .is_err()
    );
    assert_eq!(
        sql::query("SELECT uuid FROM catalog")
            .exec(&mut db)
            .await
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        sql::query("SELECT uuid FROM file")
            .exec(&mut db)
            .await
            .unwrap()
            .len(),
        2
    );
    sql::statement("UPDATE catalog SET url = 's3://bucket/archived' WHERE uuid = ?")
        .bind(old_catalog)
        .exec(&mut db)
        .await
        .unwrap();
    // Catalog migration commits; the independent file migration still fails.
    assert!(
        MigrationSet::new(&migrations.migrations()[..18])
            .apply(&db)
            .await
            .is_err()
    );
    assert!(sql::query("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'catalog' AND COLUMN_NAME = 'deleted_at'")
        .exec(&mut db).await.unwrap().is_empty());
    sql::statement("UPDATE file SET fullname = '/archived' WHERE uuid = ?")
        .bind(old_file)
        .exec(&mut db)
        .await
        .unwrap();
    assert_eq!(
        MigrationSet::new(&migrations.migrations()[..18])
            .apply(&db)
            .await
            .unwrap()
            .applied(),
        18 - 12
    );
    assert_eq!(Catalog::all().exec(&mut db).await.unwrap().len(), 2);
    assert_eq!(
        sql::query("SELECT uuid FROM file")
            .exec(&mut db)
            .await
            .unwrap()
            .len(),
        2
    );
    let row = sql::query("SELECT catalog_uuid FROM file WHERE uuid=?")
        .bind(old_file)
        .exec(&mut db)
        .await
        .unwrap();
    assert_eq!(row[0].as_record_unwrap()[0], catalog.to_string());
    cleanup(&url, &mut db).await;
}

#[tokio::test]
async fn auth_migrations_add_numeric_ids_and_preserve_identities_and_credentials() {
    let Some(url) = database_url() else { return };
    let mut db = legacy_database(&url).await;
    let migrations: &'static MigrationSet = &MIGRATIONS;
    MigrationSet::new(&migrations.migrations()[..13])
        .apply(&db)
        .await
        .unwrap();
    let user = server::auth::User::new("local", "alice", "Alice".into(), None);
    let profile = serde_json::to_value(&user).unwrap();
    let timestamp: Timestamp = "2026-01-02T03:04:05.123456Z".parse().unwrap();
    // Old schemas use natural primary keys and TIMESTAMP columns.
    sql::statement("INSERT INTO auth_user (id, profile, updated_at) VALUES (?, ?, ?)")
        .bind(user.id.clone())
        .bind(profile.to_string())
        .bind(timestamp)
        .exec(&mut db)
        .await
        .unwrap();
    sql::statement("INSERT INTO auth_session (token_hash, user_id, expires_at) VALUES (?, ?, ?)")
        .bind("a".repeat(64))
        .bind(user.id.clone())
        .bind(timestamp)
        .exec(&mut db)
        .await
        .unwrap();
    sql::statement("INSERT INTO auth_login_flow (state_hash, browser_hash, nonce, verifier, expires_at) VALUES (?, ?, 'nonce', 'verifier', ?)")
        .bind("b".repeat(64)).bind("c".repeat(64)).bind(timestamp).exec(&mut db).await.unwrap();
    assert_eq!(
        initialize(&url).await.unwrap().applied(),
        MIGRATIONS.migrations().len() - 13
    );
    let stored = UserRecord::filter_by_user_id(&user.id)
        .get(&mut db)
        .await
        .unwrap();
    let session = Session::filter_by_token_hash("a".repeat(64))
        .get(&mut db)
        .await
        .unwrap();
    let flow = LoginFlow::filter_by_state_hash("b".repeat(64))
        .get(&mut db)
        .await
        .unwrap();
    assert!(stored.id > 0 && session.id > 0 && flow.id > 0);
    assert_eq!(stored.user_id, user.id);
    assert_eq!(stored.profile, profile);
    assert_eq!(stored.updated_at, timestamp);
    assert_eq!(session.user_id, user.id);
    assert_eq!(session.expires_at, timestamp);
    assert_eq!(flow.browser_hash, "c".repeat(64));
    assert_eq!(flow.nonce, "nonce");
    assert_eq!(flow.verifier, "verifier");
    assert_eq!(flow.expires_at, timestamp);
    assert_eq!(initialize(&url).await.unwrap().applied(), 0);
    assert_eq!(
        UserRecord::filter_by_user_id(&user.id)
            .get(&mut db)
            .await
            .unwrap()
            .id,
        stored.id
    );
    assert_eq!(
        Session::filter_by_token_hash("a".repeat(64))
            .get(&mut db)
            .await
            .unwrap()
            .id,
        session.id
    );
    assert_eq!(
        LoginFlow::filter_by_state_hash("b".repeat(64))
            .get(&mut db)
            .await
            .unwrap()
            .id,
        flow.id
    );
    exercise_auth_schema(&mut db).await;
    cleanup(&url, &mut db).await;
}

#[tokio::test]
async fn auth_schema_snapshot_supports_numeric_ids_and_unique_credentials() {
    let Some(url) = database_url() else { return };
    let mut db = legacy_database(&url).await;
    // Exercise the fresh-install DDL, independently of the upgrade migrations.
    for table in ["auth_user", "auth_session", "auth_login_flow"] {
        let marker = format!("CREATE TABLE IF NOT EXISTS {table} (");
        let sql = include_str!("../../rayflow.sql");
        let start = sql.find(&marker).unwrap();
        let end = start + sql[start..].find(';').unwrap() + 1;
        sql::statement(&sql[start..end])
            .exec(&mut db)
            .await
            .unwrap();
    }
    exercise_auth_schema(&mut db).await;
    cleanup(&url, &mut db).await;
}

async fn exercise_auth_schema(db: &mut Db) {
    let future: Timestamp = "2045-06-07T08:09:10.123456Z".parse().unwrap();
    let user = server::auth::User::new("test", "new", "张三 🦀".into(), None);
    let profile = serde_json::to_value(&user).unwrap();
    let stored = UserRecord::create()
        .user_id(&user.id)
        .profile(profile.clone())
        .updated_at(future)
        .exec(db)
        .await
        .unwrap();
    let second = UserRecord::create()
        .user_id("d".repeat(64))
        .profile(profile.clone())
        .updated_at(future)
        .exec(db)
        .await
        .unwrap();
    assert!(stored.id > 0 && second.id > stored.id);
    let session = Session::create()
        .token_hash("e".repeat(64))
        .user_id(&user.id)
        .expires_at(future)
        .exec(db)
        .await
        .unwrap();
    let flow = LoginFlow::create()
        .state_hash("f".repeat(64))
        .browser_hash("b".repeat(64))
        .nonce("nonce")
        .verifier("verifier")
        .expires_at(future)
        .exec(db)
        .await
        .unwrap();
    assert!(session.id > 0 && flow.id > 0);
    assert_eq!(
        UserRecord::filter_by_id(stored.id)
            .get(db)
            .await
            .unwrap()
            .profile,
        profile
    );
    assert_eq!(
        Session::filter_by_id(session.id)
            .get(db)
            .await
            .unwrap()
            .expires_at,
        future
    );
    assert_eq!(
        LoginFlow::filter_by_id(flow.id)
            .get(db)
            .await
            .unwrap()
            .expires_at,
        future
    );
    assert!(
        UserRecord::create()
            .user_id(&user.id)
            .profile(profile)
            .updated_at(future)
            .exec(db)
            .await
            .is_err()
    );
    assert!(
        Session::create()
            .token_hash(&session.token_hash)
            .user_id(&user.id)
            .expires_at(future)
            .exec(db)
            .await
            .is_err()
    );
    assert!(
        LoginFlow::create()
            .state_hash(&flow.state_hash)
            .browser_hash("b".repeat(64))
            .nonce("other")
            .verifier("other")
            .expires_at(future)
            .exec(db)
            .await
            .is_err()
    );
    assert_eq!(sql::query("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN ('auth_user', 'auth_session', 'auth_login_flow') AND ENGINE = 'InnoDB' AND TABLE_COLLATION = 'utf8mb4_bin' AND TABLE_COMMENT <> ''")
        .exec(db).await.unwrap().len(), 3);
    assert_eq!(sql::query("SELECT TABLE_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN ('auth_user', 'auth_session', 'auth_login_flow') AND COLUMN_NAME = 'id' AND COLUMN_KEY = 'PRI' AND DATA_TYPE = 'bigint' AND COLUMN_TYPE LIKE '%unsigned' AND EXTRA = 'auto_increment'")
        .exec(db).await.unwrap().len(), 3);
    assert_eq!(sql::query("SELECT TABLE_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN ('auth_user', 'auth_session', 'auth_login_flow') AND COLUMN_NAME IN ('updated_at', 'expires_at') AND DATA_TYPE = 'datetime' AND DATETIME_PRECISION = 6")
        .exec(db).await.unwrap().len(), 3);
    // Credential lookups still support revocation and single-use flow removal.
    Session::filter_by_token_hash(&session.token_hash)
        .delete()
        .exec(db)
        .await
        .unwrap();
    LoginFlow::filter_by_state_hash(&flow.state_hash)
        .delete()
        .exec(db)
        .await
        .unwrap();
    assert!(
        Session::filter_by_token_hash(&session.token_hash)
            .first()
            .exec(db)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        LoginFlow::filter_by_state_hash(&flow.state_hash)
            .first()
            .exec(db)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn catalog_uploaded_migration_defaults_to_unacknowledged_and_preserves_metadata() {
    let Some(url) = database_url() else { return };
    let mut db = legacy_database(&url).await;
    let migrations: &'static MigrationSet = &MIGRATIONS;
    MigrationSet::new(&migrations.migrations()[..9])
        .apply(&db)
        .await
        .unwrap();
    let uuid = Uuid::new_v4();
    // Seed the old schema, which has no uploaded column for the current model.
    sql::statement("INSERT INTO catalog (uuid, url, size, format) VALUES (?, 's3://bucket/existing', 42, 'bin')")
        .bind(uuid).exec(&mut db).await.unwrap();
    let file_uuid = Uuid::new_v4();
    sql::statement("INSERT INTO file (uuid, fullname, filename, catalog_uuid) VALUES (?, '/existing', 'existing', ?)")
        .bind(file_uuid).bind(uuid).exec(&mut db).await.unwrap();
    let report = MigrationSet::new(&migrations.migrations()[..18])
        .apply(&db)
        .await
        .unwrap();
    assert_eq!(report.applied(), 18 - 9);
    let existing = db.get_catalog(uuid).await.unwrap().unwrap();
    assert!(!existing.uploaded);
    assert_eq!(existing.url, "s3://bucket/existing");
    assert_eq!(existing.size, 42);
    let row = sql::query("SELECT catalog_uuid FROM file WHERE uuid=?")
        .bind(file_uuid)
        .exec(&mut db)
        .await
        .unwrap();
    assert_eq!(row[0].as_record_unwrap()[0], existing.uuid.to_string());
    Catalog::filter_by_uuid(uuid)
        .update()
        .uploaded(true)
        .exec(&mut db)
        .await
        .unwrap();
    assert_eq!(
        MigrationSet::new(&migrations.migrations()[..18])
            .apply(&db)
            .await
            .unwrap()
            .applied(),
        0
    );
    assert!(db.get_catalog(uuid).await.unwrap().unwrap().uploaded);
    let created = db
        .upsert_catalog(&CatalogInput::new("/new", "s3://bucket/new", 0, "bin"))
        .await
        .unwrap();
    assert!(!db.get_catalog(created).await.unwrap().unwrap().uploaded);
    cleanup(&url, &mut db).await;
}

#[tokio::test]
async fn run_config_migration_preserves_annotations_and_job_references() {
    let Some(url) = database_url() else { return };
    let mut db = legacy_database(&url).await;
    let migrations: &'static MigrationSet = &MIGRATIONS;
    MigrationSet::new(&migrations.migrations()[..8])
        .apply(&db)
        .await
        .unwrap();
    let run = create_run(&mut db, Uuid::new_v4(), Uuid::new_v4(), 4).await;
    Run::filter_by_id(run.id)
        .update()
        .revision(6)
        .exec(&mut db)
        .await
        .unwrap();
    // Legacy columns no longer exist in the ORM, so seed them using SQL.
    for (id, version, revision, label) in [(10u64, 1u64, 0u64, "original"), (11, 2, 2, "latest")] {
        sql::statement("INSERT INTO run_config (id, run_id, version, revision, annotation, created_by, updated_by) VALUES (?, ?, ?, ?, ?, 'creator', 'updater')")
            .bind(id).bind(run.uuid).bind(version).bind(revision)
            .bind(serde_json::json!({"label":label}).to_string())
            .exec(&mut db).await.unwrap();
    }
    let original_job = Uuid::new_v4();
    sql::statement("INSERT INTO job (uuid, task_id, run_id, run_config_id) VALUES (?, ?, ?, 10)")
        .bind(original_job)
        .bind(Uuid::new_v4())
        .bind(run.uuid)
        .exec(&mut db)
        .await
        .unwrap();

    let report = initialize(&url).await.unwrap();
    assert_eq!(report.applied(), MIGRATIONS.migrations().len() - 8);
    assert_eq!(report.skipped(), 8);
    assert!(sql::query("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'run_config' AND COLUMN_NAME IN ('version', 'revision')")
        .exec(&mut db).await.unwrap().is_empty());
    assert!(sql::query("SELECT INDEX_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'run_config' AND INDEX_NAME = 'uk_run_config_run_revision'")
        .exec(&mut db).await.unwrap().is_empty());
    let configs = RunConfig::filter_by_run_id(run.uuid)
        .order_by(RunConfig::fields().id().asc())
        .exec(&mut db)
        .await
        .unwrap();
    assert_eq!(configs.len(), 2);
    for (config, id, label) in [(&configs[0], 10, "original"), (&configs[1], 11, "latest")] {
        assert_eq!(config.id, id);
        assert_eq!(config.annotation.0["label"], label);
        assert_eq!(config.created_by, "creator");
        assert_eq!(config.updated_by, "updater");
        let json = serde_json::to_value(config).unwrap();
        assert!(json.get("version").is_none());
        assert!(json.get("revision").is_none());
    }
    assert_eq!(
        Job::filter_by_uuid(original_job)
            .get(&mut db)
            .await
            .unwrap()
            .run_config_id,
        10
    );
    let current = Run::filter_by_id(run.id).get(&mut db).await.unwrap();
    assert_eq!((current.version, current.revision), (4, 6));

    let now = Timestamp::now();
    let node_id = Uuid::new_v4();
    let node_run = NodeRun::create()
        .uuid(Uuid::new_v4())
        .run_id(run.uuid)
        .data_id(run.data_id)
        .graph_id(run.graph_id)
        .version(1)
        .node_id(node_id)
        .revision(0)
        .status("Ready")
        .created_at(now)
        .updated_at(now)
        .exec(&mut db)
        .await
        .unwrap();
    let mut selected_node_config = 0;
    for version in [3, 5] {
        let config = NodeConfig::create()
            .node_id(node_id)
            .version(version)
            .annotation_match("")
            .config(serde_json::json!({}))
            .lease(Some(30_000))
            .required_cpu_cores(1)
            .required_memory_bytes(1024)
            .created_at(now)
            .updated_at(now)
            .created_by("")
            .updated_by("")
            .exec(&mut db)
            .await
            .unwrap();
        if version == 3 {
            selected_node_config = config.id;
        }
    }
    let task = Task::new(run.data_id, run.graph_id, node_id, node_run.id);
    task.emit(&mut db).await.unwrap();
    let first = Job::create_for_task(&task, &mut db).await.unwrap();
    assert_eq!(first.run_config_id, 11);
    assert_eq!(
        first.node_config_id, selected_node_config,
        "node settings use Run.version"
    );

    // New annotation snapshots need neither execution field. Existing jobs keep
    // their original IDs, while new jobs pick the newest snapshot for their run.
    let other = create_run(&mut db, Uuid::new_v4(), Uuid::new_v4(), 7).await;
    let mut newest = 0;
    for run_id in [run.uuid, other.uuid] {
        let config = RunConfig::create()
            .run_id(run_id)
            .annotation(toasty::Json(std::collections::HashMap::new()))
            .created_at(now)
            .updated_at(now)
            .created_by("")
            .updated_by("")
            .exec(&mut db)
            .await
            .unwrap();
        if run_id == run.uuid {
            newest = config.id;
        }
    }
    let next_task = Task::new(run.data_id, run.graph_id, node_id, node_run.id);
    next_task.emit(&mut db).await.unwrap();
    let next = Job::create_for_task(&next_task, &mut db).await.unwrap();
    assert_eq!(next.run_config_id, newest);
    assert_eq!(
        Job::filter_by_uuid(first.uuid)
            .get(&mut db)
            .await
            .unwrap()
            .run_config_id,
        11
    );
    assert_eq!(initialize(&url).await.unwrap().applied(), 0);
    cleanup(&url, &mut db).await;
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
    sql::statement("INSERT INTO node_run (uuid, run_id, data_id, graph_id, node_id, revision) VALUES (?, ?, ?, ?, ?, 1)")
        .bind(Uuid::new_v4()).bind(run.uuid).bind(run.data_id).bind(run.graph_id).bind(node)
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
    let catalog_id = db
        .upsert_catalog(&CatalogInput::new("/key", "oss://bucket/key", 10, "text"))
        .await
        .unwrap();
    let file_id = db
        .upsert_file(&FileInput::new(Uuid::new_v4(), "key", catalog_id))
        .await
        .unwrap();
    let loaded = db.get_file(file_id).await.unwrap().unwrap();
    assert_eq!(loaded.file.uuid, file_id);
    assert_eq!(loaded.catalog.uuid, catalog_id);
    let now = Timestamp::now();
    let user = server::auth::User::new("local", "alice", "Alice".into(), None);
    server::auth::UserRecord::create()
        .user_id(user.id.clone())
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

#[tokio::test]
async fn node_run_uuid_and_file_owner_migrations_preserve_rows_and_encoded_paths() {
    let Some(url) = database_url() else { return };
    let mut db = legacy_database(&url).await;
    let migrations: &'static MigrationSet = &MIGRATIONS;
    MigrationSet::new(&migrations.migrations()[..18])
        .apply(&db)
        .await
        .unwrap();
    let graph_id = Uuid::new_v4();
    let node = Uuid::new_v4();
    let run = create_run(&mut db, Uuid::new_v4(), graph_id, 3).await;
    let name = "make/数据 %";
    let graph = serde_json::json!({"nodes":[{"id":node,"name":name}]});
    sql::statement("INSERT INTO graph (uuid, name, version, graph) VALUES (?, 'flow', 3, ?)")
        .bind(graph_id)
        .bind(graph.to_string())
        .exec(&mut db)
        .await
        .unwrap();
    sql::statement("INSERT INTO node_run (run_id,data_id,graph_id,version,node_id,revision,status) VALUES (?,?,?,3,?,2,'Succeeded')")
        .bind(run.uuid).bind(run.data_id).bind(graph_id).bind(node).exec(&mut db).await.unwrap();
    let old_row = sql::query("SELECT id,created_at,updated_at FROM node_run WHERE run_id=?")
        .bind(run.uuid)
        .exec(&mut db)
        .await
        .unwrap();
    const ENCODING: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'_')
        .remove(b'.')
        .remove(b'~');
    let encoded = percent_encoding::utf8_percent_encode(name, ENCODING).to_string();
    let key = format!("/prefix/{}/flow/3/{encoded}/2/value.bin", run.uuid);
    let catalog = Uuid::new_v4();
    let file = Uuid::new_v4();
    sql::statement(
        "INSERT INTO catalog (uuid,`key`,url,size,format,uploaded) VALUES (?,?,?,12,'bin',TRUE)",
    )
    .bind(catalog)
    .bind(key.clone())
    .bind(format!("s3://bucket{key}"))
    .exec(&mut db)
    .await
    .unwrap();
    sql::statement("INSERT INTO file (uuid,fullpath,filename,catalog_uuid,created_at,updated_at) VALUES (?,?,'value.bin',?,'2025-01-01','2025-03-01')")
        .bind(file).bind(key.clone()).bind(catalog).exec(&mut db).await.unwrap();
    assert_eq!(initialize(&url).await.unwrap().applied(), 3);
    let node_run = NodeRun::filter_by_run_id_and_node_id_and_revision(run.uuid, node, 2)
        .get(&mut db)
        .await
        .unwrap();
    assert!(!node_run.uuid.is_nil());
    let after = sql::query("SELECT id,created_at,updated_at FROM node_run WHERE run_id=?")
        .bind(run.uuid)
        .exec(&mut db)
        .await
        .unwrap();
    assert_eq!(old_row, after);
    let loaded = db.get_file(file).await.unwrap().unwrap();
    assert_eq!(loaded.file.node_run_id, node_run.uuid);
    assert_eq!(loaded.file.catalog_uuid, catalog);
    assert_eq!(loaded.catalog.key, key);
    assert_eq!(
        loaded.file.created_at,
        "2025-01-01T00:00:00Z".parse::<Timestamp>().unwrap()
    );
    assert_eq!(
        loaded.file.updated_at,
        "2025-03-01T00:00:00Z".parse::<Timestamp>().unwrap()
    );
    assert!(
        serde_json::to_value(loaded.file)
            .unwrap()
            .get("fullpath")
            .is_none()
    );
    assert_eq!(initialize(&url).await.unwrap().applied(), 0);
    assert_eq!(
        NodeRun::filter_by_id(node_run.id)
            .get(&mut db)
            .await
            .unwrap()
            .uuid,
        node_run.uuid
    );
    cleanup(&url, &mut db).await;
}

#[tokio::test]
async fn unresolved_file_ownership_blocks_path_removal_and_can_be_mapped_before_retry() {
    let Some(url) = database_url() else { return };
    let mut db = legacy_database(&url).await;
    let migrations: &'static MigrationSet = &MIGRATIONS;
    MigrationSet::new(&migrations.migrations()[..18])
        .apply(&db)
        .await
        .unwrap();
    let catalog = Uuid::new_v4();
    let file = Uuid::new_v4();
    sql::statement("INSERT INTO catalog (uuid,`key`,url,size,format) VALUES (?,'/standalone','s3://bucket/standalone',0,'bin')").bind(catalog).exec(&mut db).await.unwrap();
    sql::statement("INSERT INTO file (uuid,fullpath,filename,catalog_uuid) VALUES (?,'/standalone','standalone',?)").bind(file).bind(catalog).exec(&mut db).await.unwrap();
    assert!(initialize(&url).await.is_err());
    let rows = sql::query("SELECT fullpath,node_run_id FROM file WHERE uuid=?")
        .bind(file)
        .exec(&mut db)
        .await
        .unwrap();
    assert_eq!(rows[0].as_record_unwrap()[0], "/standalone");
    let run = create_run(&mut db, Uuid::new_v4(), Uuid::new_v4(), 1).await;
    let now = Timestamp::now();
    let owner = NodeRun::create()
        .uuid(Uuid::new_v4())
        .run_id(run.uuid)
        .data_id(run.data_id)
        .graph_id(run.graph_id)
        .version(1)
        .node_id(Uuid::new_v4())
        .revision(0)
        .status("Succeeded")
        .created_at(now)
        .updated_at(now)
        .exec(&mut db)
        .await
        .unwrap();
    sql::statement("UPDATE file SET node_run_id=? WHERE uuid=?")
        .bind(owner.uuid)
        .bind(file)
        .exec(&mut db)
        .await
        .unwrap();
    assert_eq!(initialize(&url).await.unwrap().applied(), 1);
    assert_eq!(
        db.get_file(file).await.unwrap().unwrap().file.node_run_id,
        owner.uuid
    );
    cleanup(&url, &mut db).await;
}
