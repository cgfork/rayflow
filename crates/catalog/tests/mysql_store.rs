//! Set DATABASE_URL to exercise MySQL persistence. Each test creates and drops
//! only its own randomly prefixed tables, using the deployed schema snapshot.

use catalog::{
    CatalogInput, CatalogOptionalData, CatalogSystem, Error, FileInput, FileKey, FileOptionalData,
    FileSystem,
    model::{Catalog, File},
};
use jiff::Timestamp;
use toasty::Db;
use uuid::Uuid;

async fn database() -> Option<(Db, String)> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping catalog MySQL test: DATABASE_URL is not set");
        return None;
    };
    let prefix = format!("catalog_test_{}_", Uuid::new_v4().simple());
    let mut db = Db::builder()
        .models(catalog::models())
        .table_name_prefix(&prefix)
        .connect(&url)
        .await
        .unwrap();
    // Raw DDL verifies rayflow.sql against the ORM; only test tables are created.
    for table in ["catalog", "file"] {
        let start = format!("CREATE TABLE IF NOT EXISTS {table} (");
        let ddl = include_str!("../../../rayflow.sql")
            .split("\n\n")
            .find(|statement| statement.starts_with(&start))
            .unwrap()
            .replacen(&start, &format!("CREATE TABLE `{prefix}{table}` ("), 1);
        toasty::sql::statement(ddl).exec(&mut db).await.unwrap();
    }
    Some((db, prefix))
}

async fn cleanup(db: &mut Db, prefix: &str) {
    assert!(prefix.starts_with("catalog_test_"));
    for table in ["file", "catalog"] {
        toasty::sql::statement(format!("DROP TABLE `{prefix}{table}`"))
            .exec(db)
            .await
            .unwrap();
    }
}

fn object() -> CatalogInput {
    CatalogInput::new("/a/b.csv", "s3://bucket/a/b.csv", 10, "text/csv")
}
fn file(catalog: Uuid, fullpath: &str) -> FileInput<Uuid> {
    FileInput::new(fullpath, "b.csv", catalog)
}

#[tokio::test]
async fn data_only_upserts_reuse_ids_by_key_and_fullpath() {
    let Some((mut db, prefix)) = database().await else {
        return;
    };
    let mut data = object();
    let id = db
        .upsert_catalog(&data as &dyn catalog::Catalog)
        .await
        .unwrap();
    let original = db.get_catalog(id).await.unwrap().unwrap();
    assert!(original.id > 0 && !id.is_nil());
    assert!(original.created_at > Timestamp::UNIX_EPOCH);
    data.url = "oss://other-bucket/moved.csv".into();
    data.size = u64::MAX;
    data.format = "csv".into();
    assert_eq!(db.upsert_catalog(&data).await.unwrap(), id);
    let changed = db.get_catalog(id).await.unwrap().unwrap();
    assert_eq!(changed.id, original.id);
    assert_eq!(changed.created_at, original.created_at);
    assert_eq!(changed.key, original.key);
    assert_eq!(changed.url, data.url);
    assert_eq!(changed.size, data.size);
    assert_eq!(Catalog::all().exec(&mut db).await.unwrap().len(), 1);
    let mut data = file(id, "/data/b.csv");
    let file_id = db
        .upsert_file(&data as &dyn catalog::File<CatalogId = Uuid>)
        .await
        .unwrap();
    let original = db.get_file(FileKey::Id(file_id)).await.unwrap().unwrap();
    data.filename = "new.csv".into();
    assert_eq!(db.upsert_file(&data).await.unwrap(), file_id);
    let changed = db
        .get_file(FileKey::Fullpath(&data.fullpath))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(changed.file.id, original.file.id);
    assert_eq!(changed.file.created_at, original.file.created_at);
    assert_eq!(changed.file.filename, data.filename);
    assert_eq!(changed.catalog, original.catalog);
    assert_eq!(File::all().exec(&mut db).await.unwrap().len(), 1);
    assert_ne!(
        db.upsert_file(&file(id, "/data/B.csv")).await.unwrap(),
        file_id
    );
    assert_ne!(
        db.upsert_catalog(&CatalogInput::new(
            "/a/B.csv",
            "s3://bucket/a/B.csv",
            10,
            "csv"
        ))
        .await
        .unwrap(),
        id
    );
    cleanup(&mut db, &prefix).await;
}

#[tokio::test]
async fn catalog_keys_are_unique_and_can_be_renamed_by_identity() {
    let Some((mut db, prefix)) = database().await else {
        return;
    };
    let data = object();
    let id = db.upsert_catalog(&data).await.unwrap();
    let file_id = db.upsert_file(&file(id, &data.key)).await.unwrap();
    let original = db.get_catalog(id).await.unwrap().unwrap();
    let now = Timestamp::now();
    assert!(
        Catalog::create()
            .uuid(Uuid::new_v4())
            .key(&data.key)
            .url("s3://bucket/duplicate-key")
            .size(0_u64)
            .format("bin")
            .created_at(now)
            .updated_at(now)
            .exec(&mut db)
            .await
            .is_err()
    );
    let other = db
        .upsert_catalog(&CatalogInput::new("/other", "s3://bucket/other", 0, "bin"))
        .await
        .unwrap();
    assert!(matches!(
        db.update_catalog(
            other,
            CatalogOptionalData {
                key: Some(data.key.clone()),
                ..Default::default()
            }
        )
        .await,
        Err(Error::Database(_))
    ));
    for key in [String::new(), "bad\0key".into(), "k".repeat(751)] {
        assert!(matches!(
            db.update_catalog(
                id,
                CatalogOptionalData {
                    key: Some(key),
                    ..Default::default()
                }
            )
            .await,
            Err(Error::InvalidField("key"))
        ));
    }
    db.update_catalog(
        id,
        CatalogOptionalData {
            key: Some("/renamed".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        db.upsert_catalog(CatalogOptionalData {
            key: Some("/renamed".into()),
            ..Default::default()
        })
        .await
        .unwrap(),
        id
    );
    let loaded = db.get_file(FileKey::Id(file_id)).await.unwrap().unwrap();
    assert_eq!(loaded.catalog.key, "/renamed");
    assert_eq!(loaded.catalog.uuid, original.uuid);
    assert_eq!(loaded.catalog.created_at, original.created_at);
    assert_eq!(loaded.file.fullpath, data.key);
    cleanup(&mut db, &prefix).await;
}

#[tokio::test]
async fn file_upserts_validate_references_and_deletion_releases_names() {
    let Some((mut db, prefix)) = database().await else {
        return;
    };
    let id = db.upsert_catalog(&object()).await.unwrap();
    let file_id = db.upsert_file(&file(id, "/a")).await.unwrap();
    assert!(matches!(
        db.upsert_file(&file(Uuid::new_v4(), "/a")).await,
        Err(Error::CatalogNotFound(_))
    ));
    assert_eq!(
        db.get_file(FileKey::Id(file_id))
            .await
            .unwrap()
            .unwrap()
            .catalog
            .uuid,
        id
    );
    Catalog::filter_by_uuid(id)
        .delete()
        .exec(&mut db)
        .await
        .unwrap();
    assert!(db.get_catalog(id).await.unwrap().is_none());
    assert!(db.get_file(FileKey::Id(file_id)).await.unwrap().is_none());
    let replacement = db.upsert_catalog(&object()).await.unwrap();
    assert_ne!(replacement, id);
    assert_eq!(
        db.upsert_file(&file(replacement, "/a")).await.unwrap(),
        file_id
    );
    assert_eq!(
        db.get_file(FileKey::Id(file_id))
            .await
            .unwrap()
            .unwrap()
            .catalog
            .uuid,
        replacement
    );
    File::filter_by_uuid(file_id)
        .delete()
        .exec(&mut db)
        .await
        .unwrap();
    assert_ne!(
        db.upsert_file(&file(replacement, "/a")).await.unwrap(),
        file_id
    );
    cleanup(&mut db, &prefix).await;
}

#[tokio::test]
async fn callers_can_roll_back_catalog_and_file_upserts_together() {
    let Some((mut db, prefix)) = database().await else {
        return;
    };
    let mut tx = db.transaction().await.unwrap();
    let id = tx.upsert_catalog(&object()).await.unwrap();
    let file_id = tx.upsert_file(&file(id, "/a")).await.unwrap();
    let mut changed = object();
    changed.size = 20;
    changed.uploaded = true;
    assert_eq!(tx.upsert_catalog(&changed).await.unwrap(), id);
    assert_eq!(
        tx.upsert_file(&FileInput::new("/a", "new.csv", id))
            .await
            .unwrap(),
        file_id
    );
    tx.rollback().await.unwrap();
    assert!(db.get_catalog(id).await.unwrap().is_none());
    assert!(File::all().exec(&mut db).await.unwrap().is_empty());
    cleanup(&mut db, &prefix).await;
}

#[tokio::test]
async fn concurrent_upserts_converge_on_one_identity_after_retry() {
    let Some((mut db, prefix)) = database().await else {
        return;
    };
    let mut writer = db.clone();
    let data = object();
    let (a, b) = tokio::join!(db.upsert_catalog(&data), writer.upsert_catalog(&data));
    assert!(a.is_ok() || b.is_ok());
    let mut ids = Vec::new();
    for result in [a, b] {
        ids.push(match result {
            Ok(id) => id,
            Err(Error::Database(error)) => {
                // Select-then-insert can race on the unique key. Retry with a fresh transaction.
                assert!(
                    error.is_serialization_failure() || error.to_string().contains("1062 (23000)"),
                    "{error}"
                );
                db.upsert_catalog(&data).await.unwrap()
            }
            Err(error) => panic!("unexpected upsert error: {error}"),
        });
    }
    let id = ids[0];
    assert_eq!(id, ids[1]);
    let data = file(id, "/shared");
    let (a, b) = tokio::join!(db.upsert_file(&data), writer.upsert_file(&data));
    assert!(a.is_ok() || b.is_ok());
    let mut ids = Vec::new();
    for result in [a, b] {
        ids.push(match result {
            Ok(id) => id,
            Err(Error::Database(error)) => {
                assert!(
                    error.is_serialization_failure() || error.to_string().contains("1062 (23000)"),
                    "{error}"
                );
                db.upsert_file(&data).await.unwrap()
            }
            Err(error) => panic!("unexpected upsert error: {error}"),
        });
    }
    assert_eq!(ids[0], ids[1]);
    assert_eq!(Catalog::all().exec(&mut db).await.unwrap().len(), 1);
    assert_eq!(File::all().exec(&mut db).await.unwrap().len(), 1);
    cleanup(&mut db, &prefix).await;
}

#[tokio::test]
async fn optional_catalog_fields_preserve_omissions_and_write_false_and_zero() {
    let Some((mut db, prefix)) = database().await else {
        return;
    };
    let data = object();
    let id = db.upsert_catalog(&data).await.unwrap();
    let original = db.get_catalog(id).await.unwrap().unwrap();
    let mut patch = CatalogOptionalData {
        key: Some(data.key.clone()),
        ..Default::default()
    };
    assert_eq!(db.upsert_catalog(&patch).await.unwrap(), id);
    assert_eq!(db.get_catalog(id).await.unwrap().unwrap(), original);

    patch.uploaded = Some(true);
    assert_eq!(db.upsert_catalog(&patch).await.unwrap(), id);
    let acknowledged = db.get_catalog(id).await.unwrap().unwrap();
    assert!(acknowledged.uploaded);
    assert_eq!(acknowledged.size, original.size);
    assert_eq!(acknowledged.format, original.format);
    assert_eq!(acknowledged.created_at, original.created_at);

    patch.uploaded = None;
    patch.size = Some(0);
    db.upsert_catalog(&patch).await.unwrap();
    let empty = db.get_catalog(id).await.unwrap().unwrap();
    assert_eq!(empty.size, 0);
    assert!(empty.uploaded);
    assert_eq!(empty.format, original.format);

    patch.size = None;
    patch.format = Some("bin".into());
    patch.uploaded = Some(false);
    db.upsert_catalog(patch).await.unwrap();
    let reset = db.get_catalog(id).await.unwrap().unwrap();
    assert_eq!(reset.size, 0);
    assert_eq!(reset.format, "bin");
    assert!(!reset.uploaded);
    assert_eq!(
        (reset.id, reset.uuid, reset.created_at),
        (original.id, original.uuid, original.created_at)
    );
    assert_eq!(Catalog::all().exec(&mut db).await.unwrap().len(), 1);
    cleanup(&mut db, &prefix).await;
}

#[tokio::test]
async fn optional_catalog_data_requires_creation_fields_and_participates_in_rollback() {
    let Some((mut db, prefix)) = database().await else {
        return;
    };
    let mut tx = db.transaction().await.unwrap();
    assert!(matches!(
        tx.upsert_catalog(CatalogOptionalData::default()).await,
        Err(Error::MissingField("key"))
    ));
    let mut data = CatalogOptionalData {
        key: Some(object().key),
        ..Default::default()
    };
    assert!(matches!(
        tx.upsert_catalog(&data).await,
        Err(Error::MissingField("url"))
    ));
    data.url = Some(object().url);
    assert!(matches!(
        tx.upsert_catalog(&data).await,
        Err(Error::MissingField("size"))
    ));
    data.size = Some(0);
    assert!(matches!(
        tx.upsert_catalog(&data).await,
        Err(Error::MissingField("format"))
    ));
    data.format = Some(String::new());
    assert!(matches!(
        tx.upsert_catalog(&data).await,
        Err(Error::InvalidField("format"))
    ));
    assert!(Catalog::all().exec(&mut tx).await.unwrap().is_empty());
    data.format = Some("bin".into());
    let id = tx.upsert_catalog(&data).await.unwrap();
    assert!(!tx.get_catalog(id).await.unwrap().unwrap().uploaded);
    tx.commit().await.unwrap();
    let original = db.get_catalog(id).await.unwrap().unwrap();

    let mut tx = db.transaction().await.unwrap();
    data.size = None;
    data.format = None;
    data.uploaded = Some(true);
    assert_eq!(tx.upsert_catalog(&data).await.unwrap(), id);
    assert!(tx.get_catalog(id).await.unwrap().unwrap().uploaded);
    tx.rollback().await.unwrap();
    assert_eq!(db.get_catalog(id).await.unwrap().unwrap(), original);
    cleanup(&mut db, &prefix).await;
}

#[tokio::test]
async fn sparse_writes_preserve_fields_changed_since_the_callers_snapshot() {
    let Some((mut db, prefix)) = database().await else {
        return;
    };
    let data = object();
    let id = db.upsert_catalog(&data).await.unwrap();
    let mut writer = db.clone();
    let mut tx = db.transaction().await.unwrap();
    assert_eq!(tx.get_catalog(id).await.unwrap().unwrap().size, data.size);
    writer
        .upsert_catalog(CatalogOptionalData {
            key: Some(data.key.clone()),
            size: Some(99),
            ..Default::default()
        })
        .await
        .unwrap();
    let patch = CatalogOptionalData {
        key: Some(data.key),
        uploaded: Some(true),
        ..Default::default()
    };
    match tx.upsert_catalog(&patch).await {
        Ok(updated) => {
            assert_eq!(updated, id);
            tx.commit().await.unwrap();
        }
        Err(Error::Database(error)) => {
            assert!(
                error.is_serialization_failure() || error.to_string().contains("1020 (HY000)"),
                "{error}"
            );
            tx.rollback().await.unwrap();
            db.upsert_catalog(&patch).await.unwrap();
        }
        Err(error) => panic!("unexpected upsert error: {error}"),
    }
    let current = db.get_catalog(id).await.unwrap().unwrap();
    assert_eq!(current.size, 99);
    assert_eq!(current.format, data.format);
    assert!(current.uploaded);
    cleanup(&mut db, &prefix).await;
}

#[tokio::test]
async fn caller_can_retry_upserts_after_a_concurrent_insert_into_its_snapshot() {
    let Some((mut db, prefix)) = database().await else {
        return;
    };
    let id = db.upsert_catalog(&object()).await.unwrap();
    let data = file(id, "/new");
    let mut writer = db.clone();
    let mut tx = db.transaction().await.unwrap();
    assert!(File::all().exec(&mut tx).await.unwrap().is_empty());
    let original = writer.upsert_file(&data).await.unwrap();
    let changed = FileInput::new("/new", "changed", id);
    match tx.upsert_file(&changed).await {
        Ok(id) => assert_eq!(id, original),
        Err(Error::Database(error)) => assert!(
            error.is_serialization_failure()
                || error.to_string().contains("1020 (HY000)")
                || error.to_string().contains("1062 (23000)"),
            "{error}"
        ),
        Err(error) => panic!("unexpected upsert error: {error}"),
    }
    tx.rollback().await.unwrap();
    assert_eq!(
        db.get_file(FileKey::Id(original))
            .await
            .unwrap()
            .unwrap()
            .file
            .filename,
        data.filename
    );
    assert_eq!(db.upsert_file(&changed).await.unwrap(), original);
    cleanup(&mut db, &prefix).await;
}

#[tokio::test]
async fn catalog_writes_persist_the_supplied_upload_acknowledgement() {
    let Some((mut db, prefix)) = database().await else {
        return;
    };
    for uploaded in [false, true] {
        let mut data = object();
        data.key = format!("/uploaded-{uploaded}");
        data.url = format!("s3://bucket/uploaded-{uploaded}");
        data.uploaded = uploaded;
        let id = db.upsert_catalog(&data).await.unwrap();
        let original = db.get_catalog(id).await.unwrap().unwrap();
        assert_eq!(original.uploaded, uploaded);
        for uploaded in [!uploaded, uploaded] {
            data.uploaded = uploaded;
            assert_eq!(db.upsert_catalog(&data).await.unwrap(), id);
            assert_eq!(
                db.get_catalog(id).await.unwrap().unwrap().uploaded,
                uploaded
            );
        }
        data.size += 1;
        data.format = "csv".into();
        data.uploaded = true;
        assert_eq!(db.upsert_catalog(&data).await.unwrap(), id);
        assert!(db.get_catalog(id).await.unwrap().unwrap().uploaded);

        // Owned records implement the same data contract as CatalogInput.
        let record = db.get_catalog(id).await.unwrap().unwrap();
        assert_eq!(record.created_at, original.created_at);
        assert_eq!(db.upsert_catalog(&record).await.unwrap(), id);
        assert!(db.get_catalog(id).await.unwrap().unwrap().uploaded);
    }
    cleanup(&mut db, &prefix).await;
}

#[tokio::test]
async fn optional_file_fields_preserve_omissions_and_require_creation_fields() {
    let Some((mut db, prefix)) = database().await else {
        return;
    };
    let catalog = db.upsert_catalog(&object()).await.unwrap();
    let replacement = db
        .upsert_catalog(&CatalogInput::new(
            "/replacement",
            "s3://bucket/replacement",
            0,
            "bin",
        ))
        .await
        .unwrap();
    let mut data = FileOptionalData::<Uuid>::default();
    assert!(matches!(
        db.upsert_file(&data).await,
        Err(Error::MissingField("fullpath"))
    ));
    data.fullpath = Some("/partial".into());
    assert!(matches!(
        db.upsert_file(&data).await,
        Err(Error::MissingField("filename"))
    ));
    data.filename = Some("first".into());
    assert!(matches!(
        db.upsert_file(&data).await,
        Err(Error::MissingField("catalog_id"))
    ));
    data.catalog_id = Some(Uuid::new_v4());
    assert!(matches!(
        db.upsert_file(&data).await,
        Err(Error::CatalogNotFound(_))
    ));
    assert!(File::all().exec(&mut db).await.unwrap().is_empty());
    data.catalog_id = Some(catalog);
    let id = db.upsert_file(&data).await.unwrap();
    let original = db.get_file(FileKey::Id(id)).await.unwrap().unwrap();
    data.filename = None;
    data.catalog_id = None;
    assert_eq!(db.upsert_file(&data).await.unwrap(), id);
    assert_eq!(
        db.get_file(FileKey::Id(id)).await.unwrap().unwrap(),
        original
    );
    data.filename = Some("second".into());
    db.upsert_file(&data).await.unwrap();
    let changed = db.get_file(FileKey::Id(id)).await.unwrap().unwrap();
    assert_eq!(changed.file.filename, "second");
    assert_eq!(changed.catalog, original.catalog);
    data.filename = None;
    data.catalog_id = Some(replacement);
    db.upsert_file(&data).await.unwrap();
    let changed = db.get_file(FileKey::Id(id)).await.unwrap().unwrap();
    assert_eq!(changed.file.filename, "second");
    assert_eq!(changed.catalog.uuid, replacement);
    assert_eq!(
        (changed.file.id, changed.file.created_at),
        (original.file.id, original.file.created_at)
    );
    data.filename = Some(String::new());
    assert!(matches!(
        db.upsert_file(&data).await,
        Err(Error::InvalidField("filename"))
    ));
    assert_eq!(
        db.get_file(FileKey::Id(id)).await.unwrap().unwrap(),
        changed
    );
    cleanup(&mut db, &prefix).await;
}

#[tokio::test]
async fn updates_by_id_preserve_omissions_and_can_rename_without_changing_identity() {
    let Some((mut db, prefix)) = database().await else {
        return;
    };
    let catalog_id = db.upsert_catalog(&object()).await.unwrap();
    let file_id = db.upsert_file(&file(catalog_id, "/before")).await.unwrap();
    let original = db.get_file(FileKey::Id(file_id)).await.unwrap().unwrap();
    db.update_catalog(catalog_id, CatalogOptionalData::default())
        .await
        .unwrap();
    db.update_file(file_id, FileOptionalData::default())
        .await
        .unwrap();
    assert_eq!(
        db.get_file(FileKey::Id(file_id)).await.unwrap().unwrap(),
        original
    );

    db.update_catalog(
        catalog_id,
        CatalogOptionalData {
            uploaded: Some(true),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    db.update_file(
        file_id,
        FileOptionalData {
            filename: Some("after.csv".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let changed = db.get_file(FileKey::Id(file_id)).await.unwrap().unwrap();
    assert!(changed.catalog.uploaded);
    assert_eq!(changed.catalog.size, original.catalog.size);
    assert_eq!(changed.catalog.format, original.catalog.format);
    assert_eq!(changed.file.fullpath, original.file.fullpath);
    assert_eq!(changed.file.filename, "after.csv");
    assert_eq!(changed.file.catalog_uuid, catalog_id);

    db.update_catalog(
        catalog_id,
        CatalogOptionalData {
            url: Some("s3://bucket/renamed".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    db.update_file(
        file_id,
        FileOptionalData {
            fullpath: Some("/after".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let renamed = db
        .get_file(FileKey::Fullpath("/after"))
        .await
        .unwrap()
        .unwrap();
    assert!(
        db.get_file(FileKey::Fullpath("/before"))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(renamed.catalog.url, "s3://bucket/renamed");
    assert!(renamed.catalog.uploaded);
    assert_eq!(
        (renamed.catalog.uuid, renamed.catalog.created_at),
        (catalog_id, original.catalog.created_at)
    );
    assert_eq!(
        (renamed.file.uuid, renamed.file.created_at),
        (file_id, original.file.created_at)
    );

    let mut tx = db.transaction().await.unwrap();
    tx.update_catalog(
        catalog_id,
        CatalogOptionalData {
            size: Some(0),
            uploaded: Some(false),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    tx.update_file(
        file_id,
        FileOptionalData {
            filename: Some("rolled-back".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(
        db.get_file(FileKey::Id(file_id)).await.unwrap().unwrap(),
        renamed
    );
    cleanup(&mut db, &prefix).await;
}

#[tokio::test]
async fn updates_reject_missing_ids_invalid_references_and_unique_key_conflicts() {
    let Some((mut db, prefix)) = database().await else {
        return;
    };
    let missing = Uuid::new_v4();
    assert!(
        matches!(db.update_catalog(missing, CatalogOptionalData::default()).await, Err(Error::CatalogNotFound(id)) if id == missing)
    );
    assert!(
        matches!(db.update_file(missing, FileOptionalData::default()).await, Err(Error::FileNotFound(id)) if id == missing)
    );
    assert!(Catalog::all().exec(&mut db).await.unwrap().is_empty());
    assert!(File::all().exec(&mut db).await.unwrap().is_empty());
    let catalog_id = db.upsert_catalog(&object()).await.unwrap();
    let file_id = db.upsert_file(&file(catalog_id, "/a")).await.unwrap();
    db.upsert_catalog(&CatalogInput::new("/taken", "s3://bucket/taken", 0, "bin"))
        .await
        .unwrap();
    db.upsert_file(&file(catalog_id, "/taken")).await.unwrap();
    let original = db.get_file(FileKey::Id(file_id)).await.unwrap().unwrap();
    assert!(
        matches!(db.update_file(file_id, FileOptionalData { catalog_id: Some(missing), ..Default::default() }).await, Err(Error::CatalogNotFound(id)) if id == missing)
    );
    assert!(matches!(
        db.update_catalog(
            catalog_id,
            CatalogOptionalData {
                url: Some("relative".into()),
                ..Default::default()
            }
        )
        .await,
        Err(Error::InvalidUrl)
    ));
    assert!(matches!(
        db.update_file(
            file_id,
            FileOptionalData {
                fullpath: Some(String::new()),
                ..Default::default()
            }
        )
        .await,
        Err(Error::InvalidField("fullpath"))
    ));
    assert!(matches!(
        db.update_catalog(
            catalog_id,
            CatalogOptionalData {
                url: Some("s3://bucket/taken".into()),
                uploaded: Some(true),
                ..Default::default()
            }
        )
        .await,
        Err(Error::Database(_))
    ));
    assert!(matches!(
        db.update_file(
            file_id,
            FileOptionalData {
                fullpath: Some("/taken".into()),
                filename: Some("conflict".into()),
                ..Default::default()
            }
        )
        .await,
        Err(Error::Database(_))
    ));
    assert_eq!(
        db.get_file(FileKey::Id(file_id)).await.unwrap().unwrap(),
        original
    );
    cleanup(&mut db, &prefix).await;
}

#[tokio::test]
async fn catalog_keys_and_urls_enforce_the_varchar_character_limit() {
    let Some((mut db, prefix)) = database().await else {
        return;
    };
    let longest = CatalogInput::new(
        "鍵".repeat(750),
        format!("s3://bucket/{}", "界".repeat(750 - "s3://bucket/".len())),
        0,
        "型".repeat(255),
    );
    let id = db.upsert_catalog(&longest).await.unwrap();
    let record = db.get_catalog(id).await.unwrap().unwrap();
    assert_eq!(record.key, longest.key);
    assert_eq!(record.url, longest.url);
    assert_eq!(record.format, longest.format);
    let fullpath = "名".repeat(512);
    let filename = "名".repeat(255);
    let file_id = db
        .upsert_file(&FileInput::new(&fullpath, &filename, id))
        .await
        .unwrap();
    let loaded = db.get_file(FileKey::Id(file_id)).await.unwrap().unwrap();
    assert_eq!(loaded.file.fullpath, fullpath);
    assert_eq!(loaded.file.filename, filename);
    assert!(matches!(
        db.update_file(
            file_id,
            FileOptionalData {
                filename: Some(format!("{filename}x")),
                ..Default::default()
            }
        )
        .await,
        Err(Error::InvalidField("filename"))
    ));
    let mut overlong = longest.clone();
    overlong.key.push('x');
    assert!(matches!(
        db.upsert_catalog(&overlong).await,
        Err(Error::InvalidField("key"))
    ));
    let mut overlong = longest;
    overlong.url.push('x');
    assert!(matches!(
        db.upsert_catalog(&overlong).await,
        Err(Error::InvalidField("url"))
    ));
    cleanup(&mut db, &prefix).await;
}
