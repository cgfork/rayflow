use std::{env, future::Future, panic};

use toasty::Db;
use url::Url;
use uuid::Uuid;

/// Runs a test in its own migrated schema. DATABASE_URL supplies the connection
/// credentials, which must allow creating and dropping test databases.
pub async fn with_test_database<F>(test: impl FnOnce(Db) -> F + 'static)
where
    F: Future<Output = ()> + 'static,
{
    let Ok(database_url) = env::var("DATABASE_URL") else {
        eprintln!("skipping MySQL integration test because DATABASE_URL is unset");
        return;
    };
    if database_url.trim().is_empty() {
        eprintln!("skipping MySQL integration test because DATABASE_URL is empty");
        return;
    }
    let mut url = Url::parse(&database_url).expect("valid MySQL DATABASE_URL");
    assert_eq!(url.scheme(), "mysql", "DATABASE_URL must use MySQL");
    url.set_path("/information_schema");
    let mut admin = Db::builder()
        .models(server::models())
        .connect(url.as_str())
        .await
        .expect("connect to MySQL for test schema cleanup");
    let name = format!("rayflow_test_{}", Uuid::new_v4().simple());
    url.set_path(&format!("/{name}"));
    let outcome = tokio::task::LocalSet::new()
        .run_until(async move {
            tokio::task::spawn_local(async move {
                server::migration::initialize(url.as_str())
                    .await
                    .expect("migrate isolated MySQL test schema");
                let db = Db::builder()
                    .models(server::models())
                    .connect(url.as_str())
                    .await
                    .expect("connect to isolated MySQL test schema");
                test(db).await;
            })
            .await
        })
        .await;

    let cleanup = toasty::sql::statement(format!("DROP DATABASE IF EXISTS `{name}`"))
        .exec(&mut admin)
        .await;
    if let Err(error) = outcome {
        if let Err(cleanup_error) = cleanup {
            eprintln!("failed to clean up schema {name}: {cleanup_error}");
        }
        panic::resume_unwind(error.into_panic());
    }
    cleanup.expect("drop isolated MySQL test schema");
}
