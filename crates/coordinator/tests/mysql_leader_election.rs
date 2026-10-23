//! Integration tests for the MySQL-backed leader lease.
//!
//! Set `DATABASE_URL` to run these tests. Without it they skip.

#![cfg(feature = "mysql")]

use std::time::Duration;

use coordinator::mysql::{LeaderElectionModel, MySqlLeaderElection};
use jiff::Timestamp;
use toasty::Db;
use uuid::Uuid;

async fn setup() -> Option<Db> {
    let url = std::env::var("DATABASE_URL").ok()?;

    let mut builder = Db::builder();
    builder.models(coordinator::mysql::models());
    let db = builder
        .connect(&url)
        .await
        .expect("connect to DATABASE_URL");

    db.reset_db().await.expect("reset database");
    db.push_schema().await.expect("push schema");

    Some(db)
}

#[tokio::test]
async fn one_leader_holds_the_lease_until_it_expires_or_resigns() {
    let Some(db) = setup().await else {
        eprintln!("skipping: set DATABASE_URL to run the MySQL leader election test");
        return;
    };

    let role = format!("coordinator-test-{}", Uuid::new_v4());
    let first =
        MySqlLeaderElection::new(db.clone(), role.clone(), "first", Duration::from_secs(30));
    let second =
        MySqlLeaderElection::new(db.clone(), role.clone(), "second", Duration::from_secs(30));

    assert!(first.try_acquire_or_renew().await.expect("acquire first"));
    assert!(!second.try_acquire_or_renew().await.expect("second waits"));
    assert!(first.is_leader().await.expect("first state"));
    assert!(!second.is_leader().await.expect("second state"));

    first.resign().await.expect("resign");
    assert!(second.try_acquire_or_renew().await.expect("acquire second"));
    first.resign().await.expect("former leader resigns again");
    assert!(second.is_leader().await.expect("successor keeps lease"));

    let expired = Timestamp::now() - Duration::from_secs(1);
    LeaderElectionModel::filter_by_name(role)
        .update()
        .expires_at(expired)
        .updated_at(expired)
        .exec(&mut db.clone())
        .await
        .expect("expire lease");

    assert!(first.try_acquire_or_renew().await.expect("take over"));
    assert!(!second.is_leader().await.expect("second lost lease"));

    // An expired lease cannot be stolen while its protected transaction is
    // still running. The contender must wait for that transaction to finish.
    let mut connection = db.clone();
    let mut tx = connection.transaction().await.expect("transaction");
    assert!(first.acquire_in(&mut tx).await.expect("lock lease"));
    LeaderElectionModel::filter_by_name(first.name().to_owned())
        .update()
        .expires_at(Timestamp::now() - Duration::from_secs(1))
        .exec(&mut tx)
        .await
        .expect("expire locked lease");
    assert!(
        tokio::time::timeout(Duration::from_millis(100), second.try_acquire_or_renew())
            .await
            .is_err(),
        "takeover must wait for protected work"
    );
    tx.commit().await.expect("commit protected work");
    assert!(
        second
            .try_acquire_or_renew()
            .await
            .expect("take over after commit")
    );
}
