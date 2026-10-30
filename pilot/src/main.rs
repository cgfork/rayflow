//! Runs a rayflow leader-elected consumer.

use std::error::Error as StdError;

use coordinator::mysql::MySqlLeaderElection;
use graph::store::MySqlGraphStore;
use pilot::{Config, Pilot};
use runtime::backend::Backend;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    let config = Config::load()?;

    let mut builder = toasty::Db::builder();
    builder.models(scheduler::models());
    let db = builder.connect(&config.database_url).await?;
    let graph_store = MySqlGraphStore::connect(&config.database_url).await?;
    let backend = Backend::new(db, graph_store);
    let node_id = Uuid::new_v4().to_string();
    let election = MySqlLeaderElection::new(
        backend.db().clone(),
        config.leader_name.clone(),
        node_id.clone(),
        config.leader_lease(),
    );
    let mut pilot = Pilot::new(election, backend, config.batch, config.poll_interval())
        .with_gap_timeout(config.gap_timeout())
        .with_job_timeout_gap(config.job_timeout_gap);

    println!(
        "rayflow pilot {node_id} competing for coordinator role '{}'",
        config.leader_name
    );

    pilot
        .run(async {
            if let Err(error) = tokio::signal::ctrl_c().await {
                eprintln!("rayflow pilot could not listen for shutdown: {error}");
            }
            println!("rayflow pilot shutting down");
        })
        .await?;
    Ok(())
}
