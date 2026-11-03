//! Runs a rayflow leader-elected consumer.

use std::error::Error as StdError;

use coordinator::mysql::MySqlLeaderElection;
use pilot::{Config, Pilot};
use runtime::backend::Backend;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        .init();

    let config = Config::load()?;

    let mut builder = toasty::Db::builder();
    builder.models(pilot::models());
    let db = builder.connect(&config.database_url).await?;
    let backend = Backend::new(db.clone(), db);
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

    tracing::info!(
        %node_id,
        leader_name = %config.leader_name,
        "rayflow pilot competing for coordinator role"
    );

    pilot
        .run(async {
            if let Err(error) = tokio::signal::ctrl_c().await {
                tracing::error!(%error, "rayflow pilot could not listen for shutdown");
            }
            tracing::info!("rayflow pilot shutting down");
        })
        .await?;
    Ok(())
}
