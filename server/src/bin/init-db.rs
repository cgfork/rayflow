use server::{Config, migration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        .init();

    let config = Config::load()?;
    let report = migration::initialize(&config.database_url).await?;
    tracing::info!(
        applied = report.applied(),
        skipped = report.skipped(),
        "database migrations complete"
    );
    Ok(())
}
