//! Rayflow API server.

use server::{App, Config};

/// Runs the API server.
///
/// Configuration is layered from defaults, then `rayflow.yaml`, then the
/// environment; see [`server::config`]. Everything the server needs — the bind
/// address and the database URL — comes from that config.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        .init();

    let config = Config::load()?;
    let mut builder = toasty::Db::builder();
    builder.models(server::models());
    let db = builder.connect(&config.database_url).await?;
    let state = App::new(db.clone(), config.clone(), db);

    let listener = tokio::net::TcpListener::bind(config.addr).await?;
    tracing::info!(address = %config.addr, "rayflow server listening");
    axum::serve(listener, server::router(state)).await?;
    Ok(())
}
