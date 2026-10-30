//! Rayflow API server.

use graph::store::MySqlGraphStore;
use server::{App, Config};

/// Runs the API server.
///
/// Configuration is layered from defaults, then `rayflow.yaml`, then the
/// environment; see [`server::config`]. Everything the server needs — the bind
/// address and the database URL — comes from that config.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::load()?;
    let graph_store = MySqlGraphStore::connect(&config.database_url).await?;
    let state = App::connect(&config.database_url, config.clone(), graph_store).await?;

    let listener = tokio::net::TcpListener::bind(config.addr).await?;
    println!("rayflow server listening on {}", config.addr);
    axum::serve(listener, server::router(state)).await?;
    Ok(())
}
