//! Agent executable entry point.

use std::error::Error as StdError;

use agent::{Agentic, errors::Error, settings::Settings};

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        .init();

    let settings = Settings::load()?;
    let id = settings.id;
    let server_url = settings.server_url.clone();
    let agent = Agentic::new(settings).await?;
    let running = agent.start();
    tokio::pin!(running);
    tracing::info!(agent_id = %id, %server_url, "rayflow agent started");

    tokio::select! {
        result = &mut running => result?,
        signal = shutdown_signal() => {
            tracing::info!(agent_id = %id, "rayflow agent shutting down");
            let (running, shutdown) = tokio::join!(&mut running, agent.shutdown());
            running?;
            shutdown?;
            signal?;
        },
    }
    Ok(())
}

async fn shutdown_signal() -> Result<(), Error> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => Ok(result?),
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    Ok(tokio::signal::ctrl_c().await?)
}
