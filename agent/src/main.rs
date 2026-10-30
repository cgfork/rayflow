//! Agent executable entry point.

use std::error::Error as StdError;

use agent::{Agentic, errors::Error, settings::Settings};

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    let settings = Settings::load()?;
    let id = settings.id;
    let agent = Agentic::new(settings).await?;
    let running = agent.start();
    tokio::pin!(running);
    println!("rayflow agent {id} started");

    tokio::select! {
        result = &mut running => result?,
        signal = shutdown_signal() => {
            println!("rayflow agent {id} shutting down");
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
