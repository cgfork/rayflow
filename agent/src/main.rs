//! Agent executable entry point.

use std::{error::Error, io};

use agent::{Agentic, settings::Settings};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let settings = Settings::load()?;
    let id = settings.id;
    let agent = Agentic::new(settings)?;
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

async fn shutdown_signal() -> io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await
}
