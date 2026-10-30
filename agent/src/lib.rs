//! Agent-side job execution and durable per-job filesystem support.
//!
//! An [`Agentd`] owns its execution provider and job workdir. Remote polling
//! and result reporting remain independent from the local daemon.

use std::{io, path::Path};

use serde::{Serialize, de::DeserializeOwned};
use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt,
};
use uuid::Uuid;

pub mod agentd;
pub mod agentic;
pub mod client;
pub mod containerd;
pub mod errors;
pub mod graphfs;
pub mod jobfs;
pub mod settings;

pub use agentd::{AgentResult, Agentd, JobOutcome, StopReason};
pub use agentic::Agentic;

pub(crate) async fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T, errors::Error> {
    let contents = fs::read(path).await?;
    serde_json::from_slice(&contents)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error).into())
}

pub(crate) async fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<(), errors::Error> {
    let contents = serde_json::to_vec(value)?;
    let dir = path
        .parent()
        .ok_or_else(|| io::Error::other("JSON path has no parent"))?;
    fs::create_dir_all(dir).await?;

    let name = path
        .file_name()
        .ok_or_else(|| io::Error::other("JSON path has no file name"))?
        .to_string_lossy();
    let temporary = dir.join(format!(".{name}.{}.tmp", Uuid::new_v4()));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options.open(&temporary).await?;
    let result = async {
        file.write_all(&contents).await?;
        file.flush().await?;
        file.sync_all().await?;
        drop(file);
        fs::rename(&temporary, path).await
    }
    .await;
    if result.is_err() {
        let _ = fs::remove_file(&temporary).await;
    }
    result.map_err(Into::into)
}
