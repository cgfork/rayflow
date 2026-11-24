use std::fs::{self, File, OpenOptions, TryLockError};

use serde::Serialize;
use uuid::Uuid;

use crate::{errors::Error, settings::Settings};

#[derive(Serialize)]
struct OwnerRecord {
    format_version: u32,
    agent_id: Uuid,
    generation: Uuid,
    pid: u32,
    binary_version: &'static str,
    mode: &'static str,
}

pub(super) fn acquire(settings: &Settings) -> Result<File, Error> {
    fs::create_dir_all(&settings.workdir)?;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(settings.workdir.join(".agent.lock"))?;
    match file.try_lock() {
        Ok(()) => {}
        Err(TryLockError::WouldBlock) => {
            return Err(Error::JobManager(
                "agent workdir is already owned by another process".into(),
            ));
        }
        Err(TryLockError::Error(error)) => return Err(error.into()),
    }
    let record = OwnerRecord {
        format_version: 1,
        agent_id: settings.id,
        generation: Uuid::new_v4(),
        pid: std::process::id(),
        binary_version: env!("CARGO_PKG_VERSION"),
        mode: "running",
    };
    let contents = serde_json::to_vec(&record)?;
    let temporary = settings
        .workdir
        .join(format!(".agent-owner.{}.tmp", Uuid::new_v4()));
    fs::write(&temporary, contents)?;
    if let Err(error) = fs::rename(&temporary, settings.workdir.join(".agent-owner.json")) {
        let _ = fs::remove_file(&temporary);
        return Err(error.into());
    }
    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::*;
    use scheduler::ResourceCapacity;
    use tempfile::tempdir;

    #[test]
    fn owner_lock_excludes_a_second_manager_and_releases_on_drop() {
        let root = tempdir().unwrap();
        let settings = Settings::new(
            Uuid::new_v4(),
            "http://127.0.0.1:8080",
            root.path().join("jobs"),
            ResourceCapacity {
                cpu_cores: 1,
                memory_bytes: 1,
            },
        );
        let owner = acquire(&settings).unwrap();
        assert!(matches!(
            acquire(&settings),
            Err(Error::JobManager(message)) if message.contains("already owned")
        ));
        drop(owner);
        assert!(acquire(&settings).is_ok());
    }
}
