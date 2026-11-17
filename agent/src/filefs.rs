//! Artifact data scoped by graph run, node, and revision, independent of JobFs.

use crate::errors::Error;
use std::{
    fs::{File, TryLockError},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{fs, sync::Mutex, time::sleep};
use uuid::Uuid;

pub struct FileFs {
    pub run_id: Uuid,
    pub node_id: Uuid,
    pub revision: u64,
    root_dir: PathBuf,
    lock_file: Mutex<Option<File>>,
}

impl FileFs {
    pub fn new(data_dir: impl AsRef<Path>, run_id: Uuid, node_id: Uuid, revision: u64) -> Self {
        Self {
            run_id,
            node_id,
            revision,
            root_dir: data_dir.as_ref().to_path_buf(),
            lock_file: Mutex::new(None),
        }
    }

    pub fn dir(&self) -> PathBuf {
        self.root_dir
            .join(self.run_id.to_string())
            .join(self.node_id.to_string())
            .join(self.revision.to_string())
    }
    pub fn inputs_dir(&self) -> PathBuf {
        self.dir().join("inputs")
    }
    pub fn outputs_dir(&self) -> PathBuf {
        self.dir().join("outputs")
    }
    pub fn state_dir(&self, job_id: Uuid) -> PathBuf {
        self.dir().join(".rayflow").join(job_id.to_string())
    }

    pub async fn prepare(&self) -> Result<(), Error> {
        fs::create_dir_all(self.inputs_dir()).await?;
        fs::create_dir_all(self.outputs_dir()).await?;
        Ok(())
    }

    pub(crate) async fn lock(&self) -> Result<(), Error> {
        let mut held = self.lock_file.lock().await;
        if held.is_some() {
            return Ok(());
        }
        fs::create_dir_all(self.dir()).await?;
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.dir().join(".lock"))
            .await?
            .into_std()
            .await;
        loop {
            match file.try_lock() {
                Ok(()) => break,
                Err(TryLockError::WouldBlock) => sleep(Duration::from_millis(10)).await,
                Err(TryLockError::Error(e)) => return Err(e.into()),
            }
        }
        *held = Some(file);
        Ok(())
    }

    pub(crate) async fn unlock(&self) -> Result<(), Error> {
        let mut held = self.lock_file.lock().await;
        if let Some(file) = held.as_ref() {
            file.unlock()?;
        }
        *held = None;
        Ok(())
    }

    /// Explicitly removes this node revision after its graph run no longer needs it.
    /// Job acknowledgement deliberately retains artifacts for downstream nodes.
    pub async fn clear(&self) -> Result<(), Error> {
        self.lock().await?;
        let result = async {
            let mut entries = fs::read_dir(self.dir()).await?;
            while let Some(entry) = entries.next_entry().await? {
                if entry.file_name() == ".lock" {
                    continue;
                }
                if entry.file_type().await?.is_dir() {
                    fs::remove_dir_all(entry.path()).await?;
                } else {
                    fs::remove_file(entry.path()).await?;
                }
            }
            Ok(())
        }
        .await;
        let unlock = self.unlock().await;
        result.and(unlock)
    }
}
