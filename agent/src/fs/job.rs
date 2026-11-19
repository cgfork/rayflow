use std::{
    collections::HashMap,
    fs::{File as StdFile, TryLockError},
    io,
    path::{Path, PathBuf},
    time::Duration,
};

use jiff::Timestamp;
use scheduler::Job;
use serde::de::DeserializeOwned;
use tokio::{
    fs::{self, OpenOptions},
    sync::Mutex,
    time::sleep,
};
use uuid::Uuid;

use crate::{
    errors::Error,
    fs::{Container, JobFs as JobFsTrait, JobResult},
    read_json,
    settings::Settings,
    write_json,
};

pub struct JobFs {
    settings: Settings,
    job: Job,
    lock_file: Mutex<Option<StdFile>>,
    container: Option<Container>,
    result: Option<JobResult>,
    started_at: Option<Timestamp>,
    pid: Option<i64>,
}

impl JobFs {
    pub fn new(job: Job, settings: Settings) -> Self {
        Self {
            settings,
            job,
            lock_file: Mutex::new(None),
            container: None,
            result: None,
            started_at: None,
            pid: None,
        }
    }

    pub(crate) async fn restore(settings: Settings, job_id: Uuid) -> Result<Self, Error> {
        let job_fs = Self::restore_locked(settings, job_id).await?;
        job_fs.unlock().await?;
        Ok(job_fs)
    }

    pub(crate) async fn restore_locked(settings: Settings, job_id: Uuid) -> Result<Self, Error> {
        let dir = settings.workdir.join(job_id.to_string());
        let lock_file = lock_job_dir(&dir).await?;
        let job: Job = read_json(&dir.join("job.json")).await?;
        if job.uuid != job_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "job UUID does not match its filesystem directory",
            )
            .into());
        }
        let mut job_fs = Self::new(job, settings);
        job_fs.lock_file = Mutex::new(Some(lock_file));
        job_fs.load_cached().await?;
        Ok(job_fs)
    }

    /// Lists persisted jobs in a workdir without reading their definitions.
    pub async fn job_ids(settings: &Settings) -> Result<Vec<Uuid>, Error> {
        let mut entries = fs::read_dir(&settings.workdir).await?;
        let mut jobs = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            if !entry.file_type().await?.is_dir() {
                continue;
            }
            if let Ok(job_id) = entry.file_name().to_string_lossy().parse()
                && fs::try_exists(entry.path().join("job.json")).await?
            {
                jobs.push(job_id);
            }
        }
        Ok(jobs)
    }

    /// Discovers persisted container-to-job mappings in a workdir.
    /// Job directories without readable container metadata are skipped.
    pub async fn discover(settings: Settings) -> Result<HashMap<String, Uuid>, Error> {
        let mut containers = HashMap::new();
        for job_id in Self::job_ids(&settings).await? {
            let job_fs = match Self::restore(settings.clone(), job_id).await {
                Ok(job_fs) => job_fs,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };
            if let Some(container) = job_fs.container() {
                containers.insert(container.id.clone(), job_id);
            }
        }
        Ok(containers)
    }

    fn dir_path(&self) -> PathBuf {
        self.settings.workdir.join(self.job.uuid.to_string())
    }

    fn file_dir(&self) -> PathBuf {
        if self.job.node_run.is_unloaded() {
            return self.settings.data_dir.join(self.job.uuid.to_string());
        }
        let node_run = self.job.node_run.get();
        self.settings
            .data_dir
            .join(self.job.run_id.to_string())
            .join(node_run.node_id.to_string())
            .join(node_run.revision.to_string())
    }

    fn input_path(&self) -> PathBuf {
        self.file_dir().join("inputs")
    }
    fn output_path(&self) -> PathBuf {
        self.file_dir().join("outputs")
    }
    fn state_path(&self, job_id: Uuid) -> PathBuf {
        self.file_dir().join(".rayflow").join(job_id.to_string())
    }

    /// Persists the job definition held by this filesystem.
    pub(crate) async fn persist_job(&self) -> Result<(), Error> {
        write_json(&self.dir_path().join("job.json"), &self.job).await
    }

    /// Acquire the exclusive job lock, waiting until it is available.
    /// The lock is held until `unlock` is called or this `JobFs` is dropped.
    pub(crate) async fn lock(&self) -> Result<(), Error> {
        let mut lock_file = self.lock_file.lock().await;
        if lock_file.is_some() {
            return Ok(());
        }
        let dir = self.dir_path();
        fs::create_dir_all(&dir).await?;
        *lock_file = Some(lock_job_dir(&dir).await?);
        Ok(())
    }

    pub(crate) async fn unlock(&self) -> Result<(), Error> {
        let mut lock_file = self.lock_file.lock().await;
        if let Some(file) = lock_file.as_ref() {
            file.unlock()?;
            *lock_file = None;
        }
        Ok(())
    }

    pub(crate) async fn write_job_result(&mut self, result: JobResult) -> Result<(), Error> {
        write_json(&self.dir_path().join("result.json"), &result).await?;
        self.result = Some(result);
        Ok(())
    }

    pub(crate) async fn write_container(&mut self, container: Container) -> Result<(), Error> {
        write_json(&self.dir_path().join("container.json"), &container).await?;
        self.container = Some(container);
        Ok(())
    }

    pub(crate) async fn write_runtime(
        &mut self,
        pid: Option<i64>,
        started_at: Option<Timestamp>,
    ) -> Result<(), Error> {
        let runtime = self.dir_path().join("runtime");
        fs::create_dir_all(&runtime).await?;
        if let Some(pid) = pid.filter(|pid| *pid > 0) {
            fs::write(self.dir_path().join("pid"), pid.to_string()).await?;
            self.pid = Some(pid);
        }
        if let Some(started_at) = started_at {
            fs::write(runtime.join("started_at"), started_at.to_string()).await?;
            self.started_at = Some(started_at);
        }
        Ok(())
    }

    pub(crate) async fn clear(&mut self) -> Result<(), Error> {
        let dir = self.dir_path();
        let mut entries = fs::read_dir(dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            if entry.file_name() == "lock" {
                // Existing waiters must continue to use the same lock inode.
                continue;
            }
            if entry.file_type().await?.is_dir() {
                fs::remove_dir_all(entry.path()).await?;
            } else {
                fs::remove_file(entry.path()).await?;
            }
        }
        self.container = None;
        self.result = None;
        self.started_at = None;
        self.pid = None;
        Ok(())
    }

    pub(crate) async fn load_cached(&mut self) -> Result<(), Error> {
        self.container = read_optional_json(&self.dir_path().join("container.json")).await?;
        self.result = read_optional_json(&self.dir_path().join("result.json")).await?;
        self.started_at = read_optional_string(&self.dir_path().join("runtime/started_at"))
            .await?
            .map(|value| {
                value
                    .parse()
                    .map_err(|error| Error::Io(io::Error::new(io::ErrorKind::InvalidData, error)))
            })
            .transpose()?;
        self.pid = read_optional_string(&self.dir_path().join("pid"))
            .await?
            .map(|value| {
                value
                    .parse()
                    .map_err(|error| Error::Io(io::Error::new(io::ErrorKind::InvalidData, error)))
            })
            .transpose()?;
        Ok(())
    }
}

async fn lock_job_dir(dir: &Path) -> Result<StdFile, Error> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(dir.join("lock"))
        .await?
        .into_std()
        .await;
    loop {
        match file.try_lock() {
            Ok(()) => return Ok(file),
            Err(TryLockError::WouldBlock) => sleep(Duration::from_millis(10)).await,
            Err(TryLockError::Error(error)) => return Err(error.into()),
        }
    }
}

async fn read_optional_json<T: DeserializeOwned>(path: &Path) -> Result<Option<T>, Error> {
    match read_json(path).await {
        Ok(value) => Ok(Some(value)),
        Err(Error::Io(error)) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

async fn read_optional_string(path: &Path) -> Result<Option<String>, Error> {
    match fs::read_to_string(path).await {
        Ok(value) => Ok(Some(value.trim().to_owned())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

impl JobFsTrait for JobFs {
    fn job(&self) -> &Job {
        &self.job
    }
    fn metadata_dir(&self) -> &Path {
        &self.settings.workdir
    }
    fn data_dir(&self) -> &Path {
        &self.settings.data_dir
    }
    fn dir(&self) -> PathBuf {
        self.dir_path()
    }
    fn input_dir(&self) -> PathBuf {
        self.input_path()
    }
    fn output_dir(&self) -> PathBuf {
        self.output_path()
    }
    fn state_dir(&self, job_id: Uuid) -> PathBuf {
        self.state_path(job_id)
    }
    fn container(&self) -> Option<&Container> {
        self.container.as_ref()
    }
    async fn write_container(&mut self, container: Container) -> Result<(), Error> {
        self.write_container(container).await
    }
    fn job_result(&self) -> Option<&JobResult> {
        self.result.as_ref()
    }
    async fn write_job_result(&mut self, result: JobResult) -> Result<(), Error> {
        self.write_job_result(result).await
    }
    async fn write_runtime(
        &mut self,
        pid: Option<i64>,
        started_at: Option<Timestamp>,
    ) -> Result<(), Error> {
        self.write_runtime(pid, started_at).await
    }
    fn started_at(&self) -> Option<&Timestamp> {
        self.started_at.as_ref()
    }
    fn pid(&self) -> Option<i64> {
        self.pid
    }
    async fn clear(&mut self) -> Result<(), Error> {
        self.clear().await
    }
    async fn lock(&self) -> Result<(), Error> {
        self.lock().await
    }
    async fn unlock(&self) -> Result<(), Error> {
        self.unlock().await
    }
    async fn prepare_input<G: crate::fs::GraphFs + Clone, T: crate::fs::Transfer<Err = Error>>(
        &self,
        graph: &G,
        transfer: &T,
    ) -> Result<(), Error> {
        transfer.prepare_input(self, graph).await
    }
    async fn complete_output<G: crate::fs::GraphFs + Clone, T: crate::fs::Transfer<Err = Error>>(
        &self,
        graph: &G,
        transfer: &T,
    ) -> Result<(), Error> {
        transfer.complete_output(self, graph).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::JobFs as JobFsTrait;
    use scheduler::ResourceCapacity;
    use tempfile::tempdir;

    fn settings(root: &Path) -> Settings {
        Settings::new(
            Uuid::new_v4(),
            "http://127.0.0.1:8080",
            root.join("jobs"),
            ResourceCapacity {
                cpu_cores: 1,
                memory_bytes: 1,
            },
        )
    }

    fn job() -> Job {
        let task = event::task::Task::new(Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4(), 1);
        Job::new(&task, Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4())
    }

    #[tokio::test]
    async fn restores_cached_container_result_and_runtime() {
        let root = tempdir().unwrap();
        let settings = settings(root.path());
        let job = job();
        let mut job_fs = JobFs::new(job.clone(), settings.clone());

        job_fs.lock().await.unwrap();
        job_fs.persist_job().await.unwrap();
        job_fs
            .write_container(Container {
                r#type: "test".into(),
                id: "container-1".into(),
            })
            .await
            .unwrap();
        job_fs
            .write_job_result(JobResult {
                exit_code: 0,
                outcome: Some("succeed".into()),
                finished_at: None,
                duration_ms: Some(10),
            })
            .await
            .unwrap();
        let started_at = Timestamp::now();
        job_fs
            .write_runtime(Some(42), Some(started_at))
            .await
            .unwrap();
        job_fs.unlock().await.unwrap();

        let restored = JobFs::restore(settings, job.uuid).await.unwrap();
        assert_eq!(
            restored.container().map(|value| value.id.as_str()),
            Some("container-1")
        );
        assert_eq!(restored.job_result().map(|value| value.exit_code), Some(0));
        assert_eq!(restored.pid(), Some(42));
        assert_eq!(restored.started_at(), Some(&started_at));
    }
}
