use std::{
    collections::HashMap,
    fs::{File as StdFile, TryLockError},
    io,
    path::{Path, PathBuf},
    time::Duration,
};

use graph::types::Node;
use jiff::Timestamp;
use scheduler::Job;
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{self, File, OpenOptions},
    sync::Mutex,
    time::sleep,
};
use uuid::Uuid;

use crate::{errors::Error, read_json, write_json};

pub struct JobFs {
    pub job_id: Uuid,
    pub root_dir: PathBuf,
    lock_file: Mutex<Option<StdFile>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct JobResult {
    pub exit_code: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Container {
    pub r#type: String,
    pub id: String,
}

impl JobFs {
    pub fn new<P: AsRef<Path>>(dir: P, job_id: Uuid) -> Self {
        Self {
            root_dir: dir.as_ref().to_path_buf(),
            job_id,
            lock_file: Mutex::new(None),
        }
    }

    /// Lists UUID-named job directories in a workdir without reading job files.
    pub async fn job_ids<P: AsRef<Path>>(dir: P) -> Result<Vec<Uuid>, Error> {
        let mut entries = fs::read_dir(dir).await?;
        let mut jobs = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            if !entry.file_type().await?.is_dir() {
                continue;
            }
            if let Ok(job_id) = entry.file_name().to_string_lossy().parse() {
                jobs.push(job_id);
            }
        }
        Ok(jobs)
    }

    /// Discovers persisted container-to-job mappings in a workdir.
    /// Job directories without readable container metadata are skipped.
    pub async fn discover<P: AsRef<Path>>(dir: P) -> Result<HashMap<String, Uuid>, Error> {
        let dir = dir.as_ref();
        let mut containers = HashMap::new();
        for job_id in Self::job_ids(dir).await? {
            let job_fs = Self::new(dir, job_id);
            if let Ok(container) = job_fs.container().await {
                containers.insert(container.id, job_id);
            }
        }
        Ok(containers)
    }

    pub fn dir(&self) -> PathBuf {
        self.root_dir.join(self.job_id.to_string())
    }

    /// Read jobs/{job}/job.json.
    ///
    /// The content is json-marshal Job struct.
    pub async fn job(&self) -> Result<Job, Error> {
        let job: Job = read_json(&self.dir().join("job.json")).await?;
        if job.uuid != self.job_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "job UUID does not match its filesystem directory",
            )
            .into());
        }
        Ok(job)
    }

    /// Read `{workdir}/{job}/node.json`.
    pub async fn node(&self) -> Result<Node, Error> {
        read_json(&self.dir().join("node.json")).await
    }

    pub(crate) async fn write_job(&self, job: Job) -> Result<Job, Error> {
        if job.uuid != self.job_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "job UUID does not match its filesystem directory",
            )
            .into());
        }
        write_json(&self.dir().join("job.json"), &job).await?;
        Ok(job)
    }

    pub(crate) async fn write_node(&self, node: &Node) -> Result<(), Error> {
        write_json(&self.dir().join("node.json"), node).await
    }

    /// Acquire the exclusive job lock, waiting until it is available.
    /// The lock is held until `unlock` is called or this `JobFs` is dropped.
    pub(crate) async fn lock(&self) -> Result<(), Error> {
        let mut lock_file = self.lock_file.lock().await;
        if lock_file.is_some() {
            return Ok(());
        }
        let dir = self.dir();
        fs::create_dir_all(&dir).await?;
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
                Ok(()) => break,
                Err(TryLockError::WouldBlock) => sleep(Duration::from_millis(10)).await,
                Err(TryLockError::Error(error)) => return Err(error.into()),
            }
        }
        *lock_file = Some(file);
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

    /// Read jobs/{job}/runtime/started_at
    pub async fn started_at(&self) -> Result<Timestamp, Error> {
        let contents = fs::read_to_string(self.dir().join("runtime/started_at")).await?;
        contents
            .trim()
            .parse()
            .map_err(|error| Error::Io(io::Error::new(io::ErrorKind::InvalidData, error)))
    }

    /// Read jobs/{job}/runtime/stdout
    pub async fn stdout(&self) -> Result<File, Error> {
        File::open(self.dir().join("runtime/stdout"))
            .await
            .map_err(Into::into)
    }

    /// Read jobs/{job}/result.json
    pub async fn job_result(&self) -> Result<JobResult, Error> {
        read_json(&self.dir().join("result.json")).await
    }

    pub(crate) async fn write_job_result(&self, result: JobResult) -> Result<(), Error> {
        write_json(&self.dir().join("result.json"), &result).await
    }

    /// Read jobs/{job}/container.json
    ///
    /// 1. Docker container, records the docker container id.
    pub async fn container(&self) -> Result<Container, Error> {
        read_json(&self.dir().join("container.json")).await
    }

    pub(crate) async fn write_container(&self, container: Container) -> Result<(), Error> {
        write_json(&self.dir().join("container.json"), &container).await
    }

    pub(crate) async fn write_runtime(
        &self,
        pid: Option<i64>,
        started_at: Option<Timestamp>,
    ) -> Result<(), Error> {
        let runtime = self.dir().join("runtime");
        fs::create_dir_all(&runtime).await?;
        if let Some(pid) = pid.filter(|pid| *pid > 0) {
            fs::write(self.dir().join("pid"), pid.to_string()).await?;
        }
        if let Some(started_at) = started_at {
            fs::write(runtime.join("started_at"), started_at.to_string()).await?;
        }
        Ok(())
    }

    pub(crate) async fn clear(&self) -> Result<(), Error> {
        let mut entries = fs::read_dir(self.dir()).await?;
        while let Some(entry) = entries.next_entry().await? {
            if entry.file_name() == "lock" {
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
}

#[cfg(test)]
mod tests {
    use std::fs::{self, OpenOptions, TryLockError};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use event::task::Task;
    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

    fn job() -> Job {
        let task = Task::new(Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4(), 7);
        Job::new(&task, Uuid::new_v4(), 11, 13)
    }

    #[tokio::test]
    async fn job_round_trips_and_replaces_existing_contents() {
        let root = tempdir().unwrap();
        let mut job = job();
        let job_fs = JobFs::new(root.path().join("jobs"), job.uuid);
        job.agent_id = Some(Uuid::new_v4());
        let expected = serde_json::to_value(&job).unwrap();

        let stored = job_fs.write_job(job.clone()).await.unwrap();
        assert_eq!(serde_json::to_value(stored).unwrap(), expected);
        assert_eq!(
            serde_json::to_value(job_fs.job().await.unwrap()).unwrap(),
            expected
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &fs::read(job_fs.dir().join("job.json")).unwrap()
            )
            .unwrap(),
            expected
        );

        job.agent_id = None;
        job_fs.write_job(job.clone()).await.unwrap();
        assert_eq!(
            serde_json::to_value(job_fs.job().await.unwrap()).unwrap(),
            serde_json::to_value(job).unwrap()
        );
        assert_eq!(fs::read_dir(job_fs.dir()).unwrap().count(), 1);
    }

    #[tokio::test]
    async fn write_job_rejects_a_different_uuid_without_overwriting() {
        let root = tempdir().unwrap();
        let original = job();
        let job_fs = JobFs::new(root.path(), original.uuid);
        job_fs.write_job(original.clone()).await.unwrap();
        let contents = fs::read(job_fs.dir().join("job.json")).unwrap();

        let error = job_fs.write_job(job()).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(fs::read(job_fs.dir().join("job.json")).unwrap(), contents);
    }

    #[tokio::test]
    async fn node_round_trips_in_the_job_filesystem() {
        let root = tempdir().unwrap();
        let job_fs = JobFs::new(root.path().join("jobs"), Uuid::new_v4());
        let expected = Node {
            id: Uuid::new_v4(),
            name: "step".to_owned(),
            node_type: "shell".to_owned(),
            inputs: Vec::new(),
            outputs: Vec::new(),
        };

        job_fs.write_node(&expected).await.unwrap();

        assert_eq!(job_fs.node().await.unwrap(), expected);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &fs::read(job_fs.dir().join("node.json")).unwrap()
            )
            .unwrap(),
            serde_json::to_value(expected).unwrap()
        );
    }

    #[tokio::test]
    async fn result_round_trips_and_replaces_existing_contents() {
        let root = tempdir().unwrap();
        let job_fs = JobFs::new(root.path().join("jobs"), Uuid::new_v4());

        for exit_code in [-1234, 0, 42] {
            let result = JobResult {
                exit_code,
                ..Default::default()
            };
            job_fs.write_job_result(result.clone()).await.unwrap();
            assert_eq!(job_fs.job_result().await.unwrap(), result);
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(
                    &fs::read(job_fs.dir().join("result.json")).unwrap()
                )
                .unwrap(),
                json!({"exit_code": exit_code})
            );
        }
        assert_eq!(fs::read_dir(job_fs.dir()).unwrap().count(), 1);
    }

    #[tokio::test]
    async fn failed_replacement_cleans_up_the_temporary_file() {
        let root = tempdir().unwrap();
        let job_fs = JobFs::new(root.path(), Uuid::new_v4());
        let result_path = job_fs.dir().join("result.json");
        fs::create_dir_all(&result_path).unwrap();

        assert!(
            job_fs
                .write_job_result(JobResult {
                    exit_code: 0,
                    ..Default::default()
                })
                .await
                .is_err()
        );
        assert!(result_path.is_dir());
        assert_eq!(fs::read_dir(job_fs.dir()).unwrap().count(), 1);
    }

    #[tokio::test]
    async fn reads_container_metadata_using_the_type_key() {
        let root = tempdir().unwrap();
        let job_fs = JobFs::new(root.path(), Uuid::new_v4());
        fs::create_dir_all(job_fs.dir()).unwrap();
        fs::write(
            job_fs.dir().join("container.json"),
            br#"{"type":"docker","id":"container-id"}"#,
        )
        .unwrap();

        let container = job_fs.container().await.unwrap();
        assert_eq!(container.r#type, "docker");
        assert_eq!(container.id, "container-id");
        assert_eq!(
            serde_json::to_value(container).unwrap(),
            json!({"type": "docker", "id": "container-id"})
        );
    }

    #[tokio::test]
    async fn reads_runtime_timestamp_and_stdout_without_modifying_them() {
        let root = tempdir().unwrap();
        let job_fs = JobFs::new(root.path(), Uuid::new_v4());
        let runtime_dir = job_fs.dir().join("runtime");
        fs::create_dir_all(&runtime_dir).unwrap();
        let timestamp = "2026-10-28T10:20:30.123456789+08:00";
        fs::write(runtime_dir.join("started_at"), format!("{timestamp}\n")).unwrap();
        let output = b"first line\nsecond line\n\xff";
        fs::write(runtime_dir.join("stdout"), output).unwrap();

        assert_eq!(
            job_fs.started_at().await.unwrap(),
            timestamp.parse::<Timestamp>().unwrap()
        );
        let mut stdout = job_fs.stdout().await.unwrap();
        let mut contents = Vec::new();
        stdout.read_to_end(&mut contents).await.unwrap();
        assert_eq!(contents, output);
        assert!(stdout.write_all(b"overwrite").await.is_err() || stdout.flush().await.is_err());
        assert_eq!(fs::read(runtime_dir.join("stdout")).unwrap(), output);
    }

    #[tokio::test]
    async fn missing_reads_return_not_found_without_creating_files() {
        let root = tempdir().unwrap();
        let job_fs = JobFs::new(root.path(), Uuid::new_v4());

        assert_eq!(
            job_fs.job().await.unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        assert_eq!(
            job_fs.job_result().await.unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        assert_eq!(
            job_fs.container().await.unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        assert_eq!(
            job_fs.started_at().await.unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        assert_eq!(
            job_fs.stdout().await.unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        assert!(!job_fs.dir().exists());
    }

    #[tokio::test]
    async fn malformed_files_return_invalid_data() {
        let root = tempdir().unwrap();
        let job_fs = JobFs::new(root.path(), Uuid::new_v4());
        fs::create_dir_all(job_fs.dir().join("runtime")).unwrap();

        for contents in ["{", "null", "{}"] {
            for filename in ["job.json", "result.json", "container.json"] {
                fs::write(job_fs.dir().join(filename), contents).unwrap();
            }
            assert_eq!(
                job_fs.job().await.unwrap_err().kind(),
                io::ErrorKind::InvalidData
            );
            assert_eq!(
                job_fs.job_result().await.unwrap_err().kind(),
                io::ErrorKind::InvalidData
            );
            assert_eq!(
                job_fs.container().await.unwrap_err().kind(),
                io::ErrorKind::InvalidData
            );
        }
        for contents in ["", "not-a-timestamp", "2026-13-99T00:00:00Z"] {
            fs::write(job_fs.dir().join("runtime/started_at"), contents).unwrap();
            assert_eq!(
                job_fs.started_at().await.unwrap_err().kind(),
                io::ErrorKind::InvalidData
            );
        }
    }

    #[tokio::test]
    async fn writes_and_lock_propagate_directory_errors() {
        let root = tempdir().unwrap();
        let blocked_root = root.path().join("not-a-directory");
        fs::write(&blocked_root, b"original contents").unwrap();
        let job = job();
        let job_fs = JobFs::new(&blocked_root, job.uuid);

        assert!(job_fs.write_job(job).await.is_err());
        assert!(
            job_fs
                .write_job_result(JobResult {
                    exit_code: 0,
                    ..Default::default()
                })
                .await
                .is_err()
        );
        assert!(job_fs.lock().await.is_err());
        job_fs.unlock().await.unwrap();
        assert_eq!(fs::read(&blocked_root).unwrap(), b"original contents");
    }

    #[tokio::test]
    async fn lock_is_exclusive_and_can_be_reacquired_after_unlock() {
        let root = tempdir().unwrap();
        let job_id = Uuid::new_v4();
        let owner = JobFs::new(root.path().join("jobs"), job_id);
        let other = JobFs::new(root.path().join("jobs"), job_id);
        owner.unlock().await.unwrap();
        assert!(!owner.dir().exists());
        owner.lock().await.unwrap();
        owner.lock().await.unwrap();
        let probe = OpenOptions::new()
            .read(true)
            .write(true)
            .open(owner.dir().join("lock"))
            .unwrap();

        assert!(matches!(probe.try_lock(), Err(TryLockError::WouldBlock)));
        other.unlock().await.unwrap();
        assert!(matches!(probe.try_lock(), Err(TryLockError::WouldBlock)));
        owner.unlock().await.unwrap();
        owner.unlock().await.unwrap();
        probe.try_lock().unwrap();
        probe.unlock().unwrap();

        other.lock().await.unwrap();
        assert!(matches!(probe.try_lock(), Err(TryLockError::WouldBlock)));
        other.unlock().await.unwrap();
        owner.lock().await.unwrap();
        assert!(matches!(probe.try_lock(), Err(TryLockError::WouldBlock)));
    }

    #[tokio::test]
    async fn dropping_job_fs_releases_its_lock() {
        let root = tempdir().unwrap();
        let job_fs = JobFs::new(root.path(), Uuid::new_v4());
        job_fs.lock().await.unwrap();
        let probe = OpenOptions::new()
            .read(true)
            .write(true)
            .open(job_fs.dir().join("lock"))
            .unwrap();

        assert!(matches!(probe.try_lock(), Err(TryLockError::WouldBlock)));
        drop(job_fs);
        probe.try_lock().unwrap();
    }

    #[tokio::test]
    async fn contended_lock_yields_until_the_owner_releases_it() {
        let root = tempdir().unwrap();
        let job_id = Uuid::new_v4();
        let owner = JobFs::new(root.path(), job_id);
        let other = JobFs::new(root.path(), job_id);
        owner.lock().await.unwrap();
        let waiting = other.lock();
        tokio::pin!(waiting);
        tokio::select! {
            biased;
            result = &mut waiting => panic!("contended lock completed: {result:?}"),
            _ = sleep(Duration::from_millis(30)) => {}
        }
        owner.unlock().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap();
        other.unlock().await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_lock_wait_can_be_retried() {
        let root = tempdir().unwrap();
        let job_id = Uuid::new_v4();
        let owner = JobFs::new(root.path(), job_id);
        let other = JobFs::new(root.path(), job_id);
        owner.lock().await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(30), other.lock())
                .await
                .is_err()
        );
        owner.unlock().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), other.lock())
            .await
            .unwrap()
            .unwrap();
        other.unlock().await.unwrap();
    }

    #[tokio::test]
    async fn different_jobs_have_independent_locks() {
        let root = tempdir().unwrap();
        let first = JobFs::new(root.path(), Uuid::new_v4());
        let second = JobFs::new(root.path(), Uuid::new_v4());

        first.lock().await.unwrap();
        second.lock().await.unwrap();
        first.unlock().await.unwrap();
        second.unlock().await.unwrap();
    }
}
