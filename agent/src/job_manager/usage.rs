use std::{
    collections::{HashSet, VecDeque},
    io,
    path::{Path, PathBuf},
    time::Duration,
};

use futures_util::{StreamExt, stream::FuturesUnordered};
use graph::{GraphStore, types::DAG};
use jiff::Timestamp;
use scheduler::JobArtifactName;
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{self, OpenOptions},
    io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader},
    sync::Semaphore,
    time::{Instant, MissedTickBehavior, interval, interval_at, sleep},
};
use uuid::Uuid;

use crate::{
    containerd::{Container, Containerd},
    errors::Error,
    write_json,
};

use super::{JobManager, completion::CLEANUP_FILE, is_not_found, read_optional_json};

mod summary;

const SCAN_INTERVAL: Duration = Duration::from_secs(1);
const STREAM_RETRY_DELAY: Duration = Duration::from_millis(250);
const FLUSH_INTERVAL: Duration = Duration::from_millis(500);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(2);
const BATCH_SIZE: usize = 32;
const CHECKPOINT_FILE: &str = "checkpoint.jsonl";
const MAX_SUMMARIES: usize = 4;

/// One observation of an executing container. Providers omit unavailable
/// measurements.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    pub sampled_at: Timestamp,
    /// CPU time accumulated across all cores, in nanoseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_usage_ns: Option<u64>,
    /// Memory charged to the container, including file cache.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_usage_bytes: Option<u64>,
    /// Memory usage excluding inactive file cache, when reported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_working_set_bytes: Option<u64>,
    /// Memory ceiling reported by the provider, which may be the host ceiling.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_limit_bytes: Option<u64>,
}

/// Empirical percentiles of recorded values, using nearest rank
/// `ceil(p * n / 100)` with one observation per sample or interval.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsagePercentiles {
    pub p50: u64,
    pub p90: u64,
    pub p95: u64,
    pub p99: u64,
}

/// Resource usage observed over a job's recorded samples.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageSummary {
    /// The most recent observation.
    #[serde(flatten)]
    pub latest: Usage,
    /// Last cumulative counter for the execution, when no reset was detected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_cpu_usage_ns: Option<u64>,
    /// Highest average CPU use between two samples, in thousandths of a core.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peak_cpu_millicores: Option<u64>,
    /// Percentiles of valid interval CPU rates, in thousandths of a core.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_millicores_percentiles: Option<UsagePercentiles>,
    /// Highest sampled charged memory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peak_memory_usage_bytes: Option<u64>,
    /// Percentiles of sampled charged memory, in bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_usage_bytes_percentiles: Option<UsagePercentiles>,
    /// Approximate integral of charged memory over observed intervals.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_memory_usage_byte_seconds: Option<u64>,
    /// Time covered by the charged memory integral, in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_observed_ms: Option<u64>,
    /// Highest sampled memory working set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peak_memory_working_set_bytes: Option<u64>,
    /// Percentiles of the sampled memory working set, in bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_working_set_bytes_percentiles: Option<UsagePercentiles>,
    /// Approximate integral of the working set over observed intervals.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_memory_working_set_byte_seconds: Option<u64>,
    /// Time covered by the working set integral, in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_working_set_observed_ms: Option<u64>,
}

/// Finalized usage for one job. An empty summary records that no samples were available.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct JobUsage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<UsageSummary>,
}

#[derive(Deserialize)]
struct LegacyResult {
    usage: Option<UsageSummary>,
}

impl<C, R> JobManager<C, R>
where
    C: Containerd,
    R: GraphStore<Uuid, G = DAG> + Clone + Send + Sync,
{
    /// Tracks running containers, finalizes usage after their stats streams end,
    /// and retries unconfirmed usage uploads until completion or shutdown.
    pub async fn start_usage(&self) -> Result<(), Error> {
        let _watch = self
            .inner
            .watching_usage
            .try_lock()
            .map_err(|_| Error::AlreadyRunning)?;
        let summaries = Semaphore::new(MAX_SUMMARIES);
        let mut tracked = HashSet::new();
        let mut tracking = FuturesUnordered::new();
        let mut scan = interval(SCAN_INTERVAL);
        scan.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = scan.tick() => {}
                _ = self.inner.usage_changed.notified() => {}
                Some((job_id, result)) = tracking.next(), if !tracking.is_empty() => {
                    tracked.remove(&job_id);
                    if let Err(error) = result
                        && !is_not_found(&error)
                    {
                        tracing::warn!(%job_id, %error, "job usage could not be processed");
                    }
                    continue;
                }
            }
            let job_ids = match self.job_ids().await {
                Ok(ids) => ids,
                Err(error) => {
                    tracing::warn!(%error, "could not discover jobs for usage monitoring");
                    continue;
                }
            };
            for job_id in job_ids {
                if tracked.insert(job_id) {
                    let summaries = &summaries;
                    tracking
                        .push(async move { (job_id, self.advance_usage(job_id, summaries).await) });
                }
            }
        }
    }

    async fn advance_usage(&self, job_id: Uuid, summaries: &Semaphore) -> Result<(), Error> {
        let job_dir = self.job_dir(job_id);
        let usage_path = job_dir.join(JobArtifactName::Usage.as_str());
        if fs::try_exists(job_dir.join(CLEANUP_FILE)).await?
            || !fs::try_exists(job_dir.join("job.json")).await?
        {
            return Ok(());
        }
        if !fs::try_exists(&usage_path).await? {
            let result_path = job_dir.join(JobArtifactName::Result.as_str());
            if !fs::try_exists(&result_path).await? {
                let Some(metadata) = self.read_container(job_id).await? else {
                    return Ok(());
                };
                track_usage(job_id, &job_dir, &result_path, &self.container(&metadata)?).await?;
            }

            // This job's tracking future has stopped appending. Other jobs and
            // lifecycle reconciliation can proceed while the history is read.
            let summary = {
                let _slot = summaries
                    .acquire()
                    .await
                    .expect("summary semaphore stays open");
                match legacy_usage(&result_path).await? {
                    Some(summary) => Some(summary),
                    None => summarize_checkpoints(job_id, &job_dir).await?,
                }
            };
            let _lock = self.lock_job(job_id).await?;
            if !fs::try_exists(&result_path).await? {
                return Ok(());
            }
            if !fs::try_exists(&usage_path).await? {
                write_json(&usage_path, &JobUsage { summary }).await?;
            }
        }
        if let Err(error) = self.reclaim_container(job_id).await
            && !is_not_found(&error)
        {
            tracing::warn!(%job_id, %error, "could not reclaim stopped container");
        }
        if !fs::try_exists(job_dir.join("usage.json.upload")).await?
            && self.upload_artifact(job_id, JobArtifactName::Usage).await?
        {
            self.inner.lifecycle_changed.notify_one();
        }
        Ok(())
    }

    /// Keeps the old embedded summary if the result is about to be rewritten.
    /// The caller holds the job lock.
    pub(super) async fn preserve_legacy_usage(&self, job_id: Uuid) -> Result<(), Error> {
        let job_dir = self.job_dir(job_id);
        let usage_path = job_dir.join(JobArtifactName::Usage.as_str());
        if !fs::try_exists(&usage_path).await?
            && let Some(summary) =
                legacy_usage(&job_dir.join(JobArtifactName::Result.as_str())).await?
        {
            write_json(
                &usage_path,
                &JobUsage {
                    summary: Some(summary),
                },
            )
            .await?;
        }
        Ok(())
    }
}

async fn legacy_usage(path: &Path) -> Result<Option<UsageSummary>, Error> {
    Ok(read_optional_json::<LegacyResult>(path)
        .await?
        .and_then(|result| result.usage))
}

async fn track_usage(
    job_id: Uuid,
    job_dir: &Path,
    result_path: &Path,
    container: &impl Container,
) -> Result<(), Error> {
    let mut samples = VecDeque::new();
    let mut flush = interval_at(Instant::now() + FLUSH_INTERVAL, FLUSH_INTERVAL);
    flush.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut flush_on_full = true;
    let mut stream = container.usage();
    let retry = sleep(STREAM_RETRY_DELAY);
    let drain = sleep(DRAIN_TIMEOUT);
    tokio::pin!(retry, drain);
    let mut reconnecting = false;
    let mut draining = false;
    loop {
        tokio::select! {
            biased;
            () = &mut drain, if draining => break,
            _ = flush.tick() => {
                if !draining && fs::try_exists(result_path).await? {
                    if reconnecting {
                        break;
                    }
                    draining = true;
                    drain.as_mut().reset(Instant::now() + DRAIN_TIMEOUT);
                }
                flush_on_full = flush_samples(job_id, job_dir, &mut samples).await;
            }
            () = &mut retry, if reconnecting => {
                if fs::try_exists(result_path).await? {
                    break;
                }
                stream = container.usage();
                reconnecting = false;
            }
            item = stream.next(), if !reconnecting => match item {
                Some(Ok(sample)) => {
                    // ponytail: retain one batch during write failures. Increase
                    // the cap if longer outages need to preserve more samples.
                    if samples.len() == BATCH_SIZE {
                        samples.pop_front();
                    }
                    samples.push_back(sample);
                    if samples.len() == BATCH_SIZE && flush_on_full {
                        flush_on_full = flush_samples(job_id, job_dir, &mut samples).await;
                    }
                }
                Some(Err(error)) => {
                    tracing::debug!(%job_id, %error, "could not read container usage");
                }
                None => {
                    if draining || fs::try_exists(result_path).await? {
                        break;
                    }
                    reconnecting = true;
                    retry.as_mut().reset(Instant::now() + STREAM_RETRY_DELAY);
                }
            }
        }
    }

    // After the bounded drain, retain the last batch until it is safely on disk.
    drop(stream);
    while !flush_samples(job_id, job_dir, &mut samples).await {
        sleep(FLUSH_INTERVAL).await;
    }
    Ok(())
}

async fn flush_samples(job_id: Uuid, job_dir: &Path, samples: &mut VecDeque<Usage>) -> bool {
    if samples.is_empty() {
        return true;
    }
    if let Err(error) = append_samples(job_dir, samples).await {
        tracing::debug!(%job_id, %error, "could not save container usage");
        return false;
    }
    samples.clear();
    true
}

async fn checkpoint_path(job_dir: &Path, create: bool) -> Result<PathBuf, Error> {
    let usage_dir = job_dir.join("usage");
    match fs::symlink_metadata(&usage_dir).await {
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => return Err(invalid_path(&usage_dir)),
        Err(error) if create && error.kind() == io::ErrorKind::NotFound => {
            fs::create_dir(&usage_dir).await?;
        }
        Err(error) => return Err(error.into()),
    }
    let path = usage_dir.join(CHECKPOINT_FILE);
    match fs::symlink_metadata(&path).await {
        Ok(metadata) if metadata.is_file() => {}
        Ok(_) => return Err(invalid_path(&path)),
        Err(error) if create && error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(path)
}

fn invalid_path(path: &Path) -> Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("invalid usage checkpoint path: {}", path.display()),
    )
    .into()
}

async fn append_samples(job_dir: &Path, samples: &VecDeque<Usage>) -> Result<(), Error> {
    let path = checkpoint_path(job_dir, true).await?;
    let mut options = OpenOptions::new();
    options.create(true).read(true).append(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options.open(path).await?;
    let mut contents = Vec::new();
    if file.metadata().await?.len() > 0 {
        file.seek(io::SeekFrom::End(-1)).await?;
        if file.read_u8().await? != b'\n' {
            // Separate a line interrupted by a crash from the new samples.
            contents.push(b'\n');
        }
    }
    for sample in samples {
        serde_json::to_writer(&mut contents, sample)?;
        contents.push(b'\n');
    }
    file.write_all(&contents).await?;
    file.flush().await?;
    file.sync_data().await?;
    Ok(())
}

async fn summarize_checkpoints(
    job_id: Uuid,
    job_dir: &Path,
) -> Result<Option<UsageSummary>, Error> {
    let path = match checkpoint_path(job_dir, false).await {
        Ok(path) => path,
        Err(error) if is_not_found(&error) => return Ok(None),
        Err(error) => return Err(error),
    };
    let mut lines = BufReader::new(fs::File::open(path).await?).split(b'\n');
    let mut samples = Vec::new();
    while let Some(line) = lines.next_segment().await? {
        if line.is_empty() {
            continue;
        }
        match serde_json::from_slice::<Usage>(&line) {
            Ok(sample) => samples.push(sample),
            Err(error) => tracing::debug!(%job_id, %error, "could not read usage checkpoint line"),
        }
    }
    Ok(tokio::task::spawn_blocking(move || summary::summarize(samples)).await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn resumes_writing_usage_after_a_torn_checkpoint_line() {
        let dir = tempfile::tempdir().unwrap();
        let path = checkpoint_path(dir.path(), true).await.unwrap();
        fs::write(path, b"{\"sampled_at\":").await.unwrap();
        let sample = Usage {
            sampled_at: Timestamp::UNIX_EPOCH,
            cpu_usage_ns: Some(199),
            memory_usage_bytes: None,
            memory_working_set_bytes: None,
            memory_limit_bytes: None,
        };
        append_samples(dir.path(), &VecDeque::from([sample.clone()]))
            .await
            .unwrap();

        let usage = summarize_checkpoints(Uuid::nil(), dir.path())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(usage.latest, sample);
    }
}
