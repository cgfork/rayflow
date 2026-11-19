//! Filesystem abstractions used by the agent daemon.

use std::path::{Path, PathBuf};

use ::graph::{GraphStore, types::DAG};
use catalog::PresignedUrl;
use jiff::Timestamp;
use serde::{Deserialize, Serialize};

pub mod graph;
pub mod job;
pub mod transfer;

pub use graph::GraphFs as GraphCache;
pub use job::JobFs as JobStore;
use scheduler::Job;
pub use transfer::JobTransfer;
use uuid::Uuid;

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

#[allow(async_fn_in_trait)]
pub trait Transfer {
    type Err;

    async fn prepare_upload(&self, job: &Job, filename: &str) -> Result<Uuid, Self::Err>;

    async fn commit_upload(
        &self,
        job: &Job,
        filename: &str,
        catalog_id: Uuid,
    ) -> Result<(), Self::Err>;

    async fn presign_download(&self, job: &Job, filename: &str) -> Result<PresignedUrl, Self::Err>;

    async fn prepare_input<J: JobFs, G: GraphFs + Clone>(
        &self,
        job_fs: &J,
        graph: &G,
    ) -> Result<(), Self::Err>;

    async fn complete_output<J: JobFs, G: GraphFs + Clone>(
        &self,
        job_fs: &J,
        graph: &G,
    ) -> Result<(), Self::Err>;
}

/// The durable filesystem for one job.
///
/// Metadata contains the job definition, container identity, and runtime
/// records. Data contains the input and output files exposed to the job.
#[allow(async_fn_in_trait)]
pub trait JobFs {
    /// Returns the job held by this filesystem.
    fn job(&self) -> &Job;

    /// Directory containing the job metadata and runtime records.
    fn metadata_dir(&self) -> &Path;

    /// Directory containing the job input and output files.
    fn data_dir(&self) -> &Path;

    fn dir(&self) -> PathBuf;

    fn input_dir(&self) -> PathBuf;

    fn output_dir(&self) -> PathBuf;

    fn state_dir(&self, job_id: Uuid) -> PathBuf;

    fn container(&self) -> Option<&Container>;

    async fn write_container(&mut self, container: Container) -> Result<(), crate::errors::Error>;

    fn job_result(&self) -> Option<&JobResult>;

    async fn write_job_result(&mut self, result: JobResult) -> Result<(), crate::errors::Error>;

    async fn write_runtime(
        &mut self,
        pid: Option<i64>,
        started_at: Option<jiff::Timestamp>,
    ) -> Result<(), crate::errors::Error>;

    fn started_at(&self) -> Option<&Timestamp>;

    fn pid(&self) -> Option<i64>;

    async fn clear(&mut self) -> Result<(), crate::errors::Error>;

    /// Removes the per-job artifact directory after remote acknowledgement.
    async fn clear_data(&mut self) -> Result<(), crate::errors::Error>;

    async fn lock(&self) -> Result<(), crate::errors::Error>;

    async fn unlock(&self) -> Result<(), crate::errors::Error>;

    async fn prepare_input<G: GraphFs + Clone, T: Transfer<Err = crate::errors::Error>>(
        &self,
        graph: &G,
        transfer: &T,
    ) -> Result<(), crate::errors::Error>;

    async fn complete_output<G: GraphFs + Clone, T: Transfer<Err = crate::errors::Error>>(
        &self,
        graph: &G,
        transfer: &T,
    ) -> Result<(), crate::errors::Error>;
}

/// A graph cache that can be used by the agent.
pub trait GraphFs: GraphStore<Uuid, G = DAG> {}

impl<T> GraphFs for T where T: GraphStore<Uuid, G = DAG> {}
