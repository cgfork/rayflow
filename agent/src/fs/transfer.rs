//! Transfers declared inputs and outputs through the job filesystem.
//!
//! Metadata requests use the fleet client; object requests use a separate client
//! without fleet credentials. Upload state survives restarts and success is
//! reported only after every output and directory manifest has been published.

use crate::client::Transfer as CatalogTransfer;
use catalog::directory::{
    DIRECTORY_FORMAT, DIRECTORY_MANIFEST, DirectoryEntry, DirectoryManifest, EntryKind,
    MAX_MANIFEST_BYTES,
};
use catalog::path::validate_relative_path;
use catalog::{PresignOperation, PresignedUrl};
use graph::types::{DAG, Kind, Node};
use scheduler::Job;
use std::{
    io,
    path::{Path, PathBuf},
};
use tokio::{fs, io::AsyncWriteExt};
use uuid::Uuid;

use crate::{
    client::Client,
    errors::Error,
    fs::{GraphFs, JobFs as JobFsTrait, Transfer},
    read_json, write_json,
};

#[derive(Clone)]
pub struct JobTransfer {
    client: Client,
    objects: reqwest::Client,
    agent_id: Uuid,
}

impl JobTransfer {
    pub fn new(client: Client) -> Result<Self, Error> {
        let request_timeout = client.settings().request_timeout;
        let agent_id = client.settings().id;
        let objects = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(request_timeout)
            .build()?;
        Ok(Self {
            client,
            agent_id,
            objects,
        })
    }

    pub async fn prepare_input<J: JobFsTrait, G: GraphFs + Clone>(
        &self,
        file_fs: &J,
        graph_store: &G,
    ) -> Result<(), Error> {
        let job = file_fs.job().clone();
        let node_run = job.node_run.get();
        let graph = graph_store
            .clone()
            .load(node_run.graph_id, Some(node_run.version))
            .await
            .map_err(|error| artifact(format!("load graph: {error}")))?;
        let node = graph
            .nodes
            .iter()
            .find(|node| node.id == node_run.node_id)
            .cloned()
            .ok_or_else(|| artifact("job node is missing from graph"))?;
        if node.inputs.is_empty() {
            return Ok(());
        }
        file_fs.lock().await?;
        let result = self.download_locked(file_fs, &job, &node).await;
        let unlock = file_fs.unlock().await;
        result.and(unlock)
    }

    async fn download_locked<J: JobFsTrait>(
        &self,
        file_fs: &J,
        job: &Job,
        node: &Node,
    ) -> Result<(), Error> {
        validate_node_paths(node)?;
        let job_id = job.uuid;
        if node.inputs.is_empty() || read_marker(file_fs, job_id, "inputs-ready").await? {
            return Ok(());
        }
        // Before startup, unfinished staging is disposable. Never follow a link
        // supplied in place of the input root.
        let root = file_fs.input_dir();
        match fs::symlink_metadata(&root).await {
            Ok(metadata) if metadata.is_dir() => fs::remove_dir_all(&root).await?,
            Ok(_) => return Err(artifact("input root is not a directory")),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        fs::create_dir_all(&root).await?;
        let graph = self.job_graph(job).await?;
        let state = self
            .client
            .run_state(job.run_id, job.node_run.get().revision)
            .await?;
        for input in &node.inputs {
            let Some(_filename) = self
                .input_filename(job, &graph, &state, &input.source)
                .await?
            else {
                if input.required {
                    return Err(artifact(format!(
                        "required input is unavailable: {}",
                        input.file_name
                    )));
                }
                continue;
            };
            let Some(transfer) = self
                .client
                .download_input(job_id, self.agent_id, &input.file_name)
                .await?
            else {
                if input.required {
                    return Err(artifact(format!(
                        "required input is unavailable: {}",
                        input.file_name
                    )));
                }
                continue;
            };
            let destination = root.join(&input.file_name);
            if transfer.catalog.format == DIRECTORY_FORMAT {
                let bytes = self.manifest_bytes(&transfer).await?;
                let manifest: DirectoryManifest = serde_json::from_slice(&bytes)
                    .map_err(|e| artifact(format!("invalid directory manifest: {e}")))?;
                manifest.validate().map_err(|e| artifact(e.to_string()))?;
                fs::create_dir_all(&destination).await?;
                for entry in &manifest.entries {
                    let path = destination.join(&entry.path);
                    match entry.kind {
                        EntryKind::Directory => fs::create_dir_all(&path).await?,
                        EntryKind::File => {
                            let member_filename = format!("{}/{}", input.file_name, entry.path);
                            let member = self
                                .client
                                .download_input(job_id, self.agent_id, &member_filename)
                                .await?
                                .ok_or_else(|| {
                                    artifact(format!(
                                        "directory member is unavailable: {}",
                                        entry.path
                                    ))
                                })?;
                            if member.catalog.size != entry.size
                                || member.catalog.format == DIRECTORY_FORMAT
                            {
                                return Err(artifact(
                                    "directory member metadata does not match manifest",
                                ));
                            }
                            self.download_file(&member, &path).await?;
                        }
                    }
                }
                write_json(&destination.join(DIRECTORY_MANIFEST), &manifest).await?;
            } else {
                self.download_file(&transfer, &destination).await?;
            }
        }
        write_marker(file_fs, job_id, "inputs-ready").await
    }

    pub async fn complete_output<J: JobFsTrait, G: GraphFs + Clone>(
        &self,
        file_fs: &J,
        graph_store: &G,
    ) -> Result<(), Error> {
        let job = file_fs.job().clone();
        let node_run = job.node_run.get();
        let graph = graph_store
            .clone()
            .load(node_run.graph_id, Some(node_run.version))
            .await
            .map_err(|error| artifact(format!("load graph: {error}")))?;
        let node = graph
            .nodes
            .iter()
            .find(|node| node.id == node_run.node_id)
            .cloned()
            .ok_or_else(|| artifact("job node is missing from graph"))?;
        if node.outputs.is_empty() {
            return Ok(());
        }
        file_fs.lock().await?;
        let result = self.upload_locked(file_fs, &job, &node).await;
        let unlock = file_fs.unlock().await;
        result.and(unlock)
    }

    async fn upload_locked<J: JobFsTrait>(
        &self,
        file_fs: &J,
        job: &Job,
        node: &Node,
    ) -> Result<(), Error> {
        validate_node_paths(node)?;
        let job_id = job.uuid;
        if node.outputs.is_empty() || read_marker(file_fs, job_id, "outputs-uploaded").await? {
            return Ok(());
        }
        let root = file_fs.output_dir();
        for output in &node.outputs {
            let path = safe_existing(&root, &output.file_name).await?;
            if output.kind == Kind::Directory || output.multiple {
                let manifest = directory_manifest(&path).await?;
                let bytes = serde_json::to_vec(&manifest)?;
                if bytes.len() as u64 > MAX_MANIFEST_BYTES {
                    return Err(artifact("directory manifest is too large"));
                }
                for entry in &manifest.entries {
                    if entry.kind == EntryKind::File {
                        let file_path = safe_existing(&path, &entry.path).await?;
                        self.upload_one(
                            job_id,
                            &format!("{}/{}", output.file_name, entry.path),
                            &file_path,
                            &format_for(&entry.path),
                        )
                        .await?;
                    }
                }
                let manifest_path = path.join(DIRECTORY_MANIFEST);
                // Replace a previous internal manifest atomically, never follow it.
                write_json(&manifest_path, &manifest).await?;
                self.upload_one(
                    job_id,
                    &format!("{}/{}", output.file_name, DIRECTORY_MANIFEST),
                    &manifest_path,
                    DIRECTORY_FORMAT,
                )
                .await?;
            } else {
                self.upload_one(
                    job_id,
                    &output.file_name,
                    &path,
                    &format_for(&output.file_name),
                )
                .await?;
            }
        }
        write_marker(file_fs, job_id, "outputs-uploaded").await
    }

    async fn upload_one(
        &self,
        job_id: Uuid,
        filename: &str,
        path: &Path,
        format: &str,
    ) -> Result<(), Error> {
        let metadata = fs::symlink_metadata(path).await?;
        if !metadata.is_file() {
            return Err(artifact("output is not a regular file"));
        }
        let transfer = self
            .client
            .prepare_upload(job_id, self.agent_id, filename, metadata.len(), format)
            .await?;
        if transfer.presigned.operation != PresignOperation::Upload {
            return Err(artifact("server returned the wrong upload operation"));
        }
        let file = fs::File::open(path).await?;
        let body = reqwest::Body::wrap_stream(tokio_util::io::ReaderStream::new(file));
        let mut put = self
            .objects
            .put(&transfer.presigned.url)
            .header(reqwest::header::CONTENT_LENGTH, metadata.len())
            .body(body);
        for (name, value) in &transfer.presigned.headers {
            put = put.header(name, value);
        }
        put.send()
            .await
            .map_err(reqwest::Error::without_url)?
            .error_for_status()
            .map_err(reqwest::Error::without_url)?;
        self.client
            .commit_upload(
                job_id,
                self.agent_id,
                filename,
                transfer.catalog.uuid,
                metadata.len(),
                format,
            )
            .await
    }

    async fn job_graph(&self, job: &Job) -> Result<DAG, Error> {
        if job.run.is_unloaded() || job.node_run.is_unloaded() {
            return Err(artifact("job execution relations are not loaded"));
        }
        let node_run = job.node_run.get();
        let graph = self
            .client
            .graph(node_run.graph_id, Some(node_run.version))
            .await?;
        if graph.id != node_run.graph_id || graph.version != node_run.version {
            return Err(artifact("graph does not match the job's run version"));
        }
        Ok(graph)
    }

    async fn input_filename(
        &self,
        job: &Job,
        graph: &DAG,
        state: &crate::client::RunState,
        source: &str,
    ) -> Result<Option<String>, Error> {
        let mut sources = graph
            .nodes
            .iter()
            .flat_map(|node| node.outputs.iter().map(move |output| (node, output)))
            .filter(|(node, output)| source == format!("{}.{}", node.name, output.file_name));
        let (producer, output) = sources
            .next()
            .ok_or_else(|| artifact("input source is not declared"))?;
        if sources.next().is_some() {
            return Err(artifact("ambiguous input source"));
        }
        let Some(producer_run) = state
            .nodes
            .iter()
            .filter(|run| {
                run.node_id == producer.id
                    && run.revision <= job.node_run.get().revision
                    && run.ineffective == 0
            })
            .max_by_key(|run| run.revision)
            .filter(|run| run.status == "Succeeded")
        else {
            return Ok(None);
        };
        let producer_graph = if producer_run.version == graph.version {
            graph.clone()
        } else {
            self.client
                .graph(graph.id, Some(producer_run.version))
                .await?
        };
        if producer_graph.id != graph.id || producer_graph.version != producer_run.version {
            return Err(artifact(
                "producer graph does not match its recorded version",
            ));
        }
        let Some(producer_node) = producer_graph
            .nodes
            .iter()
            .find(|node| node.id == producer.id)
        else {
            return Ok(None);
        };
        let Some(output) = producer_node
            .outputs
            .iter()
            .find(|candidate| candidate.file_name == output.file_name)
        else {
            return Ok(None);
        };
        let key = if output.kind == Kind::Directory || output.multiple {
            format!("{}/{}", output.file_name, DIRECTORY_MANIFEST)
        } else {
            output.file_name.clone()
        };
        Ok(Some(key))
    }

    async fn response(&self, transfer: &CatalogTransfer) -> Result<reqwest::Response, Error> {
        if transfer.presigned.operation != PresignOperation::Download {
            return Err(artifact("server returned the wrong download operation"));
        }
        let mut request = self.objects.get(&transfer.presigned.url);
        for (name, value) in &transfer.presigned.headers {
            request = request.header(name, value);
        }
        Ok(request
            .send()
            .await
            .map_err(reqwest::Error::without_url)?
            .error_for_status()
            .map_err(reqwest::Error::without_url)?)
    }

    async fn manifest_bytes(&self, transfer: &CatalogTransfer) -> Result<Vec<u8>, Error> {
        if transfer.catalog.size > MAX_MANIFEST_BYTES {
            return Err(artifact("directory manifest is too large"));
        }
        let mut response = self.response(transfer).await?;
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(reqwest::Error::without_url)?
        {
            if bytes.len() as u64 + chunk.len() as u64 > transfer.catalog.size {
                return Err(artifact("manifest size exceeds catalog metadata"));
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.len() as u64 != transfer.catalog.size {
            return Err(artifact("incomplete directory manifest"));
        }
        Ok(bytes)
    }

    async fn download_file(
        &self,
        transfer: &CatalogTransfer,
        destination: &Path,
    ) -> Result<(), Error> {
        let parent = destination
            .parent()
            .ok_or_else(|| artifact("input has no parent"))?;
        fs::create_dir_all(parent).await?;
        let temporary = parent.join(format!(".{}.download", Uuid::new_v4()));
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .await?;
        let result = async {
            let mut response = self.response(transfer).await?;
            let mut size = 0u64;
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(reqwest::Error::without_url)?
            {
                size = size
                    .checked_add(chunk.len() as u64)
                    .ok_or_else(|| artifact("input size overflow"))?;
                if size > transfer.catalog.size {
                    return Err(artifact("input exceeds catalog size"));
                }
                file.write_all(&chunk).await?;
            }
            if size != transfer.catalog.size {
                return Err(artifact("incomplete input download"));
            }
            file.sync_all().await?;
            fs::rename(&temporary, destination).await?;
            Ok(())
        }
        .await;
        if result.is_err() {
            let _ = fs::remove_file(&temporary).await;
        }
        result
    }
}

impl Transfer for JobTransfer {
    type Err = Error;

    async fn prepare_upload(&self, job: &Job, filename: &str) -> Result<Uuid, Self::Err> {
        Ok(self
            .client
            .prepare_upload(job.uuid, self.agent_id, filename, 0, "")
            .await?
            .catalog
            .uuid)
    }

    async fn commit_upload(
        &self,
        job: &Job,
        filename: &str,
        catalog_id: Uuid,
    ) -> Result<(), Self::Err> {
        self.client
            .commit_upload(job.uuid, self.agent_id, filename, catalog_id, 0, "")
            .await
    }

    async fn presign_download(&self, job: &Job, filename: &str) -> Result<PresignedUrl, Self::Err> {
        self.client
            .download_input(job.uuid, self.agent_id, filename)
            .await?
            .map(|transfer| transfer.presigned)
            .ok_or_else(|| artifact("input is unavailable"))
    }

    async fn prepare_input<J: JobFsTrait, G: GraphFs + Clone>(
        &self,
        job_fs: &J,
        graph: &G,
    ) -> Result<(), Self::Err> {
        JobTransfer::prepare_input(self, job_fs, graph).await
    }

    async fn complete_output<J: JobFsTrait, G: GraphFs + Clone>(
        &self,
        job_fs: &J,
        graph: &G,
    ) -> Result<(), Self::Err> {
        JobTransfer::complete_output(self, job_fs, graph).await
    }
}

fn artifact(message: impl Into<String>) -> Error {
    Error::Artifact(message.into())
}

fn format_for(path: &str) -> String {
    Path::new(path)
        .extension()
        .and_then(|s| s.to_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("application/octet-stream")
        .to_owned()
}

fn validate_node_paths(node: &Node) -> Result<(), Error> {
    for paths in [
        node.inputs
            .iter()
            .map(|i| i.file_name.as_str())
            .collect::<Vec<_>>(),
        node.outputs.iter().map(|o| o.file_name.as_str()).collect(),
    ] {
        for (i, path) in paths.iter().enumerate() {
            validate_relative_path(path).map_err(|e| artifact(e.to_string()))?;
            if path.split('/').any(|p| p == DIRECTORY_MANIFEST) {
                return Err(artifact("reserved manifest name"));
            }
            if paths.iter().enumerate().any(|(j, other)| {
                i != j && (*other == *path || other.starts_with(&format!("{path}/")))
            }) {
                return Err(artifact("artifact declarations overlap"));
            }
        }
    }
    Ok(())
}

async fn safe_existing(root: &Path, relative: &str) -> Result<PathBuf, Error> {
    validate_relative_path(relative).map_err(|e| artifact(e.to_string()))?;
    let mut current = root.to_path_buf();
    let root_meta = fs::symlink_metadata(&current)
        .await
        .map_err(|e| artifact(format!("output root: {e}")))?;
    if !root_meta.is_dir() {
        return Err(artifact("output root is not a directory"));
    }
    for part in relative.split('/') {
        current.push(part);
        let metadata = fs::symlink_metadata(&current)
            .await
            .map_err(|e| artifact(format!("missing output {relative}: {e}")))?;
        if !metadata.is_file() && !metadata.is_dir() {
            return Err(artifact("symlinks and special files cannot be uploaded"));
        }
    }
    Ok(current)
}

async fn directory_manifest(root: &Path) -> Result<DirectoryManifest, Error> {
    if !fs::symlink_metadata(root).await?.is_dir() {
        return Err(artifact("directory output is not a directory"));
    }
    let mut entries = Vec::new();
    let mut pending = vec![PathBuf::new()];
    while let Some(relative) = pending.pop() {
        let mut children = fs::read_dir(root.join(&relative)).await?;
        while let Some(child) = children.next_entry().await? {
            let name = child
                .file_name()
                .into_string()
                .map_err(|_| artifact("artifact names must be UTF-8"))?;
            if name == DIRECTORY_MANIFEST {
                if relative.as_os_str().is_empty() {
                    continue;
                }
                return Err(artifact("reserved manifest name in output directory"));
            }
            let path = relative.join(&name);
            let path_string = path
                .to_str()
                .ok_or_else(|| artifact("artifact path must be UTF-8"))?
                .to_owned();
            validate_relative_path(&path_string).map_err(|e| artifact(e.to_string()))?;
            let metadata = fs::symlink_metadata(child.path()).await?;
            let kind = if metadata.is_dir() {
                pending.push(path);
                EntryKind::Directory
            } else if metadata.is_file() {
                EntryKind::File
            } else {
                return Err(artifact("symlinks and special files cannot be uploaded"));
            };
            entries.push(DirectoryEntry {
                path: path_string,
                kind,
                size: if kind == EntryKind::File {
                    metadata.len()
                } else {
                    0
                },
            });
            if entries.len() > 10_000 {
                return Err(artifact("directory has too many entries"));
            }
        }
    }
    entries.sort_by(|a, b| a.path.cmp(&b.path));
    let manifest = DirectoryManifest {
        version: 1,
        entries,
    };
    manifest.validate().map_err(|e| artifact(e.to_string()))?;
    Ok(manifest)
}

async fn read_marker<J: JobFsTrait>(file_fs: &J, job_id: Uuid, name: &str) -> Result<bool, Error> {
    match read_json(&file_fs.state_dir(job_id).join(format!("{name}.json"))).await {
        Ok(value) => Ok(value),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

async fn write_marker<J: JobFsTrait>(file_fs: &J, job_id: Uuid, name: &str) -> Result<(), Error> {
    write_json(
        &file_fs.state_dir(job_id).join(format!("{name}.json")),
        &true,
    )
    .await
}
