//! Downloads declared inputs and publishes outputs through the catalog API.
//!
//! Metadata requests use the fleet client; object requests use a separate client
//! without fleet credentials. Upload state survives restarts and success is
//! reported only after every output and directory manifest has been published.

use catalog::{
    PresignOperation,
    artifact::{
        CommitUploadRequest, DIRECTORY_FORMAT, DIRECTORY_MANIFEST, DirectoryEntry,
        DirectoryManifest, DownloadRequest, EntryKind, MAX_MANIFEST_BYTES, PrepareUploadRequest,
        Scope, Transfer, validate_relative_path,
    },
};
use graph::types::{DAG, Kind, Node, Output};
use scheduler::Job;
use std::{
    io,
    path::{Path, PathBuf},
};
use tokio::{fs, io::AsyncWriteExt};
use uuid::Uuid;

use crate::{
    client::Client, errors::Error, filefs::FileFs, read_json, settings::Settings, write_json,
};

#[derive(Clone)]
pub struct Filed {
    client: Client,
    objects: reqwest::Client,
    agent_id: Uuid,
    data_dir: PathBuf,
}

impl Filed {
    pub fn new(client: Client, settings: &Settings) -> Result<Self, Error> {
        Ok(Self {
            client,
            data_dir: settings.data_dir.clone(),
            agent_id: settings.id,
            objects: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(settings.request_timeout)
                .build()?,
        })
    }

    pub fn file_fs(&self, job: &Job) -> FileFs {
        let node_run = job.node_run.get();
        FileFs::new(
            &self.data_dir,
            job.run_id,
            node_run.node_id,
            node_run.revision,
        )
    }

    pub async fn download_inputs(&self, job: &Job, node: &Node) -> Result<(), Error> {
        if node.inputs.is_empty() {
            return Ok(());
        }
        let file_fs = self.file_fs(job);
        file_fs.lock().await?;
        let result = self.download_locked(&file_fs, job, node).await;
        let unlock = file_fs.unlock().await;
        result.and(unlock)
    }

    async fn download_locked(&self, file_fs: &FileFs, job: &Job, node: &Node) -> Result<(), Error> {
        validate_node_paths(node)?;
        let job_id = job.uuid;
        if node.inputs.is_empty() || read_marker(file_fs, job_id, "inputs-ready").await? {
            return Ok(());
        }
        // Before startup, unfinished staging is disposable. Never follow a link
        // supplied in place of the input root.
        let root = file_fs.inputs_dir();
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
            let Some(key) = self.input_key(job, &graph, &state, &input.source).await? else {
                if input.required {
                    return Err(artifact(format!(
                        "required input is unavailable: {}",
                        input.file_name
                    )));
                }
                continue;
            };
            let request = DownloadRequest {
                job_id,
                agent_id: self.agent_id,
                key,
            };
            let Some(transfer) = self.client.download_input(&request).await? else {
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
                            let member_request = DownloadRequest {
                                key: format!(
                                    "{}{path}",
                                    request.key.strip_suffix(DIRECTORY_MANIFEST).ok_or_else(
                                        || artifact("directory manifest key is invalid")
                                    )?,
                                    path = entry.path
                                ),
                                ..request.clone()
                            };
                            let member = self
                                .client
                                .download_input(&member_request)
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

    pub async fn upload_outputs(&self, job: &Job, node: &Node) -> Result<(), Error> {
        if node.outputs.is_empty() {
            return Ok(());
        }
        let file_fs = self.file_fs(job);
        file_fs.lock().await?;
        let result = self.upload_locked(&file_fs, job, node).await;
        let unlock = file_fs.unlock().await;
        result.and(unlock)
    }

    async fn upload_locked(&self, file_fs: &FileFs, job: &Job, node: &Node) -> Result<(), Error> {
        validate_node_paths(node)?;
        let job_id = job.uuid;
        if node.outputs.is_empty() || read_marker(file_fs, job_id, "outputs-uploaded").await? {
            return Ok(());
        }
        let root = file_fs.outputs_dir();
        let graph = self.job_graph(job).await?;
        let scope = Scope {
            run_id: job.run_id,
            graph_name: graph.name,
            version: graph.version,
            node_name: node.name.clone(),
            revision: job.node_run.get().revision,
        };
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
                            &scope
                                .key(&format!("{}/{}", output.file_name, entry.path))
                                .map_err(|e| artifact(e.to_string()))?,
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
                    &output_key(&scope, output)?,
                    &manifest_path,
                    DIRECTORY_FORMAT,
                )
                .await?;
            } else {
                self.upload_one(
                    job_id,
                    &output_key(&scope, output)?,
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
        key: &str,
        path: &Path,
        format: &str,
    ) -> Result<(), Error> {
        let metadata = fs::symlink_metadata(path).await?;
        if !metadata.is_file() {
            return Err(artifact("output is not a regular file"));
        }
        let request = PrepareUploadRequest {
            job_id,
            agent_id: self.agent_id,
            key: key.to_owned(),
            size: metadata.len(),
            format: format.to_owned(),
        };
        let transfer = self.client.prepare_upload(&request).await?;
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
            .commit_upload(&CommitUploadRequest {
                job_id,
                agent_id: request.agent_id,
                key: request.key,
                catalog_uuid: transfer.catalog.uuid,
                size: request.size,
                format: request.format,
            })
            .await
    }

    async fn job_graph(&self, job: &Job) -> Result<DAG, Error> {
        if job.run.is_unloaded() || job.node_run.is_unloaded() {
            return Err(artifact("job execution relations are not loaded"));
        }
        let run = job.run.get();
        let graph = self.client.graph(run.graph_id, Some(run.version)).await?;
        if graph.id != run.graph_id || graph.version != run.version {
            return Err(artifact("graph does not match the job's run version"));
        }
        Ok(graph)
    }

    async fn input_key(
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
        let scope = Scope {
            run_id: job.run_id,
            graph_name: producer_graph.name.clone(),
            version: producer_graph.version,
            node_name: producer_node.name.clone(),
            revision: producer_run.revision,
        };
        output_key(&scope, output).map(Some)
    }

    async fn response(&self, transfer: &Transfer) -> Result<reqwest::Response, Error> {
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

    async fn manifest_bytes(&self, transfer: &Transfer) -> Result<Vec<u8>, Error> {
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

    async fn download_file(&self, transfer: &Transfer, destination: &Path) -> Result<(), Error> {
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

fn artifact(message: impl Into<String>) -> Error {
    Error::Artifact(message.into())
}

fn output_key(scope: &Scope, output: &Output) -> Result<String, Error> {
    let path = if output.kind == Kind::Directory || output.multiple {
        format!("{}/{DIRECTORY_MANIFEST}", output.file_name)
    } else {
        output.file_name.clone()
    };
    scope.key(&path).map_err(|e| artifact(e.to_string()))
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

async fn read_marker(file_fs: &FileFs, job_id: Uuid, name: &str) -> Result<bool, Error> {
    match read_json(&file_fs.state_dir(job_id).join(format!("{name}.json"))).await {
        Ok(value) => Ok(value),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

async fn write_marker(file_fs: &FileFs, job_id: Uuid, name: &str) -> Result<(), Error> {
    write_json(
        &file_fs.state_dir(job_id).join(format!("{name}.json")),
        &true,
    )
    .await
}
