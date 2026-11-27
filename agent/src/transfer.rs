//! Transfers declared job inputs, outputs, and metadata.
//!
//! The fleet client authorizes transfers. The object client sends only the
//! headers supplied with the presigned request. Transfer checkpoints live
//! under the job's data directory.

use crate::client::Transfer as CatalogTransfer;
use catalog::PresignOperation;
use catalog::directory::{
    DIRECTORY_FORMAT, DIRECTORY_MANIFEST, DirectoryEntry, DirectoryManifest, EntryKind,
    MAX_MANIFEST_BYTES,
};
use catalog::path::validate_relative_path;
use graph::types::{Kind, Node};
use scheduler::{Job, JobArtifactName};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    io,
    path::{Path, PathBuf},
    time::SystemTime,
};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom},
};
use uuid::Uuid;

use crate::{client::Client, errors::Error, read_json, write_json};

mod assets;

pub(crate) const ASSET_DIR: &str = "asset";

#[derive(Clone)]
pub struct JobTransfer {
    client: Client,
    objects: reqwest::Client,
    agent_id: Uuid,
}

const MULTIPART_PART_SIZE: u64 = 8 * 1024 * 1024;
const MAX_MULTIPART_PARTS: u64 = 10_000;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct MultipartState {
    catalog: catalog::model::Catalog,
    upload_id: String,
    size: u64,
    modified: SystemTime,
    parts: BTreeMap<u32, String>,
}

struct UploadSource<'a> {
    filename: &'a str,
    path: &'a Path,
    format: &'a str,
    size: u64,
    modified: SystemTime,
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

    /// Returns false when object storage is disabled.
    pub(crate) async fn upload_artifact(
        &self,
        job_id: Uuid,
        filename: JobArtifactName,
        bytes: Vec<u8>,
    ) -> Result<bool, Error> {
        let size = bytes.len() as u64;
        let Some(transfer) = self
            .client
            .prepare_job_artifact(job_id, self.agent_id, filename, size)
            .await?
        else {
            return Ok(false);
        };
        let suffix = format!("/jobs/{job_id}/{}", filename.as_str());
        if transfer.catalog.size != size
            || transfer.catalog.format != "application/json"
            || !transfer.catalog.key.ends_with(&suffix)
        {
            return Err(artifact(
                "server returned inconsistent job artifact metadata",
            ));
        }
        if !transfer.catalog.uploaded {
            if transfer.presigned.operation != PresignOperation::Upload {
                return Err(artifact(
                    "server returned the wrong job artifact upload operation",
                ));
            }
            // Use the object client, which never carries fleet credentials.
            let mut put = self
                .objects
                .put(&transfer.presigned.url)
                .header(reqwest::header::CONTENT_LENGTH, size)
                .body(bytes);
            for (name, value) in &transfer.presigned.headers {
                put = put.header(name, value);
            }
            let status = put
                .send()
                .await
                .map_err(reqwest::Error::without_url)?
                .error_for_status()
                .map_err(reqwest::Error::without_url)?
                .status();
            if !status.is_success() {
                return Err(artifact(format!(
                    "job artifact upload returned HTTP {status}"
                )));
            }
            self.client
                .commit_job_artifact(job_id, self.agent_id, filename, transfer.catalog.uuid)
                .await?;
        }
        Ok(true)
    }

    pub async fn prepare_input(
        &self,
        job: &Job,
        data_dir: &Path,
        node: &Node,
    ) -> Result<(), Error> {
        let job_id = job.uuid;
        let state_dir = state_dir(data_dir, job_id);
        validate_node_paths(node)?;
        if node.inputs.is_empty() || read_marker(&state_dir, "inputs-ready").await? {
            return Ok(());
        }
        // Before startup, unfinished staging is disposable. Never follow a link
        // supplied in place of the input root.
        let root = data_dir.join("inputs");
        match fs::symlink_metadata(&root).await {
            Ok(metadata) if metadata.is_dir() => fs::remove_dir_all(&root).await?,
            Ok(_) => return Err(artifact("input root is not a directory")),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        fs::create_dir_all(&root).await?;
        for input in &node.inputs {
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
        write_marker(&state_dir, "inputs-ready").await
    }

    pub(crate) async fn outputs_uploaded(data_dir: &Path, job_id: Uuid) -> Result<bool, Error> {
        read_marker(&state_dir(data_dir, job_id), "outputs-uploaded").await
    }

    pub async fn complete_output(
        &self,
        job: &Job,
        data_dir: &Path,
        node: &Node,
    ) -> Result<(), Error> {
        let job_id = job.uuid;
        let state_dir = state_dir(data_dir, job_id);
        if read_marker(&state_dir, "outputs-uploaded").await? {
            return Ok(());
        }
        validate_node_paths(node)?;
        let root = data_dir.join("outputs");
        for output in &node.outputs {
            let path = safe_existing(&root, &output.file_name).await?;
            if output.kind == Kind::Directory {
                let manifest = directory_manifest(&path).await?;
                let bytes = serde_json::to_vec(&manifest)?;
                if bytes.len() as u64 > MAX_MANIFEST_BYTES {
                    return Err(artifact("directory manifest is too large"));
                }
                for entry in &manifest.entries {
                    if entry.kind == EntryKind::File {
                        let file_path = safe_existing(&path, &entry.path).await?;
                        self.upload_one(
                            &state_dir,
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
                    &state_dir,
                    job_id,
                    &format!("{}/{}", output.file_name, DIRECTORY_MANIFEST),
                    &manifest_path,
                    DIRECTORY_FORMAT,
                )
                .await?;
            } else {
                self.upload_one(
                    &state_dir,
                    job_id,
                    &output.file_name,
                    &path,
                    &format_for(&output.file_name),
                )
                .await?;
            }
        }
        write_marker(&state_dir, "outputs-uploaded").await
    }

    async fn upload_one(
        &self,
        state_dir: &Path,
        job_id: Uuid,
        filename: &str,
        path: &Path,
        format: &str,
    ) -> Result<(), Error> {
        let metadata = fs::symlink_metadata(path).await?;
        if !metadata.is_file() {
            return Err(artifact("output is not a regular file"));
        }
        if metadata.len() > self.client.settings().multipart_threshold {
            return self
                .upload_multipart(
                    state_dir,
                    job_id,
                    UploadSource {
                        filename,
                        path,
                        format,
                        size: metadata.len(),
                        modified: metadata.modified()?,
                    },
                )
                .await;
        }
        let transfer = self
            .client
            .prepare_upload(job_id, self.agent_id, filename, metadata.len(), format)
            .await?;
        if transfer.presigned.operation != PresignOperation::Upload {
            return Err(io::Error::other("server returned the wrong upload operation").into());
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
        let response = put.send().await.map_err(reqwest::Error::without_url)?;
        successful_upload_response(response)?;
        self.client
            .commit_upload(job_id, self.agent_id, filename, transfer.catalog.uuid)
            .await
    }

    async fn upload_multipart(
        &self,
        state_dir: &Path,
        job_id: Uuid,
        source: UploadSource<'_>,
    ) -> Result<(), Error> {
        let UploadSource {
            filename,
            path,
            format,
            size,
            modified,
        } = source;
        if size.div_ceil(MULTIPART_PART_SIZE) > MAX_MULTIPART_PARTS {
            return Err(artifact("output is too large for multipart upload"));
        }
        let checkpoint = state_dir.join("multipart").join(format!("{filename}.json"));
        let mut state = match read_json::<MultipartState>(&checkpoint).await {
            Ok(state) if state.size == size && state.modified == modified => state,
            Ok(state) => {
                self.abort_multipart(job_id, filename, state.catalog.uuid, &state.upload_id)
                    .await;
                let _ = fs::remove_file(&checkpoint).await;
                self.create_multipart_state(job_id, filename, format, size, modified)
                    .await?
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                self.create_multipart_state(job_id, filename, format, size, modified)
                    .await?
            }
            Err(error) => return Err(error),
        };
        if let Err(error) = write_json(&checkpoint, &state).await {
            self.abort_multipart(job_id, filename, state.catalog.uuid, &state.upload_id)
                .await;
            return Err(error);
        }
        let result = self
            .upload_multipart_parts(job_id, filename, path, size, &checkpoint, &mut state)
            .await;
        if let Err(error) = result {
            self.abort_multipart(job_id, filename, state.catalog.uuid, &state.upload_id)
                .await;
            let _ = fs::remove_file(&checkpoint).await;
            return Err(error);
        }
        let _ = fs::remove_file(&checkpoint).await;
        self.client
            .commit_upload(job_id, self.agent_id, filename, state.catalog.uuid)
            .await
    }

    async fn create_multipart_state(
        &self,
        job_id: Uuid,
        filename: &str,
        format: &str,
        size: u64,
        modified: SystemTime,
    ) -> Result<MultipartState, Error> {
        let transfer = self
            .client
            .create_multipart_upload(job_id, self.agent_id, filename, size, format)
            .await?;
        if transfer.presigned.operation != PresignOperation::CreateMultipart {
            return Err(
                io::Error::other("server returned the wrong multipart create operation").into(),
            );
        }
        let upload_id = self.start_multipart(&transfer).await?;
        Ok(MultipartState {
            catalog: transfer.catalog,
            upload_id,
            size,
            modified,
            parts: BTreeMap::new(),
        })
    }

    async fn start_multipart(&self, transfer: &CatalogTransfer) -> Result<String, Error> {
        let mut request = self.objects.post(&transfer.presigned.url);
        for (name, value) in &transfer.presigned.headers {
            request = request.header(name, value);
        }
        let response = request.send().await.map_err(reqwest::Error::without_url)?;
        let body = successful_upload_response(response)?
            .bytes()
            .await
            .map_err(reqwest::Error::without_url)?;
        xml_element(&body, "UploadId")
            .filter(|upload_id| !upload_id.is_empty())
            .ok_or_else(|| io::Error::other("multipart create response has no upload ID").into())
    }

    async fn upload_multipart_parts(
        &self,
        job_id: Uuid,
        filename: &str,
        path: &Path,
        size: u64,
        checkpoint: &Path,
        state: &mut MultipartState,
    ) -> Result<(), Error> {
        let count = u32::try_from(size.div_ceil(MULTIPART_PART_SIZE))
            .map_err(|_| artifact("too many multipart parts"))?;
        if count == 0
            || u64::from(count) > MAX_MULTIPART_PARTS
            || state.upload_id.is_empty()
            || state
                .parts
                .keys()
                .any(|number| *number == 0 || *number > count)
        {
            return Err(artifact("invalid multipart checkpoint"));
        }
        let mut file = fs::File::open(path).await?;
        let mut offset = 0;
        let mut part_number = 1u32;
        while offset < size {
            let length = (size - offset).min(MULTIPART_PART_SIZE) as usize;
            if state.parts.contains_key(&part_number) {
                offset += length as u64;
                part_number = part_number
                    .checked_add(1)
                    .ok_or_else(|| artifact("multipart part number overflow"))?;
                continue;
            }
            let mut bytes = vec![0; length];
            file.seek(SeekFrom::Start(offset)).await?;
            file.read_exact(&mut bytes).await?;
            let presigned = self
                .client
                .upload_multipart_part(
                    job_id,
                    self.agent_id,
                    filename,
                    state.catalog.uuid,
                    &state.upload_id,
                    part_number,
                )
                .await?;
            if presigned.operation
                != (PresignOperation::UploadPart {
                    upload_id: state.upload_id.clone(),
                    part_number,
                })
            {
                return Err(
                    io::Error::other("server returned the wrong multipart part operation").into(),
                );
            }
            let mut request = self
                .objects
                .put(&presigned.url)
                .header(reqwest::header::CONTENT_LENGTH, bytes.len());
            for (name, value) in &presigned.headers {
                request = request.header(name, value);
            }
            let response = request
                .body(bytes)
                .send()
                .await
                .map_err(reqwest::Error::without_url)?;
            let etag = successful_upload_response(response)?
                .headers()
                .get(reqwest::header::ETAG)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned)
                .ok_or_else(|| io::Error::other("multipart upload part has no ETag"))?;
            state.parts.insert(part_number, etag);
            write_json(checkpoint, state).await?;
            offset += length as u64;
            part_number = part_number
                .checked_add(1)
                .ok_or_else(|| artifact("multipart part number overflow"))?;
        }
        let metadata = fs::metadata(path).await?;
        if metadata.len() != state.size || metadata.modified()? != state.modified {
            return Err(artifact("output changed during multipart upload"));
        }
        let presigned = self
            .client
            .complete_multipart_upload(
                job_id,
                self.agent_id,
                filename,
                state.catalog.uuid,
                &state.upload_id,
            )
            .await?;
        if presigned.operation
            != (PresignOperation::CompleteMultipart {
                upload_id: state.upload_id.clone(),
            })
        {
            return Err(
                io::Error::other("server returned the wrong multipart complete operation").into(),
            );
        }
        let body = complete_multipart_xml(&state.parts);
        let mut request = self.objects.post(&presigned.url);
        for (name, value) in &presigned.headers {
            request = request.header(name, value);
        }
        let response = request
            .header(reqwest::header::CONTENT_TYPE, "application/xml")
            .body(body)
            .send()
            .await
            .map_err(reqwest::Error::without_url)?;
        let body = successful_upload_response(response)?
            .bytes()
            .await
            .map_err(reqwest::Error::without_url)?;
        // S3 may return HTTP 200 and put the completion error in the XML.
        if xml_element(&body, "Code").is_some() {
            return Err(io::Error::other("object storage rejected multipart completion").into());
        }
        Ok(())
    }

    async fn abort_multipart(
        &self,
        job_id: Uuid,
        filename: &str,
        catalog_uuid: Uuid,
        upload_id: &str,
    ) {
        let Ok(presigned) = self
            .client
            .abort_multipart_upload(job_id, self.agent_id, filename, catalog_uuid, upload_id)
            .await
        else {
            return;
        };
        if presigned.operation
            != (PresignOperation::AbortMultipart {
                upload_id: upload_id.to_owned(),
            })
        {
            return;
        }
        let mut request = self.objects.delete(&presigned.url);
        for (name, value) in &presigned.headers {
            request = request.header(name, value);
        }
        let _ = request.send().await;
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
        self.download_file_checked(transfer, destination, None)
            .await
    }

    async fn download_file_checked(
        &self,
        transfer: &CatalogTransfer,
        destination: &Path,
        expected_sha256: Option<&str>,
    ) -> Result<(), Error> {
        if transfer.multipart && transfer.catalog.size > self.client.settings().multipart_threshold
        {
            return self
                .download_file_ranges(transfer, destination, expected_sha256)
                .await;
        }
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
            let mut digest = expected_sha256.map(|_| Sha256::new());
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
                if let Some(digest) = &mut digest {
                    digest.update(&chunk);
                }
                file.write_all(&chunk).await?;
            }
            if size != transfer.catalog.size {
                return Err(artifact("incomplete input download"));
            }
            check_digest(digest, expected_sha256)?;
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

    async fn download_file_ranges(
        &self,
        transfer: &CatalogTransfer,
        destination: &Path,
        expected_sha256: Option<&str>,
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
            file.set_len(transfer.catalog.size).await?;
            let mut offset = 0u64;
            let mut etag = None::<String>;
            let mut digest = expected_sha256.map(|_| Sha256::new());
            while offset < transfer.catalog.size {
                let end = (offset + MULTIPART_PART_SIZE)
                    .min(transfer.catalog.size)
                    .saturating_sub(1);
                let mut request = self
                    .objects
                    .get(&transfer.presigned.url)
                    .header(reqwest::header::RANGE, format!("bytes={offset}-{end}"));
                for (name, value) in &transfer.presigned.headers {
                    request = request.header(name, value);
                }
                let response = request.send().await.map_err(reqwest::Error::without_url)?;
                if expected_sha256.is_some() {
                    response
                        .error_for_status_ref()
                        .map_err(reqwest::Error::without_url)?;
                }
                if response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
                    return Err(artifact(format!(
                        "ranged input download returned {}",
                        response.status()
                    )));
                }
                let range = response
                    .headers()
                    .get(reqwest::header::CONTENT_RANGE)
                    .and_then(|value| value.to_str().ok())
                    .and_then(parse_content_range)
                    .ok_or_else(|| artifact("ranged input has invalid Content-Range"))?;
                if range.0 != offset || range.1 != end || range.2 != transfer.catalog.size {
                    return Err(artifact(
                        "ranged input Content-Range does not match request",
                    ));
                }
                let current_etag = response
                    .headers()
                    .get(reqwest::header::ETAG)
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_owned);
                if let Some(expected) = &etag {
                    if current_etag.as_deref() != Some(expected.as_str()) {
                        return Err(artifact("ranged input ETag changed during download"));
                    }
                } else if current_etag.is_some() {
                    etag = current_etag;
                }
                file.seek(SeekFrom::Start(offset)).await?;
                let mut received = 0u64;
                let expected = end - offset + 1;
                let mut response = response;
                while let Some(chunk) = response
                    .chunk()
                    .await
                    .map_err(reqwest::Error::without_url)?
                {
                    received = received
                        .checked_add(chunk.len() as u64)
                        .ok_or_else(|| artifact("input size overflow"))?;
                    if received > expected {
                        return Err(artifact("ranged input exceeds requested range"));
                    }
                    if let Some(digest) = &mut digest {
                        digest.update(&chunk);
                    }
                    file.write_all(&chunk).await?;
                }
                if received != expected {
                    return Err(artifact("incomplete ranged input download"));
                }
                offset = end + 1;
            }
            check_digest(digest, expected_sha256)?;
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

fn check_digest(digest: Option<Sha256>, expected: Option<&str>) -> Result<(), Error> {
    if let Some((digest, expected)) = digest.zip(expected)
        && !format!("{:x}", digest.finalize()).eq_ignore_ascii_case(expected)
    {
        return Err(artifact(
            "asset SHA-256 does not match the published version",
        ));
    }
    Ok(())
}

fn successful_upload_response(response: reqwest::Response) -> Result<reqwest::Response, Error> {
    let response = response
        .error_for_status()
        .map_err(reqwest::Error::without_url)?;
    let status = response.status();
    if !status.is_success() {
        return Err(io::Error::other(format!("output upload returned HTTP {status}")).into());
    }
    Ok(response)
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

fn xml_element(body: &[u8], name: &str) -> Option<String> {
    let body = std::str::from_utf8(body).ok()?;
    let open = format!("<{name}>");
    let close = format!("</{name}>");
    let start = body.find(&open)? + open.len();
    let end = body[start..].find(&close)? + start;
    Some(body[start..end].trim().to_owned())
}

fn complete_multipart_xml(parts: &BTreeMap<u32, String>) -> String {
    let mut body = String::from("<CompleteMultipartUpload>");
    for (number, etag) in parts {
        body.push_str("<Part><PartNumber>");
        body.push_str(&number.to_string());
        body.push_str("</PartNumber><ETag>");
        xml_escape_into(&mut body, etag);
        body.push_str("</ETag></Part>");
    }
    body.push_str("</CompleteMultipartUpload>");
    body
}

fn xml_escape_into(output: &mut String, value: &str) {
    for character in value.chars() {
        match character {
            '&' => output.push_str("&amp;"),
            '<' => output.push_str("&lt;"),
            '>' => output.push_str("&gt;"),
            '"' => output.push_str("&quot;"),
            '\'' => output.push_str("&apos;"),
            character => output.push(character),
        }
    }
}

fn parse_content_range(value: &str) -> Option<(u64, u64, u64)> {
    let rest = value.strip_prefix("bytes ")?;
    let (range, total) = rest.split_once('/')?;
    let (start, end) = range.split_once('-')?;
    Some((start.parse().ok()?, end.parse().ok()?, total.parse().ok()?))
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

fn state_dir(data_dir: &Path, job_id: Uuid) -> PathBuf {
    data_dir.join(".rayflow").join(job_id.to_string())
}

async fn read_marker(state_dir: &Path, name: &str) -> Result<bool, Error> {
    match read_json(&state_dir.join(format!("{name}.json"))).await {
        Ok(value) => Ok(value),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

async fn write_marker(state_dir: &Path, name: &str) -> Result<(), Error> {
    write_json(&state_dir.join(format!("{name}.json")), &true).await
}
