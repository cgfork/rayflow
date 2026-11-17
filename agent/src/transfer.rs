//! Concrete S3 multipart/range transfers using claim-scoped signed requests.
//! Long-lived storage credentials never enter the agent.

use std::{
    collections::BTreeMap,
    future::Future,
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, SystemTime},
};

use catalog::{PresignOperation, PresignedUrl};
use futures_util::{StreamExt, TryStreamExt, stream};
use quick_xml::{Reader, events::Event};
use reqwest::{Response, StatusCode, header};
use serde::{Deserialize, Serialize};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    time::sleep,
};

use crate::{
    client::{Client, Transfer},
    errors::Error,
    read_json, write_json,
};

/// Agent-side identity of a prepared output, reused across provider operations.
#[derive(Clone, Debug)]
pub(crate) struct UploadContext {
    pub job_id: uuid::Uuid,
    pub agent_id: uuid::Uuid,
    pub key: String,
    pub catalog_id: uuid::Uuid,
}

const MIB: u64 = 1024 * 1024;
const MAX_PART: u64 = 5 * 1024 * MIB;

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct Config {
    pub enabled: bool,
    pub multipart_threshold: u64,
    pub part_size: u64,
    pub concurrency: usize,
    pub max_attempts: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: true,
            multipart_threshold: 64 * MIB,
            part_size: 8 * MIB,
            concurrency: 4,
            max_attempts: 3,
        }
    }
}

impl Config {
    pub fn validate(&self) -> Result<(), Error> {
        if self.multipart_threshold < 5 * MIB
            || !(5 * MIB..=MAX_PART).contains(&self.part_size)
            || !(1..=32).contains(&self.concurrency)
            || !(1..=5).contains(&self.max_attempts)
        {
            return Err(Error::Settings(
                "transfer requires threshold >= 5 MiB, part_size 5 MiB..=5 GiB, concurrency 1..=32 and max_attempts 1..=5",
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct S3TransferClient {
    http: reqwest::Client,
    config: Config,
}

#[derive(Serialize, Deserialize)]
struct UploadState {
    upload_id: String,
    size: u64,
    modified: SystemTime,
    part_size: u64,
    parts: BTreeMap<u32, String>,
}

#[derive(Deserialize)]
struct Initiated {
    #[serde(rename = "UploadId")]
    upload_id: String,
}

#[derive(Serialize)]
#[serde(rename = "CompleteMultipartUpload")]
struct Complete {
    #[serde(rename = "Part")]
    parts: Vec<Part>,
}

#[derive(Serialize)]
struct Part {
    #[serde(rename = "PartNumber")]
    number: u32,
    #[serde(rename = "ETag")]
    etag: String,
}

impl S3TransferClient {
    pub fn new(http: reqwest::Client, config: Config) -> Result<Self, Error> {
        config.validate()?;
        Ok(Self { http, config })
    }

    pub fn supports(&self, transfer: &Transfer) -> bool {
        self.config.enabled
            && transfer.multipart
            && transfer.catalog.size >= self.config.multipart_threshold
    }

    async fn request(&self, signed: &PresignedUrl) -> Result<reqwest::RequestBuilder, Error> {
        let method = reqwest::Method::from_bytes(signed.operation.method().as_bytes()).unwrap();
        let mut request = self.http.request(method, &signed.url);
        for (name, value) in &signed.headers {
            request = request.header(name, value);
        }
        Ok(request)
    }

    async fn retry<T, F, Fut>(&self, mut operation: F) -> Result<T, Error>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, Error>>,
    {
        for attempt in 0..self.config.max_attempts {
            match operation().await {
                Ok(value) => return Ok(value),
                Err(error) => {
                    let terminal = matches!(&error, Error::ClaimLost(_))
                        || matches!(&error, Error::Http(e) if e.status().is_some_and(|s| s.is_client_error() && s != StatusCode::REQUEST_TIMEOUT && s != StatusCode::TOO_MANY_REQUESTS));
                    if terminal || attempt + 1 == self.config.max_attempts {
                        return Err(error);
                    }
                    sleep(Duration::from_millis(100 * (1 << attempt))).await;
                }
            }
        }
        unreachable!("validated nonzero retry count")
    }

    async fn signed(
        &self,
        api: &Client,
        request: &UploadContext,
        operation: PresignOperation,
    ) -> Result<PresignedUrl, Error> {
        let signed = match &operation {
            PresignOperation::CreateMultipart => api.multipart_create(request).await?,
            PresignOperation::UploadPart {
                upload_id,
                part_number,
            } => {
                api.multipart_upload(request, upload_id, *part_number)
                    .await?
            }
            PresignOperation::CompleteMultipart { upload_id } => {
                api.multipart_complete(request, upload_id).await?
            }
            PresignOperation::AbortMultipart { upload_id } => {
                api.multipart_abort(request, upload_id).await?
            }
            _ => return Err(failed("expected a multipart operation")),
        };
        if signed.operation != operation {
            return Err(failed("server returned a different multipart operation"));
        }
        Ok(signed)
    }

    async fn abort(
        &self,
        api: &Client,
        request: &UploadContext,
        upload_id: &str,
    ) -> Result<(), Error> {
        let signed = self
            .signed(
                api,
                request,
                PresignOperation::AbortMultipart {
                    upload_id: upload_id.into(),
                },
            )
            .await?;
        let response = self
            .request(&signed)
            .await?
            .send()
            .await
            .map_err(reqwest::Error::without_url)?;
        if response.status() != StatusCode::NOT_FOUND {
            response
                .error_for_status()
                .map_err(reqwest::Error::without_url)?;
        }
        Ok(())
    }

    /// Completed part ETags are checkpointed atomically. A process restart resumes
    /// the same upload only if the source file size, mtime, and part size still match.
    pub(crate) async fn upload(
        &self,
        api: &Client,
        request: &UploadContext,
        transfer: &Transfer,
        path: &Path,
        checkpoint: &Path,
    ) -> Result<(), Error> {
        let metadata = fs::metadata(path).await?;
        if metadata.len() != transfer.catalog.size {
            return Err(failed("output size changed before upload"));
        }
        let modified = metadata.modified()?;
        // Grow parts as needed to stay within the provider's 10,000-part limit.
        let part_size = self.config.part_size.max(metadata.len().div_ceil(10_000));
        if part_size > MAX_PART {
            return Err(failed("object exceeds multipart capacity"));
        }
        let old: Option<UploadState> = match read_json(checkpoint).await {
            Ok(state) => Some(state),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        let mut state = if let Some(state) = old {
            if state.size == metadata.len()
                && state.modified == modified
                && state.part_size == part_size
            {
                state
            } else {
                self.abort(api, request, &state.upload_id).await?;
                fs::remove_file(checkpoint).await?;
                self.create(
                    api,
                    request,
                    metadata.len(),
                    modified,
                    part_size,
                    checkpoint,
                )
                .await?
            }
        } else {
            self.create(
                api,
                request,
                metadata.len(),
                modified,
                part_size,
                checkpoint,
            )
            .await?
        };
        let result = self
            .upload_parts(api, request, path, &mut state, checkpoint)
            .await;
        if result.is_err() {
            // Explicit failures clean up remote parts. A killed process instead
            // leaves its checkpoint for recovery (or the bucket's lifecycle rule).
            if self.abort(api, request, &state.upload_id).await.is_ok() {
                let _ = fs::remove_file(checkpoint).await;
            }
        } else {
            fs::remove_file(checkpoint).await?;
        }
        result
    }

    async fn create(
        &self,
        api: &Client,
        request: &UploadContext,
        size: u64,
        modified: SystemTime,
        part_size: u64,
        checkpoint: &Path,
    ) -> Result<UploadState, Error> {
        let signed = self
            .signed(api, request, PresignOperation::CreateMultipart)
            .await?;
        let response = self
            .request(&signed)
            .await?
            .send()
            .await
            .map_err(reqwest::Error::without_url)?;
        let text = xml_response(response, "InitiateMultipartUploadResult").await?;
        let initiated: Initiated = quick_xml::de::from_str(&text)
            .map_err(|_| failed("invalid multipart initiation XML"))?;
        if initiated.upload_id.is_empty() || initiated.upload_id.len() > 2048 {
            return Err(failed("invalid multipart upload ID"));
        }
        let state = UploadState {
            upload_id: initiated.upload_id,
            size,
            modified,
            part_size,
            parts: BTreeMap::new(),
        };
        if let Err(error) = write_json(checkpoint, &state).await {
            let _ = self.abort(api, request, &state.upload_id).await;
            return Err(error);
        }
        Ok(state)
    }

    async fn upload_parts(
        &self,
        api: &Client,
        request: &UploadContext,
        path: &Path,
        state: &mut UploadState,
        checkpoint: &Path,
    ) -> Result<(), Error> {
        let count = u32::try_from(state.size.div_ceil(state.part_size))
            .map_err(|_| failed("too many parts"))?;
        if count == 0 || count > 10_000 || state.parts.keys().any(|n| *n == 0 || *n > count) {
            return Err(failed("invalid multipart checkpoint"));
        }
        tracing::info!(
            parts = count,
            bytes = state.size,
            resumed_parts = state.parts.len(),
            "uploading S3 multipart artifact"
        );
        let todo: Vec<_> = (1..=count)
            .filter(|n| !state.parts.contains_key(n))
            .collect();
        let upload_id = state.upload_id.clone();
        let upload_id = &upload_id;
        let part_size = state.part_size;
        let size = state.size;
        let stopped = AtomicBool::new(false);
        let mut pending = stream::iter(todo)
            .take_while(|_| std::future::ready(!stopped.load(Ordering::Relaxed)))
            .map(|number| async move {
                let offset = u64::from(number - 1) * part_size;
                let length = part_size.min(size - offset);
                let etag = self
                    .retry(|| async {
                        let signed = self
                            .signed(
                                api,
                                request,
                                PresignOperation::UploadPart {
                                    upload_id: upload_id.clone(),
                                    part_number: number,
                                },
                            )
                            .await?;
                        let mut file = fs::File::open(path).await?;
                        file.seek(std::io::SeekFrom::Start(offset)).await?;
                        let body = reqwest::Body::wrap_stream(tokio_util::io::ReaderStream::new(
                            file.take(length),
                        ));
                        let response = self
                            .request(&signed)
                            .await?
                            .header(header::CONTENT_LENGTH, length)
                            .body(body)
                            .send()
                            .await
                            .map_err(reqwest::Error::without_url)?
                            .error_for_status()
                            .map_err(reqwest::Error::without_url)?;
                        etag(&response)
                    })
                    .await?;
                Ok::<_, Error>((number, etag))
            })
            .buffer_unordered(self.config.concurrency);
        let mut failure = None;
        while let Some(part) = pending.next().await {
            let saved = match part {
                Ok((number, etag)) => {
                    state.parts.insert(number, etag);
                    write_json(checkpoint, state).await
                }
                Err(error) => Err(error),
            };
            if let Err(error) = saved {
                stopped.store(true, Ordering::Relaxed);
                failure.get_or_insert(error);
            }
        }
        drop(pending);
        // Abort only after in-flight parts settle, otherwise a late UploadPart
        // can race provider-side multipart cleanup.
        if let Some(error) = failure {
            return Err(error);
        }
        let metadata = fs::metadata(path).await?;
        if metadata.len() != state.size || metadata.modified()? != state.modified {
            return Err(failed("output changed during multipart upload"));
        }
        let body = quick_xml::se::to_string(&Complete {
            parts: state
                .parts
                .iter()
                .map(|(&number, etag)| Part {
                    number,
                    etag: etag.clone(),
                })
                .collect(),
        })
        .map_err(|_| failed("cannot serialize multipart completion"))?;
        let signed = self
            .signed(
                api,
                request,
                PresignOperation::CompleteMultipart {
                    upload_id: state.upload_id.clone(),
                },
            )
            .await?;
        let response = self
            .request(&signed)
            .await?
            .header(header::CONTENT_TYPE, "application/xml")
            .body(body)
            .send()
            .await
            .map_err(reqwest::Error::without_url)?;
        // CompleteMultipartUpload may return an embedded <Error> with HTTP 200.
        xml_response(response, "CompleteMultipartUploadResult").await?;
        Ok(())
    }

    /// Download ranges into independent file handles, bounding both requests and
    /// memory. If-Match pins all ranges to the first response's object identity.
    pub async fn download(&self, transfer: &Transfer, destination: &Path) -> Result<(), Error> {
        if transfer.presigned.operation != PresignOperation::Download {
            return Err(failed("unexpected download operation"));
        }
        let size = transfer.catalog.size;
        tracing::info!(
            bytes = size,
            part_size = self.config.part_size,
            "downloading S3 artifact in byte ranges"
        );
        let file = fs::OpenOptions::new().write(true).open(destination).await?;
        file.set_len(size).await?;
        let part = self.config.part_size;
        let first_end = part
            .min(size)
            .checked_sub(1)
            .ok_or_else(|| failed("empty ranged download"))?;
        let identity = self
            .retry(|| self.download_range(transfer, destination, 0, first_end, None))
            .await?;
        stream::iter(1..size.div_ceil(part))
            .map(Ok::<_, Error>)
            .try_for_each_concurrent(self.config.concurrency, |index| {
                let identity = &identity;
                async move {
                    let start = index * part;
                    let end = start.saturating_add(part).min(size) - 1;
                    self.retry(|| {
                        self.download_range(transfer, destination, start, end, Some(identity))
                    })
                    .await?;
                    Ok(())
                }
            })
            .await?;
        file.sync_all().await?;
        Ok(())
    }

    async fn download_range(
        &self,
        transfer: &Transfer,
        destination: &Path,
        start: u64,
        end: u64,
        identity: Option<&str>,
    ) -> Result<String, Error> {
        let mut request = self
            .request(&transfer.presigned)
            .await?
            .header(header::RANGE, format!("bytes={start}-{end}"));
        if let Some(identity) = identity {
            request = request.header(header::IF_MATCH, identity);
        }
        let mut response = request
            .send()
            .await
            .map_err(reqwest::Error::without_url)?
            .error_for_status()
            .map_err(reqwest::Error::without_url)?;
        let expected = format!("bytes {start}-{end}/{}", transfer.catalog.size);
        if response.status() != StatusCode::PARTIAL_CONTENT
            || response
                .headers()
                .get(header::CONTENT_RANGE)
                .and_then(|v| v.to_str().ok())
                != Some(expected.as_str())
        {
            return Err(failed("storage did not return the requested byte range"));
        }
        let etag = etag(&response)?;
        if identity.is_some_and(|identity| identity != etag) {
            return Err(failed("object changed during ranged download"));
        }
        let mut file = fs::OpenOptions::new().write(true).open(destination).await?;
        file.seek(std::io::SeekFrom::Start(start)).await?;
        let mut received = 0;
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(reqwest::Error::without_url)?
        {
            received += chunk.len() as u64;
            if received > end - start + 1 {
                return Err(failed("download range exceeds expected length"));
            }
            file.write_all(&chunk).await?;
        }
        if received != end - start + 1 {
            return Err(failed("incomplete download range"));
        }
        file.flush().await?;
        Ok(etag)
    }
}

fn etag(response: &Response) -> Result<String, Error> {
    response
        .headers()
        .get(header::ETAG)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| failed("storage response has no ETag"))
}

async fn xml_response(response: Response, root: &str) -> Result<String, Error> {
    let mut response = response
        .error_for_status()
        .map_err(reqwest::Error::without_url)?;
    let mut data = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(reqwest::Error::without_url)?
    {
        if data.len() + chunk.len() > 65_536 {
            return Err(failed("oversized storage XML response"));
        }
        data.extend_from_slice(&chunk);
    }
    let text = String::from_utf8(data).map_err(|_| failed("invalid storage XML encoding"))?;
    let mut reader = Reader::from_str(&text);
    loop {
        match reader.read_event() {
            Ok(Event::Start(event) | Event::Empty(event))
                if event.local_name().as_ref() == root.as_bytes() =>
            {
                break;
            }
            Ok(Event::Decl(_) | Event::Text(_) | Event::Comment(_)) => continue,
            _ => {
                return Err(failed(
                    "storage returned an invalid or unsuccessful multipart XML response",
                ));
            }
        }
    }
    // Parse the entire document, including its closing tags, before publishing.
    let _: serde::de::IgnoredAny =
        quick_xml::de::from_str(&text).map_err(|_| failed("invalid storage XML document"))?;
    Ok(text)
}

fn failed(message: &str) -> Error {
    Error::Artifact(message.into())
}

#[cfg(test)]
mod tests;
