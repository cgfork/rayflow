use graph::GraphStore;
use graph::types::DAG;
use scheduler::{Job, JobArtifactName, ResourceCapacity};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{errors::Error, settings::Settings};

#[derive(Clone)]
pub struct Client {
    http: reqwest::Client,
    settings: Settings,
}

/// Storage metadata and authorization returned by the job artifact API.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Transfer {
    pub catalog: catalog::model::Catalog,
    pub presigned: catalog::PresignedUrl,
    #[serde(default)]
    pub multipart: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct AssetTransfer {
    pub asset_id: Uuid,
    pub sha256: String,
    #[serde(flatten)]
    pub transfer: Transfer,
}

#[derive(Serialize)]
struct MultipartPresignRequest<'a> {
    job_id: Uuid,
    agent_id: Uuid,
    filename: &'a str,
    catalog_uuid: Uuid,
    upload_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    part_number: Option<u32>,
}

#[derive(Serialize)]
pub struct PollRequest {
    pub agent_id: Uuid,
    pub batch: usize,
    pub resources: ResourceCapacity,
    pub active_jobs: Vec<Uuid>,
}

#[derive(Deserialize)]
pub struct PollResponse {
    pub jobs: Vec<Job>,
}

impl Client {
    /// Prepares the catalog for durable job metadata, including expired jobs.
    /// Returns `None` when the server has no object storage configured.
    /// Returns `ClaimLost` for terminal jobs with no recorded result owner.
    pub async fn prepare_job_artifact(
        &self,
        job_id: Uuid,
        agent_id: Uuid,
        filename: JobArtifactName,
        size: u64,
    ) -> Result<Option<Transfer>, Error> {
        let response = self
            .http
            .post(format!("{}/job/result/prepare", self.base_url()))
            .json(&serde_json::json!({
                "job_id": job_id, "agent_id": agent_id, "filename": filename, "size": size,
            }))
            .send()
            .await?;
        if let Err(error) = response.error_for_status_ref() {
            use reqwest::StatusCode;

            let status = response.status();
            if matches!(
                status,
                StatusCode::SERVICE_UNAVAILABLE | StatusCode::CONFLICT
            ) && let Ok(body) = response.json::<serde_json::Value>().await
            {
                match (status, body["error_code"].as_str()) {
                    (StatusCode::SERVICE_UNAVAILABLE, Some("RAYFLOW_STORAGE_UNAVAILABLE")) => {
                        return Ok(None);
                    }
                    (StatusCode::CONFLICT, Some("RAYFLOW_JOB_RESULT_OWNER_UNKNOWN")) => {
                        return Err(Error::ClaimLost(job_id));
                    }
                    _ => {}
                }
            }
            return Err(error.into());
        }
        Ok(Some(successful_response(response)?.json().await?))
    }

    /// Publishes uploaded job metadata.
    pub async fn commit_job_artifact(
        &self,
        job_id: Uuid,
        agent_id: Uuid,
        filename: JobArtifactName,
        catalog_uuid: Uuid,
    ) -> Result<(), Error> {
        let response = self
            .http
            .post(format!("{}/job/result/commit", self.base_url()))
            .json(&serde_json::json!({
                "job_id": job_id, "agent_id": agent_id, "filename": filename,
                "catalog_uuid": catalog_uuid,
            }))
            .send()
            .await?;
        let acknowledged: Uuid = successful_response(response)?.json().await?;
        if acknowledged != catalog_uuid {
            return Err(Error::CatalogMismatch {
                expected: catalog_uuid,
                actual: acknowledged,
            });
        }
        Ok(())
    }

    /// Prepares an output catalog and returns its ordinary upload signature.
    pub async fn prepare_upload(
        &self,
        job_id: Uuid,
        agent_id: Uuid,
        filename: &str,
        size: u64,
        format: &str,
    ) -> Result<Transfer, Error> {
        let body = serde_json::json!({"job_id": job_id, "agent_id": agent_id, "filename": filename, "size": size, "format": format});
        let response = self
            .http
            .post(format!("{}/job/output/prepare", self.base_url()))
            .json(&body)
            .send()
            .await?;
        let response = claimed_response(response, job_id)?;
        let value: serde_json::Value = response.json().await?;
        serde_json::from_value(value.get("transfer").cloned().unwrap_or(value)).map_err(Into::into)
    }

    pub async fn commit_upload(
        &self,
        job_id: Uuid,
        agent_id: Uuid,
        filename: &str,
        catalog_uuid: Uuid,
    ) -> Result<(), Error> {
        let body = serde_json::json!({"job_id": job_id, "agent_id": agent_id, "filename": filename, "catalog_uuid": catalog_uuid});
        let response = self
            .http
            .post(format!("{}/job/output/commit", self.base_url()))
            .json(&body)
            .send()
            .await?;
        // The output commit returns the published file UUID.
        claimed_response(response, job_id)?.json::<Uuid>().await?;
        Ok(())
    }

    pub async fn create_multipart_upload(
        &self,
        job_id: Uuid,
        agent_id: Uuid,
        filename: &str,
        size: u64,
        format: &str,
    ) -> Result<Transfer, Error> {
        let body = serde_json::json!({
            "job_id": job_id,
            "agent_id": agent_id,
            "filename": filename,
            "size": size,
            "format": format,
        });
        let response = self
            .http
            .post(format!("{}/job/output/multipart/create", self.base_url()))
            .json(&body)
            .send()
            .await?;
        let response = claimed_response(response, job_id)?;
        let value: serde_json::Value = response.json().await?;
        serde_json::from_value(value.get("transfer").cloned().unwrap_or(value)).map_err(Into::into)
    }

    async fn presign_multipart(
        &self,
        path: &str,
        request: MultipartPresignRequest<'_>,
    ) -> Result<catalog::PresignedUrl, Error> {
        let response = self
            .http
            .post(format!("{}{path}", self.base_url()))
            .json(&request)
            .send()
            .await?;
        Ok(claimed_response(response, request.job_id)?.json().await?)
    }

    pub async fn upload_multipart_part(
        &self,
        job_id: Uuid,
        agent_id: Uuid,
        filename: &str,
        catalog_uuid: Uuid,
        upload_id: &str,
        part_number: u32,
    ) -> Result<catalog::PresignedUrl, Error> {
        self.presign_multipart(
            "/job/output/multipart/upload",
            MultipartPresignRequest {
                job_id,
                agent_id,
                filename,
                catalog_uuid,
                upload_id,
                part_number: Some(part_number),
            },
        )
        .await
    }

    pub async fn complete_multipart_upload(
        &self,
        job_id: Uuid,
        agent_id: Uuid,
        filename: &str,
        catalog_uuid: Uuid,
        upload_id: &str,
    ) -> Result<catalog::PresignedUrl, Error> {
        self.presign_multipart(
            "/job/output/multipart/complete",
            MultipartPresignRequest {
                job_id,
                agent_id,
                filename,
                catalog_uuid,
                upload_id,
                part_number: None,
            },
        )
        .await
    }

    pub async fn abort_multipart_upload(
        &self,
        job_id: Uuid,
        agent_id: Uuid,
        filename: &str,
        catalog_uuid: Uuid,
        upload_id: &str,
    ) -> Result<catalog::PresignedUrl, Error> {
        self.presign_multipart(
            "/job/output/multipart/abort",
            MultipartPresignRequest {
                job_id,
                agent_id,
                filename,
                catalog_uuid,
                upload_id,
                part_number: None,
            },
        )
        .await
    }

    pub async fn download_input(
        &self,
        job_id: Uuid,
        agent_id: Uuid,
        filename: &str,
    ) -> Result<Option<Transfer>, Error> {
        let body =
            serde_json::json!({"job_id": job_id, "agent_id": agent_id, "filename": filename});
        let response = self
            .http
            .post(format!("{}/job/input/presign", self.base_url()))
            .json(&body)
            .send()
            .await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Ok(Some(response.error_for_status()?.json().await?))
    }

    pub(crate) async fn download_asset(
        &self,
        job_id: Uuid,
        agent_id: Uuid,
        filename: &str,
    ) -> Result<AssetTransfer, Error> {
        let body =
            serde_json::json!({"job_id": job_id, "agent_id": agent_id, "filename": filename});
        Ok(self
            .http
            .post(format!("{}/job/asset/presign", self.base_url()))
            .json(&body)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    pub fn new(settings: Settings) -> Result<Self, Error> {
        if settings.request_timeout.is_zero() {
            return Err(Error::Settings("request timeout must be positive"));
        }
        let mut headers = reqwest::header::HeaderMap::new();
        if let Some(token) = &settings.auth_token {
            if token.is_empty() {
                return Err(Error::Settings("auth_token must not be empty"));
            }
            let mut value = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|_| Error::Settings("auth_token must be a valid HTTP header"))?;
            value.set_sensitive(true);
            headers.insert(reqwest::header::AUTHORIZATION, value);
        }
        Ok(Self {
            http: reqwest::Client::builder()
                .default_headers(headers)
                .redirect(reqwest::redirect::Policy::none())
                .timeout(settings.request_timeout)
                .build()?,
            settings,
        })
    }

    pub(crate) fn settings(&self) -> &Settings {
        &self.settings
    }

    fn base_url(&self) -> String {
        format!("{}/api/v1", self.settings.server_url.trim_end_matches('/'))
    }

    pub async fn poll(&self, request: &PollRequest) -> Result<PollResponse, Error> {
        Ok(self
            .http
            .post(format!("{}/jobs/poll", self.base_url()))
            .json(request)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    /// Reads a published graph, optionally selecting an exact version.
    pub async fn graph(&self, graph_id: Uuid, version: Option<u64>) -> Result<DAG, Error> {
        let mut request = self
            .http
            .get(format!("{}/graphs/{graph_id}", self.base_url()));
        if let Some(version) = version {
            request = request.query(&[("version", version)]);
        }
        Ok(request.send().await?.error_for_status()?.json().await?)
    }

    pub async fn fail(&self, agent_id: Uuid, job_id: Uuid) -> Result<(), Error> {
        self.job_action("fail", agent_id, job_id).await
    }

    /// Reports a yielded job so its node can be resumed later.
    pub async fn yield_job(&self, agent_id: Uuid, job_id: Uuid) -> Result<(), Error> {
        self.job_action("yield", agent_id, job_id).await
    }

    pub async fn succeed(&self, agent_id: Uuid, job_id: Uuid) -> Result<(), Error> {
        self.job_action("succeed", agent_id, job_id).await
    }

    pub async fn terminate(&self, agent_id: Uuid, job_id: Uuid) -> Result<(), Error> {
        self.job_action("terminate", agent_id, job_id).await
    }

    async fn job_action(&self, action: &str, agent_id: Uuid, job_id: Uuid) -> Result<(), Error> {
        let response = self
            .http
            .post(format!("{}/job/{action}", self.base_url()))
            .json(&serde_json::json!({ "job_id": job_id, "agent_id": agent_id }))
            .send()
            .await?;
        claimed_response(response, job_id)?;
        Ok(())
    }
}

/// Only claim-scoped Rayflow API conflicts mean ownership was lost. Object
/// storage conflicts remain transfer errors so callers can retry them.
fn claimed_response(response: reqwest::Response, job_id: Uuid) -> Result<reqwest::Response, Error> {
    if response.status() == reqwest::StatusCode::CONFLICT {
        return Err(Error::ClaimLost(job_id));
    }
    successful_response(response)
}

fn successful_response(response: reqwest::Response) -> Result<reqwest::Response, Error> {
    let response = response.error_for_status()?;
    if !response.status().is_success() {
        return Err(Error::UnexpectedStatus(response.status()));
    }
    Ok(response)
}

impl GraphStore<Uuid> for Client {
    type G = DAG;
    type Err = Error;

    async fn graphs(
        &mut self,
        page: graph::PageRequest,
        graph_id: Option<Uuid>,
    ) -> Result<graph::Page<Self::G>, Error> {
        let mut query = vec![("limit", page.limit().to_string())];
        if let Some(before) = page.before() {
            query.push(("before", before.to_string()));
        }
        if let Some(graph_id) = graph_id {
            query.push(("graph_id", graph_id.to_string()));
        }
        Ok(self
            .http
            .get(format!("{}/graphs", self.base_url()))
            .query(&query)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    async fn load(&mut self, graph_id: Uuid, version: Option<u64>) -> Result<DAG, Error> {
        self.graph(graph_id, version).await
    }
}
