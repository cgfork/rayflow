use graph::GraphStore;
use graph::types::DAG;
use runtime::command::{NodeRun, Run};
use scheduler::{Job, ResourceCapacity};
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

#[derive(Deserialize)]
pub(crate) struct RunView {
    pub run: Run,
    pub node_runs: Vec<(NodeRun, graph::types::Node)>,
}

#[derive(Deserialize)]
pub(crate) struct RunState {
    pub nodes: Vec<NodeState>,
}

#[derive(Deserialize)]
pub(crate) struct NodeState {
    pub node_id: Uuid,
    pub version: u64,
    pub revision: u64,
    pub status: String,
    pub ineffective: u32,
}

impl Client {
    pub(crate) async fn run_state(&self, run_id: Uuid, revision: u64) -> Result<RunState, Error> {
        let view: RunView = self
            .http
            .get(format!("{}/runs/{run_id}", self.base_url()))
            .query(&[("revision", revision)])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        if view.run.uuid != run_id {
            return Err(Error::Artifact("server returned a different run".into()));
        }
        Ok(RunState {
            nodes: view
                .node_runs
                .into_iter()
                .map(|(node_run, _)| NodeState {
                    node_id: node_run.node_id,
                    version: node_run.version,
                    revision: node_run.revision,
                    status: node_run.status,
                    ineffective: node_run.ineffective,
                })
                .collect(),
        })
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
            .await?
            .error_for_status()?;
        let value: serde_json::Value = response.json().await?;
        serde_json::from_value(value.get("transfer").cloned().unwrap_or(value)).map_err(Into::into)
    }

    pub async fn commit_upload(
        &self,
        job_id: Uuid,
        agent_id: Uuid,
        filename: &str,
        catalog_uuid: Uuid,
        _size: u64,
        _format: &str,
    ) -> Result<(), Error> {
        let body = serde_json::json!({"job_id": job_id, "agent_id": agent_id, "filename": filename, "catalog_uuid": catalog_uuid});
        self.http
            .post(format!("{}/job/output/commit", self.base_url()))
            .json(&body)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
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

    pub async fn cancel(&self, agent_id: Uuid, job_id: Uuid) -> Result<(), Error> {
        self.job_action("cancel", agent_id, job_id).await
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
        if response.status() == reqwest::StatusCode::CONFLICT {
            return Err(Error::ClaimLost(job_id));
        }
        response.error_for_status()?;
        Ok(())
    }
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
