use graph::GraphStore;
use graph::types::DAG;
use scheduler::{Job, ResourceCapacity};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{errors::Error, settings::Settings};

#[derive(Clone)]
pub struct Client {
    http: reqwest::Client,
    server_url: String,
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
    pub fn new(settings: &Settings) -> Result<Self, Error> {
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
            server_url: settings.server_url.trim_end_matches('/').to_owned(),
        })
    }

    pub async fn poll(&self, request: &PollRequest) -> Result<PollResponse, Error> {
        Ok(self
            .http
            .post(format!("{}/jobs/poll", self.server_url))
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
            .get(format!("{}/graphs/{graph_id}", self.server_url));
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
            .post(format!("{}/job/{action}", self.server_url))
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
            .get(format!("{}/graphs", self.server_url))
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
