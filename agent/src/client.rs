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
        Ok(Self {
            http: reqwest::Client::builder()
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

    pub async fn cancel(&self, agent_id: Uuid, job_id: Uuid) -> Result<(), Error> {
        self.job_action("cancel", agent_id, job_id).await
    }

    pub async fn fail(&self, agent_id: Uuid, job_id: Uuid) -> Result<(), Error> {
        self.job_action("fail", agent_id, job_id).await
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
