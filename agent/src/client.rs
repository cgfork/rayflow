use crate::config::Config;
use scheduler::Job;
use serde::Deserialize;
use uuid::Uuid;

#[derive(Clone)]
pub struct Client {
    http: reqwest::Client,
    poll_url: String,
}

#[derive(Debug, Deserialize)]
pub struct PollJobsResponse {
    pub jobs: Vec<Job>,
    pub renewed_jobs: Vec<Uuid>,
}

#[derive(Clone, Copy, Debug)]
pub enum JobAction {
    Yield,
    Succeed,
    Fail,
    Terminate,
}

impl JobAction {
    pub fn path(self) -> &'static str {
        match self {
            Self::Yield => "yield",
            Self::Succeed => "succeed",
            Self::Fail => "fail",
            Self::Terminate => "terminate",
        }
    }
}

impl Client {
    pub fn new(config: &Config) -> Result<Self, reqwest::Error> {
        Ok(Self {
            http: reqwest::Client::builder()
                .timeout(config.request_timeout())
                .build()?,
            poll_url: format!("{}/jobs/poll", config.api_url.trim_end_matches('/')),
        })
    }

    pub async fn poll(
        &self,
        config: &Config,
        resources: scheduler::ResourceCapacity,
        active_jobs: &[Uuid],
    ) -> Result<PollJobsResponse, reqwest::Error> {
        self.http
            .post(&self.poll_url)
            .json(&serde_json::json!({
                "agent_id": config.agent_id,
                "batch": config.batch,
                "resources": resources,
                "active_jobs": active_jobs,
            }))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
    }

    pub async fn finish_job(
        &self,
        job_id: Uuid,
        agent_id: Uuid,
        action: JobAction,
    ) -> Result<(), reqwest::Error> {
        let url = format!(
            "{}/job/{}",
            self.poll_url.trim_end_matches("/jobs/poll"),
            action.path(),
        );
        self.http
            .post(url)
            .json(&serde_json::json!({ "job_id": job_id, "agent_id": agent_id }))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }
}
