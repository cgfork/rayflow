use crate::config::Config;
use executor::JobContext;
use scheduler::Job;
use scheduler::actuator::Status;
use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

#[derive(Clone)]
pub struct Client {
    http: reqwest::Client,
    poll_url: String,
}

#[derive(Debug, Deserialize)]
pub struct PolledJob {
    pub job: Job,
    pub context: JobContext,
    pub config: Value,
    pub lease_ms: u64,
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
    ) -> Result<Vec<PolledJob>, reqwest::Error> {
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

    pub async fn report_status(
        &self,
        job_id: Uuid,
        agent_id: Uuid,
        status: Status,
    ) -> Result<(), reqwest::Error> {
        let url = format!(
            "{}/jobs/{job_id}/status",
            self.poll_url.trim_end_matches("/jobs/poll")
        );
        self.http
            .post(url)
            .json(&serde_json::json!({ "agent_id": agent_id, "status": status }))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }
}
