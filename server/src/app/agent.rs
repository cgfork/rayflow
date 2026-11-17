use jiff::Timestamp;
pub use scheduler::AgentInstance;
use scheduler::AgentRef;
use serde::Serialize;
use std::time::Duration;

use super::{App, AppError, Page, PageQuery};

#[derive(Debug, Serialize)]
pub struct AgentView {
    #[serde(flatten)]
    pub agent: AgentInstance,
    pub online: bool,
}

impl<S> App<S> {
    pub async fn list_agents(
        &self,
        page: PageQuery,
        online: Option<bool>,
    ) -> Result<Page<AgentView>, AppError> {
        let limit = page.limit()?;
        let now = Timestamp::now();
        let mut select = AgentInstance::all();
        if let Some(before) = page.before {
            select = select.filter(AgentInstance::fields().id().lt(before));
        }
        if let Some(online) = online {
            select = select.filter(if online {
                AgentInstance::fields().online_until().gt(now)
            } else {
                AgentInstance::fields().online_until().le(now)
            });
        }
        let rows = select
            .order_by(AgentInstance::fields().id().desc())
            .limit(limit + 1)
            .exec(&mut self.db.clone())
            .await?;
        let items = rows
            .into_iter()
            .map(|agent| AgentView {
                online: agent.online_until > now,
                agent,
            })
            .collect();
        Ok(Page::new(items, limit, |row| row.agent.id))
    }

    pub(super) async fn observe_agent(
        &self,
        tx: &mut toasty::Transaction<'_>,
        agent: &AgentRef,
    ) -> Result<(), AppError> {
        let grace = self
            .config
            .poll_timeout
            .saturating_mul(2)
            .saturating_add(Duration::from_secs(60));
        scheduler::agent_registry::observe(tx, agent, grace)
            .await
            .map_err(|error| match error {
                scheduler::AgentRegistryError::Database(error) => AppError::from(error),
            })?;
        Ok(())
    }
}
