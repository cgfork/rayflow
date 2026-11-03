use std::time::Duration;

use jiff::Timestamp;
use scheduler::AgentRef;
use serde::Serialize;
use toasty::schema::Model;
use toasty::{Executor, Transaction};
use uuid::Uuid;

use super::{App, AppError, Page, PageQuery};

#[derive(Debug, Clone, Serialize, toasty::Model)]
#[table = "agent_instance"]
pub struct AgentInstance {
    #[key]
    #[auto]
    pub id: u64,
    #[unique]
    pub uuid: Uuid,
    pub cpu_cores: u32,
    pub memory_bytes: u64,
    pub active_jobs: u32,
    pub first_seen_at: Timestamp,
    pub last_seen_at: Timestamp,
    #[index]
    pub online_until: Timestamp,
}

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
        tx: &mut Transaction<'_>,
        agent: &AgentRef,
    ) -> Result<(), AppError> {
        let table_id = tx.schema().mapping.model(AgentInstance::id()).table;
        let table = tx.schema().db.table(table_id).name.replace('`', "``");
        let grace = self
            .config
            .poll_timeout
            .saturating_mul(2)
            .saturating_add(Duration::from_secs(60));
        let micros = i64::try_from(grace.as_micros()).unwrap_or(i64::MAX);
        // Toasty's MySQL driver cannot express this atomic upsert or database clock.
        toasty::sql::statement(format!(
            "INSERT INTO `{table}` (uuid, cpu_cores, memory_bytes, active_jobs, first_seen_at, last_seen_at, online_until) \
             VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6), TIMESTAMPADD(MICROSECOND, ?, CURRENT_TIMESTAMP(6))) \
             ON DUPLICATE KEY UPDATE cpu_cores = VALUES(cpu_cores), memory_bytes = VALUES(memory_bytes), \
             active_jobs = VALUES(active_jobs), last_seen_at = VALUES(last_seen_at), online_until = VALUES(online_until)"
        ))
        .bind(agent.id()).bind(agent.resource().cpu_cores).bind(agent.resource().memory_bytes)
        .bind(u32::try_from(agent.active_jobs().len()).unwrap_or(u32::MAX)).bind(micros)
        .exec(tx).await?;
        Ok(())
    }
}
