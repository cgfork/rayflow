//! Persistent agent instance registration and liveness.
use crate::{AgentRef, GpuDevice};
use jiff::Timestamp;
use serde::Serialize;
use std::time::Duration;
use toasty::schema::Model;
use toasty::{Executor, Json, Transaction};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, toasty::Model)]
#[table = "agent_instance"]
pub struct Instance {
    #[key]
    #[auto]
    pub id: u64,
    #[unique]
    pub uuid: Uuid,
    pub cpu_cores: u32,
    pub memory_bytes: u64,
    #[default(0)]
    pub gpu_memory_bytes: u64,
    #[column(type = json)]
    pub gpu_devices: Option<Json<Vec<GpuDevice>>>,
    pub active_jobs: u32,
    pub first_seen_at: Timestamp,
    pub last_seen_at: Timestamp,
    #[index]
    pub online_until: Timestamp,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Database(#[from] toasty::Error),
}

pub fn models() -> toasty::ModelSet {
    toasty::models!(Instance)
}

/// Records a poll heartbeat atomically and extends liveness.
pub async fn observe(
    tx: &mut Transaction<'_>,
    agent: &AgentRef,
    grace: Duration,
) -> Result<(), Error> {
    let table_id = tx.schema().mapping.model(Instance::id()).table;
    let table = tx.schema().db.table(table_id).name.replace('`', "``");
    let micros = i64::try_from(grace.as_micros()).unwrap_or(i64::MAX);
    let gpu_devices = if agent.gpu_devices().is_empty() {
        None
    } else {
        Some(serde_json::to_string(agent.gpu_devices()).map_err(|error| {
            toasty::Error::from_args(format_args!("serialize GPU devices: {error}"))
        })?)
    };
    toasty::sql::statement(format!(
        "INSERT INTO `{table}` (uuid, cpu_cores, memory_bytes, gpu_memory_bytes, gpu_devices, active_jobs, first_seen_at, last_seen_at, online_until) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6), TIMESTAMPADD(MICROSECOND, ?, CURRENT_TIMESTAMP(6))) ON DUPLICATE KEY UPDATE cpu_cores=VALUES(cpu_cores), memory_bytes=VALUES(memory_bytes), gpu_memory_bytes=VALUES(gpu_memory_bytes), gpu_devices=VALUES(gpu_devices), active_jobs=VALUES(active_jobs), last_seen_at=VALUES(last_seen_at), online_until=VALUES(online_until)"
    ))
    .bind(agent.id()).bind(agent.resource().cpu_cores).bind(agent.resource().memory_bytes)
    .bind(agent.gpu_memory_bytes())
    .bind_typed(gpu_devices, toasty::schema::db::Type::Json)
    .bind(u32::try_from(agent.active_jobs().len()).unwrap_or(u32::MAX)).bind(micros)
    .exec(tx).await?;
    Ok(())
}
