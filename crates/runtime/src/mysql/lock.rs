//! Transaction-scoped distributed locks.
//!
//! A lock is a row in `transition_lock` with a unique `(data_id, graph_id)` key.
//! The no-op upsert takes InnoDB's exclusive row lock, and the transaction holds
//! it until commit or rollback. Competing processes on any machine therefore
//! serialize on the same run, and a crashed process releases the lock when its
//! connection is dropped.

use toasty::Executor;
use uuid::Uuid;

use super::Error;

/// Serializes command application for one run until `executor`'s transaction ends.
pub(crate) async fn lock_transition<E>(
    executor: &mut E,
    data_id: Uuid,
    graph_id: Uuid,
) -> Result<(), Error>
where
    E: Executor,
{
    toasty::sql::statement(
        "INSERT INTO transition_lock (data_id, graph_id, updated_at) \
         VALUES (?, ?, CURRENT_TIMESTAMP(6)) \
         ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP(6)",
    )
    .bind(data_id)
    .bind(graph_id)
    .exec(executor)
    .await?;
    Ok(())
}
