//! Scheduling contracts independent of the execution destination.

/// Schedules pending jobs using a backend supplied by the caller.
///
/// Implementations select eligible `Job` records and record their
/// assignments or submissions. Destinations may be Rayflow agents or external
/// job schedulers such as Kubernetes Volcano. Agent polling and job claim leases are
/// specific to the agent implementation.
///
/// `B` may be a database transaction or another scheduling context. The caller
/// serializes passes (the pilot uses its leader lease). External submissions
/// must use a durably committed identity and reconcile uncertain responses on
/// retry, since remote effects cannot be rolled back with a database transaction.
/// The pilot may create logical tasks and jobs in the same transaction as
/// this call; their generated UUIDs are not durable until commit. External
/// adapters must arrange committed submission requests before making remote
/// calls, for example by selecting only requests visible on a separate committed
/// database connection.
#[allow(async_fn_in_trait)]
pub trait Scheduler<B> {
    /// Implementation-specific scheduling error.
    type Err: std::error::Error + Send + Sync + 'static;

    /// Runs a scheduling pass and returns the number of changed assignments.
    /// Database effects belong to the caller's transaction when one is supplied.
    async fn schedule(&self, backend: &mut B) -> Result<usize, Self::Err>;
}
