//! Backend-independent leader election for Rayflow consumers.
//!
//! Enable the default `mysql` feature for the MySQL implementation.

#[cfg(feature = "mysql")]
pub mod mysql;

/// Leader-election operations required by a consumer.
#[allow(async_fn_in_trait)]
pub trait LeaderElection: Send + Sync {
    /// Backend-specific error.
    type Err: std::error::Error + Send + Sync + 'static;

    /// Attempts to acquire or renew the named lease.
    async fn try_acquire_or_renew(&self) -> Result<bool, Self::Err>;
    /// Checks current lease ownership.
    async fn is_leader(&self) -> Result<bool, Self::Err>;
    /// Releases this participant's lease.
    async fn resign(&self) -> Result<(), Self::Err>;
}

/// Election that protects work performed in a caller-owned transaction.
/// A successful acquisition must exclude takeover until that transaction ends.
#[allow(async_fn_in_trait)]
pub trait TransactionalLeaderElection<T>: LeaderElection {
    /// Acquires or renews ownership and holds it for the transaction's lifetime.
    async fn acquire_in(&self, tx: &mut T) -> Result<bool, Self::Err>;
}
