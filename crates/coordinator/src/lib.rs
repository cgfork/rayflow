//! MySQL-backed leader election for Rayflow consumers.
//!
//! A [`LeaderElection`] is a lease on a named coordinator role. Every pilot owns
//! one election handle; the holder of the unexpired lease consumes commands,
//! tasks, and task events while the other pilots stand by.

use std::time::Duration;

use jiff::Timestamp;
use toasty::Db;

pub mod mysql {
    //! MySQL persistence for [`LeaderElection`](super::LeaderElection).

    use jiff::Timestamp;

    /// The `leader_election` table: one lease row per coordinator role.
    #[derive(Debug, Clone, toasty::Model)]
    #[table = "leader_election"]
    pub struct LeaderElectionModel {
        /// Coordinator role name, e.g. `rayflow`.
        #[key]
        #[column(type = varchar(128))]
        pub name: String,
        /// Current lease holder, or an empty string when nobody holds the lease.
        #[column(type = varchar(128))]
        pub leader_id: String,
        /// Instant at which the current lease expires.
        pub expires_at: Timestamp,
        /// Instant of the last successful acquisition or renewal.
        pub updated_at: Timestamp,
    }

    /// Returns the models required by leader election.
    pub fn models() -> toasty::ModelSet {
        toasty::models!(LeaderElectionModel)
    }
}

pub use mysql::LeaderElectionModel;

/// Error returned by [`LeaderElection`].
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The database returned an error.
    #[error(transparent)]
    Toasty(#[from] toasty::Error),
    /// The lease expiry could not be represented as a timestamp.
    #[error("leader lease overflow: {0}")]
    Time(#[from] jiff::Error),
}

/// A MySQL-backed lease for one named coordinator role.
///
/// Acquisition is a single atomic `INSERT ... ON DUPLICATE KEY UPDATE`, so at
/// most one caller can hold the lease at a time. A leader renews by calling
/// [`try_acquire_or_renew`](Self::try_acquire_or_renew) before its lease
/// expires; a standby waits until the row expires before taking over.
#[derive(Clone)]
pub struct LeaderElection {
    db: Db,
    name: String,
    node_id: String,
    lease: Duration,
}

impl LeaderElection {
    /// Creates a leader-election handle for the coordinator role `name`.
    ///
    /// `node_id` identifies this process and must be unique among pilots.
    /// `lease` must be non-zero.
    pub fn new(
        db: Db,
        name: impl Into<String>,
        node_id: impl Into<String>,
        lease: Duration,
    ) -> Self {
        assert!(!lease.is_zero(), "leader lease must be non-zero");
        Self {
            db,
            name: name.into(),
            node_id: node_id.into(),
            lease,
        }
    }

    /// Returns the coordinator role this election competes for.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns this process's leader id.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Acquires the lease when it is free or expired, or renews it when held by
    /// this node.
    ///
    /// Returns `true` when this node is the leader after the call, and `false`
    /// when another node still holds an unexpired lease.
    pub async fn try_acquire_or_renew(&self) -> Result<bool, Error> {
        let now = Timestamp::now();
        let expires_at = now.checked_add(self.lease)?;

        // The condition is evaluated before each assignment in MySQL. The first
        // assignment only changes the holder for an expired lease; the second
        // then sees either the unchanged holder or our just-installed id.
        toasty::sql::statement(
            "INSERT INTO leader_election (name, leader_id, expires_at, updated_at) \
             VALUES (?, ?, ?, ?) \
             ON DUPLICATE KEY UPDATE \
               leader_id = IF(leader_id = VALUES(leader_id) OR expires_at < VALUES(updated_at), VALUES(leader_id), leader_id), \
               expires_at = IF(leader_id = VALUES(leader_id) OR expires_at < VALUES(updated_at), VALUES(expires_at), expires_at), \
               updated_at = IF(leader_id = VALUES(leader_id) OR expires_at < VALUES(updated_at), VALUES(updated_at), updated_at)",
        )
        .bind(self.name.clone())
        .bind(self.node_id.clone())
        .bind(expires_at)
        .bind(now)
        .exec(&mut self.db.clone())
        .await?;

        self.is_leader().await
    }

    /// Returns whether this node currently holds an unexpired lease.
    pub async fn is_leader(&self) -> Result<bool, Error> {
        let now = Timestamp::now();
        let row = LeaderElectionModel::filter_by_name(self.name.clone())
            .first()
            .exec(&mut self.db.clone())
            .await?;
        Ok(row.is_some_and(|row| row.leader_id == self.node_id && row.expires_at > now))
    }

    /// Releases the lease when held by this node.
    pub async fn resign(&self) -> Result<(), Error> {
        let now = Timestamp::now();
        toasty::sql::statement(
            "UPDATE leader_election SET leader_id = '', expires_at = ?, updated_at = ? \
             WHERE name = ? AND leader_id = ?",
        )
        .bind(now)
        .bind(now)
        .bind(self.name.clone())
        .bind(self.node_id.clone())
        .exec(&mut self.db.clone())
        .await?;
        Ok(())
    }
}
