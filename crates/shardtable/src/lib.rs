//! Membership-driven consistent hash table.
//!
//! The crate separates three concerns:
//!
//! - [`Membership`] exposes the live member set and notifications.
//! - [`HashTable`] owns the virtual-node hash ring and precomputes the owner of
//!   every shard, making [`HashTable::shard_owner`] a direct indexed lookup.
//! - [`Coordinator`] is cloneable and concurrent: it rebuilds the published
//!   table when membership changes and reports shard migrations.
//!
//! The hash table is independent of membership; the coordinator connects them.

use std::fmt::Debug;
use std::hash::Hash;

use tokio::sync::watch;

mod coordinator;
mod table;

pub use coordinator::{Coordinator, DEFAULT_REPLICAS};
pub use table::{HashTable, ShardMigration};

/// Supplies the current set of cluster members.
pub trait Membership {
    /// The member type used by the hash table.
    type Member: Clone + Debug + Eq + Ord + Hash;

    /// Returns the members currently participating in the cluster.
    fn members(&self) -> impl IntoIterator<Item = Self::Member>;

    /// Subscribes to member-list snapshots.
    ///
    /// Implementations should send the new member list whenever it changes.
    /// Tokio's `watch` channel is used because only the latest membership matters
    /// and multiple consumers can observe the same update.
    fn subscribe(&self) -> MemberChanges<Self::Member>;
}

/// A stream of member-list snapshots from a [`Membership`].
pub type MemberChanges<M> = watch::Receiver<Vec<M>>;
