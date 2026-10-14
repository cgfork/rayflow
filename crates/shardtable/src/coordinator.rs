//! Membership-driven hash table coordination.

use std::sync::{Arc, Mutex, MutexGuard, RwLock};

use crate::{HashTable, MemberChanges, Membership, ShardMigration};
use tokio::sync::watch;

/// Default number of virtual nodes per member.
pub const DEFAULT_REPLICAS: usize = 128;

/// Watches membership and publishes an updated [`HashTable`].
///
/// Coordinators can be cloned and shared. The current table is published as an
/// immutable [`Arc`] snapshot, so readers can look up shard owners concurrently
/// while refresh operations build and swap in a new table.
pub struct Coordinator<M>
where
    M: Membership,
{
    inner: Arc<Inner<M>>,
}

struct Inner<M>
where
    M: Membership,
{
    membership: M,
    changes: Mutex<MemberChanges<M::Member>>,
    table: RwLock<Arc<HashTable<M::Member>>>,
    replicas: usize,
}

impl<M> Clone for Coordinator<M>
where
    M: Membership,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<M> Coordinator<M>
where
    M: Membership + Send + Sync,
    M::Member: Send + Sync,
{
    /// Creates a coordinator with `shards` shards and [`DEFAULT_REPLICAS`]
    /// virtual nodes per member.
    pub fn new(membership: M, shards: u32) -> Self {
        Self::with_replicas(membership, shards, DEFAULT_REPLICAS)
    }

    /// Creates a coordinator with the given shard and virtual-node counts.
    ///
    /// # Panics
    ///
    /// Panics when `shards` or `replicas` is zero.
    pub fn with_replicas(membership: M, shards: u32, replicas: usize) -> Self {
        assert!(replicas > 0, "replicas must be at least 1");
        let changes = membership.subscribe();
        let table = build_table(&membership, shards, replicas);
        Self {
            inner: Arc::new(Inner {
                membership,
                changes: Mutex::new(changes),
                table: RwLock::new(Arc::new(table)),
                replicas,
            }),
        }
    }

    /// Rebuilds the table from the membership's current members.
    ///
    /// Returns the shards whose ownership changed. Unlike [`changed`](Self::changed),
    /// this can be called concurrently and does not require a mutable coordinator.
    pub fn refresh(&self) -> Vec<ShardMigration<M::Member>> {
        let members = {
            let mut changes = self.changes();
            let members = self.inner.membership.members().into_iter().collect();
            changes.borrow_and_update();
            members
        };
        self.rebuild(members)
    }

    /// Rebuilds the table when membership has changed since the last observation.
    pub fn try_refresh(&self) -> Option<Vec<ShardMigration<M::Member>>> {
        let members = {
            let mut changes = self.changes();
            if !changes.has_changed().unwrap_or(false) {
                return None;
            }
            changes.borrow_and_update().clone()
        };
        Some(self.rebuild(members))
    }

    /// Waits for a membership change, rebuilds the table, and returns migrations.
    ///
    /// Multiple callers can wait concurrently through clones of the same
    /// coordinator. Rebuilds are serialized only while the published table is
    /// being replaced.
    pub async fn changed(&self) -> Result<Vec<ShardMigration<M::Member>>, watch::error::RecvError> {
        let mut listener = self.changes().clone();
        listener.changed().await?;

        let members = self.changes().borrow_and_update().clone();
        Ok(self.rebuild(members))
    }

    fn rebuild(&self, members: Vec<M::Member>) -> Vec<ShardMigration<M::Member>> {
        let shards = self.table().shards();
        let next = build_table_from(members, shards, self.replicas());
        let mut table = self
            .inner
            .table
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let migrations = table.migrations(&next);
        *table = Arc::new(next);
        migrations
    }

    /// Returns the current immutable shard-table snapshot.
    ///
    /// The snapshot remains valid even if a subsequent refresh publishes a new
    /// table.
    pub fn table(&self) -> Arc<HashTable<M::Member>> {
        let table = self
            .inner
            .table
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Arc::clone(&table)
    }

    /// Returns the membership source.
    pub fn membership(&self) -> &M {
        &self.inner.membership
    }

    /// Returns the virtual-node count per member.
    pub fn replicas(&self) -> usize {
        self.inner.replicas
    }

    /// Subscribes to future membership changes.
    pub fn subscribe(&self) -> MemberChanges<M::Member> {
        self.inner.membership.subscribe()
    }

    fn changes(&self) -> MutexGuard<'_, MemberChanges<M::Member>> {
        self.inner
            .changes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

fn build_table<M>(membership: &M, shards: u32, replicas: usize) -> HashTable<M::Member>
where
    M: Membership,
{
    build_table_from(membership.members(), shards, replicas)
}

fn build_table_from<I, M>(members: I, shards: u32, replicas: usize) -> HashTable<M>
where
    I: IntoIterator<Item = M>,
    M: Clone + std::hash::Hash + Eq + Ord,
{
    HashTable::new(shards, replicas, members)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SHARDS: u32 = 512;

    #[derive(Clone)]
    struct TestMembership {
        changes: watch::Sender<Vec<&'static str>>,
    }

    impl TestMembership {
        fn new(members: Vec<&'static str>) -> Self {
            let (changes, _) = watch::channel(members);
            Self { changes }
        }
    }

    impl Membership for TestMembership {
        type Member = &'static str;

        fn members(&self) -> impl IntoIterator<Item = Self::Member> {
            self.changes.borrow().clone()
        }

        fn subscribe(&self) -> MemberChanges<Self::Member> {
            self.changes.subscribe()
        }
    }

    #[test]
    fn initial_table_contains_membership_members() {
        let coordinator =
            Coordinator::with_replicas(TestMembership::new(vec!["a", "b", "c"]), SHARDS, 32);
        let table = coordinator.table();

        assert_eq!(table.len(), 3);
        for shard in 0..SHARDS {
            assert!(
                table
                    .shard_owner(shard)
                    .is_some_and(|owner| ["a", "b", "c"].contains(owner))
            );
        }
    }

    #[tokio::test]
    async fn notification_rebuilds_the_table_and_reports_migrations() {
        let membership = TestMembership::new(vec!["a", "b", "c", "d"]);
        let changes = membership.changes.clone();
        let coordinator = Coordinator::with_replicas(membership, SHARDS, 32);

        let update = async move {
            changes.send_replace(vec!["a", "b", "c", "d", "e"]);
        };
        let (migrations, ()) = tokio::join!(coordinator.changed(), update);
        let migrations = migrations.expect("membership still connected");

        assert_eq!(coordinator.table().len(), 5);
        assert!(!migrations.is_empty());
        assert!(migrations.iter().all(|migration| migration.to == Some("e")));
    }

    #[test]
    fn refresh_reports_removed_member_shards() {
        let membership = TestMembership::new(vec!["a", "b", "c"]);
        let changes = membership.changes.clone();
        let coordinator = Coordinator::with_replicas(membership, SHARDS, 32);

        changes.send_replace(vec!["a", "b"]);
        let migrations = coordinator.try_refresh().expect("membership changed");

        assert_eq!(coordinator.table().len(), 2);
        assert!(!migrations.is_empty());
        assert!(
            migrations
                .iter()
                .all(|migration| migration.from == Some("c"))
        );
    }

    #[tokio::test]
    async fn clones_share_updates_while_old_snapshots_remain_readable() {
        let membership = TestMembership::new(vec!["a", "b", "c"]);
        let changes = membership.changes.clone();
        let coordinator = Coordinator::with_replicas(membership, SHARDS, 32);
        let reader = coordinator.clone();
        let old_snapshot = reader.table();

        let update = async move {
            changes.send_replace(vec!["a", "b", "c", "d"]);
        };
        let (migrations, ()) = tokio::join!(coordinator.changed(), update);
        migrations.expect("membership still connected");

        assert_eq!(old_snapshot.len(), 3);
        assert_eq!(reader.table().len(), 4);
    }
}
