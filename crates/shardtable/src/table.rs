//! Precomputed shard-to-member mapping.

use std::hash::{Hash, Hasher};

use siphasher::sip::SipHasher13;

/// A shard whose owning member changed between two tables.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardMigration<M> {
    /// Shard that changed ownership.
    pub shard: u32,
    /// Previous owner, or `None` when the shard was unassigned.
    pub from: Option<M>,
    /// New owner, or `None` when the shard became unassigned.
    pub to: Option<M>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct VirtualNode {
    hash: u64,
    member: usize,
}

/// A precomputed mapping from shard index to owning member.
///
/// Ownership is resolved from an internal virtual-node ring when the table is
/// built and then stored directly, so lookups do not traverse the ring.
#[derive(Clone, Debug)]
pub struct HashTable<M> {
    shards: u32,
    members: Vec<M>,
    ring: Vec<VirtualNode>,
    mapping: Vec<Option<M>>,
}

impl<M> HashTable<M>
where
    M: Clone + Hash + Eq + Ord,
{
    /// Builds a table with `replicas` virtual nodes per member.
    ///
    /// # Panics
    ///
    /// Panics when `shards` or `replicas` is zero.
    pub fn new<I>(shards: u32, replicas: usize, members: I) -> Self
    where
        I: IntoIterator<Item = M>,
    {
        assert!(shards > 0, "shards must be at least 1");
        assert!(replicas > 0, "replicas must be at least 1");

        let mut members: Vec<M> = members.into_iter().collect();
        members.sort();
        members.dedup();

        let mut table = Self {
            shards,
            members,
            ring: Vec::new(),
            mapping: Vec::new(),
        };
        table.build_ring(replicas);
        table.mapping = (0..shards)
            .map(|shard| table.owner_of(&shard).cloned())
            .collect();
        table
    }

    fn build_ring(&mut self, replicas: usize) {
        let ring_size = self.members.len().saturating_mul(replicas);
        let mut ring = Vec::with_capacity(ring_size);

        for (member_index, member) in self.members.iter().enumerate() {
            for replica in 0..replicas {
                ring.push(VirtualNode {
                    hash: hash_item(&(member, replica)),
                    member: member_index,
                });
            }
        }

        let members = &self.members;
        ring.sort_by(|left, right| {
            (left.hash, &members[left.member]).cmp(&(right.hash, &members[right.member]))
        });
        self.ring = ring;
    }

    fn owner_of<T: Hash>(&self, item: &T) -> Option<&M> {
        if self.ring.is_empty() {
            return None;
        }

        let item_hash = hash_item(item);
        let index = self
            .ring
            .binary_search_by_key(&(item_hash, 0), |vnode| (vnode.hash, 1))
            .expect_err("virtual node keys always have a non-zero sequence marker");
        let index = if index == self.ring.len() { 0 } else { index };
        Some(&self.members[self.ring[index].member])
    }

    /// Returns the number of shards in the table.
    pub fn shards(&self) -> u32 {
        self.shards
    }

    /// Returns the stored shard-to-member mapping, indexed by shard number.
    pub fn mapping(&self) -> &[Option<M>] {
        &self.mapping
    }

    /// Returns the member owning `shard` through a direct indexed lookup.
    pub fn shard_owner(&self, shard: u32) -> Option<&M> {
        self.mapping.get(shard as usize).and_then(Option::as_ref)
    }

    /// Returns the number of distinct members.
    pub fn len(&self) -> usize {
        self.members.len()
    }

    /// Returns whether the table has no members.
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// Iterates over members in stable order.
    pub fn members(&self) -> impl Iterator<Item = &M> {
        self.members.iter()
    }

    /// Returns every shard assigned to `member`.
    pub fn owned_shards(&self, member: &M) -> Vec<u32> {
        self.mapping
            .iter()
            .enumerate()
            .filter_map(|(shard, owner)| (owner.as_ref() == Some(member)).then_some(shard as u32))
            .collect()
    }

    /// Returns the shards whose owner differs between `self` and `next`.
    pub fn migrations(&self, next: &Self) -> Vec<ShardMigration<M>> {
        let shards = self.shards.min(next.shards);
        (0..shards)
            .filter_map(|shard| {
                let from = self.shard_owner(shard).cloned();
                let to = next.shard_owner(shard).cloned();
                (from != to).then_some(ShardMigration { shard, from, to })
            })
            .collect()
    }
}

fn hash_item<T: Hash>(item: &T) -> u64 {
    let mut hasher = SipHasher13::new();
    item.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    const SHARDS: u32 = 512;
    const REPLICAS: usize = 32;

    fn members(members: &[&'static str]) -> Vec<&'static str> {
        members.to_vec()
    }

    #[test]
    fn table_has_one_mapping_entry_per_shard() {
        let table = HashTable::new(SHARDS, REPLICAS, members(&["a", "b", "c"]));
        assert_eq!(table.mapping().len(), SHARDS as usize);
        assert!(
            table
                .mapping()
                .iter()
                .all(|owner| owner.is_some_and(|owner| ["a", "b", "c"].contains(&owner)))
        );
    }

    #[test]
    fn empty_membership_has_no_owner() {
        let table = HashTable::<&str>::new(8, REPLICAS, Vec::new());
        assert!(table.is_empty());
        assert!(table.mapping().iter().all(Option::is_none));
    }

    #[test]
    fn single_member_owns_every_shard() {
        let table = HashTable::new(SHARDS, REPLICAS, members(&["only"]));
        for shard in 0..SHARDS {
            assert_eq!(table.shard_owner(shard), Some(&"only"));
        }
    }

    #[test]
    fn mapping_is_stable_for_the_same_membership() {
        let first = HashTable::new(SHARDS, REPLICAS, members(&["a", "b", "c"]));
        let second = HashTable::new(SHARDS, REPLICAS, members(&["c", "a", "b", "a"]));

        assert_eq!(
            first.members().copied().collect::<Vec<_>>(),
            vec!["a", "b", "c"]
        );
        for shard in 0..SHARDS {
            assert_eq!(first.shard_owner(shard), second.shard_owner(shard));
        }
    }

    #[test]
    fn shard_owner_is_a_direct_indexed_lookup() {
        let table = HashTable::new(16, REPLICAS, members(&["a", "b"]));
        for shard in 0..16 {
            assert_eq!(
                table.shard_owner(shard),
                table.mapping().get(shard as usize).and_then(Option::as_ref)
            );
        }
        assert_eq!(table.shard_owner(16), None);
    }

    #[test]
    fn adding_a_member_only_migrates_shards_to_the_new_member() {
        let before = HashTable::new(SHARDS, REPLICAS, members(&["a", "b", "c", "d"]));
        let after = HashTable::new(SHARDS, REPLICAS, members(&["a", "b", "c", "d", "e"]));

        let migrations = before.migrations(&after);
        assert!(!migrations.is_empty());
        assert!(migrations.iter().all(|migration| migration.to == Some("e")));
        assert!(migrations.len() < SHARDS as usize / 3);
    }
}
