//! Shared per-shard consumed offsets.

use std::sync::Arc;

use tokio::sync::watch;

/// The consumed offset of each shard, shared between the app and its waiters.
///
/// A shard's offset only moves forward. [`set`](Self::set) publishes a new value
/// and wakes every waiter blocked in [`wait`](Self::wait).
#[derive(Clone)]
pub struct Offsets {
    senders: Arc<Vec<watch::Sender<u64>>>,
}

impl Offsets {
    /// Creates one offset per shard, all starting at `0`.
    pub fn new(shards: u32) -> Self {
        let senders = (0..shards).map(|_| watch::channel(0).0).collect();
        Self {
            senders: Arc::new(senders),
        }
    }

    /// Returns `shard`'s current offset.
    pub fn get(&self, shard: u32) -> u64 {
        *self.senders[shard as usize].borrow()
    }

    /// Advances `shard`'s offset to `offset` and notifies all waiters.
    ///
    /// The offset only moves forward: a value that does not advance it is
    /// ignored (and notifies nobody).
    pub fn set(&self, shard: u32, offset: u64) {
        self.senders[shard as usize].send_if_modified(|current| {
            if offset > *current {
                *current = offset;
                true
            } else {
                false
            }
        });
    }

    /// Waits until `shard`'s offset reaches `offset`.
    ///
    /// Returns immediately when the offset has already been reached and blocks
    /// (indefinitely) otherwise, waking when [`set`](Self::set) advances it.
    pub async fn wait(&self, shard: u32, offset: u64) {
        let mut receiver = self.senders[shard as usize].subscribe();
        // The sender lives in `self`, so `wait_for` only errors if it is dropped.
        let _ = receiver.wait_for(|current| *current >= offset).await;
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn set_only_moves_forward() {
        let offsets = Offsets::new(1);
        offsets.set(0, 3);
        offsets.set(0, 1);
        assert_eq!(offsets.get(0), 3);
    }

    #[tokio::test]
    async fn wait_returns_immediately_when_reached() {
        let offsets = Offsets::new(2);
        offsets.set(1, 4);
        tokio::time::timeout(Duration::from_secs(1), offsets.wait(1, 4))
            .await
            .expect("wait should return immediately");
    }

    #[tokio::test]
    async fn set_wakes_all_waiters() {
        let offsets = Offsets::new(1);
        let first = offsets.clone();
        let second = offsets.clone();
        let a = tokio::spawn(async move { first.wait(0, 3).await });
        let b = tokio::spawn(async move { second.wait(0, 3).await });

        tokio::time::sleep(Duration::from_millis(50)).await;
        offsets.set(0, 2);
        assert!(
            !a.is_finished() && !b.is_finished(),
            "waits before the target"
        );

        offsets.set(0, 3);
        tokio::time::timeout(Duration::from_secs(1), a)
            .await
            .expect("first waiter woken")
            .expect("first task");
        tokio::time::timeout(Duration::from_secs(1), b)
            .await
            .expect("second waiter woken")
            .expect("second task");
    }
}
