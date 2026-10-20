//! Shared consumed command offset.

use std::sync::Arc;

use tokio::sync::watch;

/// The consumed command offset, shared between the app and its waiters.
///
/// The offset only moves forward. [`set`](Self::set) publishes a new value and
/// wakes every waiter blocked in [`wait`](Self::wait).
#[derive(Clone)]
pub struct Offsets {
    sender: Arc<watch::Sender<u64>>,
}

impl Default for Offsets {
    fn default() -> Self {
        Self::new()
    }
}

impl Offsets {
    /// Creates an offset starting at `0`.
    pub fn new() -> Self {
        Self {
            sender: Arc::new(watch::channel(0).0),
        }
    }

    /// Returns the current offset.
    pub fn get(&self) -> u64 {
        *self.sender.borrow()
    }

    /// Advances the offset to `offset` and notifies all waiters.
    ///
    /// The offset only moves forward: a value that does not advance it is
    /// ignored (and notifies nobody).
    pub fn set(&self, offset: u64) {
        self.sender.send_if_modified(|current| {
            if offset > *current {
                *current = offset;
                true
            } else {
                false
            }
        });
    }

    /// Waits until the offset reaches `offset`.
    ///
    /// Returns immediately when the offset has already been reached and blocks
    /// (indefinitely) otherwise, waking when [`set`](Self::set) advances it.
    pub async fn wait(&self, offset: u64) {
        let mut receiver = self.sender.subscribe();
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
        let offsets = Offsets::new();
        offsets.set(3);
        offsets.set(1);
        assert_eq!(offsets.get(), 3);
    }

    #[tokio::test]
    async fn wait_returns_immediately_when_reached() {
        let offsets = Offsets::new();
        offsets.set(4);
        tokio::time::timeout(Duration::from_secs(1), offsets.wait(4))
            .await
            .expect("wait should return immediately");
    }

    #[tokio::test]
    async fn set_wakes_all_waiters() {
        let offsets = Offsets::new();
        let first = offsets.clone();
        let second = offsets.clone();
        let a = tokio::spawn(async move { first.wait(3).await });
        let b = tokio::spawn(async move { second.wait(3).await });

        tokio::time::sleep(Duration::from_millis(50)).await;
        offsets.set(2);
        assert!(
            !a.is_finished() && !b.is_finished(),
            "waits before the target"
        );

        offsets.set(3);
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
