use std::collections::VecDeque;
use std::time::Duration;

use event::{Poll, Reactive};
use tokio::sync::oneshot;

#[derive(Clone)]
struct Entry(u64);

#[derive(Default)]
struct Backend {
    pending: VecDeque<Entry>,
    handled: Vec<u64>,
    acked: Vec<u64>,
    polls: usize,
    fail_handle: bool,
    fail_ack: bool,
    stop_during_handle: bool,
    shutdown: Option<oneshot::Sender<()>>,
}

impl Poll<Backend> for Entry {
    type Err = std::io::Error;

    async fn poll(
        backend: &mut Backend,
        maximum: usize,
    ) -> Result<impl Iterator<Item = Self>, Self::Err> {
        backend.polls += 1;
        Ok(backend
            .pending
            .iter()
            .take(maximum)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter())
    }

    async fn ack(&self, backend: &mut Backend) -> Result<(), Self::Err> {
        if backend.fail_ack {
            return Err(std::io::Error::other("ack failed"));
        }
        assert_eq!(backend.handled.last(), Some(&self.0));
        assert_eq!(backend.pending.pop_front().unwrap().0, self.0);
        backend.acked.push(self.0);
        if backend.pending.is_empty()
            && let Some(shutdown) = backend.shutdown.take()
        {
            let _ = shutdown.send(());
        }
        Ok(())
    }
}

impl Reactive<Backend> for Entry {
    async fn handle(&self, backend: &mut Backend) -> Result<(), Self::Err> {
        if backend.fail_handle {
            return Err(std::io::Error::other("handle failed"));
        }
        if backend.stop_during_handle
            && let Some(shutdown) = backend.shutdown.take()
        {
            let _ = shutdown.send(());
            tokio::task::yield_now().await;
        }
        backend.handled.push(self.0);
        Ok(())
    }
}

#[tokio::test]
async fn drains_full_batches_in_order_and_stops_on_shutdown() {
    let (tx, rx) = oneshot::channel();
    let mut backend = Backend {
        pending: (1..=5).map(Entry).collect(),
        shutdown: Some(tx),
        ..Backend::default()
    };
    let count = Entry::run(&mut backend, 2, Duration::from_secs(60), async {
        let _ = rx.await;
    })
    .await
    .unwrap();
    assert_eq!(count, 5);
    assert_eq!(backend.handled, vec![1, 2, 3, 4, 5]);
    assert_eq!(backend.acked, backend.handled);
    assert_eq!(backend.polls, 3);
}

#[tokio::test]
async fn failures_leave_record_available_for_restart() {
    for fail_ack in [false, true] {
        let mut backend = Backend {
            pending: vec![Entry(1), Entry(2)].into(),
            fail_handle: !fail_ack,
            fail_ack,
            ..Backend::default()
        };
        assert!(
            Entry::run(
                &mut backend,
                2,
                Duration::from_secs(1),
                std::future::pending()
            )
            .await
            .is_err()
        );
        assert!(backend.acked.is_empty());
        assert_eq!(backend.pending.len(), 2);
        let (tx, rx) = oneshot::channel();
        backend.shutdown = Some(tx);
        backend.fail_handle = false;
        backend.fail_ack = false;
        assert_eq!(
            Entry::run(&mut backend, 2, Duration::from_secs(1), async {
                let _ = rx.await;
            })
            .await
            .unwrap(),
            2
        );
        assert_eq!(backend.acked, vec![1, 2]);
    }
}

#[tokio::test]
async fn idle_wait_is_interruptible_and_does_not_spin() {
    let mut backend = Backend::default();
    let count = Entry::run(
        &mut backend,
        2,
        Duration::from_secs(60),
        tokio::time::sleep(Duration::from_millis(10)),
    )
    .await
    .unwrap();
    assert_eq!(count, 0);
    assert_eq!(backend.polls, 1);
}

#[tokio::test]
async fn already_requested_shutdown_does_not_poll() {
    let mut backend = Backend::default();
    assert_eq!(
        Entry::run(
            &mut backend,
            2,
            Duration::from_secs(1),
            std::future::ready(())
        )
        .await
        .unwrap(),
        0
    );
    assert_eq!(backend.polls, 0);
}

#[tokio::test]
async fn shutdown_finishes_current_record_and_leaves_rest_unacknowledged() {
    let (tx, rx) = oneshot::channel();
    let mut backend = Backend {
        pending: vec![Entry(1), Entry(2)].into(),
        shutdown: Some(tx),
        stop_during_handle: true,
        ..Backend::default()
    };
    let count = Entry::run(&mut backend, 2, Duration::from_secs(1), async {
        let _ = rx.await;
    })
    .await
    .unwrap();
    assert_eq!(count, 1);
    assert_eq!(backend.handled, vec![1]);
    assert_eq!(backend.acked, vec![1]);
    assert_eq!(backend.pending.front().unwrap().0, 2);
}
