//! Task scheduler: consumes tasks and drives node status by task events.
//!
//! The scheduler ties the [`runtime`] primitives together. It consumes the tasks
//! that the backend enqueues as a run progresses and moves their nodes through
//! the state machine:
//!
//! ```text
//! ready -schedule-> running -event(pend)-> pending -event(terminal)-> succeeded/failed/terminated
//! ready -schedule-> running -event(terminal)-> succeeded/failed/terminated
//! ready -event(pend)-> pending -event(terminal)-> succeeded/failed/terminated
//! ```
//!
//! Scheduling a task emits [`Command::Run`] for its node. A [`TaskRunner`] then
//! runs the scheduled task, recording the [`TaskEvent`] it produces; a
//! [`TaskEventPoll`] supplies those events and acknowledges them once the
//! scheduler emits the command that drives each node to its next status as a
//! [`CommandEntry`].
//! Commands are appended to the store's command log (via
//! [`CommandEmit`](event::Emit)) rather than applied inline: consuming
//! and applying them (via [`CommandPoll`](event::Poll) +
//! [`CommandApply`](runtime::CommandApply)) is what drives node status. Because
//! the terminal transitions are ordinary events, a node that is `Pending` can
//! also be driven to a terminal status by feeding a [`TaskEvent`] back through
//! [`Scheduler::drive`].
//!
//! The scheduler is backend-agnostic: it depends only on the [`Store`],
//! [`TaskRunner`], and [`TaskEventPoll`] traits. A
//! MySQL implementations are provided for [`Store`] and [`Dispatcher`].

use event::command::Command;
use event::command::CommandEntry;
use event::task::Task;
use uuid::Uuid;

pub mod dispatcher;

pub use event::task_event::TaskEvent;
pub use event::task_event::TaskEventEntry;

pub use dispatcher::Dispatcher;

pub mod scheduled;
pub use scheduled::{ScheduledTask, ScheduledTaskRead, ScheduledTaskWrite};

/// Polls and acknowledges the task events produced by runs.
#[allow(async_fn_in_trait)]
pub trait TaskEventPoll: Send + Sync {
    /// Error returned when polling or acknowledging a task event.
    type Err: std::error::Error + Send + Sync + 'static;

    /// Returns up to `max` task events from the first unacknowledged contiguous
    /// offset.
    async fn poll(&self, max: usize) -> Result<Vec<TaskEventEntry>, Self::Err>;

    /// Acknowledges `event`, so it is not polled again.
    async fn ack(&self, event: &TaskEventEntry) -> Result<(), Self::Err>;
}

/// Executes scheduled tasks.
#[allow(async_fn_in_trait)]
pub trait TaskRunner: Send + Sync {
    /// Error returned when running a task.
    type Err: std::error::Error + Send + Sync + 'static;

    /// Runs `task`, executing the work. Calls may be retried after a failure or
    /// crash before acknowledgement; implementations must deduplicate by task ID.
    ///
    /// The event the run produces is recorded and later returned by
    /// [`TaskEventPoll::poll`], so running a task does not itself report the
    /// event.
    async fn run(&self, task: &Task) -> Result<(), Self::Err>;
}

/// The store the scheduler consumes tasks from and drives commands into.
#[allow(async_fn_in_trait)]
pub trait Store {
    /// Error returned by the store.
    type Err: std::error::Error + Send + Sync + 'static;

    /// A transaction opened by the store.
    ///
    /// The scheduler emits commands and acknowledges tasks through this handle,
    /// so a command and the task it schedules commit atomically.
    type Tx<'a>: StoreTx<Err = Self::Err>
    where
        Self: 'a;

    /// Consumes up to `max` tasks from the first unacknowledged contiguous
    /// offset.
    async fn poll_tasks(&self, max: usize) -> Result<Vec<Task>, Self::Err>;

    /// Opens a transaction the scheduler drives commands and task acks through.
    ///
    /// Implementing the transaction here keeps the store in charge of atomicity:
    /// an emit and an ack performed on the same transaction are committed
    /// together.
    async fn transaction(&mut self) -> Result<Self::Tx<'_>, Self::Err>;
}

/// A store transaction: a unit of work that emits commands and acknowledges tasks.
///
/// The scheduler performs the command emit and the task ack of a single step on
/// one transaction, so the two advance together when it commits.
#[allow(async_fn_in_trait)]
pub trait StoreTx {
    /// Error returned by the transaction.
    type Err: std::error::Error + Send + Sync + 'static;

    /// Appends `entry` to the command log.
    async fn emit(&mut self, entry: &CommandEntry) -> Result<(), Self::Err>;

    /// Acknowledges `task`.
    async fn ack(&mut self, task: &Task) -> Result<(), Self::Err>;

    /// Commits the transaction.
    async fn commit(self) -> Result<(), Self::Err>;
}

/// Error returned by the [`Scheduler`].
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The store returned an error.
    #[error("task store: {0}")]
    Store(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// The task runner returned an error.
    #[error("task runner or event consumer: {0}")]
    Runner(#[source] Box<dyn std::error::Error + Send + Sync>),
}

fn store_err<E>(error: E) -> Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    Error::Store(Box::new(error))
}

fn runner_err<E>(error: E) -> Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    Error::Runner(Box::new(error))
}

/// Consumes tasks and drives their nodes through the run state machine.
pub struct Scheduler<S, R> {
    store: S,
    runner: R,
    batch: usize,
}

impl<S, R> Scheduler<S, R>
where
    S: Store,
    R: TaskRunner + TaskEventPoll,
{
    /// Creates a scheduler that consumes at most `batch` tasks per [`tick`](Self::tick).
    pub fn new(store: S, runner: R, batch: usize) -> Self {
        assert!(batch > 0, "batch must be at least 1");
        Self {
            store,
            runner,
            batch,
        }
    }

    /// Returns a shared reference to the underlying store.
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Returns a mutable reference to the underlying store.
    pub fn store_mut(&mut self) -> &mut S {
        &mut self.store
    }

    /// Consumes the scheduler, returning its store.
    pub fn into_store(self) -> S {
        self.store
    }

    /// Returns the maximum number of tasks consumed per [`tick`](Self::tick).
    pub fn batch(&self) -> usize {
        self.batch
    }

    /// Schedules a consumed task, driving `Ready -> Running`.
    ///
    /// The `Run` command is emitted and the task is acknowledged on one store
    /// transaction, so a task is scheduled at most once.
    pub async fn schedule(&mut self, task: &Task) -> Result<(), Error> {
        let entry = Self::entry(task, Command::Run { node: task.node_id });
        let mut tx = self.store.transaction().await.map_err(store_err)?;
        tx.emit(&entry).await.map_err(store_err)?;
        tx.ack(task).await.map_err(store_err)?;
        tx.commit().await.map_err(store_err)
    }

    /// Drives a node's status by applying a task event.
    ///
    /// This is how a node that is `Pending` (or still `Running`) reaches a
    /// terminal status from an event delivered out of band.
    pub async fn drive(&mut self, task: &Task, event: TaskEvent) -> Result<(), Error> {
        self.emit_event(task.data_id, task.graph_id, task.node_id, event)
            .await
    }

    /// Emits the command that drives `node` by `event` for the run identified by
    /// `data_id` and `graph_id`.
    async fn emit_event(
        &mut self,
        data_id: Uuid,
        graph_id: Uuid,
        node: Uuid,
        event: TaskEvent,
    ) -> Result<(), Error> {
        let entry = CommandEntry::new(data_id, graph_id, event.command(node));
        let mut tx = self.store.transaction().await.map_err(store_err)?;
        tx.emit(&entry).await.map_err(store_err)?;
        tx.commit().await.map_err(store_err)
    }

    /// Consumes up to [`batch`](Self::batch) tasks, schedules and runs each, then
    /// polls the events the runs produced and drives each node by its event.
    ///
    /// A task remains unacknowledged until its runner succeeds and the scheduling
    /// transaction commits. Runner effects may survive a failed commit, so a
    /// retry uses the same task ID and runners must handle repeated calls.
    /// Returns the number of tasks processed.
    pub async fn tick(&mut self) -> Result<usize, Error> {
        let tasks = self.store.poll_tasks(self.batch).await.map_err(store_err)?;
        for task in &tasks {
            let entry = Self::entry(task, Command::Run { node: task.node_id });
            let mut tx = self.store.transaction().await.map_err(store_err)?;
            tx.emit(&entry).await.map_err(store_err)?;
            self.runner.run(task).await.map_err(runner_err)?;
            tx.ack(task).await.map_err(store_err)?;
            tx.commit().await.map_err(store_err)?;
        }
        self.drive_events().await?;
        Ok(tasks.len())
    }

    /// Polls up to [`batch`](Self::batch) task events and drives each node by its
    /// event.
    ///
    /// Returns the number of events driven.
    pub async fn drive_events(&mut self) -> Result<usize, Error> {
        let events = self.runner.poll(self.batch).await.map_err(runner_err)?;
        for entry in &events {
            self.emit_event(entry.data_id, entry.graph_id, entry.node_id, entry.event)
                .await?;
            self.runner.ack(entry).await.map_err(runner_err)?;
        }
        Ok(events.len())
    }

    /// Runs [`tick`](Self::tick) until a tick consumes no tasks.
    ///
    /// Returns the total number of tasks processed.
    pub async fn drain(&mut self) -> Result<usize, Error> {
        let mut total = 0;
        loop {
            let processed = self.tick().await?;
            total += processed;
            if processed == 0 {
                return Ok(total);
            }
        }
    }

    fn entry(task: &Task, command: Command) -> CommandEntry {
        CommandEntry::new(task.data_id, task.graph_id, command)
    }
}

mod mysql {
    //! [`Store`] implementation backed by the MySQL task backend.

    use event::{Emit as CommandEmit, Poll as TaskPoll, command::CommandEntry, task::Task};
    use graph::Graph;
    use graph::GraphStore;
    use runtime::{Error, backend::Backend};
    use uuid::Uuid;

    use super::{Store, StoreTx};

    impl<S> Store for Backend<S>
    where
        S: GraphStore<Uuid>,
        <S as GraphStore<Uuid>>::Err: Send + Sync + 'static,
        <S as GraphStore<Uuid>>::G: Graph<NodeId = Uuid>,
    {
        type Err = Error;
        type Tx<'a>
            = BackendTx<'a>
        where
            Self: 'a;

        async fn poll_tasks(&self, max: usize) -> Result<Vec<Task>, Self::Err> {
            let mut db = self.db().clone();
            let tasks: Vec<Task> = Task::poll(&mut db, max).await?.collect();
            Ok(tasks)
        }

        async fn transaction(&mut self) -> Result<Self::Tx<'_>, Self::Err> {
            Ok(BackendTx {
                tx: self.db_mut().transaction().await?,
            })
        }
    }

    /// A [`StoreTx`] backed by a MySQL transaction.
    ///
    /// The command append and the task ack of a scheduling step share one
    /// transaction, so committing advances both at once.
    pub struct BackendTx<'a> {
        tx: toasty::Transaction<'a>,
    }

    impl StoreTx for BackendTx<'_> {
        type Err = Error;

        async fn emit(&mut self, entry: &CommandEntry) -> Result<(), Self::Err> {
            entry.emit(&mut self.tx).await?;
            Ok(())
        }

        async fn ack(&mut self, task: &Task) -> Result<(), Self::Err> {
            task.ack(&mut self.tx).await?;
            Ok(())
        }

        async fn commit(self) -> Result<(), Self::Err> {
            self.tx.commit().await?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::convert::Infallible;
    use std::sync::Mutex;

    use super::*;

    #[derive(Default)]
    struct MemStore {
        pending: RefCell<VecDeque<Task>>,
        acked: RefCell<Vec<Uuid>>,
        emitted: RefCell<Vec<CommandEntry>>,
    }

    impl Store for MemStore {
        type Err = Infallible;
        type Tx<'a>
            = MemTx<'a>
        where
            Self: 'a;

        async fn poll_tasks(&self, max: usize) -> Result<Vec<Task>, Self::Err> {
            Ok(self.pending.borrow().iter().take(max).cloned().collect())
        }

        async fn transaction(&mut self) -> Result<Self::Tx<'_>, Self::Err> {
            Ok(MemTx {
                store: &*self,
                emitted: Vec::new(),
                acked: Vec::new(),
            })
        }
    }

    /// A [`StoreTx`] that records emitted commands and acked tasks in memory.
    struct MemTx<'a> {
        store: &'a MemStore,
        emitted: Vec<CommandEntry>,
        acked: Vec<Uuid>,
    }

    impl StoreTx for MemTx<'_> {
        type Err = Infallible;

        async fn emit(&mut self, entry: &CommandEntry) -> Result<(), Self::Err> {
            self.emitted.push(entry.clone());
            Ok(())
        }

        async fn ack(&mut self, task: &Task) -> Result<(), Self::Err> {
            self.acked.push(task.id);
            Ok(())
        }

        async fn commit(self) -> Result<(), Self::Err> {
            self.store
                .pending
                .borrow_mut()
                .retain(|task| !self.acked.contains(&task.id));
            self.store.emitted.borrow_mut().extend(self.emitted);
            self.store.acked.borrow_mut().extend(self.acked);
            Ok(())
        }
    }

    /// A [`TaskRunner`] that records a fixed event for every task it runs.
    struct FixedRunner {
        event: TaskEvent,
        events: Mutex<Vec<TaskEventEntry>>,
    }

    impl FixedRunner {
        fn new(event: TaskEvent) -> Self {
            Self {
                event,
                events: Mutex::new(Vec::new()),
            }
        }
    }

    impl TaskRunner for FixedRunner {
        type Err = Infallible;

        async fn run(&self, task: &Task) -> Result<(), Self::Err> {
            self.events
                .lock()
                .unwrap()
                .push(TaskEventEntry::new(task, self.event));
            Ok(())
        }
    }

    impl TaskEventPoll for FixedRunner {
        type Err = Infallible;

        async fn poll(&self, max: usize) -> Result<Vec<TaskEventEntry>, Self::Err> {
            let mut events = self.events.lock().unwrap();
            let mut taken = Vec::new();
            events.retain(|entry| {
                if taken.len() < max {
                    taken.push(entry.clone());
                    false
                } else {
                    true
                }
            });
            Ok(taken)
        }

        async fn ack(&self, _event: &TaskEventEntry) -> Result<(), Self::Err> {
            Ok(())
        }
    }

    fn task(node: u128) -> Task {
        Task {
            task_offset: 0,
            id: Uuid::new_v4(),
            data_id: Uuid::from_u128(0xda7a),
            graph_id: Uuid::from_u128(0x9a1),
            node_id: Uuid::from_u128(node),
            node_run_id: 1,
            created_at: jiff::Timestamp::now(),
        }
    }

    struct RetryRunner {
        attempts: std::sync::atomic::AtomicUsize,
        inner: FixedRunner,
    }

    impl TaskRunner for RetryRunner {
        type Err = std::io::Error;

        async fn run(&self, task: &Task) -> Result<(), Self::Err> {
            if self
                .attempts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                == 0
            {
                return Err(std::io::Error::other("temporary runner failure"));
            }
            self.inner.run(task).await.unwrap();
            Ok(())
        }
    }

    impl TaskEventPoll for RetryRunner {
        type Err = Infallible;

        async fn poll(&self, max: usize) -> Result<Vec<TaskEventEntry>, Self::Err> {
            self.inner.poll(max).await
        }

        async fn ack(&self, event: &TaskEventEntry) -> Result<(), Self::Err> {
            self.inner.ack(event).await
        }
    }

    #[tokio::test]
    async fn runner_failure_leaves_task_available_for_retry() {
        let mut scheduler = Scheduler::new(
            MemStore::default(),
            RetryRunner {
                attempts: std::sync::atomic::AtomicUsize::new(0),
                inner: FixedRunner::new(TaskEvent::Succeed),
            },
            16,
        );
        let task = task(1);
        scheduler
            .store_mut()
            .pending
            .borrow_mut()
            .push_back(task.clone());
        assert!(scheduler.tick().await.is_err());
        assert!(scheduler.store().acked.borrow().is_empty());
        assert!(scheduler.store().emitted.borrow().is_empty());
        assert_eq!(scheduler.store().pending.borrow().len(), 1);
        assert_eq!(scheduler.tick().await.unwrap(), 1);
        assert_eq!(&*scheduler.store().acked.borrow(), &[task.id]);
        assert_eq!(scheduler.store().emitted.borrow().len(), 2);
        assert_eq!(scheduler.tick().await.unwrap(), 0);
    }

    #[test]
    fn events_map_to_commands() {
        let node = Uuid::from_u128(7);
        assert_eq!(TaskEvent::Pend.command(node), Command::Pend { node });
        assert_eq!(TaskEvent::Succeed.command(node), Command::Succeed { node });
        assert_eq!(TaskEvent::Fail.command(node), Command::Fail { node });
        assert_eq!(
            TaskEvent::Terminate.command(node),
            Command::Terminate { node }
        );
        assert_eq!(TaskEvent::Cancel.command(node), Command::Cancel { node });
    }

    #[tokio::test]
    async fn schedules_ready_task_and_drives_by_event() {
        let mut scheduler = Scheduler::new(
            MemStore::default(),
            FixedRunner::new(TaskEvent::Succeed),
            16,
        );
        let task = task(1);
        scheduler
            .store_mut()
            .pending
            .borrow_mut()
            .push_back(task.clone());

        let processed = scheduler.tick().await.unwrap();
        assert_eq!(processed, 1);

        let store = scheduler.into_store();
        let emitted = store.emitted.borrow();
        assert_eq!(emitted.len(), 2);
        assert_eq!(emitted[0].command.0, Command::Run { node: task.node_id });
        assert_eq!(
            emitted[1].command.0,
            Command::Succeed { node: task.node_id }
        );
        assert_eq!(&*store.acked.borrow(), &[task.id]);
    }

    #[tokio::test]
    async fn pend_then_terminal_event_drives_pending_node() {
        let mut scheduler =
            Scheduler::new(MemStore::default(), FixedRunner::new(TaskEvent::Pend), 16);
        let task = task(2);
        scheduler
            .store_mut()
            .pending
            .borrow_mut()
            .push_back(task.clone());

        scheduler.tick().await.unwrap();
        scheduler.drive(&task, TaskEvent::Fail).await.unwrap();

        let store = scheduler.into_store();
        let emitted = store.emitted.borrow();
        let commands: Vec<_> = emitted
            .iter()
            .map(|entry| entry.command.0.clone())
            .collect();
        assert_eq!(
            commands,
            vec![
                Command::Run { node: task.node_id },
                Command::Pend { node: task.node_id },
                Command::Fail { node: task.node_id },
            ]
        );
    }

    #[tokio::test]
    async fn tick_respects_batch_and_drain_empties_queue() {
        let mut scheduler =
            Scheduler::new(MemStore::default(), FixedRunner::new(TaskEvent::Succeed), 2);
        for node in 0..5 {
            scheduler
                .store_mut()
                .pending
                .borrow_mut()
                .push_back(task(node));
        }

        assert_eq!(scheduler.tick().await.unwrap(), 2);
        assert_eq!(scheduler.drain().await.unwrap(), 3);

        let store = scheduler.into_store();
        assert_eq!(store.acked.borrow().len(), 5);
        assert_eq!(store.emitted.borrow().len(), 10);
    }
}
