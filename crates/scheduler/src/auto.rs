//! Automatic task runner.
//!
//! [`AutoRunner`] records a `Succeed` event for every task without executing any
//! work. It is useful for driving a run forward automatically.

use runtime::mysql::Error;
use runtime::{Task, TaskEvent, TaskEventEntry, TaskEventPoll, TaskEventWrite};
use toasty::Db;

use super::TaskRunner;

/// A [`TaskRunner`] and [`TaskEventPoll`] that records `Succeed` for every
/// task it runs.
pub struct AutoRunner {
    db: Db,
}

impl AutoRunner {
    /// Creates a runner that records task events in `db`.
    pub fn new(db: Db) -> Self {
        Self { db }
    }
}

impl TaskRunner for AutoRunner {
    type Err = Error;

    /// "Runs" `task` by recording a `Succeed` event; nothing is executed.
    async fn run(&self, task: &Task) -> Result<(), Self::Err> {
        let entry = TaskEventEntry::new(task, TaskEvent::Succeed);
        let mut db = self.db.clone();
        entry.write(&mut db).await
    }
}

impl TaskEventPoll for AutoRunner {
    type Err = Error;

    async fn poll(&self, max: usize, shards: &[u32]) -> Result<Vec<TaskEventEntry>, Self::Err> {
        let mut db = self.db.clone();
        Ok(TaskEventEntry::poll(&mut db, max, shards).await?.collect())
    }

    async fn ack(&self, event: &TaskEventEntry) -> Result<(), Self::Err> {
        let mut db = self.db.clone();
        event.ack(&mut db).await
    }
}
