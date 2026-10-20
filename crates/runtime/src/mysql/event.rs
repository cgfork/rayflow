//! [`TaskEventEntry`] event-log persistence for the MySQL backend.

use jiff::Timestamp;
use toasty::Executor;

use super::Error;
use super::model::TaskEventModel;
use super::poll;
use crate::{TaskEventEntry, TaskEventWrite};

const LOG: &str = "task_event";

impl<B> TaskEventWrite<B> for TaskEventEntry
where
    B: Executor,
{
    type Err = Error;

    async fn write(&self, backend: &mut B) -> Result<(), Self::Err> {
        TaskEventModel::create()
            .uuid(self.id)
            .task_id(self.task_id)
            .data_id(self.data_id)
            .graph_id(self.graph_id)
            .node_id(self.node_id)
            .node_run_id(self.node_run_id)
            .event(self.event.to_string())
            .created_at(self.created_at)
            .exec(&mut *backend)
            .await?;
        Ok(())
    }
}

impl TaskEventEntry {
    /// Returns up to `max` task events from the first unacknowledged contiguous
    /// offset.
    pub async fn poll<B>(backend: &mut B, max: usize) -> Result<impl Iterator<Item = Self>, Error>
    where
        B: Executor,
    {
        let state = poll::load(&mut *backend, LOG).await?;
        let fields = TaskEventModel::fields();
        let rows = TaskEventModel::all()
            .filter(fields.id().gt(state.offset))
            .order_by(fields.id().asc())
            .limit(max)
            .exec(&mut *backend)
            .await?;

        let plan = poll::plan(rows, state, Timestamp::now(), |row| row.id);
        if plan.changed {
            poll::save(&mut *backend, LOG, plan.state).await?;
        }

        Ok(plan
            .rows
            .into_iter()
            .map(TaskEventModel::into_task_event_entry)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter())
    }

    /// Acknowledges this event by advancing the recorded event offset.
    pub async fn ack<B>(&self, backend: &mut B) -> Result<(), Error>
    where
        B: Executor,
    {
        poll::ack(&mut *backend, LOG, self.event_offset).await
    }
}
