//! [`TaskEventEntry`] event-log persistence for the MySQL backend.

use jiff::Timestamp;
use toasty::Executor;

use super::Error;
use super::model::{TaskEventModel, TaskEventPollOffsetModel, unacked_filter};
use crate::{TaskEventEntry, TaskEventWrite};

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
            .shard(self.shard)
            .event(self.event.to_string())
            .created_at(self.created_at)
            .exec(&mut *backend)
            .await?;
        Ok(())
    }
}

impl TaskEventEntry {
    /// Returns up to `max` task events that have not yet been acknowledged.
    pub async fn poll<B>(
        backend: &mut B,
        max: usize,
        shards: &[u32],
    ) -> Result<impl Iterator<Item = Self>, Error>
    where
        B: Executor,
    {
        let mut offset_query = TaskEventPollOffsetModel::all();
        if !shards.is_empty() {
            offset_query =
                offset_query.filter(TaskEventPollOffsetModel::fields().shard().in_list(shards));
        }
        let offsets: Vec<(u32, u64)> = offset_query
            .exec(&mut *backend)
            .await?
            .into_iter()
            .map(|row| (row.shard, row.offset))
            .collect();

        let fields = TaskEventModel::fields();
        let mut query = TaskEventModel::all();
        if !shards.is_empty() {
            query = query.filter(fields.shard().in_list(shards));
        }
        if let Some(filter) = unacked_filter(&offsets, fields.shard(), fields.id()) {
            query = query.filter(filter);
        }
        let rows = query
            .order_by(fields.id().asc())
            .limit(max)
            .exec(&mut *backend)
            .await?;

        Ok(rows
            .into_iter()
            .map(TaskEventModel::into_task_event_entry)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter())
    }

    /// Acknowledges this event by advancing its shard's recorded offset.
    pub async fn ack<B>(&self, backend: &mut B) -> Result<(), Error>
    where
        B: Executor,
    {
        // Advance the shard's offset, but never move it backwards.
        let now = Timestamp::now();
        match TaskEventPollOffsetModel::filter_by_shard(self.shard)
            .first()
            .exec(&mut *backend)
            .await?
        {
            Some(row) if row.offset < self.event_offset => {
                TaskEventPollOffsetModel::filter_by_id(row.id)
                    .update()
                    .offset(self.event_offset)
                    .updated_at(now)
                    .exec(&mut *backend)
                    .await?;
            }
            None => {
                TaskEventPollOffsetModel::create()
                    .shard(self.shard)
                    .offset(self.event_offset)
                    .updated_at(now)
                    .exec(&mut *backend)
                    .await?;
            }
            _ => {}
        }
        Ok(())
    }
}
