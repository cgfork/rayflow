//! [`TaskEventEntry`] event-log persistence for the MySQL backend.

use jiff::Timestamp;
use toasty::Executor;

use super::Error;
use super::model::{TaskEventModel, TaskEventPollOffsetModel, unacked_filter};
use crate::{TaskEventEntry, TaskEventPoll, TaskEventWrite};

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

impl<B> TaskEventPoll<B> for TaskEventEntry
where
    B: Executor,
{
    type Err = Error;

    async fn poll(
        backend: &mut B,
        maxium: usize,
        shard: Option<u32>,
    ) -> Result<impl Iterator<Item = Self>, Self::Err> {
        let fields = TaskEventModel::fields();
        let rows = match shard {
            Some(shard) => {
                let offset = TaskEventPollOffsetModel::filter_by_shard(shard)
                    .first()
                    .exec(&mut *backend)
                    .await?
                    .map(|row| row.offset)
                    .unwrap_or(0);
                TaskEventModel::filter_by_shard(shard)
                    .filter(fields.id().gt(offset))
                    .order_by(fields.id().asc())
                    .limit(maxium)
                    .exec(&mut *backend)
                    .await?
            }
            None => {
                let offsets: Vec<(u32, u64)> = TaskEventPollOffsetModel::all()
                    .exec(&mut *backend)
                    .await?
                    .into_iter()
                    .map(|row| (row.shard, row.offset))
                    .collect();
                let mut query = TaskEventModel::all();
                if let Some(filter) = unacked_filter(&offsets, fields.shard(), fields.id()) {
                    query = query.filter(filter);
                }
                query
                    .order_by(fields.id().asc())
                    .limit(maxium)
                    .exec(&mut *backend)
                    .await?
            }
        };

        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            events.push(row.into_task_event_entry()?);
        }
        Ok(events.into_iter())
    }

    async fn ack(&self, backend: &mut B) -> Result<(), Self::Err> {
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
