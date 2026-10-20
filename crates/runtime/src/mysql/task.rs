//! [`Task`] persistence for the MySQL backend.

use jiff::Timestamp;
use toasty::Executor;
use uuid::Uuid;

use super::Error;
use super::model::{TaskModel, TaskPollOffsetModel, unacked_filter};
use crate::{Task, TaskPoll, TaskRead, TaskWrite};

impl<B> TaskWrite<B> for Task
where
    B: Executor,
{
    type Err = Error;

    async fn write(&self, backend: &mut B) -> Result<(), Self::Err> {
        TaskModel::create()
            .uuid(self.id)
            .data_id(self.data_id)
            .graph_id(self.graph_id)
            .node_id(self.node_id)
            .node_run_id(self.node_run_id)
            .shard(self.shard)
            .created_at(self.created_at)
            .exec(&mut *backend)
            .await?;
        Ok(())
    }
}

impl<B> TaskRead<Uuid, B> for Task
where
    B: Executor,
{
    type Err = Error;

    async fn read(backend: &mut B, task_id: Uuid) -> Result<Self, Self::Err> {
        let row = TaskModel::filter_by_uuid(task_id)
            .first()
            .exec(&mut *backend)
            .await?;
        row.ok_or(Error::TaskNotFound(task_id))?.into_task()
    }
}

impl<B> TaskPoll<B> for Task
where
    B: Executor,
{
    type Err = Error;

    async fn poll(
        backend: &mut B,
        maxium: usize,
        shards: &[u32],
    ) -> Result<impl Iterator<Item = Self>, Self::Err> {
        let mut offset_query = TaskPollOffsetModel::all();
        if !shards.is_empty() {
            offset_query =
                offset_query.filter(TaskPollOffsetModel::fields().shard().in_list(shards));
        }
        let offsets: Vec<(u32, u64)> = offset_query
            .exec(&mut *backend)
            .await?
            .into_iter()
            .map(|row| (row.shard, row.offset))
            .collect();

        let fields = TaskModel::fields();
        let mut query = TaskModel::all();
        if !shards.is_empty() {
            query = query.filter(fields.shard().in_list(shards));
        }
        if let Some(filter) = unacked_filter(&offsets, fields.shard(), fields.id()) {
            query = query.filter(filter);
        }
        let rows = query
            .order_by(fields.id().asc())
            .limit(maxium)
            .exec(&mut *backend)
            .await?;

        Ok(rows
            .into_iter()
            .map(TaskModel::into_task)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter())
    }

    async fn ack(&self, backend: &mut B) -> Result<(), Self::Err> {
        // Advance the shard's offset, but never move it backwards.
        let now = Timestamp::now();
        match TaskPollOffsetModel::filter_by_shard(self.shard)
            .first()
            .exec(&mut *backend)
            .await?
        {
            Some(row) if row.offset < self.task_offset => {
                TaskPollOffsetModel::filter_by_id(row.id)
                    .update()
                    .offset(self.task_offset)
                    .updated_at(now)
                    .exec(&mut *backend)
                    .await?;
            }
            None => {
                TaskPollOffsetModel::create()
                    .shard(self.shard)
                    .offset(self.task_offset)
                    .updated_at(now)
                    .exec(&mut *backend)
                    .await?;
            }
            _ => {}
        }
        Ok(())
    }
}
