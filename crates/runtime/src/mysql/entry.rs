//! [`CommandEntry`] event-log persistence for the MySQL backend.

use jiff::Timestamp;
use toasty::Executor;

use super::Error;
use super::model::{CommandEntryModel, CommandPollOffsetModel, unacked_filter};
use crate::{CommandEmit, CommandEntry, CommandPoll};

impl<B> CommandEmit<B> for CommandEntry
where
    B: Executor,
{
    type Err = Error;

    async fn emit(&self, backend: &mut B) -> Result<u64, Self::Err> {
        let c = CommandEntryModel::create()
            .uuid(self.id)
            .data_id(self.data_id)
            .graph_id(self.graph_id)
            .shard(self.shard)
            .command(self.command.clone())
            .created_at(self.created_at)
            .exec(&mut *backend)
            .await?;
        Ok(c.id)
    }
}

impl<B> CommandPoll<B> for CommandEntry
where
    B: Executor,
{
    type Err = Error;

    async fn poll(
        backend: &mut B,
        maxium: usize,
        shards: &[u32],
    ) -> Result<impl Iterator<Item = Self>, Self::Err> {
        let mut offset_query = CommandPollOffsetModel::all();
        if !shards.is_empty() {
            offset_query =
                offset_query.filter(CommandPollOffsetModel::fields().shard().in_list(shards));
        }
        let offsets: Vec<(u32, u64)> = offset_query
            .exec(&mut *backend)
            .await?
            .into_iter()
            .map(|row| (row.shard, row.offset))
            .collect();

        let fields = CommandEntryModel::fields();
        let mut query = CommandEntryModel::all();
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
            .map(CommandEntryModel::into_command_entry)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter())
    }

    async fn ack(&self, backend: &mut B) -> Result<(), Self::Err> {
        // Advance the shard's offset, but never move it backwards.
        let now = Timestamp::now();
        match CommandPollOffsetModel::filter_by_shard(self.shard)
            .first()
            .exec(&mut *backend)
            .await?
        {
            Some(row) if row.offset < self.command_offset => {
                CommandPollOffsetModel::filter_by_id(row.id)
                    .update()
                    .offset(self.command_offset)
                    .updated_at(now)
                    .exec(&mut *backend)
                    .await?;
            }
            None => {
                CommandPollOffsetModel::create()
                    .shard(self.shard)
                    .offset(self.command_offset)
                    .updated_at(now)
                    .exec(&mut *backend)
                    .await?;
            }
            _ => {}
        }
        Ok(())
    }
}
