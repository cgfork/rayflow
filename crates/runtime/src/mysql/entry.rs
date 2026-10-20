//! [`CommandEntry`] event-log persistence for the MySQL backend.

use jiff::Timestamp;
use toasty::Executor;

use super::Error;
use super::model::CommandEntryModel;
use super::poll;
use crate::{CommandEmit, CommandEntry, CommandPoll};

const LOG: &str = "command_entry";

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
        maximum: usize,
    ) -> Result<impl Iterator<Item = Self>, Self::Err> {
        let state = poll::load(&mut *backend, LOG).await?;
        let fields = CommandEntryModel::fields();
        let rows = CommandEntryModel::all()
            .filter(fields.id().gt(state.offset))
            .order_by(fields.id().asc())
            .limit(maximum)
            .exec(&mut *backend)
            .await?;

        let plan = poll::plan(rows, state, Timestamp::now(), |row| row.id);
        if plan.changed {
            poll::save(&mut *backend, LOG, plan.state).await?;
        }

        Ok(plan
            .rows
            .into_iter()
            .map(CommandEntryModel::into_command_entry)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter())
    }

    async fn ack(&self, backend: &mut B) -> Result<(), Self::Err> {
        poll::ack(&mut *backend, LOG, self.command_offset).await
    }
}
