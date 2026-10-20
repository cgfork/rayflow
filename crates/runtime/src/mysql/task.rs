//! [`Task`] persistence for the MySQL backend.

use jiff::Timestamp;
use toasty::Executor;
use uuid::Uuid;

use super::Error;
use super::model::TaskModel;
use super::poll;
use crate::{Task, TaskPoll, TaskRead, TaskWrite};

const LOG: &str = "task";

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
        maximum: usize,
    ) -> Result<impl Iterator<Item = Self>, Self::Err> {
        let state = poll::load(&mut *backend, LOG).await?;
        let fields = TaskModel::fields();
        let rows = TaskModel::all()
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
            .map(TaskModel::into_task)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter())
    }

    async fn ack(&self, backend: &mut B) -> Result<(), Self::Err> {
        poll::ack(&mut *backend, LOG, self.task_offset).await
    }
}
