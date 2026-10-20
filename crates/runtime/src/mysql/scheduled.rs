//! [`ScheduledTask`] persistence for the MySQL backend.

use toasty::Executor;
use uuid::Uuid;

use super::Error;
use super::model::ScheduledTaskModel;
use crate::{ScheduledTask, ScheduledTaskRead, ScheduledTaskWrite};

impl<B> ScheduledTaskWrite<B> for ScheduledTask
where
    B: Executor,
{
    type Err = Error;

    async fn write(&self, backend: &mut B) -> Result<(), Self::Err> {
        ScheduledTaskModel::create()
            .uuid(self.uuid)
            .task_id(self.task_id)
            .run_id(self.run_id)
            .node_run_id(self.node_run_id)
            .node_config_id(self.node_config_id)
            .run_config_id(self.run_config_id)
            .status(self.status.to_string())
            .created_at(self.created_at)
            .updated_at(self.updated_at)
            .exec(&mut *backend)
            .await?;
        Ok(())
    }
}

impl<B> ScheduledTaskRead<Uuid, B> for ScheduledTask
where
    B: Executor,
{
    type Err = Error;

    async fn read(backend: &mut B, task_id: Uuid) -> Result<Self, Self::Err> {
        ScheduledTaskModel::filter_by_task_id(task_id)
            .first()
            .exec(&mut *backend)
            .await?
            .ok_or(Error::ScheduledTaskNotFound(task_id))?
            .into_scheduled_task()
    }
}
