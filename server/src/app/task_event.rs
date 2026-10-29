//! Task-event emission.

use event::Emit as TaskEventWrite;
use event::Read as TaskRead;
use event::task::Task;
use event::task_event::TaskEvent;
use event::task_event::TaskEventEntry;
use uuid::Uuid;

use crate::app::{App, AppError};

impl<S> App<S> {
    /// Emits `event` for the task identified by `task_id`.
    ///
    /// The task is resolved first so the event carries the task's run and node
    /// references without trusting the caller to supply them. Returns the
    /// recorded event.
    pub async fn emit_task_event(
        &self,
        task_id: Uuid,
        event: TaskEvent,
    ) -> Result<TaskEventEntry, AppError> {
        let mut db = self.db.clone();
        let task = Task::read(&mut db, task_id)
            .await
            .map_err(|error| match error {
                event::Error::TaskNotFound(task_id) => AppError::TaskNotFound(task_id),
                error => AppError::from(error),
            })?;
        let entry = TaskEventEntry::new(&task, event);
        entry.emit(&mut db).await?;
        Ok(entry)
    }
}

#[cfg(test)]
mod tests {
    use event::Emit as TaskWrite;
    use event::task_event::TaskEventEntry as TaskEventModel;

    use super::*;
    use crate::app::test::{TestGraphStore, setup};
    use crate::config::Config;

    fn config() -> Config {
        Config {
            addr: "0.0.0.0:0".parse().unwrap(),
            database_url: String::new(),
            ..Config::default()
        }
    }

    #[tokio::test]
    async fn emits_task_event_and_reports_missing_tasks() {
        let Some(mut db) = setup().await else {
            eprintln!("skipping: set DATABASE_URL to run the MySQL app test");
            return;
        };

        let task = Task {
            task_offset: 0,
            id: Uuid::new_v4(),
            data_id: Uuid::from_u128(0xda7a),
            graph_id: Uuid::from_u128(0x9a1),
            node_id: Uuid::from_u128(1),
            node_run_id: 7,
            created_at: jiff::Timestamp::now(),
        };
        task.emit(&mut db).await.expect("write task");

        let app = App::new(db.clone(), config(), TestGraphStore::default());
        let entry = app
            .emit_task_event(task.id, TaskEvent::Yield)
            .await
            .expect("emit task event");

        assert_eq!(entry.task_id, task.id);
        assert_eq!(entry.data_id, task.data_id);
        assert_eq!(entry.graph_id, task.graph_id);
        assert_eq!(entry.node_id, task.node_id);
        assert_eq!(entry.node_run_id, task.node_run_id);
        assert_eq!(entry.event, TaskEvent::Yield);
        let row = TaskEventModel::filter_by_id(entry.id)
            .first()
            .exec(&mut db)
            .await
            .expect("read task event")
            .expect("task event written");
        assert!(row.event_offset > 0);
        assert_eq!(row.event, TaskEvent::Yield);

        let error = app
            .emit_task_event(Uuid::new_v4(), TaskEvent::Succeed)
            .await
            .unwrap_err();
        assert!(matches!(error, AppError::TaskNotFound(_)));
    }
}
