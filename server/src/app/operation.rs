use event::Emit as CommandEmit;
use event::command::Command;
use event::command::CommandEntry;
use uuid::Uuid;

use crate::app::{AppError, RunApp};

impl<S> RunApp<S> {
    /// Waits until the command log reaches `offset`.
    ///
    /// The wait is served from the app's shared command offset, so it resolves as
    /// soon as a consumer publishes the advance — no database polling, and it may
    /// block for a long time.
    pub async fn wait_offset(&self, offset: u64) {
        self.offsets.wait(offset).await;
    }

    /// Starts the run, beginning at `nodes` (empty starts the graph's roots).
    ///
    /// Returns the emitted command entry.
    pub async fn start(&mut self, nodes: Vec<Uuid>) -> Result<CommandEntry, AppError> {
        self.emit(Command::Start { nodes }).await
    }

    /// Starts the run and waits until the command has been consumed.
    ///
    /// Waits on the emitted command's offset via [`wait_offset`](Self::wait_offset),
    /// so it resolves once a consumer advances past the command.
    pub async fn start_wait(&mut self, nodes: Vec<Uuid>) -> Result<(), AppError> {
        self.emit_and_wait(Command::Start { nodes }).await
    }

    /// Reruns `node`, resetting it to `Ready` at a new revision.
    ///
    /// Returns the emitted command entry.
    pub async fn rerun(&mut self, node: Uuid) -> Result<CommandEntry, AppError> {
        self.emit(Command::Rerun { node }).await
    }

    /// Reruns `node` and waits until the command has been consumed.
    ///
    /// Waits on the emitted command's offset via [`wait_offset`](Self::wait_offset),
    /// so it resolves once a consumer advances past the command.
    pub async fn rerun_wait(&mut self, node: Uuid) -> Result<(), AppError> {
        self.emit_and_wait(Command::Rerun { node }).await
    }

    pub async fn resume(&mut self, node: Uuid) -> Result<CommandEntry, AppError> {
        self.emit(Command::Resume { node }).await
    }

    pub async fn resume_wait(&mut self, node: Uuid) -> Result<(), AppError> {
        self.emit_and_wait(Command::Resume { node }).await
    }

    /// Dry-runs `nodes`, mock-succeeding their predecessors (empty starts the roots).
    ///
    /// Returns the emitted command entry.
    pub async fn dryrun(&mut self, nodes: Vec<Uuid>) -> Result<CommandEntry, AppError> {
        self.emit(Command::Dryrun { nodes }).await
    }

    /// Dry-runs `nodes` and waits until the command has been consumed.
    ///
    /// Waits on the emitted command's offset via [`wait_offset`](Self::wait_offset),
    /// so it resolves once a consumer advances past the command.
    pub async fn dryrun_wait(&mut self, nodes: Vec<Uuid>) -> Result<(), AppError> {
        self.emit_and_wait(Command::Dryrun { nodes }).await
    }

    /// Appends `command` to the command log and waits until the command has been
    /// consumed.
    async fn emit_and_wait(&mut self, command: Command) -> Result<(), AppError> {
        let entry = self.emit(command).await?;
        self.wait_offset(entry.command_offset).await;
        Ok(())
    }

    /// Appends `command` to the command log and returns the recorded entry (with
    /// the offset the log assigned it).
    async fn emit(&mut self, command: Command) -> Result<CommandEntry, AppError> {
        let mut entry = CommandEntry::new(self.data_id, self.graph_id, command);
        let offset = entry.emit(&mut self.db).await?;
        entry.command_offset = offset;
        Ok(entry)
    }
}

#[cfg(test)]
mod tests {
    use event::command::CommandEntry as CommandEntryModel;
    use toasty::Db;

    use super::*;
    use crate::app::App;
    use crate::app::test::{TestGraphStore, setup};
    use crate::config::Config;

    fn app(db: Db) -> App<TestGraphStore> {
        let config = Config {
            addr: "0.0.0.0:0".parse().unwrap(),
            database_url: String::new(),
        };
        App::new(db, config, TestGraphStore::default())
    }

    #[tokio::test]
    async fn start_rerun_resume_and_dryrun_emit_commands() {
        use axum::{
            body::Body,
            http::{Request, StatusCode},
        };
        use tower::ServiceExt;

        let Some(mut db) = setup().await else {
            eprintln!("skipping: set DATABASE_URL to run the MySQL app test");
            return;
        };

        let data_id = Uuid::from_u128(0xda7a);
        let graph_id = Uuid::from_u128(0x9a1);
        let node = Uuid::from_u128(1);
        let app = app(db.clone());
        let mut run = app.run_with(data_id, graph_id);

        run.start(vec![]).await.expect("start");
        run.rerun(node).await.expect("rerun");
        let response = crate::api::router(app.clone())
            .oneshot(
                Request::post("/runs/resume")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "data_id": data_id, "graph_id": graph_id, "node": node,
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        run.dryrun(vec![node]).await.expect("dryrun");

        let mut entries: Vec<_> = CommandEntryModel::all()
            .exec(&mut db)
            .await
            .expect("read command log")
            .into_iter()
            .collect();
        entries.sort_by_key(|entry| entry.command_offset);

        let commands: Vec<Command> = entries
            .iter()
            .map(|entry| entry.command.0.clone())
            .collect();
        assert_eq!(
            commands,
            vec![
                Command::Start { nodes: vec![] },
                Command::Rerun { node },
                Command::Resume { node },
                Command::Dryrun { nodes: vec![node] },
            ]
        );
        for entry in &entries {
            assert_eq!(entry.data_id, data_id);
            assert_eq!(entry.graph_id, graph_id);
        }
    }

    #[tokio::test]
    async fn start_wait_resolves_after_offset_advances() {
        let Some(mut db) = setup().await else {
            eprintln!("skipping: set DATABASE_URL to run the MySQL app test");
            return;
        };

        let data_id = Uuid::from_u128(0xda7a);
        let graph_id = Uuid::from_u128(0x9a1);
        let app = app(db.clone());
        let offsets = app.offsets().clone();
        let mut run = app.run_with(data_id, graph_id);

        let waiter = tokio::spawn(async move { run.start_wait(vec![]).await });

        // Read back the offset the command was assigned, then publish it.
        let offset = loop {
            let rows = CommandEntryModel::all()
                .exec(&mut db)
                .await
                .expect("read command log");
            if let Some(row) = rows.into_iter().next() {
                break row.command_offset;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        };

        assert!(
            !waiter.is_finished(),
            "start_wait should block until the command is consumed"
        );
        offsets.set(offset);

        tokio::time::timeout(std::time::Duration::from_secs(5), waiter)
            .await
            .expect("start_wait should resolve once the offset is published")
            .expect("waiter task")
            .expect("start_wait");
    }
}
