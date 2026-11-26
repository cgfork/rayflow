use std::collections::HashMap;

use event::Emit as CommandEmit;
use event::command::Command;
use event::command::CommandEntry;
use jiff::Timestamp;
use runtime::command::RunConfig;
use toasty::Json;
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
        self.emit(Command::Start {
            nodes,
            run_config_id: None,
        })
        .await
    }

    /// Saves the annotations and enqueues Start with their config UUID in one transaction.
    pub async fn start_with_annotations(
        &mut self,
        nodes: Vec<Uuid>,
        annotations: HashMap<String, String>,
        actor: &str,
    ) -> Result<CommandEntry, AppError> {
        let mut tx = self.db.transaction().await?;
        let now = Timestamp::now();
        let config = RunConfig::create()
            .uuid(Uuid::new_v4())
            .annotation(Json(annotations))
            .created_at(now)
            .updated_at(now)
            .created_by(actor)
            .updated_by(actor)
            .exec(&mut tx)
            .await?;
        let mut entry = CommandEntry::new(
            self.data_id,
            self.graph_id,
            Command::Start {
                nodes,
                run_config_id: Some(config.uuid),
            },
        );
        entry.command_offset = entry.emit(&mut tx).await?;
        tx.commit().await?;
        Ok(entry)
    }

    /// Starts the run and waits until the command has been consumed.
    ///
    /// Waits on the emitted command's offset via [`wait_offset`](Self::wait_offset),
    /// so it resolves once a consumer advances past the command.
    pub async fn start_wait(&mut self, nodes: Vec<Uuid>) -> Result<(), AppError> {
        let entry = self.start(nodes).await?;
        self.wait_offset(entry.command_offset).await;
        Ok(())
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

    pub async fn upgrade(&mut self, version: u64, node: Uuid) -> Result<CommandEntry, AppError> {
        self.emit(Command::Upgrade { version, node }).await
    }

    pub async fn upgrade_wait(&mut self, version: u64, node: Uuid) -> Result<(), AppError> {
        self.emit_and_wait(Command::Upgrade { version, node }).await
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
