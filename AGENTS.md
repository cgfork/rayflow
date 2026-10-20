# Repository Guidelines

Rayflow is a Rust workspace (edition 2024) implementing a DAG-based task runtime.
It splits into five core crates: `dag` (pure graph primitives), `coordinator`
(MySQL-backed leader election),
`runtime` (execution state machine with a MySQL-backed store behind the default
`mysql` feature),
`scheduler` (consumes tasks, drives node status by task events, and hosts the
MySQL-backed `AutoRunner` and `Dispatcher`), and `executor` (job primitives that
execute task work). The `server` and `pilot` directories are binary crates built
on those libraries.

## Project Structure & Module Organization
- `crates/dag/` — graph model. `src/lib.rs` defines the `Graph`/`Id` traits; `src/types.rs` holds node/edge types. No I/O here.
- `crates/coordinator/` — leader election. `LeaderElection` competes for a named, expiring lease in the `leader_election` table; pilots renew it before every consumer pass.
- `crates/runtime/` — execution runtime. `src/lib.rs` holds core traits and types (`Command`, `Status`, `Task`, `TaskEvent`, `RunConfig`, `NodeConfig`, `GraphStore`). `src/mysql/` implements them: `mod.rs` (Backend, Error), `command.rs` (node state transitions), `task.rs` (task persistence and polling), `entry.rs` (command log), `event.rs` (task event log), and `poll.rs` (global contiguous offsets with a persisted 10-second gap timeout).
- `crates/runtime/src/mysql/lock.rs` — the `transition_lock` row lock used by `CommandApply` to serialize command application for a run across processes.
- `crates/scheduler/` — task scheduler. `src/lib.rs` defines `TaskEvent`, the `TaskRunner`/`Store` traits, and `Scheduler`, which consumes tasks (`Ready -> Running`) and drives status by applying task events. Task and event polls consume the first unacknowledged contiguous offset. A MySQL `Store` impl, `AutoRunner` (`src/auto.rs`), and `Dispatcher` (`src/dispatcher.rs`) live behind the default `mysql` feature.
- `crates/executor/` — task executor. `src/lib.rs` defines `JobContext`, `Runnable`, `TaskState`, and `ErrorCode`; `src/script.rs` implements the script job. Keep these job primitives feature-agnostic.
- `pilot/` — leader-elected consumer binary. `src/pilot.rs` renews the leader lease and, while leader, continuously consumes commands, schedules tasks, consumes task events, and applies the resulting commands. `src/config.rs` layers `rayflow.yaml` and `RAYFLOW_PILOT_*` overrides.
- `server/` — axum HTTP API binary.
- `runtime.sql` — MySQL schema (`task`, `run`, `run_config`, `node_config`, `node_run`, `transition_lock`, `scheduled_task`, `command_entry`, `task_event`, `poll_offset`, and `leader_election`). Apply it before using the MySQL backend.
- `Cargo.toml` — workspace manifest with shared dependency versions.

## Build, Test, and Development Commands
- `cargo build` — build all crates.
- `cargo check -p runtime` — fast type-check of one crate.
- `cargo check --workspace` — type-check everything.
- `cargo test` — run the test suite.
- `cargo run -p pilot` — run the leader-elected consumer (requires MySQL).
- `cargo fmt` — format with rustfmt.
- `cargo clippy` — lint the workspace.

## Coding Style & Naming Conventions
- 4-space indentation; run `cargo fmt` before committing.
- `snake_case` for functions/modules, `CamelCase` for types, `SCREAMING_SNAKE_CASE` for constants.
- Keep `dag` free of I/O; database logic belongs in `runtime::mysql` or `coordinator`. The scheduler's core stays backend-agnostic behind `Store`/`TaskRunner`; the MySQL-backed `AutoRunner`/`Dispatcher` stay behind the `mysql` feature.
- Gate MySQL code with the `mysql` feature; keep core types feature-agnostic.

## Testing Guidelines
- Use Rust's built-in harness via `#[cfg(test)]` modules or `crates/<crate>/tests/`.
- Add focused unit tests beside the code they cover and integration tests under `crates/<crate>/tests/`. MySQL integration tests skip unless `DATABASE_URL` is set.
- Name tests after the behavior, e.g. `dryrun_mocks_predecessors`; run with `cargo test -p runtime`.

## Commit & Pull Request Guidelines
- Commit messages use Conventional-Commit style, e.g. `feat(task): mem task`, `feat(trait): core api`. Terse `update` messages appear in history but are discouraged.
- Pull requests: describe the change, list affected crate(s), link related issues, and confirm `cargo fmt`, `cargo clippy`, and `cargo test` pass. Update `runtime.sql` alongside any storage changes.
