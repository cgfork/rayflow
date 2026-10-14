# Repository Guidelines

Rayflow is a Rust workspace (edition 2024) implementing a DAG-based task runtime.
It splits into four crates: `dag` (pure graph primitives), `runtime` (execution
state machine with a MySQL-backed store behind the default `mysql` feature),
`scheduler` (consumes tasks and drives node status by task events), and `runner`
(a local `TaskRunner` that executes image and script jobs).

## Project Structure & Module Organization
- `crates/dag/` — graph model. `src/lib.rs` defines the `Graph`/`Id` traits; `src/types.rs` holds node/edge types. No I/O here.
- `crates/runtime/` — execution runtime. `src/lib.rs` holds core traits and types (`Command`, `Status`, `Task`, `TaskEvent`, `RunConfig`, `NodeConfig`, `GraphStore`, `shard_of`). `src/mysql/` implements them: `mod.rs` (Backend, Error), `command.rs` (node state transitions), `task.rs` (task persistence and polling), `entry.rs` (command log), `event.rs` (task event log).
- `crates/scheduler/` — task scheduler. `src/lib.rs` defines `TaskEvent`, the `TaskRunner`/`Store` traits, and `Scheduler`, which consumes tasks (`Ready -> Running`) and drives status by applying task events. A MySQL `Store` impl lives behind the default `mysql` feature.
- `crates/runner/` — task runner. `src/lib.rs` defines the `Job`/`Image`/`Script` job types; `src/local.rs` implements `LocalRunner` (a `TaskRunner` that runs jobs as local processes and persists their events to `task_event`). Gated behind the default `mysql` feature; keep the job types feature-agnostic.
- `runtime.sql` — MySQL schema (`task`, `run`, `run_config`, `node_config`, `node_run`, `command_entry`, `task_event`, and per-shard poll-offset tables). Apply it before using the MySQL backend.
- `Cargo.toml` — workspace manifest with shared dependency versions.

## Build, Test, and Development Commands
- `cargo build` — build all crates.
- `cargo check -p runtime` — fast type-check of one crate.
- `cargo check --workspace` — type-check everything.
- `cargo test` — run the test suite.
- `cargo fmt` — format with rustfmt.
- `cargo clippy` — lint the workspace.

## Coding Style & Naming Conventions
- 4-space indentation; run `cargo fmt` before committing.
- `snake_case` for functions/modules, `CamelCase` for types, `SCREAMING_SNAKE_CASE` for constants.
- Keep `dag` free of I/O; database logic belongs in `runtime::mysql`. The `scheduler` crate stays backend-agnostic behind `Store`/`TaskRunner`.
- Gate MySQL code with the `mysql` feature; keep core types feature-agnostic.

## Testing Guidelines
- Use Rust's built-in harness via `#[cfg(test)]` modules or `crates/<crate>/tests/`.
- No tests exist yet; add focused unit tests beside the code they cover and integration tests under `crates/<crate>/tests/`.
- Name tests after the behavior, e.g. `dryrun_mocks_predecessors`; run with `cargo test -p runtime`.

## Commit & Pull Request Guidelines
- Commit messages use Conventional-Commit style, e.g. `feat(task): mem task`, `feat(trait): core api`. Terse `update` messages appear in history but are discouraged.
- Pull requests: describe the change, list affected crate(s), link related issues, and confirm `cargo fmt`, `cargo clippy`, and `cargo test` pass. Update `runtime.sql` alongside any storage changes.
