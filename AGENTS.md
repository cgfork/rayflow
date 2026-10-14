# Repository Guidelines

Rayflow is a Rust workspace (edition 2024) implementing a DAG-based task runtime.
It splits into five crates: `dag` (pure graph primitives), `shardtable`
(membership, consistent hash ring, precomputed shard table, and coordinator),
`runtime` (execution state machine with a MySQL-backed store behind the default
`mysql` feature),
`scheduler` (consumes tasks, drives node status by task events, and hosts the
MySQL-backed `AutoRunner` and `Dispatcher`), and `executor` (job primitives that
execute task work).

## Project Structure & Module Organization
- `crates/dag/` — graph model. `src/lib.rs` defines the `Graph`/`Id` traits; `src/types.rs` holds node/edge types. No I/O here.
- `crates/shardtable/` — consistent hash table. `lib.rs` defines the notifying `Membership` trait, `table.rs` implements a `HashTable` that stores only the precomputed shard-to-member mapping (with an internal SipHash virtual-node ring) and direct indexed lookup, and `coordinator.rs` provides a cloneable concurrent coordinator that watches membership, publishes immutable table snapshots, rebuilds on changes, and reports shard migrations. Keep it I/O-free.
- `crates/runtime/` — execution runtime. `src/lib.rs` holds core traits and types (`Command`, `Status`, `Task`, `TaskEvent`, `RunConfig`, `NodeConfig`, `GraphStore`, `shard_of`). `src/mysql/` implements them: `mod.rs` (Backend, Error), `command.rs` (node state transitions), `task.rs` (task persistence and polling), `entry.rs` (command log), `event.rs` (task event log).
- `crates/runtime/src/mysql/lock.rs` — the `transition_lock` row lock used by `CommandApply` to serialize command application for a run across processes.
- `crates/scheduler/` — task scheduler. `src/lib.rs` defines `TaskEvent`, the `TaskRunner`/`Store` traits, and `Scheduler`, which consumes tasks (`Ready -> Running`) and drives status by applying task events. Schedulers poll either one shard (`with_shard`) or an owned shard set (`with_shards`/`set_shards`) taken from the shard table. A MySQL `Store` impl, `AutoRunner` (`src/auto.rs`), and `Dispatcher` (`src/dispatcher.rs`) live behind the default `mysql` feature.
- `crates/executor/` — task executor. `src/lib.rs` defines `JobContext`, `Runnable`, `TaskState`, and `ErrorCode`; `src/script.rs` implements the script job. Keep these job primitives feature-agnostic.
- `runtime.sql` — MySQL schema (`task`, `run`, `run_config`, `node_config`, `node_run`, `transition_lock`, `scheduled_task`, `command_entry`, `task_event`, and per-shard poll-offset tables). Apply it before using the MySQL backend.
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
- Keep `dag` and `shardtable` free of I/O; database logic belongs in `runtime::mysql`. The scheduler's core stays backend-agnostic behind `Store`/`TaskRunner`; the MySQL-backed `AutoRunner`/`Dispatcher` stay behind the `mysql` feature.
- Gate MySQL code with the `mysql` feature; keep core types feature-agnostic.

## Testing Guidelines
- Use Rust's built-in harness via `#[cfg(test)]` modules or `crates/<crate>/tests/`.
- No tests exist yet; add focused unit tests beside the code they cover and integration tests under `crates/<crate>/tests/`.
- Name tests after the behavior, e.g. `dryrun_mocks_predecessors`; run with `cargo test -p runtime`.

## Commit & Pull Request Guidelines
- Commit messages use Conventional-Commit style, e.g. `feat(task): mem task`, `feat(trait): core api`. Terse `update` messages appear in history but are discouraged.
- Pull requests: describe the change, list affected crate(s), link related issues, and confirm `cargo fmt`, `cargo clippy`, and `cargo test` pass. Update `runtime.sql` alongside any storage changes.
