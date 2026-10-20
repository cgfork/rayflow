# Repository Guidelines

Rayflow is a Rust workspace (edition 2024) implementing a DAG-based task runtime.
It splits into six core crates: `event` (backend-independent append-only log traits), `graph` (pure graph primitives), `coordinator`
(MySQL-backed leader election),
`runtime` (execution state machine with a MySQL-backed store behind the default
`mysql` feature),
`scheduler` (consumes tasks, drives node status by task events, and hosts the
MySQL-backed `Dispatcher`), and `executor` (job primitives that
execute task work). The `server` and `pilot` directories are binary crates built
on those libraries.

## Project Structure & Module Organization
- `crates/event/` — `Event`, `Emit`, `Read`, `Poll`, and continuous `Reactive` consumer contracts. The always-available `command`, `task`, and `task_event` modules provide MySQL log persistence without depending on `runtime`.
- `crates/graph/` — graph model. `src/lib.rs` defines the `Graph`/`Id`/`GraphStore` traits; `src/types.rs` holds node/edge types. No I/O here.
- `crates/coordinator/` — leader election. `mysql::MySqlLeaderElection` (behind the default `mysql` feature) implements `LeaderElection` and competes for a named, expiring lease in the `leader_election` table; pilots renew it before every consumer pass.
- `crates/runtime/` — command application and execution backend. `src/lib.rs` defines `CommandApply`; `src/backend.rs` holds `Status`; `src/command.rs` owns the `Run` and `NodeRun` models; scheduler owns `RunConfig` and `NodeConfig`. `src/backend.rs` also provides the MySQL backend; `command.rs` holds run state models and transitions. MySQL support is unconditional, as in `event`. All log types and polling come from `event`.
- `crates/scheduler/` — task scheduler. `src/lib.rs` defines the `TaskRunner`/`TaskEventPoll`/`Store` traits, and `Scheduler`, which consumes tasks (`Ready -> Running`) and drives status by applying task events. Task and event polls consume the first unacknowledged contiguous offset. `scheduled.rs` owns `ScheduledTask`, its persistence model and registration, along with run/node configuration models. MySQL persistence and `Dispatcher` are always available.
- `crates/executor/` — task executor. `src/lib.rs` defines `JobContext`, `Runnable`, `TaskState`, and `ErrorCode`; `src/script.rs` implements the script job. Keep these job primitives feature-agnostic.
- `pilot/` — leader-elected consumer binary. `src/pilot.rs` renews the leader lease and, while leader, continuously consumes commands, schedules tasks, consumes task events, and applies the resulting commands. `src/config.rs` layers `rayflow.yaml` and `RAYFLOW_PILOT_*` overrides.
- `server/` — axum HTTP API binary.
- `runtime.sql` — MySQL schema (`task`, `run`, `run_config`, `node_config`, `node_run`, `scheduled_task`, `command_entry`, `task_event`, `poll_offset`, and `leader_election`). Apply it before using the MySQL backend.
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
- Keep `graph` free of I/O; database logic belongs in the persistence modules. The scheduler's core stays backend-agnostic behind `Store`/`TaskRunner`; its MySQL-backed `Dispatcher` is always available.
- Gate coordinator MySQL code with its `mysql` feature. MySQL support in `event`, `runtime`, and `scheduler` is always available; keep core traits backend-agnostic.

## Testing Guidelines
- Use Rust's built-in harness via `#[cfg(test)]` modules or `crates/<crate>/tests/`.
- Add focused unit tests beside the code they cover and integration tests under `crates/<crate>/tests/`. MySQL integration tests skip unless `DATABASE_URL` is set.
- Name tests after the behavior, e.g. `dryrun_mocks_predecessors`; run with `cargo test -p runtime`.

## Commit & Pull Request Guidelines
- Commit messages use Conventional-Commit style, e.g. `feat(task): mem task`, `feat(trait): core api`. Terse `update` messages appear in history but are discouraged.
- Pull requests: describe the change, list affected crate(s), link related issues, and confirm `cargo fmt`, `cargo clippy`, and `cargo test` pass. Update `runtime.sql` alongside any storage changes.
