# Repository Guidelines

Rayflow is a Rust workspace (edition 2024) implementing a DAG-based task runtime.
It splits into five core crates: `event` (backend-independent append-only log traits),
`graph` (pure graph primitives), `coordinator` (MySQL-backed leader election),
`runtime` (execution state machine with a MySQL-backed store), and `scheduler`
(actuates graph state and abstracts scheduling destinations). The `server`, `pilot`, and `agent` directories are binary crates built
on those libraries.

## Project Structure & Module Organization
- `crates/event/` — `Event`, `Emit`, `Read`, `Poll`, and continuous `Reactive` consumer contracts. The always-available `command`, `task`, and `task_event` modules provide MySQL log persistence without depending on `runtime`.
- `crates/graph/` — graph model. `src/lib.rs` defines the `Graph`/`Id`/`GraphStore` traits; `src/types.rs` holds node/edge types. Primitives and validation have no I/O. `src/store.rs` provides the MySQL `GraphRecord` model and `MySqlGraphStore`; `graph::models()` registers the versioned `graph` table. `load(uuid, None)` selects the highest version; explicit versions are exact matches. Creation and update audit fields are persisted.
- `crates/coordinator/` — leader election. `mysql::MySqlLeaderElection` (behind the default `mysql` feature) implements `LeaderElection` and competes for a named, expiring lease in the `leader_election` table; pilots renew it before every consumer pass.
- `crates/runtime/` — command application and execution backend. `src/lib.rs` defines `CommandApply`; `src/backend.rs` holds `Status`; `src/command.rs` owns the `Run` and `NodeRun` models; scheduler owns `RunConfig` and `NodeConfig`. `src/backend.rs` also provides the MySQL backend; `command.rs` holds run state models and transitions. `Resume` transitions a Pending node to Ready and emits a new task for the same node run; the actuator moves it to Running when creating its job without emitting another task. `TaskEvent::Yield` maps to `Command::Pend` and ends the job as Yielded. MySQL support is unconditional, as in `event`. All log types and polling come from `event`.
- `crates/scheduler/` — `Actuator` (`actuator.rs`) consumes command/task/task-event logs, applies commands, and creates `Job` instances from logical `Task` records that drive the FSM. `Scheduler<B>` (`scheduler.rs`) abstracts job scheduling to agents or external schedulers such as Kubernetes Volcano. `actuator.rs` also owns job and configuration models plus `JobRead`/`JobWrite` persistence contracts. No worker dependency; MySQL support is unconditional.
- `crates/scheduler/src/agent.rs` — `AgentScheduler` fails Polled jobs with `expired_at < now - timeout_gap` and emits Fail events atomically. The default gap is 10 seconds. `ResourceCapacity` is the total agent budget; existing claims reserve resources. New claims and inactive Polled redeliveries reset `expired_at` from the pinned `NodeConfig.lease`. Active jobs are never renewed. Expired jobs are not redelivered or requeued. Terminal events clear ownership while preserving expiry. No registry or queue tables are used.
- `pilot/` — leader-elected consumer binary. `src/pilot.rs` renews the leader lease and, while leader, continuously consumes commands, schedules tasks, consumes task events, and applies the resulting commands. `src/config.rs` layers `rayflow.yaml` and `RAYFLOW_PILOT_*` overrides.
- `server/` — axum HTTP API binary. `/jobs/poll` accepts integer `wait_ms` from zero up to `poll_timeout`; omitted or null waits are randomized in `[poll_timeout / 2, poll_timeout)`. Server settings use `Duration`, deserialized from strings such as `10s` with Jiff. Configure `poll_timeout` (default `30s`, minimum `2ms`) via the config file or `RAYFLOW_POLL_TIMEOUT`. Explicit zero polls immediately. Idle polls retry every `poll_retry_interval` (default `250ms`, minimum `1ms`), configured via the config file or `RAYFLOW_POLL_RETRY_INTERVAL`, with a fresh `AgentRef`, outside completed `App::poll` transactions. Retry sleeps are capped by the request deadline. Never hold database transactions or locks while waiting. Nonempty `active_jobs` always returns after one attempt without extending active job lifetimes.
- `agent/` — execution library and consumer binary. `Agent` owns the execution provider and job workdir, not HTTP access. It prepares pinned job configurations and images, starts named containers with CPU/memory limits, and cleans stopped jobs. `client::Client` independently handles poll and result-report requests; inject it with `Agent` and `Settings` into `Agentic::with_agent`, or use `Agentic::new(settings)` for default Docker/HTTP connections. `Agentic` discovers jobs in WORKDIR on startup, reuses existing containers, retries persisted results, polls sequentially, and runs jobs concurrently. Active IDs prevent duplicate starts; expiry kills and removes the container and persists exit code 124 before retrying failure reports. Container waits and reconnecting provider event streams detect completion. Shutdown leaves isolated containers for recovery. `JobFs` uses async Tokio file access, atomic JSON writes, and exclusive OS locks with asynchronous contention waits. The binary loads `Settings` from `rayflow.yaml` (or `RAYFLOW_AGENT_CONFIG` / `RAYFLOW_CONFIG`) plus `RAYFLOW_AGENT_*` overrides, accepts duration strings, and requires a stable `id` UUID. Nested resource overrides use double underscores. Default capacity is host CPU count and total memory. Ctrl-C and SIGTERM stop orchestration gracefully.
- Pilot calls `Actuator::tick_in` and `Scheduler::schedule` inside one leader-protected transaction. The default `AgentScheduler` fails expired jobs after `job_timeout_gap` (default `10s`, configured via `RAYFLOW_PILOT_JOB_TIMEOUT_GAP`). `Pilot::with_scheduler` injects other destinations; external adapters must submit committed requests and reconcile retries after rollback.
- Missing log offsets use configurable `gap_timeout_ms` (10000 by default), wired through `Actuator::with_gap_timeout`. Direct event consumers use matching `event::poll::Config` for `poll_with_config` and `ack_with_config`; `Poll` trait methods retain defaults.
- `rayflow.sql` — MySQL schema (`graph`, `task`, `run`, `run_config`, `node_config`, `node_run`, `job`, `command_entry`, `task_event`, `poll_offset`, and `leader_election`). Apply it before using the MySQL backend.
- `Cargo.toml` — workspace manifest with shared dependency versions.

Tasks are created only when a node run is created or transitions to `Ready`.
`Start`, `Rerun`, `Dryrun`, `Resume`, and successor activation in `Succeed` add
tasks directly without calling `run`. `Run` only transitions `Ready -> Running`.
Actuation consumes the existing task and activates its node without another task.

## Build, Test, and Development Commands
- `cargo build` — build all crates.
- `cargo check -p runtime` — fast type-check of one crate.
- `cargo check --workspace` — type-check everything.
- `cargo test` — run the test suite.
- `cargo run -p pilot` — run the leader-elected consumer (requires MySQL).
- `cargo run -p agent` — run the container agent (requires a configured `id` UUID and Docker).
- `cargo fmt` — format with rustfmt.
- `cargo clippy` — lint the workspace.

## Coding Style & Naming Conventions
- 4-space indentation; run `cargo fmt` before committing.
- `snake_case` for functions/modules, `CamelCase` for types, `SCREAMING_SNAKE_CASE` for constants.
- Keep graph primitives and validation free of I/O; database logic belongs in persistence modules such as `graph::store`. Keep graph actuation in `Actuator`, resource matching and claims in `AgentScheduler`, and persistence models free of workflow logic.
- Prefer Toasty ORM operations for database access. Use raw SQL only when the ORM cannot express the required behavior, such as MySQL atomic upserts, explicit row locks, or database-clock expressions, and explain the exception in a comment.
- Gate coordinator MySQL code with its `mysql` feature. MySQL support in `event`, `runtime`, and `scheduler` is always available; keep core traits backend-agnostic.

## Testing Guidelines
- Use Rust's built-in harness via `#[cfg(test)]` modules or `crates/<crate>/tests/`.
- Add focused unit tests beside the code they cover and integration tests under `crates/<crate>/tests/`. MySQL integration tests skip unless `DATABASE_URL` is set.
- Name tests after the behavior, e.g. `dryrun_mocks_predecessors`; run with `cargo test -p runtime`.

## Commit & Pull Request Guidelines
- Commit messages use Conventional-Commit style, e.g. `feat(task): mem task`, `feat(trait): core api`. Terse `update` messages appear in history but are discouraged.
- Pull requests: describe the change, list affected crate(s), link related issues, and confirm `cargo fmt`, `cargo clippy`, and `cargo test` pass. Update `rayflow.sql` alongside any storage changes.
