# Repository Guidelines

Rayflow is a Rust workspace (edition 2024) implementing a DAG-based task runtime.
It splits into six core crates: `event` (backend-independent append-only log traits), `graph` (pure graph primitives), `coordinator`
(MySQL-backed leader election),
`runtime` (execution state machine with a MySQL-backed store),
`scheduler` (actuates graph state and abstracts scheduling destinations),
and `executor` (job primitives that
execute task work). The `server`, `pilot`, and `agent` directories are binary crates built
on those libraries.

## Project Structure & Module Organization
- `crates/event/` — `Event`, `Emit`, `Read`, `Poll`, and continuous `Reactive` consumer contracts. The always-available `command`, `task`, and `task_event` modules provide MySQL log persistence without depending on `runtime`.
- `crates/graph/` — graph model. `src/lib.rs` defines the `Graph`/`Id`/`GraphStore` traits; `src/types.rs` holds node/edge types. No I/O here.
- `crates/coordinator/` — leader election. `mysql::MySqlLeaderElection` (behind the default `mysql` feature) implements `LeaderElection` and competes for a named, expiring lease in the `leader_election` table; pilots renew it before every consumer pass.
- `crates/runtime/` — command application and execution backend. `src/lib.rs` defines `CommandApply`; `src/backend.rs` holds `Status`; `src/command.rs` owns the `Run` and `NodeRun` models; scheduler owns `RunConfig` and `NodeConfig`. `src/backend.rs` also provides the MySQL backend; `command.rs` holds run state models and transitions. `Resume` transitions a Pending node to Ready and emits a new task for the same node run; the actuator moves it to Running when creating its job without emitting another task. `TaskEvent::Yield` maps to `Command::Pend` and ends the job as Yielded. MySQL support is unconditional, as in `event`. All log types and polling come from `event`.
- `crates/scheduler/` — `Actuator` (`actuator.rs`) consumes command/task/task-event logs, applies commands, and creates `Job` instances from logical `Task` records that drive the FSM. `Scheduler<B>` (`scheduler.rs`) abstracts job scheduling to agents or external schedulers such as Kubernetes Volcano. `actuator.rs` also owns job and configuration models plus `JobRead`/`JobWrite` persistence contracts. No worker dependency; MySQL support is unconditional.
- `crates/scheduler/src/agent.rs` — `AgentScheduler` implements direct resource-aware job polling with guarded ORM claims and lease expiry. `ResourceCapacity` reports the agent CPU/memory budget including existing claims; jobs copy flattened requirements from `NodeConfig`. `renew_claims` renews only explicitly listed active Polled jobs. Polls reserve existing claims' resources and claim fitting jobs. Unstarted claims renew when redelivered; expired Polled claims recover; Yielded jobs are terminal and release their claims. Terminal events clear claims. No registry or queue tables are used.
- `crates/executor/` — task executor. `src/lib.rs` defines `JobContext { job: Job, agent: Agent }`, `Agent` (UUID and polling resource capacity), `Runnable`, `TaskState`, and `ErrorCode`; `src/script.rs` implements the script job. Keep these job primitives feature-agnostic.
- `pilot/` — leader-elected consumer binary. `src/pilot.rs` renews the leader lease and, while leader, continuously consumes commands, schedules tasks, consumes task events, and applies the resulting commands. `src/config.rs` layers `rayflow.yaml` and `RAYFLOW_PILOT_*` overrides.
- `server/` — axum HTTP API binary.
- `agent/` — samples live CPU/memory with sysinfo, polls `POST /jobs/poll` via reqwest with resource capacity and active job UUIDs, and executes pinned script configs concurrently. Poll responses contain `Job` records directly in `jobs`, with loaded `run`, `node_run`, and `node_config` relations, plus confirmed `renewed_jobs`; only confirmed renewals extend local execution deadlines. Terminal actions use `POST /job/yield`, `/job/succeed`, `/job/fail`, or `/job/terminate` with `job_id` and `agent_id`. Server updates the job and appends its task event atomically. Retried actions append no duplicate events. Settings use `rayflow.yaml` and `RAYFLOW_AGENT_*` overrides; CPU/memory settings are optional caps. Each process needs a unique UUID and polls sequentially. Shutdown cancels interpreters and attempts termination within `shutdown_timeout_ms`.
- Pilot calls `Actuator::tick_in` and `Scheduler::schedule` inside one leader-protected transaction. The default `AgentScheduler` reclaims expired agent claims after `job_lease_ms` (30000 by default). Match that timeout on the server. `Pilot::with_scheduler` injects other destinations; external adapters must submit committed requests and reconcile retries after rollback.
- Missing log offsets use configurable `gap_timeout_ms` (10000 by default), wired through `Actuator::with_gap_timeout`. Direct event consumers use matching `event::poll::Config` for `poll_with_config` and `ack_with_config`; `Poll` trait methods retain defaults. Agent HTTP deadlines use `request_timeout_ms`.
- `rayflow.sql` — MySQL schema (`task`, `run`, `run_config`, `node_config`, `node_run`, `job`, `command_entry`, `task_event`, `poll_offset`, and `leader_election`). Apply it before using the MySQL backend.
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
- `cargo run -p agent` — poll and claim jobs from the HTTP API.
- `cargo fmt` — format with rustfmt.
- `cargo clippy` — lint the workspace.

## Coding Style & Naming Conventions
- 4-space indentation; run `cargo fmt` before committing.
- `snake_case` for functions/modules, `CamelCase` for types, `SCREAMING_SNAKE_CASE` for constants.
- Keep `graph` free of I/O; database logic belongs in the persistence modules. Keep graph actuation in `Actuator`, resource matching and claims in `AgentScheduler`, and persistence models free of workflow logic.
- Prefer Toasty ORM operations for database access. Use raw SQL only when the ORM cannot express the required behavior, such as MySQL atomic upserts, explicit row locks, or database-clock expressions, and explain the exception in a comment.
- Gate coordinator MySQL code with its `mysql` feature. MySQL support in `event`, `runtime`, and `scheduler` is always available; keep core traits backend-agnostic.

## Testing Guidelines
- Use Rust's built-in harness via `#[cfg(test)]` modules or `crates/<crate>/tests/`.
- Add focused unit tests beside the code they cover and integration tests under `crates/<crate>/tests/`. MySQL integration tests skip unless `DATABASE_URL` is set.
- Name tests after the behavior, e.g. `dryrun_mocks_predecessors`; run with `cargo test -p runtime`.

## Commit & Pull Request Guidelines
- Commit messages use Conventional-Commit style, e.g. `feat(task): mem task`, `feat(trait): core api`. Terse `update` messages appear in history but are discouraged.
- Pull requests: describe the change, list affected crate(s), link related issues, and confirm `cargo fmt`, `cargo clippy`, and `cargo test` pass. Update `rayflow.sql` alongside any storage changes.
