# Rayflow

Rayflow is a Rust workspace (edition 2024) implementing a DAG-based task runtime.
A run walks the nodes of a graph through a small status state machine, persisting
every transition so the work can be driven forward, deferred, and resumed across
processes.

The workspace splits into six core modules plus the `server` and `pilot`
binaries:

- **`event`** — backend-independent `Event`, `Emit`, `Read`, and `Poll` contracts.
- **`graph`** — pure graph primitives. No I/O.
- **`coordinator`** — MySQL-backed leader election for the active consumer.
- **`runtime`** — command application and execution state, with an always-available MySQL backend.
- **`scheduler`** — the task scheduler: consumes tasks and drives node status by
  task events, with the MySQL-backed `Dispatcher`.
- **`executor`** — job primitives for executing task work, including the built-in
  script job.
- **`server`** — an HTTP API (axum) that serves the runtime.
- **`pilot`** — a continuously running, leader-elected command, task, and
  task-event consumer.

```text
                 +---------+      +----------------+
   graph model   |  graph  |<-----|    runtime     |
                 +---------+      |  Command/Status |
                                  |  Task read/write|
                                  +--------+--------+
                                           ^
                                           |
                                  +---+--------+---+
                                      ^        ^
                                      |        |
                         +------------+--+  +--+-----------+
                         |    scheduler    |  |   executor   |
                         | consume + drive |  | job + script |
                         | auto + dispatch |  +--------------+
                         +-----------------+

                                  +-----------------+
                                  |   coordinator   |
                                  | leader election |
                                  +-----------------+
```

## Core Modules

### `graph` — graph model

Pure, I/O-free graph primitives.

- `Id` — identifier trait (`Copy + PartialEq + Debug + Display`), implemented for
  `Uuid`.
- `Graph` — the graph trait: `graph_id`/`version`, node and edge iteration
  (`nodes`, `nodes_mut`, `edges`), mutation (`add_node`, `delete_node`,
  `add_edge`, `delete_edge`), and derived queries (`root_nodes`,
  `predecessors`, `successors`, `incoming_edges`, `outgoing_edges`, `in_degree`,
  `out_degree`).
- `types` — the concrete `DAG` type plus `Node`, `Input`, `Output`, and `Kind`
  (`File`/`Directory`). A node carries a name, type, inputs, and outputs.

### `coordinator` — leader election

`LeaderElection` abstracts lease ownership; `MySqlLeaderElection` grants one pilot at a time the right to consume the global
logs. Each role has one row in `leader_election`; acquisition and renewal use a
single atomic MySQL upsert, so standbys observe the lease holder rather than
racing it.

- `coordinator::mysql::MySqlLeaderElection::new(db, name, node_id, lease)` creates a handle for a named
  coordinator role.
- `try_acquire_or_renew` takes an expired lease or renews the current holder's
  lease and returns whether this node is the leader.
- `is_leader` checks that this node still owns an unexpired lease, and `resign`
  releases it during shutdown.
The MySQL implementation, models, and error type live in `coordinator::mysql`,
behind the default-enabled `mysql` feature. Disable default features to use only
the backend-independent election traits.

- `LeaderElectionModel` is registered with the runtime models so the same
  `toasty::Db` can host both execution state and coordination state.

### `runtime` — execution runtime

Runtime owns `CommandApply` and the execution backend. Command entries, tasks,
and task events come from `event`; `GraphStore` comes from `graph`.
Like `event`, runtime uses flat modules, a shared `Error` in `lib.rs`, and
unconditional MySQL support.

- `CommandApply` applies an `event::command::CommandEntry` to a backend.
- `backend::Status` describes execution state. `command::{Run, NodeRun}` own
  command state models; scheduler owns run and node configuration models.
- `backend::Backend::apply_in` applies a command in a caller-owned transaction so
  its effects can commit with the event-log acknowledgement.
- Command consumers must serialize application, as the pilot does through its
  leader lease. Transitions append new tasks through `event::Emit`.
- `runtime::models()` registers backend state, the canonical `event` models, and
  leader election. Dispatch and configuration storage are owned by `scheduler`; applications using
  `Dispatcher` should register `scheduler::scheduled::models()` instead.

`event::poll` owns the shared contiguous offsets and persisted 10-second gap
timeout. `runtime.sql` remains the deployment schema for all crates; this
ownership split does not change its table layouts.

`Command` variants and the transition each applies:

| Command | Effect |
| --- | --- |
| `Start { nodes }` | Create the run and its initial `Ready` node runs (empty `nodes` = graph roots); enqueue a task per node. One-time per `(data_id, graph_id)`. |
| `Rerun { node }` | Advance the run revision and reset `node` to `Ready`; enqueue a task. |
| `Dryrun { nodes }` | Mark each target's predecessors `Succeeded` and the target `Ready`; enqueue a task. |
| `Run { node }` | `Ready -> Running`. |
| `Pend { node }` | `Ready`/`Running -> Pending`. |
| `Succeed { node }` | `Pending`/`Running -> Succeeded`; mark each runnable successor `Ready` and enqueue its task. |
| `Fail { node }` | `Pending`/`Running -> Failed`. |
| `Cancel { node }` | `Pending`/`Running -> Terminated`. |
| `Terminate { node }` | `Pending`/`Running -> Terminated`. |

### `scheduler` — task scheduler

Consumes tasks and drives node status by task events. It is backend-agnostic and
depends only on three traits:

- `Store` — `poll_tasks` (consume the first unacknowledged contiguous tasks) and
  `transaction`, which opens a `StoreTx` the scheduler drives commands and task
  acks through. Atomicity lives in the store: the MySQL
  `runtime::backend::Backend` opens a real SQL transaction, so a command append and
  its task ack commit together.
- `TaskRunner` — runs a scheduled task (`run`, which only executes the work).
- `TaskEventPoll` — supplies the events task runs produced (`poll`) and
  acknowledges them (`ack`) once
  driven. Running and consuming are separate: `run` records the event, `poll`
  returns it, and `ack` consumes it.

- `TaskEvent` — `Pend`, `Succeed`, `Fail`, `Terminate`, `Cancel`; each maps to the
  matching `Command` via `TaskEvent::command(node)`. Defined in `event::task_event` and
  re-exported here.
- `TaskEventEntry` — the recorded event (its `task_id`/`node_id` plus the
  `TaskEvent`), so a polled event can be routed back to its node; built from a
  task with `TaskEventEntry::new`. Also defined in `event::task_event` and re-exported.
- `Scheduler` — `schedule` (consume a task and emit `Run`, i.e. `Ready -> Running`,
  then `ack`), `drive` (emit a `TaskEvent`), `drive_events` (poll task events and
  drive each), `tick` (one batch: poll tasks, schedule and run each, then poll
  events and drive them), and `drain` (loop until a tick consumes nothing).
- `Dispatcher` (in `src/dispatcher.rs`) — wraps another
  `TaskRunner` and adds dispatch tracking. On `run`, it first records a
  `ScheduledTask` referencing the task, its run, node run, and the node and run
  configurations in effect (the node's latest version and the run's latest
  revision at or before the run's revision), then forwards the task. As a
  `TaskEventPoll`, `poll` is forwarded unchanged and `ack` settles the record's
  status to the acknowledged event's status before forwarding.

`ScheduledTask`, its MySQL model, and the read/write implementations live in
`scheduler::scheduled` and are re-exported at the crate root.
`TaskEventPoll` is the scheduler-owned contract for a runner's event stream.

The scheduler is a command **producer**: it appends commands to the command log
(`CommandEmit`) rather than applying them. Each step runs on a `Store::transaction`:
`schedule` opens a transaction, emits `Run` and acks the consumed task on it, then
commits, so the command and the task offset advance together; `drive` emits the
event command on its own transaction. Node status is driven when a consumer polls
that log (`CommandPoll`) and applies each entry (`CommandApply`); because applying
`Succeed` enqueues successors, a producer and consumer are run until the run
reaches quiescence (see `crates/scheduler/tests/mysql_scheduler.rs`).

### Command event log (event sourcing)

Commands are recorded append-only in `command_entry`, keyed by a monotonically
increasing offset (`id`). `CommandEntry::new` builds an entry for a
`(data_id, graph_id)` run, and `CommandEmit::emit` appends it through any
`toasty::Executor`, letting the database assign the offset. Consumers advance
through the log with `CommandPoll::poll` (read the first unacknowledged
contiguous entries in offset order) and `CommandPoll::ack` (persist the global
consumed offset in `poll_offset`). Because the log is the source of truth, a
run's status can be driven — or replayed — from the recorded command sequence.

### `executor` — task jobs

- `Runnable` — a job that runs in a `JobContext` and returns a `TaskState`.
  `Box<T>` is runnable when `T` is, so jobs can be stored behind a pointer.
- `JobContext` — identifies the run, graph version, node, and revision a job is
  executing for.
- `TaskState` / `ErrorCode` — report the event and exit code a run produced, and
  map execution errors to numeric codes.
- `Script` (in `src/script.rs`) — runs source through an interpreter, e.g.
  `sh -c <source>`, and reports success or failure as a task event.

### `server` — HTTP API

`server::router` builds the axum `Router` and `main` serves it. The router's
state is `server::app::App`, so the REST handlers
delegate straight to the app: they emit through `RunApp` and read state back
from it (no parallel HTTP state type). `App::connect(url, config, graph_store)`
builds it, letting the binary supply any `GraphStore`; `server::graph` provides
two — `Graphs` (an in-memory registry) and `StaticGraphStore`, which serves the
ten word-named graphs embedded from the repository's `graphs/` directory
(`alpha.json`..`juliet.json`, 10–20 nodes each) and is handy for tests.
`server::config::Config::load` layers configuration with the `config` crate —
built-in defaults, then a file (`RAYFLOW_CONFIG`; by default `rayflow.yaml` at
the repository root, or any format found under the `rayflow` name), then the
environment: `RAYFLOW_ADDR` (listen address, default `0.0.0.0:8080`) and
`DATABASE_URL` (MySQL URL; required). The checked-in `rayflow.yaml` sets `addr`
and `database_url`; the binary runs entirely from this config. Service routes are
`/` (service name) and `/health` (liveness).

The REST API is the command *producer* of the event-sourced runtime: it appends
the commands that drive a run to the command log and reads back the state they
produced once a consumer has applied them.

| Method | Path | Body / query | Effect |
| --- | --- | --- | --- |
| `POST` | `/runs/start` | `{ data_id, graph_id, nodes? }` | Emit `Command::Start` (empty `nodes` = graph roots). |
| `POST` | `/runs/rerun` | `{ data_id, graph_id, node }` | Emit `Command::Rerun` for one node. |
| `POST` | `/runs/dryrun` | `{ data_id, graph_id, nodes? }` | Emit `Command::Dryrun`. |
| `POST` | `/task_events` | `{ task_id, event }` | Emit a `TaskEvent` for an existing task. |
| `GET` | `/runs/graph_state` | `?data_id=&graph_id=` | Current `GraphState` (run `uuid`, `version`, `revision`, `edges`, and each node's state). |

Commands and task events are accepted (`202 Accepted` with the entry `id`) once
appended; `graph_state` returns `404` when no run exists for the pair, and
task-event emission returns `404` when the task does not exist.

The same surface is available as a library, in `server::app`:

- `App::new(db, config, graph_store)` builds the app over a `toasty::Db`, the
  loaded `Config`, and any `GraphStore` (so the server is graph-source agnostic);
  `App::connect` is the same but connects the `Db` from a URL first. `App` is
  `Clone`, so it is the axum router state directly. `App::offsets` exposes the
  shared command offset, and `App::run_with` yields a `RunApp` for one
  `(data_id, graph_id)` run.
- `RunApp` produces commands — `start`, `rerun`, `dryrun` append the command and
  return the emitted `CommandEntry`; the `*_wait` variants additionally block
  until the command's offset is consumed
  (`start_wait`/`rerun_wait`/`dryrun_wait`).
- `App::emit_task_event(task_id, event)` resolves the task and appends its event,
  returning the recorded `TaskEventEntry` with its assigned offset.
- `RunApp` reads state back — `node_state(node_id)` returns a `NodeState` whose
  fields embed the node's `node_run` row and the graph's `Node`, and
  `graph_state()` returns the `GraphState`: the run's revision plus every node's
  live state, taken from the node run with the greatest revision at or before the
  run's current revision.
- `AppError` maps to an HTTP response (`404` for missing runs/nodes, `500` for
  backend or graph errors).

### `pilot` — leader-elected consumer

Every pilot competes for a MySQL-backed lease on a named coordinator role. Only
the current leader consumes the global logs; standbys return an empty step and
take over after the lease expires or is released. On every pass the leader:

1. Acquires or renews the leader lease.
2. Polls and applies command entries, then acknowledges them.
3. Runs `Scheduler::tick`, which consumes tasks and task events.
4. Applies the commands emitted by that scheduling pass.

Command, task, and task-event consumption all use a persisted global offset and
skip an auto-increment gap only after it has remained missing for 10 seconds.

Configuration is layered like the server's: defaults, then `rayflow.yaml`, then
the environment. Pilot settings include `batch`, `poll_interval_ms`,
`leader_name`, and `leader_lease_ms`; `RAYFLOW_PILOT_*` variables override them.
Run it with:

```sh
DATABASE_URL=mysql://root:password@127.0.0.1:3306/rayflow cargo run -p pilot
```

## Node State Transition

A node advances through the following statuses. **Schedule** is the scheduler
consuming a `Ready` task and running it; the other arrows are **task events**
(`Pend`, `Succeed`, `Fail`, `Terminate`, `Cancel`) applied to the node.

```text
1. ready -schedule-> running -pend-> pending -event-> succeeded/failed/terminated
2. ready -schedule-> running -------------------event-> succeeded/failed/terminated
3. ready --------------------pend-> pending -----event-> succeeded/failed/terminated
```

Equivalently:

```mermaid
stateDiagram-v2
    [*] --> Ready
    Ready --> Running: schedule (Run)
    Running --> Pending: pend
    Ready --> Pending: pend
    Running --> Succeeded: succeed
    Pending --> Succeeded: succeed
    Running --> Failed: fail
    Pending --> Failed: fail
    Running --> Terminated: terminate / cancel
    Pending --> Terminated: terminate / cancel
    Succeeded --> Ready: rerun
    Failed --> Ready: rerun
    Terminated --> Ready: rerun
```

Notes:

- A task is the unit that carries a node from `Ready` to `Running`, then the
  runner reports the event that drives the node to its next status.
- `Pending` is a deferral: a `Pending` node only moves to a terminal status (the
  transitions do not resume `Pending -> Running`); use `Rerun` to make it `Ready`
  again.
- `Rerun` advances the run revision and resets a node to `Ready`; `Dryrun` mocks a
  target's predecessors as `Succeeded` so the target becomes `Ready` without
  running its upstream.

## Build, Test, and Development

```sh
cargo build                 # build all crates
cargo check --workspace     # fast type-check
cargo test --workspace      # run the test suite
cargo fmt                   # format with rustfmt
cargo clippy                # lint the workspace
```

Apply `runtime.sql` to a MySQL instance before using the MySQL backend:

```sh
mysql < runtime.sql
```

The models are the source of truth; `runtime.sql` mirrors them for direct
provisioning. The backend queries through Toasty models, so the models must be
registered on the `Db`: use `runtime::backend::Backend::connect` (which does it for
you), or chain `.models(runtime::models())` onto `Db::builder()` when you
build the `Db` yourself.

Integration tests that drive the MySQL backend are skipped unless `DATABASE_URL`
is set; when it is, they rebuild the schema straight from the models with
`Db::reset_db` + `Db::push_schema`. The database named in the URL must already
exist (the tests recreate the tables, not the database). For example:

```sh
DATABASE_URL=mysql://root:password@127.0.0.1:3306/rayflow cargo test -p scheduler
```

## Layout

```text
crates/
  graph/       pure graph model (Graph/Id traits, DAG types)
  coordinator/ MySQL-backed LeaderElection
  event/       Command/Task/TaskEvent logs, polling, and Reactive consumer
  runtime/     CommandApply + execution backend and state
  scheduler/   TaskRunner/TaskEventPoll/Store + ScheduledTask + Scheduler/Dispatcher
  executor/    JobContext/Runnable/TaskState + script.rs Script
server/        axum HTTP API binary (router + RAYFLOW_ADDR bind)
pilot/         leader-elected command/task/task-event consumer binary
runtime.sql    MySQL schema (task, run, run_config, node_config, node_run, scheduled_task, task_event, command_entry, poll offsets)
```

### Shared event log API

`Event` exposes `offset() -> u64` and
`id() -> Uuid` for each record. `Emit<B>` appends a record and
returns its offset, `Read<I, B>` reads by identity, and `Poll<B>` returns pending
records and acknowledges them in order. Acknowledgements update consumer state;
they never modify or delete log entries. Pass a transaction as `B` when effects
and acknowledgements must commit together.

The `event` crate implements these traits directly for `CommandEntry`, `Task`,
and `TaskEventEntry`. Runtime, scheduler, server, pilot, and executor import
these canonical types rather than defining separate event-log models.

The standalone `event::command` module is always available.
It owns `Command`, `CommandEntry`, and Toasty models; it does not
depend on `runtime`. Register `event::command::models()` and import the shared
`Emit`, `Read`, and `Poll` traits. Its tables match `command_entry` and
`poll_offset` in `runtime.sql`, including contiguous polling and the persisted
10-second gap timeout. Use one serialized consumer for the global command cursor.

`event::task::Task` implements the same emit/read/poll/ack contracts for the
`task` table, with no runtime dependency. Use `event::task::models()` for tasks
alone or `event::models()` to register commands, tasks, task events, and the
shared offset table together. The three logs maintain independent consumer cursors.

`event::task_event` provides `TaskEvent` and `TaskEventEntry` with the same
emit/read/poll/ack contracts. Each event has its own UUID, and multiple events
can reference one task. Event names retain their existing `VARCHAR(16)` storage.
Register `event::task_event::models()` alone or `event::models()` for all logs.

`event::Reactive<B>` extends `Poll<B>` with `handle(&self, &mut B)`
and a continuous `run(backend, batch, poll_interval, shutdown)` loop. It handles
records in order, acknowledges only successful handlers, and waits between
partial batches. Errors return to the caller with the failed record unacknowledged.
Shutdown waits for an in-flight handler and acknowledgement to finish. Handlers
must tolerate replay, and callers must ensure exclusive ownership of the cursor.

All standalone logs and polling state use the shared `event::Error` type.
