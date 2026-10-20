# Rayflow

Rayflow is a Rust workspace (edition 2024) implementing a DAG-based task runtime.
A run walks the nodes of a graph through a small status state machine, persisting
every transition so the work can be driven forward, deferred, and resumed across
processes.

The workspace splits into five core modules plus the `server` and `pilot`
binaries:

- **`dag`** — pure graph primitives. No I/O.
- **`coordinator`** — MySQL-backed leader election for the active consumer.
- **`runtime`** — command driving and task read/write/consume, with a MySQL-backed
  store behind the default `mysql` feature.
- **`scheduler`** — the task scheduler: consumes tasks and drives node status by
  task events, with the MySQL-backed `AutoRunner` and `Dispatcher`.
- **`executor`** — job primitives for executing task work, including the built-in
  script job.
- **`server`** — an HTTP API (axum) that serves the runtime.
- **`pilot`** — a continuously running, leader-elected command, task, and
  task-event consumer.

```text
                 +---------+      +----------------+
   graph model   |   dag   |<-----|    runtime     |
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

### `dag` — graph model

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

`LeaderElection` grants one pilot at a time the right to consume the global
logs. Each role has one row in `leader_election`; acquisition and renewal use a
single atomic MySQL upsert, so standbys observe the lease holder rather than
racing it.

- `LeaderElection::new(db, name, node_id, lease)` creates a handle for a named
  coordinator role.
- `try_acquire_or_renew` takes an expired lease or renews the current holder's
  lease and returns whether this node is the leader.
- `is_leader` checks that this node still owns an unexpired lease, and `resign`
  releases it during shutdown.
- `LeaderElectionModel` is registered with the runtime models so the same
  `toasty::Db` can host both execution state and coordination state.

### `runtime` — execution runtime

The execution state machine and the storage-facing traits.

- `Status` — `Ready`, `Running`, `Pending`, `Succeeded`, `Failed`, `Terminated`.
- `Command` / `CommandEntry` — the commands that drive a node and their
  append-only event log. An entry carries `(data_id, graph_id, command)` plus its
  log position (`command_offset`, `id`, `created_at`). `CommandApply`
  applies an entry to a backend; `CommandEmit` appends one and `CommandPoll`
  consumes the first unacknowledged contiguous run (`poll` + `ack`). They are
  generic over any `toasty::Executor`, so they run against a `Db` or a
  `Transaction`.
- `transition_lock` — a transaction-scoped distributed lock keyed by
  `(data_id, graph_id)`. `CommandApply` upserts the lock row before applying a
  command, so concurrent consumers serialize command application for a run; the
  InnoDB row lock is released on commit or rollback.
- `Task` — a task waiting to be processed, with `TaskWrite` (produce),
  `TaskRead` (read by id), and `TaskPoll` (consume the first unacknowledged
  contiguous run with `poll` + `ack`).
- `TaskEvent` / `TaskEventEntry` — the event a task run reports and its
  append-only log. `TaskEventWrite` appends an event, and the MySQL
  `TaskEventEntry` log operations `poll` and `ack` consume them. The
  `TaskEventPoll` trait is the scheduler-facing contract for a component that
  supplies and acknowledges those events, so event reporting survives a process
  restart.
- `ScheduledTask` — a task that has been dispatched to a runner, materializing
  the task, its run, node run, and the node and run configurations in effect
  plus a status. `ScheduledTaskWrite` records one and `ScheduledTaskRead` reads
  it back by the task it dispatches.
- `RunConfig` — a run's annotation map (`HashMap<String, String>`) at a given
  revision, keyed by the owning run's `uuid`.
- `NodeConfig` — a node's configuration payload at a given version, plus the
  `annotation_match` expression that selects it.
- `GraphStore` — loads a graph by id and optional version.
- `src/mysql/` — the MySQL backend. `model.rs` defines the Toasty `Model`s for
  the schema (one per table) and `models()` registers them on the `Db`;
  `Backend::connect` builds a `Db` with those models. `command.rs` drives node
  state, `task.rs`/`entry.rs`/`event.rs`/`scheduled.rs` persist and poll tasks,
  commands, events, and scheduled tasks through the generated query builders.
  `poll.rs` stores one global offset per log in `poll_offset`. A poll selects
  rows after that offset, stops at the first missing auto-increment id, and
  persists when that gap was first observed. A gap is skipped only after it has
  remained missing for 10 seconds; the persisted offset then advances to just
  before the first returned row, so a crash cannot lose that row.
  `runtime.sql` mirrors the models as a standalone schema for deployments
  (`task`, `run` (+ `uuid`), `run_config`, `node_config`, `node_run`,
  `transition_lock`, `scheduled_task`, `command_entry`, `task_event`,
  `poll_offset`, `leader_election`).

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
  `runtime::mysql::Backend` opens a real SQL transaction, so a command append and
  its task ack commit together.
- `TaskRunner` — runs a scheduled task (`run`, which only executes the work).
- `TaskEventPoll` — supplies the events task runs produced (`poll`) and
  acknowledges them (`ack`) once
  driven. Running and consuming are separate: `run` records the event, `poll`
  returns it, and `ack` consumes it.

- `TaskEvent` — `Pend`, `Succeed`, `Fail`, `Terminate`, `Cancel`; each maps to the
  matching `Command` via `TaskEvent::command(node)`. Defined in `runtime` and
  re-exported here.
- `TaskEventEntry` — the recorded event (its `task_id`/`node_id` plus the
  `TaskEvent`), so a polled event can be routed back to its node; built from a
  task with `TaskEventEntry::new`. Also defined in `runtime` and re-exported.
- `Scheduler` — `schedule` (consume a task and emit `Run`, i.e. `Ready -> Running`,
  then `ack`), `drive` (emit a `TaskEvent`), `drive_events` (poll task events and
  drive each), `tick` (one batch: poll tasks, schedule and run each, then poll
  events and drive them), and `drain` (loop until a tick consumes nothing).
- `AutoRunner` (in `src/auto.rs`, behind `mysql`) — implements `TaskRunner` and
  `TaskEventPoll`; `run` records a successful event for the task, `poll` reads
  the unconsumed events back, and `ack` advances the global event offset once the
  scheduler has driven them.
- `Dispatcher` (in `src/dispatcher.rs`, behind `mysql`) — wraps another
  `TaskRunner` and adds dispatch tracking. On `run`, it first records a
  `ScheduledTask` referencing the task, its run, node run, and the node and run
  configurations in effect (the node's latest version and the run's latest
  revision at or before the run's revision), then forwards the task. As a
  `TaskEventPoll`, `poll` is forwarded unchanged and `ack` settles the record's
  status to the acknowledged event's status before forwarding.

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
registered on the `Db`: use `runtime::mysql::Backend::connect` (which does it for
you), or chain `.models(runtime::mysql::models())` onto `Db::builder()` when you
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
  dag/         pure graph model (Graph/Id traits, DAG types)
  coordinator/ MySQL-backed LeaderElection
  runtime/     Command/Status/Task/RunConfig/NodeConfig traits + src/mysql/ backend
  scheduler/   TaskEvent/TaskRunner/TaskEventPoll/Store + Scheduler + AutoRunner/Dispatcher
  executor/    JobContext/Runnable/TaskState + script.rs Script
server/        axum HTTP API binary (router + RAYFLOW_ADDR bind)
pilot/         leader-elected command/task/task-event consumer binary
runtime.sql    MySQL schema (task, run, run_config, node_config, node_run, transition_lock, scheduled_task, task_event, command_entry, poll offsets)
```
