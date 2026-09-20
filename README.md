# Rayflow

Rayflow is a Rust workspace (edition 2024) implementing a DAG-based task runtime.
A run walks the nodes of a graph through a small status state machine, persisting
every transition so the work can be driven forward, deferred, and resumed across
processes.

The workspace splits into four core modules:

- **`dag`** — pure graph primitives. No I/O.
- **`runtime`** — command driving and task read/write/consume, with a MySQL-backed
  store behind the default `mysql` feature.
- **`scheduler`** — the task scheduler: consumes tasks and drives node status by
  task events.
- **`runner`** — a local `TaskRunner` that executes image and script jobs as local
  processes.

```text
                 +---------+      +----------------+
   graph model   |   dag   |<-----|    runtime     |
                 +---------+      |  Command/Status |
                                  |  Task read/write|
                                  +--------+--------+
                                           ^
                                           |
                                  +--------+--------+
                                  |    scheduler    |
                                  | consume + drive |
                                  +--------+--------+
                                           ^
                                           |
                                  +--------+--------+
                                  |     runner      |
                                  | image + script  |
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

### `runtime` — execution runtime

The execution state machine and the storage-facing traits.

- `Status` — `Ready`, `Running`, `Pending`, `Succeeded`, `Failed`, `Terminated`.
- `Command` / `CommandEntry` — the commands that drive a node and their
  append-only event log. An entry carries `(data_id, graph_id, command)` plus its
  log position (`command_offset`, `id`, `shard`, `created_at`). `CommandApply`
  applies an entry to a backend; `CommandEmit` appends one and `CommandPoll`
  consumes them (poll + ack). The latter two are generic over any
  `toasty::Executor`, so they run against a `Db` or a `Transaction`.
- `Task` — a task waiting to be processed, with `TaskWrite` (produce),
  `TaskRead` (read by id), and `TaskPoll` (consume: `poll`, optionally scoped to
  one shard, + `ack`).
- `TaskEvent` / `TaskEventEntry` — the event a task run reports and its
  append-only log. `TaskEventWrite` appends an event, and `TaskEventPoll`
  consumes them (`poll`, optionally scoped to one shard, + `ack`), so event
  reporting survives a process restart.
- `RunConfig` — a run's annotation map (`HashMap<String, String>`) at a given
  revision, keyed by the owning run's `uuid`.
- `NodeConfig` — a node's configuration payload at a given version, plus the
  `annotation_match` expression that selects it.
- `GraphStore` — loads a graph by id and optional version.
- `shard_of` / `hash_data_id` — FNV-1a consistent-hash placement of a data id on
  the shard ring.
- `src/mysql/` — the MySQL backend. `model.rs` defines the Toasty `Model`s for
  the schema (one per table) and `models()` registers them on the `Db`;
  `Backend::connect` builds a `Db` with those models. `command.rs` drives node
  state, `task.rs`/`entry.rs`/`event.rs` persist and poll tasks, commands, and
  events through the generated query builders — there is no hand-written SQL, so
  guarded updates, per-shard offset filters, and offset bumps are all typed.
  `runtime.sql` mirrors the models as a standalone schema for deployments
  (`task`, `run` (+ `uuid`), `run_config`, `node_config`, `node_run`,
  `task_poll_offset`, `command_entry`, `command_poll_offset`, `task_event`,
  `task_event_poll_offset`).

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
depends only on two traits:

- `Store` — `poll_tasks` (consume) and `transaction`, which opens a `StoreTx` the
  scheduler drives commands and task acks through. Atomicity lives in the store:
  the MySQL `runtime::mysql::Backend` opens a real SQL transaction, so a command
  append and its task ack commit together.
- `TaskRunner` — runs a scheduled task (`run`, which only executes the work),
  polls the events its runs produced (`poll`, optionally scoped to one shard),
  and acknowledges them (`ack`) once driven. Running and reporting are separate:
  `run` records the event, `poll` returns it, `ack` consumes it.

- `TaskEvent` — `Pend`, `Succeed`, `Fail`, `Terminate`, `Cancel`; each maps to the
  matching `Command` via `TaskEvent::command(node)`. Defined in `runtime` and
  re-exported here.
- `TaskEventEntry` — the recorded event (its `task_id`/`node_id`/`shard` plus the
  `TaskEvent`), so a polled event can be routed back to its node; built from a
  task with `TaskEventEntry::new`. Also defined in `runtime` and re-exported.
- `Scheduler` — `schedule` (consume a task and emit `Run`, i.e. `Ready -> Running`,
  then `ack`), `drive` (emit a `TaskEvent`), `drive_events` (poll task events and
  drive each), `tick` (one batch: poll tasks, schedule and run each, then poll
  events and drive them), and `drain` (loop until a tick consumes nothing).
  `with_shard` scopes both task and event polling to a single shard; by default
  every shard is polled.

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
increasing offset (`id`) and routed to a shard derived from `data_id`.
`CommandEntry::routed` builds an entry placed on the shard of its `data_id`, and
`CommandEmit::emit` appends it through any `toasty::Executor`, letting the database
assign the offset. Consumers advance through the log with `CommandPoll::poll`
(read unacknowledged entries in offset order, optionally restricted to a single
shard) and `CommandPoll::ack` (persist the shard's consumed offset in
`command_poll_offset`). Because the log is the source of truth, a run's status can
be driven — or replayed — from the recorded command sequence.

### `runner` — local runner

Executes a task's job on the local machine and implements the scheduler's
`TaskRunner` contract: `LocalRunner::run` runs the job and persists the resulting
event to the `task_event` table, `poll` reads the un-consumed events back
(optionally scoped to one shard), and `ack` advances the shard offset once the
scheduler has driven them.

- `Job` — `Docker` (a container image run through the Docker Engine API, with an
  optional `Schedule` of resource limits) or `Script` (a script run through an
  interpreter, e.g. `sh -c <source>`). Deserialized from `node_config.config`
  with `Job::from_config`/`TryFrom<&NodeConfig>`.
- `docker::Client` (in `src/docker.rs`) — a small Docker Engine API client over
  the daemon's Unix socket; it creates, starts, waits on, and removes the
  container, mapping the job's resource limits onto `HostConfig`.
- `LocalRunner` (in `src/local.rs`) — built from a `toasty::Db`, holds the job for
  each node (`with_job`), runs scripts as a child process and docker jobs through
  the client, maps the outcome to `Succeed`/`Fail`, and stores the event via
  `TaskEventWrite`/`TaskEventPoll`.
- `Error` — `JobNotFound` when a task's node has no registered job, the
  underlying `std::io::Error`, or the task event store error.

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
  runtime/     Command/Status/Task/RunConfig/NodeConfig traits + src/mysql/ backend
  scheduler/   TaskEvent/TaskRunner/Store + Scheduler
  runner/      Job (image/script) + src/local.rs LocalRunner (task_event backed)
runtime.sql    MySQL schema (task, run, run_config, node_config, task_event, command_entry, poll offsets)
```
