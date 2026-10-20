# Rayflow

Rayflow is a Rust workspace (edition 2024) implementing a DAG-based task runtime.
A run walks the nodes of a graph through a small status state machine, persisting
every transition so the work can be driven forward, deferred, and resumed across
processes.

The workspace splits into six core modules plus the `server` binary, which
exposes them over HTTP:

- **`dag`** — pure graph primitives. No I/O.
- **`shardtable`** — a precomputed shard-to-member table driven by a pluggable
  `Membership` trait.
- **`swim`** — a small SWIM-style membership implementation behind a transport
  abstraction.
- **`runtime`** — command driving and task read/write/consume, with a MySQL-backed
  store behind the default `mysql` feature.
- **`scheduler`** — the task scheduler: consumes tasks and drives node status by
  task events, with the MySQL-backed `AutoRunner` and `Dispatcher`.
- **`executor`** — job primitives for executing task work, including the built-in
  script job.
- **`server`** — an HTTP API (axum) that serves the runtime.

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
                                  |   shardtable    |
                                  |   shard table   |
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

### `shardtable` — membership-driven consistent hash table

A virtual-node consistent hash table plus a coordinator. The table owns its ring
and is independent of where membership comes from; the coordinator connects the
two.

- `Membership` — supplies the current members (`members`) and exposes a tokio
  `watch` subscription (`subscribe`) that emits a new member-list snapshot on
  every change. SWIM or another membership implementation can provide it.
- `HashTable` — stores only the shard-to-member mapping. Internally it builds a
  SipHash virtual-node ring, deduplicates members, and resolves each explicitly
  configured shard to its first ring candidate once. `shard_owner` is a direct
  indexed lookup; `owned_shards` and `migrations` report assignments and
  ownership changes.
- `Coordinator` — cloneable and concurrent. It watches membership notifications,
  rebuilds the hash ring and table with `DEFAULT_REPLICAS` virtual nodes per
  member, and returns the `ShardMigration`s between the old and new tables.
  Readers receive immutable `Arc<HashTable>` snapshots, so lookups can continue
  while a new table is being built and published.

### `swim` — membership

A compact SWIM state machine that implements `shardtable::Membership`.

- `Swim` — tracks alive, suspect, and dead members; probes peers with ping/ack;
  refutes suspicion with increasing incarnations; and publishes alive-member
  snapshots through the shared `watch` subscription.
- `Message` / `Outbound` — protocol messages and the addresses they should be
  sent to. `Swim::tick` advances failure detection and `Swim::handle` processes
  incoming messages.
- `Transport` — a minimal async send abstraction. Callers can use UDP, an
  in-memory bus, or another transport without coupling it to the state machine.

### `runtime` — execution runtime

The execution state machine and the storage-facing traits.

- `Status` — `Ready`, `Running`, `Pending`, `Succeeded`, `Failed`, `Terminated`.
- `Command` / `CommandEntry` — the commands that drive a node and their
  append-only event log. An entry carries `(data_id, graph_id, command)` plus its
  log position (`command_offset`, `id`, `shard`, `created_at`). `CommandApply`
  applies an entry to a backend; `CommandEmit` appends one and `CommandPoll`
  consumes them (`poll`, optionally scoped to a set of shards, + `ack`). The
  latter two are generic over any
  `toasty::Executor`, so they run against a `Db` or a `Transaction`.
- `transition_lock` — a transaction-scoped distributed lock keyed by
  `(data_id, graph_id)`. `CommandApply` upserts the lock row before applying a
  command, so concurrent consumers serialize command application for a run; the
  InnoDB row lock is released on commit or rollback.
- `Task` — a task waiting to be processed, with `TaskWrite` (produce),
  `TaskRead` (read by id), and `TaskPoll` (consume: `poll`, optionally scoped to
  a set of shards, + `ack`).
- `TaskEvent` / `TaskEventEntry` — the event a task run reports and its
  append-only log. `TaskEventWrite` appends an event, and the MySQL
  `TaskEventEntry` log operations `poll` and `ack` consume them. The
  `TaskEventPoll` trait is the scheduler-facing contract for a component that
  supplies and acknowledges those events (optionally scoped to a set of shards),
  so event reporting survives a process restart.
- `ScheduledTask` — a task that has been dispatched to a runner, materializing
  the task, its run, node run, and the node and run configurations in effect
  plus a status. `ScheduledTaskWrite` records one and `ScheduledTaskRead` reads
  it back by the task it dispatches.
- `RunConfig` — a run's annotation map (`HashMap<String, String>`) at a given
  revision, keyed by the owning run's `uuid`.
- `NodeConfig` — a node's configuration payload at a given version, plus the
  `annotation_match` expression that selects it.
- `GraphStore` — loads a graph by id and optional version.
- `shard_of` / `hash_data_id` — FNV-1a placement of a data id into a shard
  index.
- `src/mysql/` — the MySQL backend. `model.rs` defines the Toasty `Model`s for
  the schema (one per table) and `models()` registers them on the `Db`;
  `Backend::connect` builds a `Db` with those models. `command.rs` drives node
  state, `task.rs`/`entry.rs`/`event.rs`/`scheduled.rs` persist and poll tasks,
  commands, events, and scheduled tasks through the generated query builders.
  Polling first reads the relevant per-shard offsets, then applies them to the
  log query so only unacknowledged rows are selected; guarded updates and offset
  bumps remain typed.
  `runtime.sql` mirrors the models as a standalone schema for deployments
  (`task`, `run` (+ `uuid`), `run_config`, `node_config`, `node_run`,
  `transition_lock`, `scheduled_task`, `task_poll_offset`, `command_entry`,
  `command_poll_offset`, `task_event`, `task_event_poll_offset`).

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

- `Store` — `poll_tasks` (consume, optionally scoped to a set of shards) and
  `transaction`, which opens a `StoreTx` the scheduler drives commands and task
  acks through. Atomicity lives in the store: the MySQL
  `runtime::mysql::Backend` opens a real SQL transaction, so a command append and
  its task ack commit together.
- `TaskRunner` — runs a scheduled task (`run`, which only executes the work).
- `TaskEventPoll` — supplies the events task runs produced (`poll`,
  optionally scoped to a set of shards) and acknowledges them (`ack`) once
  driven. Running and consuming are separate: `run` records the event, `poll`
  returns it, and `ack` consumes it.

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
  `with_shard` scopes both task and event polling to a single shard, while
  `with_shards`/`set_shards` accept the complete shard set owned by this process.
  By default every shard is polled. On a membership notification, rebuild the
  table, call `table.owned_shards(&local_member)`, and pass that
  set to `set_shards`.
- `AutoRunner` (in `src/auto.rs`, behind `mysql`) — implements `TaskRunner` and
  `TaskEventPoll`; `run` records a successful event for the task, `poll` reads
  the unconsumed events back (optionally scoped to a set of shards), and `ack`
  advances the shard offset once the scheduler has driven them.
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
increasing offset (`id`) and routed to a shard derived from `data_id`.
`CommandEntry::routed` builds an entry placed on the shard of its `data_id`, and
`CommandEmit::emit` appends it through any `toasty::Executor`, letting the database
assign the offset. Consumers advance through the log with `CommandPoll::poll`
(read unacknowledged entries in offset order, optionally restricted to a set of
shards) and `CommandPoll::ack` (persist the shard's consumed offset in
`command_poll_offset`). Because the log is the source of truth, a run's status can
be driven — or replayed — from the recorded command sequence.

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

The workspace's only binary. `server::router` builds the axum `Router` and
`main` serves it. The router's state is `server::app::App`, so the REST handlers
delegate straight to the app: they emit through `RunApp` and read state back
from it (no parallel HTTP state type). `App::connect(url, config, graph_store)`
builds it, letting the binary supply any `GraphStore`; `server::graph` provides
two — `Graphs` (an in-memory registry) and `StaticGraphStore`, which serves the
ten word-named graphs embedded from the repository's `graphs/` directory
(`alpha.json`..`juliet.json`, 10–20 nodes each) and is handy for tests.
`server::config::Config::load` layers configuration with the `config` crate —
built-in defaults, then a file (`RAYFLOW_CONFIG`; by default `rayflow.yaml` at
the repository root, or any format found under the `rayflow` name), then the
environment: `RAYFLOW_ADDR` (listen address, default `0.0.0.0:8080`),
`RAYFLOW_SHARDS` (shard count, default `1`), and `DATABASE_URL` (MySQL URL;
required). The
checked-in `rayflow.yaml` sets `addr`, `shards: 1`, and `database_url`; the
binary runs entirely from this config. Service routes are `/` (service name) and
`/health` (liveness).

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

Commands and task events are accepted (`202 Accepted` with the entry `id` and
`shard`) once appended; `graph_state` returns `404` when no run exists for the
pair, and task-event emission returns `404` when the task does not exist.

The same surface is available as a library, in `server::app`:

- `App::new(db, config, graph_store)` builds the app over a `toasty::Db`, the
  loaded `Config`, and any `GraphStore` (so the server is graph-source agnostic);
  `App::connect` is the same but connects the `Db` from a URL first. `App` is
  `Clone`, so it is the axum router state directly. `App::offsets` exposes the
  shared per-shard offsets, and `App::run_with` yields a `RunApp` for one
  `(data_id, graph_id)` run.
- `RunApp` produces commands — `start`, `rerun`, `dryrun` append the command and
  return the emitted `CommandEntry`; the `*_wait` variants additionally block
  until the command's shard offset is consumed
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
  shardtable/  Membership + HashTable + Coordinator
  swim/        SWIM membership + Transport abstraction
  runtime/     Command/Status/Task/RunConfig/NodeConfig traits + src/mysql/ backend
  scheduler/   TaskEvent/TaskRunner/TaskEventPoll/Store + Scheduler + AutoRunner/Dispatcher
  executor/    JobContext/Runnable/TaskState + script.rs Script
server/        axum HTTP API binary (router + RAYFLOW_ADDR bind)
runtime.sql    MySQL schema (task, run, run_config, node_config, node_run, transition_lock, scheduled_task, task_event, command_entry, poll offsets)
```
