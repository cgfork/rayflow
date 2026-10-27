# Rayflow

Rayflow is a Rust workspace (edition 2024) implementing a DAG-based task runtime.
A run walks the nodes of a graph through a small status state machine, persisting
every transition so the work can be driven forward, deferred, and resumed across
processes.

The workspace splits into six core modules plus the `server`, `pilot`, and `agent`
binaries:

- **`event`** — backend-independent `Event`, `Emit`, `Read`, and `Poll` contracts.
- **`graph`** — pure graph primitives. No I/O.
- **`coordinator`** — MySQL-backed leader election for the active consumer.
- **`runtime`** — command application and execution state, with an always-available MySQL backend.
- **`scheduler`** — `Actuator` consumes logs and drives graph state; the `Scheduler`
  trait abstracts scheduling to agents or external job schedulers. `AgentRef`
  implements `JobPoll` to renew and redeliver existing claims and claim new jobs
  that fit the remaining resource capacity in one transaction.
- **`executor`** — job primitives for executing task work, including the built-in
  script job.
- **`server`** — an HTTP API (axum) that serves the runtime.
- **`pilot`** — a continuously running, leader-elected command, task, and
  task-event consumer.
- **`agent`** — polls jobs that fit its reported resource capacity through the HTTP API.

```mermaid
flowchart LR
    logs[Command, task, and task-event logs] --> actuator[Actuator]
    actuator --> state[Runtime graph state]
    actuator --> jobs[Job]
    jobs --> scheduler[Scheduler trait]
    agent[Agent via HTTP] -->|POST /jobs/poll| jobs
    scheduler --> leases[Job claim leases]
    scheduler -. external adapter .-> external[Kubernetes / Volcano]
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
  `Actuator` should register `scheduler::models()` instead.

`event::poll` owns the shared contiguous offsets and a configurable gap timeout
(10 seconds by default). `rayflow.sql` remains the deployment schema for all crates; this
ownership split does not change its table layouts.

`Command` variants and the transition each applies:

| Command | Effect |
| --- | --- |
| `Start { nodes }` | Create the run and its initial `Ready` node runs (empty `nodes` = graph roots); enqueue a task per node. One-time per `(data_id, graph_id, version)`. |
| `Rerun { node }` | Advance the run revision, create a new `Ready` node run, and enqueue its task. |
| `Dryrun { nodes }` | Mock each target's non-target predecessors as `Succeeded`; mark the targets `Ready` and enqueue their tasks. |
| `Run { node }` | `Ready -> Running`; do not create a task. |
| `Pend { node }` | `Ready`/`Running -> Pending` after a `Yield` task event. |
| `Resume { node }` | `Pending -> Ready`; enqueue a new task in the same node run and revision. The actuator runs the ready task. |
| `Succeed { node }` | `Pending`/`Running -> Succeeded`; enqueue a task for each runnable successor that becomes `Ready`, skipping successors already `Ready`. |
| `Fail { node }` | `Pending`/`Running -> Failed`. |
| `Cancel { node }` | `Pending`/`Running -> Terminated`. |
| `Terminate { node }` | `Pending`/`Running -> Terminated`. |

### `scheduler` — actuation and scheduling contracts

`Actuator` consumes commands, tasks, and task events to drive graph state.
`Task` is the logical input driving the FSM. `Job` is its execution instance,
scheduled to an agent or another executor and linked back through `task_id`.
`Actuator::tick_in(tx)` applies command entries, turns tasks into `Job` records,
activating any `Ready` node as `Running` without emitting another task,
consumes task events, and applies their resulting commands. It
acknowledges logs in the same transaction as their effects.
Configuration resolution uses the node's latest configuration version and the
run's latest configuration revision at or before its current revision.
`Actuator::tick()` wraps a pass in its own transaction; callers must serialize
consumption, as the pilot does through leader election.

`Scheduler<B>` defines `schedule(&self, backend: &mut B)`, returning the number
of scheduling changes with an implementation-specific error. The default
`AgentScheduler` reclaims expired claims; agents directly claim fitting jobs
through `POST /jobs/poll`. Other implementations can submit jobs to an external
scheduler such as Kubernetes Volcano.
`Pilot::with_scheduler(implementation)` selects an implementation; the pilot
invokes it only while holding the leader lease. External adapters must submit
durably committed requests and reconcile submissions after uncertain responses.
The pilot can create task and job UUIDs inside the current transaction;
those identities may change if that pass rolls back. An adapter can select only
requests visible through a separate committed database connection and use their
persisted UUIDs to deduplicate remote jobs. A Kubernetes/Volcano adapter can
implement this trait with its own client and submission tracking; no Kubernetes
client is bundled here.

`Job` and its `JobRead`/`JobWrite` persistence contracts live in `actuator.rs`.
Register `scheduler::models()` for scheduling, runtime, event, configuration,
and election models.

Jobs have their own `actuator::Status` enum: `Scheduled`, `Polled`, `Yielded`,
`Succeeded`, `Failed`, and `Terminated`; the last four are terminal states.
Executing jobs remain `Polled`. A `Yielded` report emits `TaskEvent::Yield`,
releases the claim and its resources, and moves the node to `Pending`.
`Command::Resume` moves that node back to `Ready` and enqueues a new task.
The actuator moves it to `Running` when creating its job, without emitting
another task. The previous job stays `Yielded`.
`TaskEvent` and `TaskEventEntry` come from `event` and are re-exported here.

### Direct agent polling and resource requirements

`NodeConfig.required_cpu_cores` and `NodeConfig.required_memory_bytes` describe
minimum job resources. When the actuator creates a `Job`, it copies these values
from the node's latest configuration. `AgentRef::new(agent_id, resources,
active_jobs)` implements `JobPoll::poll(backend, maximum)` to reclaim expired
claims, renew active claims, redeliver unstarted claims, and claim fitting work
in one transaction. All existing claims reserve resources, including claims
outside the delivery limit. Both remaining CPU and
memory limits apply in the database query. Both Polled redelivery and Scheduled
claims exclude IDs in `active_jobs`, deduct each selected job from the same
remaining budget, and stop when either CPU or memory reaches zero. `JobPoll::poll`
takes `&mut self`; create a fresh `AgentRef` with the current total budget for
each request. Failed polls leave its budget unchanged. Guarded ORM
updates make claims atomic; concurrent agents cannot claim the same job.
`renew_claims` renews only owned `Polled` jobs listed in `active_jobs`.
Unstarted `Polled` claims also renew when redelivered.

Each node configuration must specify `node_config.lease` as a positive
millisecond duration that fits timestamp arithmetic. There is no default lease.
A missing configuration or `NULL` lease returns `JobLeaseNotFound`; an invalid
duration returns `InvalidJobLease`. Job creation rejects these configurations,
and a failed poll rolls back its claims. The pilot and polling endpoint reclaim
owned `Polled` jobs whose stored `expired_at` is at or before the
current time, without loading node configurations. Renewal, status reports,
and the agent's execution deadline use the job's pinned
configuration, even after a newer node version is created.
`job.expired_at` is the absolute claim deadline (a timestamp, not a duration). Claims
and renewals set it to the renewal time plus the pinned node lease. It is `NULL`
before claiming and after expiry recovery, cancellation, or terminal settlement.
Reclamation skips claims with a `NULL` deadline; existing claims must be renewed
or have their deadlines backfilled before they can be reclaimed. Renewal and
status-report checks can still derive a missing deadline from `polled_at` and
the pinned lease.
Task events set the job to `Yielded`, `Succeeded`, `Failed`, or `Terminated` and
clear its claim. A restarted agent can recover old work after the lease expires.

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
- `JobContext` — contains `job: Job` and `agent: Agent`; run, node, and
  configuration details remain on the job and its loaded relations.
- `Agent` — carries the executing agent's UUID (`id`) and resource capacity
  (`resources`) from the poll that delivered the job.
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
| `POST` | `/runs/resume` | `{ data_id, graph_id, node }` | Emit `Command::Resume` for a pending node. |
| `POST` | `/runs/dryrun` | `{ data_id, graph_id, nodes? }` | Emit `Command::Dryrun`. |
| `POST` | `/jobs/poll` | `{ agent_id, batch, resources: { cpu_cores, memory_bytes }, active_jobs? }` | Claim resource-matching jobs with execution config and context. |
| `POST` | `/job/yield` | `{ job_id, agent_id }` | End the job as Yielded and append `TaskEvent::Yield`. |
| `POST` | `/job/succeed` | `{ job_id, agent_id }` | Complete the job and append `TaskEvent::Succeed`. |
| `POST` | `/job/fail` | `{ job_id, agent_id }` | Fail the job and append `TaskEvent::Fail`. |
| `POST` | `/job/terminate` | `{ job_id, agent_id }` | Terminate the job and append `TaskEvent::Terminate`. |
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
3. Delegates task and task-event consumption to `Actuator::tick_in`, including
   application of resulting commands.
4. Calls the configured `Scheduler::schedule` implementation (by default,
   `AgentScheduler` releases expired job claims).
5. Commits log effects, offsets, and claim recovery together.

Command, task, and task-event consumption all use a persisted global offset and
skip an auto-increment gap only after `gap_timeout_ms` (10000 by default).

Configuration is layered like the server's: defaults, then `rayflow.yaml`, then
the environment. Pilot settings include `batch`, `poll_interval_ms`,
`leader_name`, `leader_lease_ms`, and `gap_timeout_ms`;
`RAYFLOW_PILOT_*` variables override them.
Run it with:

```sh
DATABASE_URL=mysql://root:password@127.0.0.1:3306/rayflow cargo run -p pilot
```

### `agent` — job consumer

The agent samples CPU usage and available memory on every poll, then sends
`POST /jobs/poll` with its identity, batch size, resource capacity, and active
job UUIDs. CPU capacity is estimated from current utilization and rounded to
whole cores. Memory respects host and cgroup limits when available. Optional
`cpu_cores` and `memory_bytes` settings cap the agent's resource budget.

```json
{"agent_id":"83f17806-0e06-4185-bfd0-f682d47c1111","batch":64,"resources":{"cpu_cores":8,"memory_bytes":17179869184},"active_jobs":[]}
```

The capacity includes active reservations because the server subtracts existing
claims before selecting more work. The agent adds reservations to live available
resources, bounded by its limits, to avoid subtracting running jobs' memory twice.
Each agent process needs its own UUID and sends one poll at a time.

The response is `{ jobs, renewed_jobs }`: `jobs` contains `Job` objects directly,
including their loaded `run`, `node_run`, and `node_config` relations.
`renewed_jobs` contains the UUIDs of active claims successfully renewed in the
poll transaction. The agent passes the full job and its own information in
`JobContext`, and derives its lease from `node_config.lease`. It executes scripts using
the pinned `node_config.config` payload:

```json
{"interpreter":"sh","source":"echo hello"}
```

For each unexpired claim returned by polling, the agent executes the script, then
reports `Succeeded` for exit code zero or `Failed` for nonzero exit, a signal,
an invalid config, or a spawn error. Jobs run concurrently while polling
continues. Ctrl-C stops the interpreters and reports `Terminated`, allowing up
to `shutdown_timeout_ms` for status delivery.

The server accepts terminal actions at `POST /job/yield`, `POST /job/succeed`,
`POST /job/fail`, and `POST /job/terminate`. Each takes `{ job_id, agent_id }`,
validates the claim, and updates the job and appends its task event in one ORM
transaction, returning `204 No Content`:

| Job status | Task event |
| --- | --- |
| `Yielded` | `Yield` |
| `Succeeded` | `Succeed` |
| `Failed` | `Fail` |
| `Terminated` | `Terminate` |

Repeated terminal actions append no duplicate event. Expired claims, reports by
another agent, and terminal regressions return `409 Conflict`. Claim renewal uses
only sequential polling with `active_jobs`, not a separate status or renewal API.
The agent waits at most one-third of the shortest active lease between polls,
bounded by `poll_interval_ms`. Confirmed renewals extend local execution deadlines; missing
acknowledgements or elapsed deadlines cancel work without a terminal report.
Renewals emit no task event. Yielding ends
the job; retries emit no additional `Yield` event. The pilot consumes task events
to drive graph state.

Start the API server and pilot, then run an agent:

```sh
cargo run -p agent
```

Agent settings come from optional `rayflow.yaml` and `RAYFLOW_AGENT_*` overrides.
Defaults are a generated UUID, `api_url=http://127.0.0.1:8080`, host CPU/memory
limits, `batch=64`, `poll_interval_ms=250`, `request_timeout_ms=30000`, and
`shutdown_timeout_ms=5000`. `RAYFLOW_AGENT_CONFIG` (or `RAYFLOW_CONFIG`) selects
another file. The agent uses reqwest and accesses only HTTP.

Polling immediately renews owned `Polled` jobs listed in
`active_jobs`, regardless of delivery limits or available capacity. Fitting
unstarted `Polled` claims are renewed when redelivered so a lost poll response
can recover. Other inactive claims keep their existing deadlines. `Polled`
status reports also renew leases and let executions detect rejected claims.
Transient HTTP failures retry within the confirmed lease; execution stops if the
claim is lost. Expired `Polled` jobs become `Scheduled` again.
Delivery remains at least once, so scripts should tolerate retries after an
agent failure. Terminal jobs retain their outcome.

Timeouts use milliseconds and can be configured in the file or environment:

| Component | Field | Default | Environment override |
| --- | --- | --- | --- |
| Pilot | `gap_timeout_ms` | 10000 | `RAYFLOW_PILOT_GAP_TIMEOUT_MS` |
| Agent | `request_timeout_ms` | 30000 | `RAYFLOW_AGENT_REQUEST_TIMEOUT_MS` |
| Agent | `shutdown_timeout_ms` | 5000 | `RAYFLOW_AGENT_SHUTDOWN_TIMEOUT_MS` |

Keep the agent's poll interval comfortably below the shortest configured node
lease. Job leases come exclusively from `node_config.lease`; server and pilot
settings do not supply a fallback. Library callers use `AgentRef` with
`JobPoll::poll` for polling and `AgentScheduler` for expiry recovery.
`Pilot::with_gap_timeout` or `Actuator::with_gap_timeout` configures missing log
offsets.

Existing databases must be migrated before restarting binaries. If necessary,
rename `scheduling_task` (or the older `scheduled_task`) to `job` first. For an
existing `job` table that does not yet have these columns:

```sql
ALTER TABLE job
    ADD COLUMN required_cpu_cores INT UNSIGNED NOT NULL DEFAULT 0,
    ADD COLUMN required_memory_bytes BIGINT UNSIGNED NOT NULL DEFAULT 0,
    ADD COLUMN agent_id CHAR(36) NULL,
    ADD COLUMN polled_at DATETIME(6) NULL,
    ADD KEY idx_job_agent_id (agent_id),
    ADD KEY idx_job_polled_at (polled_at);
ALTER TABLE node_config
    ADD COLUMN required_cpu_cores INT UNSIGNED NOT NULL DEFAULT 0,
    ADD COLUMN required_memory_bytes BIGINT UNSIGNED NOT NULL DEFAULT 0;
UPDATE job SET status = 'Scheduled' WHERE status IN ('Ready', 'Running');
ALTER TABLE job ALTER COLUMN status SET DEFAULT 'Scheduled';
DROP TABLE IF EXISTS worker_queue;
DROP TABLE IF EXISTS worker;
```

To add per-node leases to an existing database:

```sql
ALTER TABLE node_config
    ADD COLUMN lease BIGINT UNSIGNED NULL
        COMMENT 'required job claim lease in milliseconds; NULL returns a missing lease error';
```

Before restarting, explicitly backfill the lease for each configuration version
used by jobs and ensure each runnable node has a configuration with a lease.
Choose durations for those jobs; leaving the column `NULL` returns an error.
The nullable column supports reporting missing leases in legacy rows and does
not provide a fallback.

To persist claim deadlines in an existing database:

```sql
ALTER TABLE job
    ADD COLUMN expired_at DATETIME(6) NULL
        COMMENT 'claim deadline; renewed from the pinned node-config lease',
    ADD KEY idx_job_expired_at (expired_at);
```

The job `status` and task-event `event` columns are `VARCHAR(16)`. Before
restarting binaries, migrate the former job and task-event names:

```sql
UPDATE job SET status = 'Yielded', agent_id = NULL, polled_at = NULL, expired_at = NULL
WHERE status = 'Pending';
UPDATE task_event SET event = 'Yield' WHERE event = 'Pend';
```

Node status `Pending` and command `Pend` keep their names. Resume a pending node
through `POST /runs/resume` to schedule its next task.

New databases use `rayflow.sql` directly.

## Node State Transition

A node advances through the following statuses. A task is emitted when a node
is created or transitions to `Ready`. `Resume` changes a pending node to `Ready`
and emits a new task; the actuator then activates it as `Running` without
emitting another task. The other arrows are
**task events** (`Yield`, `Succeed`, `Fail`, `Terminate`, `Cancel`) applied to
the node.

```text
1. ready -run-> running -yield-> pending -resume-> ready -run-> running
2. ready -run-> running -event-> succeeded/failed/terminated
3. ready -yield-> pending -resume-> ready -run-> running
```

Equivalently:

```mermaid
stateDiagram-v2
    [*] --> Ready: new task
    Ready --> Running: Run
    Running --> Pending: yield
    Ready --> Pending: yield
    Pending --> Ready: Resume (new task)
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

- A task is emitted when a node becomes `Ready`. Scheduling moves it to
  `Running`, then the runner reports the event driving the next status.
- `Pending` is a deferral. `Resume` moves it to `Ready` and creates a fresh
  task without changing the run or node revision. The old job remains terminal
  as `Yielded`; the new task gets a new job.
- `Start`, `Rerun`, `Dryrun`, and successor activation in `Succeed` prepare
  `Ready` nodes and add their tasks directly; they do not call `run`.
  `Run` only changes status. `Rerun` advances the revision; `Dryrun` mocks
  upstream success.

## Build, Test, and Development

```sh
cargo build                 # build all crates
cargo check --workspace     # fast type-check
cargo test --workspace      # run the test suite
cargo fmt                   # format with rustfmt
cargo clippy                # lint the workspace
```

Apply `rayflow.sql` to a MySQL instance before using the MySQL backend:

```sh
mysql < rayflow.sql
```

The models are the source of truth; `rayflow.sql` mirrors them for direct
provisioning. The backend queries through Toasty models, so the models must be
registered on the `Db`: use `runtime::backend::Backend::connect` (which does it for
you), or chain `.models(runtime::models())` onto `Db::builder()` when you
build the `Db` yourself.

Integration tests that drive the MySQL backend are skipped unless `DATABASE_URL`
is set; when it is, they rebuild the schema straight from the models with
`Db::reset_db` + `Db::push_schema`. The database named in the URL must already
exist (the tests recreate the tables, not the database). For example:

```sh
DATABASE_URL=mysql://root:password@127.0.0.1:3306/rayflow cargo test -p scheduler -- --test-threads=1
```

## Layout

```text
crates/
  graph/       pure graph model (Graph/Id traits, DAG types)
  coordinator/ MySQL-backed LeaderElection
  event/       Command/Task/TaskEvent logs, polling, and Reactive consumer
  runtime/     CommandApply + execution backend and state
  scheduler/   Actuator + Job + Scheduler trait + direct agent polling
  executor/    JobContext/Runnable/TaskState + script.rs Script
server/        axum HTTP API binary (router + RAYFLOW_ADDR bind)
pilot/         leader-elected command/task/task-event consumer binary
agent/         HTTP job polling binary with resource capacity
rayflow.sql    MySQL schema (task, run, run_config, node_config, node_run, job, task_event, command_entry, poll offsets)
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
`poll_offset` in `rayflow.sql`, including contiguous polling and the persisted
gap timeout (10 seconds by default). Use one serialized consumer for the global command cursor.
For a custom timeout, call each log model's `poll_with_config` and
`ack_with_config` with the same `event::poll::Config { gap_timeout }`.
The `Poll` trait methods use the default timeout.

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
