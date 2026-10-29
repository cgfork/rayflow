# Rayflow

Rayflow is a Rust workspace (edition 2024) implementing a DAG-based task runtime.
A run walks the nodes of a graph through a small status state machine, persisting
every transition so the work can be driven forward, deferred, and resumed across
processes.

The workspace splits into five core modules plus the `server`, `pilot`, and `agent`
binaries:

- **`event`** — backend-independent `Event`, `Emit`, `Read`, and `Poll` contracts.
- **`graph`** — pure graph primitives. No I/O.
- **`coordinator`** — MySQL-backed leader election for the active consumer.
- **`runtime`** — command application and execution state, with an always-available MySQL backend.
- **`scheduler`** — `Actuator` consumes logs and drives graph state; the `Scheduler`
  trait abstracts scheduling to agents or external job schedulers. `AgentRef`
  implements `JobPoll` to redeliver inactive jobs and claim new jobs
  that fit the remaining resource capacity in one transaction.
- **`server`** — an HTTP API (axum) that serves the runtime.
- **`pilot`** — a continuously running, leader-elected command, task, and
  task-event consumer.
- **`agent`** — recovers and runs container jobs, polls the server, and reports results.

```mermaid
flowchart LR
    logs[Command, task, and task-event logs] --> actuator[Actuator]
    actuator --> state[Runtime graph state]
    actuator --> jobs[Job]
    jobs --> scheduler[Scheduler trait]
    agent[Agent via HTTP] -->|POST /jobs/poll| jobs
    scheduler --> timeouts[Job timeouts]
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
`AgentScheduler` fails timed-out jobs; agents directly claim fitting jobs
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
active_jobs)` implements `JobPoll::poll(backend, maximum)` to redeliver inactive
Polled jobs and claim fitting Scheduled jobs in one transaction. The request
provides the total CPU/memory budget, including current jobs. Existing claims
reserve resources, including claims outside the delivery limit. Both pollers
exclude IDs in `active_jobs`, deduct delivered jobs from the remaining budget,
and stop when either resource is exhausted. Create a fresh `AgentRef` for each
request. Failed polls leave its budget unchanged. Guarded ORM updates prevent
concurrent claims of the same job.

Each node configuration must specify `node_config.lease` as a positive job
lifetime in milliseconds. Missing configurations or `NULL` durations return
`JobLeaseNotFound`; invalid durations return `InvalidJobLease`. Job creation
rejects these configurations, and failed polls roll back their claims.

`job.expired_at` is the absolute runtime expiry. Claiming a Scheduled job or
redelivering a live inactive Polled job sets it to the delivery time plus the
pinned node configuration's lifetime. Active jobs are never renewed. Expired
jobs are neither redelivered nor returned to Scheduled. The timestamp is `NULL`
before first delivery and is preserved after completion.

At expiry, the agent kills the container, persists a failed result with exit
code 124, and reports failure. Reports retry through server outages. The pilot
fails remaining Polled jobs when `expired_at < now - job_timeout_gap`, appending
a Fail task event atomically. The gap defaults to `10s` and accepts duration
strings through `RAYFLOW_PILOT_JOB_TIMEOUT_GAP`. Rows with no expiry are skipped.
Task events set jobs to Yielded, Succeeded, Failed, or Terminated and clear their
ownership. Terminal jobs retain their outcome and expiry.

### Command event log (event sourcing)

Commands are recorded append-only in `command_entry`, keyed by a monotonically
increasing offset (`id`). `CommandEntry::new` builds an entry for a
`(data_id, graph_id)` run, and `CommandEmit::emit` appends it through any
`toasty::Executor`, letting the database assign the offset. Consumers advance
through the log with `CommandPoll::poll` (read the first unacknowledged
contiguous entries in offset order) and `CommandPoll::ack` (persist the global
consumed offset in `poll_offset`). Because the log is the source of truth, a
run's status can be driven — or replayed — from the recorded command sequence.

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
| `POST` | `/jobs/poll` | `{ agent_id, batch, resources: { cpu_cores, memory_bytes }, active_jobs?, wait_ms? }` | Claim resource-matching jobs with execution config and context; optionally wait for work. |
| `POST` | `/job/cancel` | `{ job_id, agent_id }` | End the job as Terminated and append `TaskEvent::Cancel`. |
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
   `AgentScheduler` fails jobs expired beyond the grace period).
5. Commits log effects, offsets, and timeout failures together.

Command, task, and task-event consumption all use a persisted global offset and
skip an auto-increment gap only after `gap_timeout_ms` (10000 by default).

Configuration is layered like the server's: defaults, then `rayflow.yaml`, then
the environment. Pilot settings include `batch`, `poll_interval_ms`,
`leader_name`, `leader_lease_ms`, `gap_timeout_ms`, and `job_timeout_gap`;
`RAYFLOW_PILOT_*` variables override them.
Run it with:

```sh
DATABASE_URL=mysql://root:password@127.0.0.1:3306/rayflow cargo run -p pilot
```

### `agent` — execution and polling

`Connector::events()` returns a `futures_util::Stream` of events or connector
errors. Its `Event<'a>` associated type lets events borrow from the connector.
`DockerConnector` streams native Docker event messages; `Provider` wraps them
in `Event::Docker` and converts errors to the shared container error type.

`Agent` owns the execution provider and job workdir, with no HTTP client or
polling settings. `client::Client` handles HTTP polling and provides explicit
`cancel`, `fail`, `succeed`, and `terminate` methods, each taking the agent and job
UUIDs. `Agentic` receives both plus `Settings`, polls sequentially,
and starts each job in a separate asynchronous task. Slow container starts,
execution, and report retries do not block polling. Active job IDs prevent
duplicate starts and reserve resources; polling does not extend active jobs' lifetimes.

Construct the components with `Client::new(&settings)`,
`Agent::new(&settings.workdir, provider)`, and
`Agentic::with_agent(agent, client, settings)?.start().await`.
`Agentic::new(settings)?` constructs the default Docker provider and HTTP client.
The binary loads settings, starts `Agentic`, and handles Ctrl-C and SIGTERM.

Run `RAYFLOW_AGENT_ID=12345678-1234-4234-8234-123456789abc cargo run -p agent`
with Docker available. Give each agent a unique, stable UUID and keep the same
workdir across restarts so it can recover its claims. Settings load from the
optional `rayflow.yaml` file; `RAYFLOW_AGENT_CONFIG` or `RAYFLOW_CONFIG` selects
an explicit file. For example:

```yaml
id: 12345678-1234-4234-8234-123456789abc
server_url: http://127.0.0.1:8080
workdir: .rayflow/jobs
batch: 64
poll_interval: 1s
request_timeout: 60s
resources:
  cpu_cores: 4
  memory_bytes: 4294967296
```

`id` is required. The other values shown are defaults, except resource capacity,
which defaults to the host's available CPU count and total memory. Environment
variables override file values using the `RAYFLOW_AGENT_` prefix, such as
`RAYFLOW_AGENT_SERVER_URL` and `RAYFLOW_AGENT_POLL_INTERVAL=250ms`. Use double
underscores for nested resource fields, for example
`RAYFLOW_AGENT_RESOURCES__CPU_CORES=2`. Docker connection settings use Bollard's
standard environment, including `DOCKER_HOST`.

`Agent::prepare` persists the job and its pinned configuration and pulls a missing
image. `Agent::run` creates a container named `rayflow-{job UUID}`, applies the
job's CPU and memory limits, writes its container metadata, and starts it without
waiting for completion. Repeated calls reuse the existing container. Running
containers keep their original job files and expiry. `Agent::destroy` removes a
stopped container and cleans its files while retaining the lock inode.

On startup, `Agentic` discovers job directories, reconnects to existing containers,
and retries unreported results. UUID-based container names allow recovery even if
the agent crashed before writing `container.json`. Container waits and provider
events detect completion; event streams reconnect after an interruption. Results
are persisted before delivery, and acknowledged jobs are marked terminal in
`job.json`. `shutdown()` stops polling and reporting while leaving isolated
containers running for a new `Agentic` instance to recover.

The intended job filesystem is:

```text
{WORKDIR}/{Job UUID}/job.json
{WORKDIR}/{Job UUID}/result.json
{WORKDIR}/{Job UUID}/pid
{WORKDIR}/{Job UUID}/lock
{WORKDIR}/{Job UUID}/container.json
{WORKDIR}/{Job UUID}/runtime/started_at
{WORKDIR}/{Job UUID}/runtime/stdout
```

`JobFs` uses asynchronous Tokio file operations to read and atomically replace
job and result JSON files. Writes flush and sync a temporary file before renaming
it into place. Results contain
an `exit_code`; container metadata contains `type` and `id`. The `started_at`
file contains a timestamp with a UTC offset (for example,
`2026-10-28T12:00:00Z`), and `stdout` is opened read-only. Missing files return
`NotFound`; malformed JSON or timestamps return `InvalidData`.

The per-job `lock` file holds an exclusive OS lock until `unlock` or
`JobFs` is dropped. Lock acquisition retries without blocking the async runtime.
Repeated locking or unlocking on the same instance is
idempotent. Callers coordinate updates with this lock; file reads and writes do
not acquire it implicitly. `Agent::jobs` discovers jobs by listing WORKDIR.

### Job API and timeouts

`POST /jobs/poll` accepts optional `wait_ms` in integer milliseconds, from zero
up to the configured `poll_timeout` (`30s` by default). When `wait_ms` is omitted
or null, each request chooses a random wait in `[poll_timeout / 2, poll_timeout)`.
Set `wait_ms: 0` for an immediate poll. Server polling settings use duration
strings, for example:

```yaml
poll_timeout: 10s
poll_retry_interval: 250ms
```

Configure `poll_timeout` in the server configuration file or with
`RAYFLOW_POLL_TIMEOUT`; it must be at least `2ms`.
With no active jobs, an empty poll waits asynchronously and retries every
`poll_retry_interval` (`250ms` by default) until work is claimed or the wait
expires. Configure the interval in the server configuration file or with
`RAYFLOW_POLL_RETRY_INTERVAL`; it must be at least `1ms`. Retry sleeps
are capped by the request's remaining wait. Each attempt uses a fresh
resource budget and commits its own short transaction before waiting; no
database transaction, connection, or lock is held during the sleep. The response
is `{ jobs }`, with an empty array on idle timeout. Available jobs and database
errors return immediately. Requests with nonempty `active_jobs` return after
one attempt.

Keep one outstanding poll per agent ID and set HTTP/client/proxy timeouts above
`wait_ms` (or `poll_timeout` when omitted) plus database processing time.
The wait limit does not cancel an
in-flight claim transaction. Long-polling reduces agent HTTP requests, not
database queries; it does not use database locks as notifications.

The server accepts terminal actions at `POST /job/cancel`, `POST /job/yield`,
`POST /job/succeed`, `POST /job/fail`, and `POST /job/terminate`.
Each takes `{ job_id, agent_id }`,
validates the claim, and updates the job and appends its task event in one ORM
transaction, returning `204 No Content`:

| Job status | Task event |
| --- | --- |
| `Yielded` | `Yield` |
| `Succeeded` | `Succeed` |
| `Failed` | `Fail` |
| `Terminated` | `Terminate` or `Cancel` |

Repeated terminal actions append no duplicate event. Reports from another
agent and terminal regressions return `409 Conflict`. After expiry, the owning
agent may still report Failed; other terminal actions return `409 Conflict`.
The pilot consumes task events to drive graph state. Polling emits no task event.

Polling resets expiry only for jobs actually delivered. IDs in `active_jobs`
are excluded from delivery and keep their existing expiry. Expired Polled jobs
are failed by the agent, or by the pilot after the configured grace period.
Delivery remains at least once, so jobs should tolerate repeated delivery.

Server polling settings use duration strings such as `10s`, `250ms`, and `1.5s`.
Pilot settings ending in `_ms` use integer milliseconds. Configure these in the
file or environment:

| Component | Field | Default | Environment override |
| --- | --- | --- | --- |
| Server | `poll_timeout` | `30s` | `RAYFLOW_POLL_TIMEOUT` |
| Server | `poll_retry_interval` | `250ms` | `RAYFLOW_POLL_RETRY_INTERVAL` |
| Pilot | `gap_timeout_ms` | 10000 | `RAYFLOW_PILOT_GAP_TIMEOUT_MS` |
| Pilot | `job_timeout_gap` | `10s` | `RAYFLOW_PILOT_JOB_TIMEOUT_GAP` |

Job lifetimes come exclusively from `node_config.lease`; server and pilot
settings do not supply a fallback. Library callers use `AgentRef` with
`JobPoll::poll` for polling and `AgentScheduler` for timeout failure handling.
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
        COMMENT 'required job lifetime in milliseconds; NULL returns a missing lease error';
```

Before restarting, explicitly backfill the lease for each configuration version
used by jobs and ensure each runnable node has a configuration with a lease.
Choose durations for those jobs; leaving the column `NULL` returns an error.
The nullable column supports reporting missing leases in legacy rows and does
not provide a fallback.

To persist job expiry timestamps in an existing database:

```sql
ALTER TABLE job
    ADD COLUMN expired_at DATETIME(6) NULL
        COMMENT 'job runtime expiry; reset on delivery from the pinned node-config lifetime',
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
server/        axum HTTP API binary (router + RAYFLOW_ADDR bind)
pilot/         leader-elected command/task/task-event consumer binary
agent/         execution, independent HTTP client, async polling loop and binary
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
and `TaskEventEntry`. Runtime, scheduler, server, and pilot import
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
