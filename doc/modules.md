# Modules

Rayflow is a Cargo workspace. The library crates contain the runtime contracts
and persistence behavior; the `server`, `pilot`, and `agent` packages assemble
those pieces into processes.

## Library crates

### `graph`

The graph crate is the backend-independent graph model. It contains:

- `Graph` and `Id` traits for graph identity, nodes, edges, and derived graph
  queries.
- `types::DAG`, `Node`, `Input`, `Output`, and `Kind` (`File` or `Directory`).
- `validator`, which checks cycles, edge endpoints, and the single-root rule;
  unused outputs are reported separately.
- `diff`, which compares graph values and reports structural changes.
- `store::MySqlGraphStore`, which persists versioned graph JSON and audit
  metadata in the `graph` table.

The graph model itself performs no I/O. Applications choose a `GraphStore`
implementation and decide when to validate or persist a graph.

### `event`

The event crate owns the append-only log contracts and their MySQL-backed
implementations:

- `command` stores `CommandEntry` records and the `Command` enum.
- `task` stores logical `Task` records.
- `task_event` stores `TaskEventEntry` records and task outcomes.
- `poll` manages contiguous offsets, acknowledgements, and configurable gap
  handling.
- `Reactive` provides a continuous poll/handle/ack loop for consumers.

`Event`, `Emit`, `Read`, and `Poll` are shared contracts. Commands, tasks, and
task events have independent cursors, but use the shared `poll_offset` table.
Register `event::models()` when an application needs all three logs.

### `coordinator`

`coordinator` defines leader-election contracts. Its default `mysql` feature
provides `MySqlLeaderElection`, which stores one expiring lease per named role.
The pilot uses this lease to ensure that only one process consumes global logs.
Applications that do not need MySQL can disable the default feature and use the
backend-independent traits.

### `runtime`

The runtime applies commands to persisted execution state. It provides:

- `CommandApply` for applying a `CommandEntry`.
- `backend::Backend` for MySQL-backed state transitions.
- `backend::Status` for node-run state.
- `command::{Run, NodeRun}` for run and node-run models.
- `models()` for registering runtime, event, and coordination models.

The backend accepts a caller-owned transaction through `apply_in`, allowing a
command effect and its log acknowledgement to commit atomically. Runtime
configuration models are owned by `scheduler`, so applications using the
actuator normally register `scheduler::models()`.

### `scheduler`

The scheduler crate connects logical tasks to executable jobs:

- `Actuator` consumes commands, tasks, and task events and materializes jobs.
- `Job`, `JobRead`, and `JobWrite` define job persistence and lifecycle.
- `Scheduler` abstracts destinations such as direct agents or external
  schedulers.
- `AgentScheduler` and `AgentRef` implement resource-aware polling and guarded
  job claims.
- `RunConfig` and `NodeConfig` pin run and node configuration to jobs.

The default `AgentScheduler` handles timeout recovery while agents claim jobs
through the server API. External scheduler adapters must make submissions
durable and reconcile remote work after a transaction rollback or uncertain
response.

## Binaries

### `server`

The server is an Axum HTTP API. It appends run commands and task events, polls
and reports jobs, and reads the current graph state. `App` is also available as
a library abstraction over a `toasty::Db`, a `Config`, and any `GraphStore`.

The binary currently uses `StaticGraphStore`, which embeds the JSON graphs in
`graphs/` at compile time. The server library is graph-store agnostic, so a
database-backed `GraphStore` can be injected by an application.

### `pilot`

The pilot is the leader-elected consumer. While it owns the coordinator lease,
it consumes command, task, and task-event logs, invokes the actuator and
scheduler, acknowledges offsets, and performs timeout recovery. Standby pilots
wait for the current lease to expire or be released.

### `agent`

The agent is the execution process. `Agentic` combines an execution `Agent`, an
HTTP `Client`, and `Settings` to poll for jobs, run containers, persist job
results, and report terminal task events. The current default execution
provider is Docker through Bollard.

`JobFs` gives each job a durable directory under the configured workdir. The
filesystem and UUID-based container names allow an agent to recover jobs after
restart.

## Workspace layout

```text
crates/
  graph/       graph traits, DAG types, validation, diffing, and graph store
  event/       command/task/task-event logs and polling contracts
  coordinator/ leader-election traits and MySQL lease implementation
  runtime/     command application, run state, and MySQL backend
  scheduler/   actuation, jobs, configuration, and scheduling destinations
server/        Axum API binary and reusable application state
pilot/         leader-elected consumer binary
agent/         container execution and HTTP polling binary
graphs/        graphs embedded by the server's static graph store
doc/           architecture, design, module, and usage documentation
rayflow.sql    MySQL schema for all persistent models
```
