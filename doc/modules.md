# Modules

Rayflow is a Cargo workspace. The library crates contain the runtime contracts
and persistence behavior; the `server`, `pilot`, and `agent` packages assemble
those pieces into processes.

## Library crates

### `catalog`

`catalog` defines `Catalog` and `File` metadata traits; `model::Catalog` and
`model::File` are their owned Toasty records, linked by `catalog_uuid`.
The traits expose core metadata; database IDs and all lifecycle timestamps live
on the persistence models. Catalog URLs and file full names are unique;
deletion removes metadata rows directly.
`CatalogSystem` owns `CatalogId<'a>`, `CatalogData<'a>`, and `CatalogOptionalData<'a>`.
`FileSystem: CatalogSystem` adds `FileId<'a>`, `FileData<'a>`, and `FileOptionalData<'a>`,
sharing the catalog system's error type. Stores can use their own identities and return
owned records or borrowed views. Upserts accept data, resolve URL/full-name
conflicts, and return IDs. Reads include loading a
file with its catalog by ID or full name.
The optional-data GATs let stores choose owned or borrowed write inputs. MySQL
uses concrete `CatalogOptionalData` and `FileOptionalData<Uuid>` structs:
omitted fields are preserved, URL/full name selects the row for upserts, and new
rows require their other metadata. `update_catalog(id, data)` and
`update_file(id, data)` apply partial data to existing identities without requiring
URL/full name. Both stores use Toasty selects, updates, and inserts.
The MySQL implementations use Toasty databases and transactions. `ObjectStorage`
extends `FileSystem` with a provider signing contract for upload and download URLs.
The optional `s3` feature provides `s3::S3<S>`, which delegates
metadata to an existing filesystem and signs S3 PUT/GET URLs with AWS SigV4.
Register `catalog::models()` and apply current migrations. See [Catalog and object
storage](catalog.md) for deletion behavior, examples, and the adapter
contract.

### `graph`

The graph crate is the backend-independent graph model. It contains:

- `Graph` and `Id` traits for graph identity, nodes, edges, and derived graph
  queries.
- `GraphStore::load` for reading published graph versions. The trait and its
  implementations use native `async fn`. Server handlers that need `Send`
  futures require `S::load(..): Send` and `S::graphs(..): Send` with return-type notation.
- `GraphStore::graphs` and `DraftGraphStore::list` for cursor pagination, sharing
  `PageRequest` and returning `Page<Self::G>`. Database stores, the HTTP client,
  and caches all return complete graph definitions, with a separate next-page
  cursor.
- `DraftGraphStore`, extending `GraphStore` with native async `read`, `write`,
  and `publish` methods. It shares the graph and error types; reads return a
  graph, while writes and publishes return `()`.
- `types::DAG`, `Node`, `Input`, `Output`, and `Kind` (`File` or `Directory`).
  `DAG` contains a node count and audit fields populated from database records;
  its audit fields are excluded from JSON.
- `validator`, which checks cycles, edge endpoints, and the single-root rule;
  unused outputs are reported separately.
- `diff`, which compares graph values and reports structural changes.
- `store`, which implements both traits directly for `toasty::Db` and
  `toasty::Transaction` through `Executor`, using mutable receivers. It persists
  graph JSON and audit metadata in the `graph` and `graph_draft` tables.

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
- `CommandResult` stores a stable global error code and message for permanent
  command-application failures before their command entries are acknowledged.
- `Job`, `JobRead`, and `JobWrite` define job persistence and lifecycle.
- `Scheduler` abstracts destinations such as direct agents or external
  schedulers.
- `AgentScheduler` and `AgentRef` implement resource-aware polling and guarded
  job claims.
- `RunConfig` holds annotation snapshots; jobs select the newest ID for their
  run and retain that reference. Execution version and revision live on `Run`.
  `NodeConfig` pins node configuration to jobs. The publish API snapshots
  `Node.settings` into `NodeConfig` at the published graph version.

The default `AgentScheduler` handles timeout recovery while agents claim jobs
through the server API. External scheduler adapters must make submissions
durable and reconcile remote work after a transaction rollback or uncertain
response.

## Binaries

### `server`

The server is an Axum HTTP API. It appends run commands and task events, polls
and reports jobs, and reads the current graph state. `App` is also available as
a library abstraction over a `toasty::Db`, a `Config`, and any `GraphStore`.

The binary uses `toasty::Db` directly, so published graph versions and editable
drafts come from the database. The server library remains graph-store agnostic,
so another `GraphStore` can be injected by an application.

HTTP handlers and application operations are grouped by resource: `graph`,
`graph_draft`, `run`/`state`, `job`, `agent`, and `catalog`. Collection operations share
`app::pagination::{PageQuery, Page}`. Graph endpoints use `GraphStore` and
`DraftGraphStore` and return complete DAGs without exposing persistence records.
Draft writes/publishing, run commands, and job actions reuse their existing
application methods. `app::AgentInstance` records poll observations in
`agent_instance`, registered by `server::models()`. Each observation commits or
rolls back with the poll transaction. Agent instances do not determine resource
matching or scheduling eligibility.

### `pilot`

The pilot is the leader-elected consumer. While it owns the coordinator lease,
it consumes command, task, and task-event logs, invokes the actuator and
scheduler, acknowledges offsets, and performs timeout recovery. Standby pilots
wait for the current lease to expire or be released.

`pilot::models()` combines scheduler and graph models, including leader election,
into one set for the database builder. Pass the combined set to `models()` once;
calling the builder's `models()` repeatedly replaces the previous registration.

### `agent`

The agent is the execution process. `Agentic` combines an execution `Agentd`, an
HTTP `Client`, and `Settings` to poll for jobs, run containers, persist job
results, and report terminal task events. The current default execution
provider is Docker through Bollard.

`JobFs` gives each job a durable directory under the configured workdir. The
filesystem and UUID-based container names allow an agent to recover jobs after
restart. `Filed` resolves artifact keys and producer versions, downloads declared
inputs, and publishes outputs using `/catalog/prepare`, `/catalog/commit`, and
`/catalog/download`. Each request supplies job/agent IDs, the key, and metadata
in JSON. The server stores metadata and signs URLs within the claimed run.
Transfers finish before execution and success reporting respectively. Its separate
`FileFs` stores bytes under `data_dir/<run_uuid>/<node_id>/<revision>/`, retaining
producer outputs after job acknowledgement. Docker mounts `inputs/` read-only
and `outputs/` writable. Directory outputs use an internal JSON manifest. See
[Catalog](catalog.md#http-api-and-agent-transfers) for configuration and protocol.

## Web UI

`ui/` is a React/TypeScript application built with Vite and React Flow. The
`Orchestration`, `Runtime`, `Schedule`, and `Agents` components implement the
four workspace sections. `FlowCanvas` is shared by the graph editor and run
inspector; `api.ts` shares HTTP access, refresh, and pagination behavior.

The UI calls the server's resource routes through a same-origin `/api` proxy.
It does not introduce a separate console API or duplicate workflow execution
logic. See [Usage](usage.md#web-ui) for development, hosting, and integration
test commands.

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
ui/            React workflow editor and runtime, job, and agent management
graphs/        sample graphs available to tests and library consumers
doc/           architecture, design, module, and usage documentation
rayflow.sql    MySQL schema for all persistent models
```
