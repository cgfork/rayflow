# Design

## Graphs

`graph::Graph` is backend-independent. It exposes graph identity and version,
node and edge iteration, mutation, and derived predecessor/successor queries.
`graph::types::DAG` is the concrete UUID-based implementation.

`validator::validate(&graph)` rejects:

- directed cycles, including cycles in disconnected components;
- edges whose endpoints are not graph nodes; and
- graphs that do not contain exactly one root node.

Unused outputs are not validation errors. `validator::unreferenced_outputs` reports
them by matching input sources in `node_name.file_name` form. Optional inputs count
as references when they name an output.

`toasty::Db` and `toasty::Transaction` implement `GraphStore` and
`DraftGraphStore` directly through a shared `Executor` implementation.
Store methods take `&mut self` and persist each `(uuid, version)` as a graph row.
Graph store traits use native `async fn` without imposing `Send` on every
returned future. Generic server routes require `S::load(..): Send` and
`S::graphs(..): Send` through
return-type notation. The server enables `#![feature(return_type_notation)]`,
and `rust-toolchain.toml` pins the nightly used to build the workspace.
`load(uuid, Some(version))` selects an exact version; `load(uuid, None)` selects
the highest version. The JSON payload contains the complete DAG, while row
metadata is checked against its UUID, name, and version when loading.

`DAG` includes a node count and the `updated_at`, `updated_by`, `created_at`,
and `created_by` audit fields. Audit fields are skipped during JSON serialization
and deserialization; stores populate them from the record columns when loading,
reading, or listing graphs. Node counts are recalculated from the graph nodes.

The same store keeps one editable `graph_draft` row per graph UUID. Draft writes
replace the JSON payload and update audit metadata without requiring the graph
to be valid. Publishing validates the draft and assigns one more than the
maximum published version for that graph UUID, starting at version 1. The draft's
version does not affect allocation. It inserts an immutable row into `graph`.
Publishing on a database starts a
transaction; publishing inside a transaction uses a savepoint, leaving the
outer commit or rollback to the caller.

Both listing methods accept a `PageRequest` (default 50, accepted
limits 1–200). Database pages use descending row IDs and an exclusive `before`
cursor. `GraphStore::graphs` includes all published versions and optionally
filters by UUID; `DraftGraphStore::list` lists one editable row per UUID. Each
query fetches at most `limit + 1` rows and returns `next_cursor` only when a
further page exists. Both methods return `Page<Self::G>` with complete graph
definitions and validate stored JSON against row metadata. Transactions list
their own uncommitted writes. Server collection endpoints call the store traits
and return `Page<DAG>`. Individual reads, draft writes, and publishing return
`DAG` directly. The HTTP client reads each graph page in one request.

```rust,ignore
use graph::{DraftGraphStore, GraphStore, PageRequest};

let page = GraphStore::graphs(&mut db, PageRequest::default(), Some(graph_id)).await?;
if let Some(cursor) = page.next_cursor {
    let next = GraphStore::graphs(
        &mut db, PageRequest::new(Some(cursor), 50)?, Some(graph_id),
    ).await?;
}
let drafts = DraftGraphStore::list(&mut db, PageRequest::default()).await?;
```

Cursors are specific to a store and filter. In-memory registries and filesystem
caches use positions in a sorted snapshot, so their contents should stay fixed
while traversing pages. Database cursors remain stable when newer rows arrive.

## Diffing

`Diff` returns `None` for equal values and `Some(Item)` for changes. `Kind`,
`Input`, `Output`, and `Node` return before/after pairs. Graph diffing matches
nodes by ID, reports changed payloads, and separately reports missing/extra nodes
and edges. Graph metadata and iteration order do not affect structural diffs.

## Commands

Commands are applied by `runtime::CommandApply`:

| Command | Effect |
| --- | --- |
| `Start { nodes }` | Creates a run and initial ready node runs; empty `nodes` select graph roots. |
| `Rerun { node }` | Advances the run revision and creates a ready node run and task. |
| `Dryrun { nodes }` | Mocks non-target predecessors as succeeded and prepares targets. |
| `Run { node }` | Changes `Ready` to `Running` without creating another task. |
| `Pend { node }` | Changes `Ready` or `Running` to `Pending` after a yield. |
| `Resume { node }` | Changes `Pending` to `Ready` and emits a new task. |
| `Succeed { node }` | Succeeds the node and prepares runnable successors. |
| `Fail { node }` | Changes `Pending` or `Running` to `Failed`. |
| `Cancel { node }` | Changes `Pending` or `Running` to `Terminated`. |
| `Terminate { node }` | Changes `Pending` or `Running` to `Terminated`. |

`runtime::Backend::apply_in` accepts a caller-owned transaction so command effects
and the command-log acknowledgement can commit atomically. Consumers must
serialize command application.

The `run` table has a unique `(data_id, graph_id)` key. A start pins the published
graph version, and publishing another version does not permit a second run for
the same pair. Use rerun for an existing run or a new data identifier for another
execution.

## Event Logs

`CommandEntry`, `Task`, and `TaskEventEntry` are append-only records. `Emit` adds
records, `Read` loads by identity, and `Poll` reads unacknowledged records in
offset order. `ack` advances shared state in `poll_offset`; it never deletes log
records.

The three logs have independent cursors. Missing auto-increment offsets are
skipped only after the configured gap timeout. `event::Reactive` provides a
continuous poll, handle, and acknowledge loop; handlers must tolerate replay.

## Actuation and Scheduling

`Actuator::tick_in` consumes commands, tasks, and task events in one caller-owned
transaction. It applies commands, creates jobs from tasks, activates ready nodes,
settles terminal jobs, and applies task-event commands.

Tasks whose node run or owning run no longer exists are acknowledged with a
warning and remain in the append-only task log. They create no job and do not
block valid commands or tasks. Other errors still roll back the consumer pass.

`Scheduler<B>` abstracts job destinations. The default `AgentScheduler` fails jobs
whose expiry is beyond the configured grace period. External implementations may
submit jobs to systems such as Kubernetes or Volcano, but must use durable request
records and reconcile retries after rollback.

`RunConfig` stores the run configuration revision and graph version. Each graph
node carries JSON task configuration, resource requirements, and a duration
timeout. Publishing snapshots these settings to `NodeConfig`, keyed by graph
version; job creation selects the latest node configuration no newer than the
run's pinned version. This keeps historical executions reproducible.

## Agent Claims

`AgentRef::poll` receives total CPU and memory capacity, including active claims.
It redelivers inactive `Polled` jobs and claims fitting `Scheduled` jobs in one
transaction. Existing claims reserve resources even outside the delivery limit;
IDs in `active_jobs` are excluded. Failed polls roll back claims and resource
accounting. Guarded updates prevent concurrent claims.

Job expiry is an absolute timestamp derived from the pinned node lease. Active
jobs are not renewed by polling. Expired jobs are failed by the agent or by the
pilot after `job_timeout_gap`. Terminal events clear ownership while preserving
the terminal outcome.
