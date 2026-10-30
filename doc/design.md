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

`graph::store::MySqlGraphStore` persists each `(uuid, version)` as a graph row.
`load(uuid, Some(version))` selects an exact version; `load(uuid, None)` selects
the highest version. The JSON payload contains the complete DAG, while row
metadata is checked against its UUID, name, and version when loading.

The same store keeps one editable `graph_draft` row per graph UUID. Draft writes
replace the JSON payload and update audit metadata without requiring the graph
to be valid. Publishing validates the draft, assigns the next graph version,
and inserts an immutable row into `graph`.

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

`Scheduler<B>` abstracts job destinations. The default `AgentScheduler` fails jobs
whose expiry is beyond the configured grace period. External implementations may
submit jobs to systems such as Kubernetes or Volcano, but must use durable request
records and reconcile retries after rollback.

`RunConfig` stores the run configuration revision and graph version. Node
configuration stores resource requirements and the required positive job lease.
When a task becomes a job, the latest applicable node configuration and run
configuration are pinned to that job.

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
