# Usage

This guide covers local development, configuration, HTTP operations, and the
durable files used by agents.

See [Authentication](authentication.md) for user login, company SSO, LDAP, and
agent credentials. Authentication is opt-in; enable it before exposing the
console or API outside a trusted development environment.

## Prerequisites

Rayflow uses Rust edition 2024 and MySQL. Install rustup; Cargo selects the
nightly pinned in `rust-toolchain.toml`, including rustfmt and Clippy. The server
requires nightly for `#![feature(return_type_notation)]`, which lets its generic
HTTP handlers require `S::load(..): Send` while `GraphStore` uses native
`async fn` declarations.

Initialize the MySQL database before starting the server or pilot. The script
requires Cargo, plus permission to create the database and create or alter its
tables:

```sh
export DATABASE_URL='mysql://root:password@127.0.0.1:3306/rayflow'
./init-db.sh
```

`init-db.sh` runs `cargo run -p server --bin init-db`. It loads `rayflow.yaml`,
`RAYFLOW_CONFIG`, and environment overrides through the server's existing
configuration loader. `DATABASE_URL` can override the configured connection.
It creates the database if needed and applies the embedded Toasty `MigrationSet`
from `server/src/migration.rs`. Applied migration IDs are recorded in
`__toasty_migrations`, so subsequent runs skip completed migrations. Node.js and
the MySQL command-line client are not required.

The Toasty models define the application schema. `rayflow.sql` documents its
current SQL form, and versioned files in `server/migrations/` describe database
changes. The initial migration creates all application tables. For later schema
changes, add a new migration file with a new ID in `MIGRATIONS` and update
`rayflow.sql`; do not edit migrations already applied to a database.
Migration 4 adds `command_result` for permanently failed command applications.
Migration 5 stores stable error codes and messages separately, preserving errors
recorded by migration 4.

Stop the server, pilot, and agents before initializing an existing database.
The script can be rerun and preserves existing rows. In addition to creating
missing tables, it adds zero defaults for obsolete `job.assignment_version` and
`job.steal_count` columns if they still exist without defaults. This allows the
current job inserts to work with the older schema while preserving old values.
It does not add these retired columns to new databases.

For existing databases using the previous versioned run key, the script adds
the unique `(data_id, graph_id)` key. If duplicate pairs exist, it stops with an
error; resolve those records explicitly before rerunning. Inspect duplicates with:

```sql
SELECT data_id, graph_id, COUNT(*) AS run_count
FROM run GROUP BY data_id, graph_id HAVING COUNT(*) > 1;
```

MySQL DDL can commit before a migration finishes. A failed migration is not
recorded as applied, and these initialization migrations can be retried after
resolving the reported error.

Migration 22 resumes partially completed UUID changes, including existing
configuration UUID columns, indexes, and temporary job reference columns. If an
earlier attempt stopped and a retry reports `Duplicate column name 'uuid'`,
update to the fixed initializer and rerun `./init-db.sh`; existing UUIDs are
preserved. Legacy zero references for absent optional node or run configurations
become nil UUIDs. Missing nonzero references, or a missing owning node run,
still stop the migration before it removes the numeric references; repair those
references explicitly and retry.

Migration 23 adds durable ownership and catalog associations for job result
uploads. Apply it before starting servers that expose `/job/result/prepare`
and `/job/result/commit`.

Do not commit credentials in `rayflow.yaml`. Use an environment variable or a
local, untracked configuration file for `database_url`.

## Local startup

The launcher requires Bash, Cargo, curl, and the Node.js/npm versions described
in [Web UI](#web-ui).

After configuring MySQL and running `./init-db.sh`, run this command from the
repository root:

```sh
./dev.sh
```

The script installs UI dependencies when needed, builds the Rust binaries, and
starts the API server, pilot, and Vite UI. It waits for the API and the UI proxy
to become ready, then prints their addresses. The default UI address is
`http://127.0.0.1:5173`, and the API listens on `127.0.0.1:5172`. Configuration
uses the existing `rayflow.yaml` and environment overrides; the script does not
create or migrate the database.

The launcher adds loopback addresses and the selected local host to its
services' proxy exclusions so they can reach the local API and UI directly.

For example, select a database explicitly or change the local ports:

```sh
DATABASE_URL=mysql://root:password@127.0.0.1:3306/rayflow ./dev.sh
RAYFLOW_DEV_API_PORT=8081 RAYFLOW_DEV_UI_PORT=5174 ./dev.sh
```

To also run a local execution agent, start Docker and use:

```sh
./dev.sh --agent
```

The script passes `RAYFLOW_AGENT_SERVER_URL` directly to the agent, using
`http://127.0.0.1:5172` or the port selected by `RAYFLOW_DEV_API_PORT`. You do not
need to add `server_url` to the configuration file for `./dev.sh --agent`.
The agent logs the resolved URL in `.rayflow/dev/agent.log` when it starts.
Unless `RAYFLOW_AGENT_ID` is set, the script generates a stable UUID in
`.rayflow/dev/agent-id` and reuses it across restarts. Other agent settings use
the existing configuration. Configure each node's
Docker image, CPU and memory requirements, and timeout in Orchestration before
publishing, then start a run from Runtime.

Logs are written to `.rayflow/dev/{server,pilot,ui,agent}.log` and replaced on
each startup. Press Ctrl-C to stop the processes started by the script. If a
service exits unexpectedly or startup times out, the script stops its remaining
services and exits with an error. Agent job containers retain the normal agent
shutdown/recovery behavior. Use `./dev.sh --help` for options.

The server, pilot, and agent use `tracing` to log to stderr, with `info` as the
default level. Set `RUST_LOG` to change the level or filter individual crates:

```sh
RUST_LOG=info,pilot=debug,agent=debug ./dev.sh --agent
```

## Build and test

```sh
cargo build
cargo check --workspace
cargo test --workspace
cargo fmt
cargo clippy
```

The pilot and server workflow tests skip when `DATABASE_URL` is absent. They
create, migrate, and drop a separate schema for each test. Supply credentials
with permission to create and drop test schemas.

```sh
export DATABASE_URL=mysql://root:password@127.0.0.1:3306/rayflow
cargo test -p pilot --test workflow
cargo test -p server --test workflow_http
```

The Docker workflow test runs the HTTP server, pilot, and agent together. Set
`RAYFLOW_TEST_DOCKER_IMAGE` to a locally available image with `sh` and `sleep`.
The agent's Docker lifecycle test uses the same image.
Add `127.0.0.1` to `NO_PROXY` if the environment uses an HTTP proxy.

```sh
export RAYFLOW_TEST_DOCKER_IMAGE=busybox:1.36
export NO_PROXY="${NO_PROXY:+$NO_PROXY,}127.0.0.1"
cargo test -p pilot --test e2e -- --ignored
cargo test -p agent --test lifecycle -- --ignored
```

## Configuration

Configuration is layered from defaults, an optional `rayflow.yaml` file, and
environment variables. An explicit file can be selected with `RAYFLOW_CONFIG`;
the pilot and agent also accept their component-specific selectors.

Duration fields use strings such as `10s`, `250ms`, and `1.5s`. Integer fields
whose names end in `_ms` remain integer milliseconds.

### Server

The server reads `addr`, `database_url`, `poll_timeout`, and
`poll_retry_interval`:

```yaml
addr: 0.0.0.0:8080
database_url: mysql://root:password@127.0.0.1:3306/rayflow
poll_timeout: 30s
poll_retry_interval: 250ms
```

Defaults and environment overrides are:

| Field | Default | Environment |
| --- | --- | --- |
| `addr` | `0.0.0.0:8080` | `RAYFLOW_ADDR` |
| `poll_timeout` | `30s` | `RAYFLOW_POLL_TIMEOUT` |
| `poll_retry_interval` | `250ms` | `RAYFLOW_POLL_RETRY_INTERVAL` |
| `database_url` | empty | `DATABASE_URL` or `RAYFLOW_DATABASE_URL` |

`poll_timeout` must be at least `2ms`; `poll_retry_interval` must be at least
`1ms`. Start the server with:

```sh
RAYFLOW_DATABASE_URL=mysql://root:password@127.0.0.1:3306/rayflow \
  cargo run -p server
```

### Pilot

The pilot uses the same configuration file and reads:

```yaml
database_url: mysql://root:password@127.0.0.1:3306/rayflow
batch: 64
poll_interval_ms: 250
leader_name: rayflow
leader_lease_ms: 10000
gap_timeout_ms: 10000
job_timeout_gap: 10s
```

`RAYFLOW_PILOT_CONFIG` selects the pilot file and takes precedence over
`RAYFLOW_CONFIG`. `RAYFLOW_PILOT_*` variables override pilot settings; the
unprefixed `DATABASE_URL` is also accepted. `poll_interval_ms` must be less
than `leader_lease_ms`.

Run a pilot with:

```sh
DATABASE_URL=mysql://root:password@127.0.0.1:3306/rayflow cargo run -p pilot
```

Only the pilot holding the named coordinator lease consumes the global logs.
Run multiple pilots for failover, but give them access to the same database and
use compatible lease and poll settings.

### Agent

The agent identity is stable across restarts. Set `id` explicitly when managing
multiple agents, or omit it and let the first startup generate it in
`<workdir>/.agent-owner.json`; later starts recover that UUID. The other
settings have defaults. `server_url` is the server base URL; the agent appends
`/api/v1` automatically.

```yaml
id: 12345678-1234-4234-8234-123456789abc
server_url: http://127.0.0.1:8080
workdir: .rayflow/jobs
data_dir: .rayflow/data
batch: 64
poll_interval: 1s
request_timeout: 60s
multipart_threshold: 67108864
resources:
  cpu_cores: 4
  memory_bytes: 4294967296
```

`RAYFLOW_AGENT_CONFIG` selects an explicit agent configuration file and
otherwise `RAYFLOW_CONFIG` is used. Environment variables use the
`RAYFLOW_AGENT_` prefix. Nested resource fields use double underscores, for
example `RAYFLOW_AGENT_RESOURCES__CPU_CORES=2`.
Files larger than `multipart_threshold` bytes use multipart upload and ranged
download; the default is 64 MiB.

Run the default Docker-backed agent with Docker available:

```sh
RAYFLOW_AGENT_ID=12345678-1234-4234-8234-123456789abc \
  RAYFLOW_AGENT_SERVER_URL=http://127.0.0.1:8080 \
  cargo run -p agent
```

Each agent needs a unique stable ID, a persistent workdir, and a persistent
`data_dir` for artifacts. Set `RAYFLOW_AGENT_DATA_DIR` to override the latter.
The agent uses
Docker's standard environment variables, including `DOCKER_HOST`, when a
non-default Docker endpoint is required.

## Web UI

The React/TypeScript application lives in `ui/`. Use Node.js 20.19+ on the 20.x
line, or 22.12+ on a newer line, with npm. Start the API server, then run:

```sh
npm ci --prefix ui
npm run dev --prefix ui
```

Open `http://127.0.0.1:5173`. Vite forwards `/api/v1/*` requests to
`http://127.0.0.1:8080/api/v1/*`. To use another API server:

```sh
RAYFLOW_API_URL=http://127.0.0.1:8081 npm run dev --prefix ui
```

The four sections use the same resource APIs as other clients:

- **Orchestration:** create graphs, connect nodes, edit inputs and outputs,
  save drafts, and publish immutable versions. The JSON editor supports import
  through pasted JSON and export. The editor flags cycles and invalid roots;
  unreferenced outputs are informational. Publishing also runs the existing
  server validator. Node positions are local to the open editor.
- **Runtime:** start runs and inspect every node in a run's pinned graph version.
  Use **Rerun** in the run list or detail header to choose a starting node, or
  select a node on the graph and use **Rerun node**. Ready, succeeded, failed,
  and terminated nodes can rerun using their latest state at or before the
  current revision; pending nodes in the current revision can resume. Rerun
  keeps the data ID and published graph version and creates a new revision
  one higher than the run's current revision, preserving earlier node runs.
  These actions append commands; the pilot applies them
  asynchronously. Historical run versions are read-only because command
  endpoints target a data/graph pair.
- **Schedule:** filter jobs by status, run, or owning agent. Cancel and terminate
  use the existing job APIs and require an active claim. The agent reconciles
  lost ownership or expiry before stopping its container.
- **Agents:** inspect online/offline status, reported resource budgets, last
  activity, and reported active job counts; open the agent's claimed jobs.

Runtime, job, and agent views refresh every five seconds while visible.
Lists use cursor pagination; text search and counts cover the displayed page.
Runtime requires a published workflow selected in the header. Search workflows
by name or ID; the selected workflow stays visible in the search bar. Runs,
pagination, and refreshes are scoped to that workflow across its published
versions. Opening Runtime from a published workflow selects it automatically.
Start a run uses the selected workflow's latest published version. Publishing
snapshots node settings into versioned execution configuration, so each run
uses the settings from its pinned graph version.

Node execution settings are stored together under `settings`:

```json
{
  "settings": {
    "config": {
      "image": "alpine:3.21",
      "command": ["sh", "-c", "echo hello"],
      "env": ["MODE=batch"],
      "working_dir": "/tmp"
    },
    "resource": { "cpu_cores": 1, "memory_bytes": 268435456 },
    "timeout": "10s"
  }
}
```

Docker requires `image`; `command`, `env`, and `working_dir` are optional.
The JSON editor preserves additional configuration keys. The Docker provider
currently uses these four fields and applies CPU/memory limits from `resource`.
The UI displays memory in MiB; JSON stores bytes. Timeouts accept duration
strings such as `10s`, `250ms`, and `2m`, with positive whole-millisecond precision.
Timeouts use the existing job lease: delivery starts the expiry clock, and
active polls do not extend it. Older graph JSON without settings remains
readable but needs valid settings before republishing.

Choose an input's source from another node's declared outputs. Selection creates
the dependency when needed and excludes sources that would create a cycle.
Producer and output renames update references; removing an output clears its
bindings so they must be selected again before publishing. Unused outputs remain
allowed. The publish API writes the graph and node configuration in one
transaction; direct `DraftGraphStore::publish` callers must also provision
`NodeConfig` before execution.

The sidebar's audit identity is stored locally in the browser and sent as the
draft/publish `actor`. It is audit metadata, not authentication. Deploy the UI
and API behind your existing access controls.

### Production hosting

```sh
npm run build --prefix ui
```

Serve `ui/dist/` at the site's root and proxy `/api/v1/` to the Rayflow server,
preserving the `/api/v1` prefix. Alternatively, build the server with `embed-ui` to serve the UI assets directly.
`RAYFLOW_API_URL` configures only the Vite development proxy. For example, inside
an nginx `server` block:

```nginx
root /srv/rayflow/ui/dist;

location / {
    try_files $uri $uri/ /index.html;
}

location /api/v1/ {
    proxy_pass http://127.0.0.1:8080;
    proxy_read_timeout 90s;
}
```

Use HTTPS for deployments beyond localhost; the editor generates UUIDs with
the browser's secure-context `crypto.randomUUID()` API.

## HTTP API

Application operations return errors with an HTTP status and a JSON body:

```json
{
  "error_code": "RAYFLOW_RUN_NOT_FOUND",
  "error": "run not found: 00000000-0000-0000-0000-000000000001"
}
```

`error_code` is the stable business identifier; use it to decide how to handle
an error. `error` is the human-readable message and may include resource IDs or
validation details. Existing clients can keep displaying `error`. The status
and code also reflect errors wrapped by storage, runtime, or scheduler adapters.

| HTTP status | Business error codes |
| --- | --- |
| `400 Bad Request` | `RAYFLOW_INVALID_REQUEST`, `RAYFLOW_GRAPH_ID_MISMATCH`, `RAYFLOW_INVALID_GRAPH`, `RAYFLOW_INVALID_UPGRADE_VERSION`, `RAYFLOW_INVALID_UPGRADE_NODE`, `RAYFLOW_INVALID_TASK_EVENT` |
| `400 Bad Request` | `RAYFLOW_INVALID_CATALOG_URL`, `RAYFLOW_INVALID_CATALOG_FIELD`, `RAYFLOW_MISSING_CATALOG_FIELD`, `RAYFLOW_INVALID_ARTIFACT_PATH`, `RAYFLOW_INVALID_STORAGE_CONFIGURATION`, `RAYFLOW_INVALID_OBJECT_URL`, `RAYFLOW_INVALID_PRESIGN_EXPIRY`, `RAYFLOW_INVALID_MULTIPART_OPERATION` |
| `404 Not Found` | `RAYFLOW_RUN_NOT_FOUND`, `RAYFLOW_NODE_RUN_NOT_FOUND`, `RAYFLOW_NODE_NOT_FOUND`, `RAYFLOW_TASK_NOT_FOUND`, `RAYFLOW_JOB_NOT_FOUND`, `RAYFLOW_GRAPH_NOT_FOUND`, `RAYFLOW_GRAPH_DRAFT_NOT_FOUND` |
| `404 Not Found` | `RAYFLOW_ARTIFACT_NOT_FOUND`, `RAYFLOW_CATALOG_NOT_FOUND`, `RAYFLOW_FILE_NOT_FOUND`, `RAYFLOW_COMMAND_NOT_FOUND`, `RAYFLOW_TASK_EVENT_NOT_FOUND` |
| `409 Conflict` | `RAYFLOW_JOB_CONFLICT`, `RAYFLOW_JOB_RESULT_OWNER_UNKNOWN`, `RAYFLOW_RUN_ALREADY_STARTED`, `RAYFLOW_INVALID_TRANSITION`, `RAYFLOW_CONCURRENT_UPDATE` |
| `500 Internal Server Error` | `RAYFLOW_DATABASE_ERROR`, `RAYFLOW_GRAPH_ERROR`, `RAYFLOW_GRAPH_METADATA_MISMATCH`, `RAYFLOW_GRAPH_DRAFT_METADATA_MISMATCH`, `RAYFLOW_GRAPH_VERSION_OVERFLOW`, `RAYFLOW_STORAGE_ERROR` |
| `500 Internal Server Error` | `RAYFLOW_INVALID_JOB_TIMEOUT_GAP`, `RAYFLOW_INVALID_JOB_LEASE`, `RAYFLOW_JOB_LEASE_NOT_FOUND` |
| `503 Service Unavailable` | `RAYFLOW_STORAGE_UNAVAILABLE`, `RAYFLOW_STORAGE_CREDENTIALS_EXPIRED` |

The lease errors above indicate invalid or missing persisted job configuration.
Rust consumers can inspect `AppError::status_code()` and `AppError::code()`;
Axum handlers use its `IntoResponse` implementation to serialize the same mapping.

The service endpoints are:

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/` | Embedded UI, or `rayflow` without `embed-ui`. |
| `GET` | `/health`, `/api/v1/health` | Liveness probe; returns `ok`. |
| `GET` | `/metrics` | Prometheus text metrics for the server process. |
| `GET` | `/api/v1/graphs` | List published graph versions; optional `graph_id` filter. |
| `GET` | `/api/v1/graphs/{graph_id}` | Read a published graph; optional exact `version`, otherwise latest. |
| `GET` | `/api/v1/graphs/{graph_id}/shared-nodes` | List shared restart node UUIDs between exact published `from_version` and newer `version`. |
| `GET` | `/api/v1/graph_drafts` | List editable DAGs. |
| `GET` | `/api/v1/runs` | List runs; optional `graph_id` filter. |
| `GET` | `/api/v1/runs/{run_id}` | Inspect the exact run UUID, its state, and its pinned graph. |
| `POST` | `/api/v1/runs/start` | Start a run; body has `data_id`, `graph_id`, and optional `nodes`. |
| `POST` | `/api/v1/runs/rerun` | Rerun one node; body has `data_id`, `graph_id`, and `node`. |
| `POST` | `/api/v1/runs/upgrade` | Upgrade an existing run; body has `data_id`, `graph_id`, the newer published `version`, and a `node` selected from the shared nodes. |
| `POST` | `/api/v1/runs/resume` | Resume one pending node; body has `data_id`, `graph_id`, and `node`. |
| `POST` | `/api/v1/runs/dryrun` | Prepare targets while mocking predecessors; body has `data_id`, `graph_id`, and optional `nodes`. |
| `GET` | `/api/v1/runs/graph_state` | Read state with `data_id` and `graph_id` query parameters. |
| `GET` | `/api/v1/graphs/{graph_id}/draft` | Read the editable graph draft. |
| `PUT` | `/api/v1/graphs/{graph_id}/draft` | Create or replace the draft. |
| `POST` | `/api/v1/graphs/{graph_id}/publish` | Validate and publish the draft as the next graph version. |
| `POST` | `/api/v1/task_events` | Emit an event for an existing task; body has `task_id` and `event`. |
| `GET` | `/api/v1/jobs` | List jobs; optional `status`, `agent_id`, and `run_id` filters. |
| `GET` | `/api/v1/agents` | List agent instances; optional `online=true` or `online=false`. |
| `POST` | `/api/v1/jobs/poll` | Long-poll and claim jobs that fit an agent's resources. |
| `POST` | `/api/v1/job/cancel` | Cancel a claimed job. |
| `POST` | `/api/v1/job/yield` | Yield a claimed job and defer the node. |
| `POST` | `/api/v1/job/succeed` | Complete a claimed job successfully. |
| `POST` | `/api/v1/job/fail` | Report a claimed job failure. |
| `POST` | `/api/v1/job/terminate` | Terminate a claimed job. |
| `POST` | `/api/v1/job/result/prepare` | Prepare a job's result catalog and return its upload signature; body has `job_id`, `agent_id`, and `size`. |
| `POST` | `/api/v1/job/result/commit` | Acknowledge the uploaded result; body has `job_id`, `agent_id`, and `catalog_uuid`. |
| `POST` | `/api/v1/job/output/prepare` | Create or reset an output catalog and return its upload signature. |
| `POST` | `/api/v1/job/output/commit` | Mark an uploaded output catalog as uploaded and publish its file. |
| `POST` | `/api/v1/job/input/presign` | Return a signed download URL for a declared input. |
| `POST` | `/api/v1/runs/{run_id}/files/download` | Return a signed download URL for a succeeded declared input or output. |
| `POST` | `/api/v1/users/resolve` | Resolve valid audit user IDs to display names for the console. |

The server exports `rayflow_http_requests_total` and
`rayflow_http_request_duration_seconds`, labeled by HTTP method, matched route
template, and response status. Route labels use templates rather than raw URLs,
so request IDs do not create unbounded metric cardinality.

The file-download response contains `catalog`, `presigned.url`,
`presigned.operation`, `presigned.headers`, and `presigned.expires_at`. The
browser or client must request the returned absolute URL exactly as provided,
including its query string, and include any returned headers. There is no
Rayflow `/files/content` proxy route; the server does not stream the object
itself.

Run commands and task events return `202 Accepted` with the appended entry ID.
In Runtime, click **Upgrade** on a run or its detail page. Select a newer
published version and an eligible shared restart node, then confirm. The UI
uses the graph's shared-node calculation and the same node-state rules as rerun.
Shared nodes include all upstream ancestors of the fork point, including the root.
To upgrade a run, send `POST /api/v1/runs/upgrade` with a body such as:

```json
{
  "data_id": "30000000-0000-4000-8000-000000000001",
  "graph_id": "10000000-0000-4000-8000-000000000001",
  "version": 2,
  "node": "60000000-0000-4000-8000-000000000003"
}
```

The pilot applies the separate `Upgrade` command after the request is accepted.
It keeps the run UUID and data ID, changes its graph version, and reruns the
selected node using the same rules as ordinary rerun. The node must be in
`target.shared_nodes(&current)` and its latest status must be ready, succeeded,
failed, or terminated. The revision advances once and all descendant node runs,
plus the selected node's records from earlier revisions, are marked
`ineffective = 1`. Their historical statuses remain available, but
their results cannot satisfy dependencies until those nodes run again. New node
runs default to `ineffective = 0`. Ancestors and other branches remain unchanged.
`GET /api/v1/runs/graph_state` and the state in `GET /api/v1/runs/{run_id}` include only effective
node runs, choosing each node's latest effective revision at or before the run's
current revision. Invalidated history remains stored but is omitted from graph state.
`node_run.version`
records historical information and does not control command application.
If the node is not shared or the shared
set is empty, the command records `RAYFLOW_INVALID_UPGRADE_NODE` and leaves the
run unchanged. Ordinary rerun continues using the existing graph version.

Successful job actions return `204 No Content`. Claim conflicts and terminal
state conflicts return `409 Conflict`; missing runs, tasks, or nodes return
`404 Not Found`.

Collection endpoints return `{ "items": [...], "next_cursor": null }` and
accept `limit` (default 50, range 1–200) and `before` (a numeric row ID). Results
are ordered by descending ID. Pass a non-null `next_cursor` as `before` to read
the next page with the same filters. Job status values are `Scheduled`, `Polled`,
`Yielded`, `Succeeded`, `Failed`, and `Terminated`.

Graph and draft lists contain complete DAGs, including `id` (the graph UUID),
`name`, `version`, `node_count`, `nodes`, and `edges`. Numeric database row IDs
are used only for pagination cursors. Graph audit fields (`created_by`,
`created_at`, `updated_by`, and `updated_at`) are included in the serialized DAG;
older payloads that omit them are read with default values.
Run detail returns `{ "run": ..., "graph": ..., "node_runs": [...] }`.
The `run` record contains the run UUID, data ID, graph/version, and revision;
`node_runs` contains effective node-run records joined with their graph nodes.

Example start request:

```sh
curl -X POST http://127.0.0.1:8080/api/v1/runs/start \
  -H 'content-type: application/json' \
  -d '{
    "data_id": "00000000-0000-0000-0000-000000000001",
    "graph_id": "00000001-0000-0000-0000-000000000000",
    "nodes": []
  }'
```

An empty `nodes` array starts graph roots. `graph_state` returns the run
revision, graph edges, and each node's current state.

### Graph drafts

Write a complete draft with `PUT /api/v1/graphs/{graph_id}/draft`:

```sh
curl -X PUT http://127.0.0.1:8080/api/v1/graphs/00000001-0000-0000-0000-000000000000/draft \
  -H 'content-type: application/json' \
  -d '{
    "actor": "editor",
    "graph": {
      "id": "00000001-0000-0000-0000-000000000000",
      "name": "example",
      "version": 0,
      "nodes": [{
        "id": "00000000-0000-0000-0000-000000000002",
        "name": "task",
        "node_type": "script",
        "inputs": [],
        "outputs": []
      }],
      "edges": []
    }
  }'
```

Draft reads and writes return the DAG directly. Drafts may be
temporarily invalid while they are being edited. `POST /api/v1/graphs/{graph_id}/publish`
accepts `{ "actor": "publisher" }`, validates the graph (including the cycle
and single-root rules), assigns the next version, and returns the immutable
published DAG directly. The version is the maximum published version for that
graph UUID plus one, starting at 1. The draft's version is ignored when allocating
the published version. Publishing does not delete the draft.

## Long polling and claims

`POST /api/v1/jobs/poll` accepts this request shape:

```json
{
  "agent_id": "12345678-1234-4234-8234-123456789abc",
  "batch": 64,
  "resources": { "cpu_cores": 4, "memory_bytes": 4294967296 },
  "active_jobs": [],
  "wait_ms": 30000
}
```

`batch` must be between 1 and 1000. `wait_ms` is optional and must be between
zero and `poll_timeout`; `0` performs an immediate poll. When omitted or null,
the server chooses a random wait in the half-open range
`[poll_timeout / 2, poll_timeout)`. The response is `{ "jobs": [...] }`.

When no jobs are available, the server retries short, independent database
transactions at `poll_retry_interval` until the deadline. It never holds a
database transaction or lock while sleeping. If `active_jobs` is nonempty, the
server returns after one attempt so lease acknowledgements and lost claims are
not delayed. Keep one outstanding poll per agent ID and set client, proxy, and
server timeouts above the long-poll wait plus request-processing time.

Existing claims reserve resources. Jobs are delivered at least once, so agents
must tolerate redelivery. Active job IDs are excluded from delivery and do not
renew their expiry. Job expiry comes from the pinned `node_config.lease`; the
server and pilot do not provide a fallback lease.

Terminal job actions accept `{ "job_id": "...", "agent_id": "..." }` and append
the corresponding task event. Repeating a terminal action does not append a
duplicate event. A yielded job releases its claim and moves the node to
`Pending`; `POST /api/v1/runs/resume` creates the next task.

### Agent instances

Each poll attempt upserts the agent UUID, total resource budget, reported
active-job count, and database timestamps into `agent_instance` inside the
same transaction that claims and loads jobs. A failed attempt rolls back both
the observation and job changes. Internal idle retries start fresh transactions
and refresh the observation; retry sleeps occur after transactions finish.

An agent is online until twice `poll_timeout` plus 60 seconds after its last
poll (two minutes with the default settings). Offline rows remain available for
inspection. This is an observation of polling, not a container health check or
scheduling eligibility rule. Existing agents appear after their next poll;
there is no separate heartbeat or notification service.

## Agent workdir

The agent keeps durable job metadata below `workdir`. Each job has an isolated
directory; `job.json` is the source used to restore the job after a restart:

```text
{WORKDIR}/{job-uuid}/job.json
{WORKDIR}/{job-uuid}/lock
{WORKDIR}/{job-uuid}/container.json
{WORKDIR}/{job-uuid}/usage/checkpoint.jsonl
{WORKDIR}/{job-uuid}/result.json
{WORKDIR}/{job-uuid}/usage.json
{WORKDIR}/{job-uuid}/completion.json
{WORKDIR}/{job-uuid}/result.json.upload
{WORKDIR}/{job-uuid}/usage.json.upload
{WORKDIR}/{job-uuid}/cleanup-ready.json
{WORKDIR}/{job-uuid}/pid
{WORKDIR}/{job-uuid}/runtime/started_at
{WORKDIR}/{job-uuid}/graph.json
{WORKDIR}/.agent.lock
{WORKDIR}/.agent-owner.json
```

`container.json`, `result.json`, `pid`, and `runtime/started_at` are written only
when the corresponding state is available. There is no persisted `node.json` or
container stdout file in this directory. The agent writes JSON atomically,
persists results before reporting them, and retains job metadata for recovery.
The `lock` file is held with an exclusive OS lock while a job is being
coordinated. On startup, `JobManager` discovers existing directories and
reconnects to containers by their persisted IDs or their Docker job names.
It resumes pending terminal reports and uploads. Keep this directory across
agent restarts.

The usage monitor consumes each running Linux Docker container's live stats
stream. Each job runs one container once. The monitor appends observations to
the job's `usage/checkpoint.jsonl` file.
If checkpoint writes fail, the agent retries on a timer and retains at most
the 32 newest unflushed observations per job. The lifecycle monitor saves
`result.json` at container exit. The usage monitor gives the stats stream a
bounded window to drain, flushes the final samples, and aggregates the saved
checkpoints outside the job lock. It writes a `JobUsage` record to `usage.json`,
including an empty record when no samples were available. After a restart,
the monitor can aggregate the saved checkpoints or load a summary embedded in
an older result.

- Total CPU is the latest available cumulative CPU time, in nanoseconds, up
  to the last CPU sample. A reset or conflicting counters at the same
  timestamp make this total unavailable.
  Peak CPU is the highest average between adjacent valid samples, rounded
  down to millicores (1000 millicores is one core).
- Peak memory and working set are the highest sampled byte values. Each
  memory total is an estimated integral in byte-seconds, calculated by
  averaging adjacent samples and multiplying by the time between them.
  The summary includes the milliseconds covered by each integral. Intervals
  with missing endpoints or longer than 15 seconds are skipped. No memory
  use is inferred before the first sample or after the last.
- P50, P90, P95, and P99 are included as `cpu_millicores_percentiles`,
  `memory_usage_bytes_percentiles`, and `memory_working_set_bytes_percentiles`.
  Each object has `p50`, `p90`, `p95`, and `p99` fields. CPU uses the valid
  interval rates used for its peak; memory uses the valid measurements at each
  distinct timestamp. Values use the nearest rank: sort the observations and
  select rank `ceil(percentile * count / 100)`, starting at rank one. Samples
  have equal weight regardless of interval duration. Missing or conflicting
  measurements are excluded, and reset-crossing CPU intervals are excluded.
  A single memory observation supplies all four memory percentiles; CPU needs
  two observations forming a valid interval. A metric without valid observations
  omits its percentile object. The metrics are stored under `summary` in
  `usage.json`.

Short jobs and jobs with unavailable statistics can finish without a summary.
Peaks reflect the recorded samples and can miss brief spikes.

The lifecycle monitor validates and commits required outputs before reporting
success. Pending output uploads delay completion. The manager records
the terminal action and the server acknowledgment in `completion.json`.
It archives the final `result.json` and `usage.json` through
`/api/v1/job/result/prepare`, the returned presigned PUT, and
`/api/v1/job/result/commit`. The archive uploads retry independently of job
completion. The agent shares four concurrent archive uploads across both files
and delays retries on failure. A server response indicating that storage is
disabled pauses archive requests for 30 seconds. The manager writes each
`.upload` receipt after the server confirms the expected catalog UUID.
The usage monitor removes the stopped container after saving `usage.json`.
Periodic cleanup removes checkpoints, staged inputs and outputs, and metadata
once the terminal acknowledgment and both archive receipts are present.
`cleanup-ready.json` lets cleanup resume after an interruption.

Migration 24 generalizes `job_result` to `job_artifact` and retains existing
catalog associations under `result.json`. Polling records ownership in the
claim transaction, and the assignment survives expiry and terminal settlement
for late uploads. For an older terminal job whose result owner is unknown,
preparation returns `RAYFLOW_JOB_RESULT_OWNER_UNKNOWN`. The agent retains local
files whose uploads lack a receipt. Object keys are
`/<prefix>/jobs/<job_uuid>/<filename>`.

The agent also holds a process-wide lock for the entire `workdir`. Start an
upgraded binary only after the previous process has exited, using the same
agent UUID, `workdir`, and `data_dir`. The new process acquires the lock,
reconnects to persisted containers, retries durable results, and resumes
active jobs. The owner JSON is diagnostic metadata; the OS lock is the
authority, so a stale owner file after a crash does not block recovery. Running
containers and unacknowledged results remain available during this handoff.
If a persisted container ID no longer exists, recovery fails closed instead of
creating a second container for the same job.

## Agent inputs and outputs

The manager keeps metadata and artifact bytes in separate roots. Metadata is under
`<workdir>/<job_uuid>`. Artifact data is under
`<data_dir>/<job_uuid>`, with `inputs/`, `outputs/`, and per-job transfer
markers under `.rayflow/<job_uuid>/`. Each job therefore has an isolated local
artifact directory, including resumed jobs that share a run or node-run.
Artifact data stays available for upload retries until the cleanup sweep.

Declare outputs on the producer node and bind downstream inputs with
`source: "producer.output_name"`. In containers, read from
`$RAYFLOW_INPUT_DIR/<input.file_name>` and write to
`$RAYFLOW_OUTPUT_DIR/<output.file_name>` (respectively `/data/inputs` and
`/data/outputs`). Directory outputs include an internal `.rayflow-directory.json`
manifest. The agent downloads inputs before execution and uploads outputs before
reporting success, with durable checkpoints for retries.

Docker jobs receive the job directory as a read-only `/rayflow` mount. The
container can read `/rayflow/job.json` and the pinned `/rayflow/graph.json`;
`RAYFLOW_JOB_FILE` and `RAYFLOW_GRAPH_FILE` point to those paths. Inputs and
outputs remain mounted separately at `/data/inputs` and `/data/outputs`.

Configure the server's S3-compatible storage before running nodes with artifacts.
See [Catalog and object storage](catalog.md#job-artifact-api) for
configuration, endpoints, key format, directory manifests, and local retention.

## Database changes

For a new database, apply `rayflow.sql` directly. Existing deployments must be
migrated before restarting binaries when schema changes are introduced. The
current schema includes versioned graph rows, editable graph drafts,
`run.version` and `run.revision`, pinned node configuration, resource requirements, agent
claims, persisted job expiry, and `agent_instance` observations. Create the
`agent_instance` table using its definition in `rayflow.sql` before running the
updated server; polling now writes this table. No existing rows need backfilling.

If a migration is needed, inspect the current `rayflow.sql` and apply an
equivalent `ALTER TABLE` using a backup and a maintenance window. Do not assume
that an old table name or status value is compatible with the current models.

Migration 9 removes `version` and `revision` from `run_config`, including its
revision uniqueness index. Run annotation rows and job references are preserved.
Apply it with `./init-db.sh` before using the updated binaries. New jobs use the
newest annotation snapshot for their run; execution version and revision remain
on `run`.

Migration 10 adds the catalog `uploaded` acknowledgement, initially false for
both new and existing records. Job upload commit sets it true; job downloads
require it. See [upload acknowledgement](catalog.md#metadata-operations) for
handling previously stored objects.

Migration 11 changes catalog URLs to `VARCHAR(750)` and adds the unique
`(url, deleted_at)` key for the schema that existed at that migration step.
Before upgrading, resolve any URLs longer than 750 characters or duplicate
URL/deletion pairs while preserving catalog UUIDs and file references. The
migration does not merge or delete records. Upload preparation now creates or
reuses the catalog by its generated key; only commit requires the returned
catalog UUID. Deploy matching server and agent binaries.

Migrations 12 and 13 remove `deleted_at` from catalog and file metadata. Catalog
URLs become unique across all rows, and file catalog references remain unique;
filenames are scoped to their node-run ownership. Resolve historical duplicates
before upgrading. Rows are preserved, and retained historical rows become
readable.

Migrations 14–16 align the authentication tables with the SQL conventions and
add auto-incrementing numeric primary IDs. The former `auth_user.id` becomes
unique `user_id`, preserving existing sessions and audit identities. Token and
state hashes retain unique keys. Apply migrations before deploying matching
server binaries.
