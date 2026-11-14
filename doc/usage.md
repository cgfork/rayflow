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

MySQL integration tests skip when `DATABASE_URL` is absent. With a database
available, run one crate's integration tests serially when they rebuild shared
tables:

```sh
DATABASE_URL=mysql://root:password@127.0.0.1:3306/rayflow \
  cargo test -p scheduler -- --test-threads=1
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

The agent requires a stable, non-nil UUID. The other settings have defaults:

```yaml
id: 12345678-1234-4234-8234-123456789abc
server_url: http://127.0.0.1:8080
workdir: .rayflow/jobs
data_dir: .rayflow/data
batch: 64
poll_interval: 1s
request_timeout: 60s
resources:
  cpu_cores: 4
  memory_bytes: 4294967296
```

`RAYFLOW_AGENT_CONFIG` selects an explicit agent configuration file and
otherwise `RAYFLOW_CONFIG` is used. Environment variables use the
`RAYFLOW_AGENT_` prefix. Nested resource fields use double underscores, for
example `RAYFLOW_AGENT_RESOURCES__CPU_CORES=2`.

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

Open `http://127.0.0.1:5173`. Vite forwards `/api/*` requests to
`http://127.0.0.1:8080/*`. To use another API server:

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
The run dialog offers recent published graphs and also accepts any published
graph UUID. Publishing snapshots the node settings into versioned execution
configuration, so each run uses the settings from its pinned graph version.

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

Serve `ui/dist/` at the site's root and proxy `/api/` to the Rayflow server,
stripping the `/api` prefix. The Rust server does not serve the UI assets.
`RAYFLOW_API_URL` configures only the Vite development proxy. For example, inside
an nginx `server` block:

```nginx
root /srv/rayflow/ui/dist;

location / {
    try_files $uri $uri/ /index.html;
}

location /api/ {
    proxy_pass http://127.0.0.1:8080/;
    proxy_read_timeout 90s;
}
```

Use HTTPS for deployments beyond localhost; the editor generates UUIDs with
the browser's secure-context `crypto.randomUUID()` API.

### UI and management API tests

Browser integration tests start Vite and intercept API requests, so they do not
require MySQL or a running Rayflow server:

```sh
cd ui
npx playwright install chromium
npm run test:e2e
```

The server integration tests exercise real HTTP handlers and MySQL persistence,
including draft publishing, pagination, run versions, filtering, and agent
observation. Run them from the repository root against a disposable database:

```sh
DATABASE_URL=mysql://root:password@127.0.0.1:3306/rayflow_test \
  cargo test -p server --test management
```

These tests create tables with unique prefixes. They skip when `DATABASE_URL`
is absent.

## HTTP API

The service endpoints are:

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/` | Reports `rayflow`. |
| `GET` | `/health` | Liveness probe; returns `ok`. |
| `GET` | `/graphs` | List published graph versions; optional `graph_id` filter. |
| `GET` | `/graphs/{graph_id}` | Read a published graph; optional exact `version`, otherwise latest. |
| `GET` | `/graphs/{graph_id}/shared-nodes` | List shared restart node UUIDs between exact published `from_version` and newer `version`. |
| `GET` | `/graph_drafts` | List editable DAGs. |
| `GET` | `/runs` | List runs; optional `graph_id` filter. |
| `GET` | `/runs/{run_id}` | Inspect the exact run UUID, its state, and its pinned graph. |
| `POST` | `/runs/start` | Start a run; body has `data_id`, `graph_id`, and optional `nodes`. |
| `POST` | `/runs/rerun` | Rerun one node; body has `data_id`, `graph_id`, and `node`. |
| `POST` | `/runs/upgrade` | Upgrade an existing run; body has `data_id`, `graph_id`, the newer published `version`, and a `node` selected from the shared nodes. |
| `POST` | `/runs/resume` | Resume one pending node; body has `data_id`, `graph_id`, and `node`. |
| `POST` | `/runs/dryrun` | Prepare targets while mocking predecessors; body has `data_id`, `graph_id`, and optional `nodes`. |
| `GET` | `/runs/graph_state` | Read state with `data_id` and `graph_id` query parameters. |
| `GET` | `/graphs/{graph_id}/draft` | Read the editable graph draft. |
| `PUT` | `/graphs/{graph_id}/draft` | Create or replace the draft. |
| `POST` | `/graphs/{graph_id}/publish` | Validate and publish the draft as the next graph version. |
| `POST` | `/task_events` | Emit an event for an existing task; body has `task_id` and `event`. |
| `GET` | `/jobs` | List jobs; optional `status`, `agent_id`, and `run_id` filters. |
| `GET` | `/agents` | List agent instances; optional `online=true` or `online=false`. |
| `POST` | `/jobs/poll` | Long-poll and claim jobs that fit an agent's resources. |
| `POST` | `/job/cancel` | Cancel a claimed job. |
| `POST` | `/job/yield` | Yield a claimed job and defer the node. |
| `POST` | `/job/succeed` | Complete a claimed job successfully. |
| `POST` | `/job/fail` | Report a claimed job failure. |
| `POST` | `/job/terminate` | Terminate a claimed job. |

Run commands and task events return `202 Accepted` with the appended entry ID.
In Runtime, click **Upgrade** on a run or its detail page. Select a newer
published version and an eligible shared restart node, then confirm. The UI
uses the graph's shared-node calculation and the same node-state rules as rerun.
Shared nodes include all upstream ancestors of the fork point, including the root.
To upgrade a run, send `POST /runs/upgrade` with a body such as:

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
`GET /runs/graph_state` and the state in `GET /runs/{run_id}` include only effective
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
are used only for pagination cursors. Audit fields are stored in database
columns and available on loaded Rust DAGs, but excluded from JSON responses.
Run detail returns
`{ "state": ..., "graph": ..., "current": true }`, where `current` indicates
whether the selected run is the latest version for its data/graph pair.

Example start request:

```sh
curl -X POST http://127.0.0.1:8080/runs/start \
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

Write a complete draft with `PUT /graphs/{graph_id}/draft`:

```sh
curl -X PUT http://127.0.0.1:8080/graphs/00000001-0000-0000-0000-000000000000/draft \
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
temporarily invalid while they are being edited. `POST /graphs/{graph_id}/publish`
accepts `{ "actor": "publisher" }`, validates the graph (including the cycle
and single-root rules), assigns the next version, and returns the immutable
published DAG directly. The version is the maximum published version for that
graph UUID plus one, starting at 1. The draft's version is ignored when allocating
the published version. Publishing does not delete the draft.

## Long polling and claims

`POST /jobs/poll` accepts this request shape:

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
`Pending`; `POST /runs/resume` creates the next task.

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

Each job has an isolated directory under `workdir`:

```text
{WORKDIR}/{job-uuid}/job.json
{WORKDIR}/{job-uuid}/node.json
{WORKDIR}/{job-uuid}/result.json
{WORKDIR}/{job-uuid}/pid
{WORKDIR}/{job-uuid}/lock
{WORKDIR}/{job-uuid}/container.json
{WORKDIR}/{job-uuid}/runtime/started_at
{WORKDIR}/{job-uuid}/runtime/stdout
```

The agent writes job and result JSON atomically, persists results before
reporting them, and retains job files for recovery. The lock file is held with
an exclusive OS lock while a job is being coordinated. On startup, `Agentic`
discovers existing directories, reconnects to containers by UUID, and retries
unreported results. Keep this directory across agent restarts.

## Agent inputs and outputs

`FileFs` stores bytes separately from `JobFs` under
`<data_dir>/<graph_run_uuid>/<node_id>/<revision>/`, with `inputs/`, `outputs/`,
and internal transfer checkpoints. The graph run UUID isolates executions of
the same graph. Job completion retains this data for downstream work; remove
completed run directories when they are no longer needed.

Declare outputs on the producer node and bind downstream inputs with
`source: "producer.output_name"`. In containers, read from
`$RAYFLOW_INPUT_DIR/<input.file_name>` and write to
`$RAYFLOW_OUTPUT_DIR/<output.file_name>` (respectively `/data/inputs` and
`/data/outputs`). Directory outputs include an internal `.rayflow-directory.json`
manifest. The agent downloads inputs before execution and uploads outputs before
reporting success, with durable checkpoints for retries.

Configure the server's S3-compatible storage before running nodes with artifacts.
See [Catalog and object storage](catalog.md#http-api-and-agent-transfers) for
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
`(url, deleted_at)` key. Before upgrading, resolve any URLs longer than 750
characters or duplicate URL/deletion pairs while preserving catalog UUIDs and
file references. The migration does not merge or delete records. Upload prepare
now creates or reuses the catalog by URL; only commit requires the returned
catalog UUID. Deploy matching server and agent binaries.

Migrations 12 and 13 remove `deleted_at` from catalog and file metadata. URLs and
full names become unique across all rows. Resolve historical duplicates before
upgrading. Rows are preserved, and retained historical rows become readable.

Migrations 14–16 align the authentication tables with the SQL conventions and
add auto-incrementing numeric primary IDs. The former `auth_user.id` becomes
unique `user_id`, preserving existing sessions and audit identities. Token and
state hashes retain unique keys. Apply migrations before deploying matching
server binaries.
