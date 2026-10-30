# Usage

This guide covers local development, configuration, HTTP operations, and the
durable files used by agents.

## Prerequisites

Rayflow uses Rust edition 2024 and MySQL. Install a Rust toolchain that supports
the workspace edition, create a MySQL database, and apply the schema before
starting the server or pilot:

```sh
mysql -h 127.0.0.1 -u root -p rayflow < rayflow.sql
```

`rayflow.sql` creates the graph, run, configuration, node-run, job, event-log,
poll-offset, and leader-election tables. The Toasty models are the source of
truth; the SQL file is the deployment schema. The database named in
`DATABASE_URL` must already exist.

Do not commit credentials in `rayflow.yaml`. Use an environment variable or a
local, untracked configuration file for `database_url`.

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

Each agent needs a unique stable ID and a persistent workdir. The agent uses
Docker's standard environment variables, including `DOCKER_HOST`, when a
non-default Docker endpoint is required.

## HTTP API

The service endpoints are:

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/` | Reports `rayflow`. |
| `GET` | `/health` | Liveness probe; returns `ok`. |
| `POST` | `/runs/start` | Start a run; body has `data_id`, `graph_id`, and optional `nodes`. |
| `POST` | `/runs/rerun` | Rerun one node; body has `data_id`, `graph_id`, and `node`. |
| `POST` | `/runs/resume` | Resume one pending node; body has `data_id`, `graph_id`, and `node`. |
| `POST` | `/runs/dryrun` | Prepare targets while mocking predecessors; body has `data_id`, `graph_id`, and optional `nodes`. |
| `GET` | `/runs/graph_state` | Read state with `data_id` and `graph_id` query parameters. |
| `POST` | `/task_events` | Emit an event for an existing task; body has `task_id` and `event`. |
| `POST` | `/jobs/poll` | Long-poll and claim jobs that fit an agent's resources. |
| `POST` | `/job/cancel` | Cancel a claimed job. |
| `POST` | `/job/yield` | Yield a claimed job and defer the node. |
| `POST` | `/job/succeed` | Complete a claimed job successfully. |
| `POST` | `/job/fail` | Report a claimed job failure. |
| `POST` | `/job/terminate` | Terminate a claimed job. |

Run commands and task events return `202 Accepted` with the appended entry ID.
Successful job actions return `204 No Content`. Claim conflicts and terminal
state conflicts return `409 Conflict`; missing runs, tasks, or nodes return
`404 Not Found`.

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

## Agent workdir

Each job has an isolated directory under `workdir`:

```text
{WORKDIR}/{job-uuid}/job.json
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

## Database changes

For a new database, apply `rayflow.sql` directly. Existing deployments must be
migrated before restarting binaries when schema changes are introduced. The
current schema includes versioned graph rows, `run_config.version`, pinned node
configuration, resource requirements, agent claims, and persisted job expiry.

If a migration is needed, inspect the current `rayflow.sql` and apply an
equivalent `ALTER TABLE` using a backup and a maintenance window. Do not assume
that an old table name or status value is compatible with the current models.
