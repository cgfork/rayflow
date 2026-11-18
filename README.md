# Rayflow

Rayflow is a Rust workspace (edition 2024) implementing a DAG-based task runtime.
A run walks graph nodes through a persisted state machine so work can be driven
forward, deferred, resumed, and recovered across processes.

## Documentation

- [Architecture](doc/architecture.md) — system boundaries, data flow, and state transitions.
- [Design](doc/design.md) — graph validation, event sourcing, transactions, scheduling, and job semantics.
- [Modules](doc/modules.md) — crate and binary responsibilities, APIs, and workspace layout.
- [Usage](doc/usage.md) — configuration, web UI, HTTP endpoints, deployment, migrations, and development commands.
- [Authentication](doc/authentication.md) — user login, company SSO, LDAP, and agent credentials.
- [Catalog](doc/catalog.md) — object metadata, logical file names, and the storage signing contract.

## Quick Start

Use rustup to install the nightly pinned in `rust-toolchain.toml`. The server
uses return-type notation to require `Send` futures from native async graph
store methods.

```sh
rustup show active-toolchain
cargo check --workspace
cargo test --workspace
```

Initialize MySQL before running the server or pilot:

```sh
export DATABASE_URL='mysql://root:password@127.0.0.1:3306/rayflow'
./init-db.sh
```

The script uses Rayflow's configuration and Toasty migrations to create the
database and update known legacy constraints without deleting rows. See the
[usage guide](doc/usage.md) for details.

After configuring the database, start the local API, pilot, and web UI together:

```sh
./dev.sh
```

Use `./dev.sh --agent` to include a local Docker agent. Press Ctrl-C to stop.

## Release builds

Build the UI and API server as separate artifacts:

```sh
./release.sh
```

Serve `ui/dist/` with a web server and proxy `/api/v1/` to
`target/release/server`. See [Production hosting](doc/usage.md#production-hosting).
The script also builds `target/release/init-db` for database initialization.
It can run from any working directory and reports the actual Cargo artifact
paths, including custom target directories and build targets.

To embed the UI into the server executable instead:

```sh
./release.sh --embed-ui
```

The resulting `target/release/server` contains the UI assets and serves the
console at `/`, with both browser and agent API requests under `/api/v1`.
Node.js and `ui/dist` are not needed on the deployment host. Set `auth.public_url` in `rayflow.yaml` to
the externally reachable server URL for authentication callbacks.

Rebuild the UI and server after UI changes. Builds without `embed-ui` retain
the API-only behavior and do not require UI assets or Node.js.

## Docker images

Build both images from the repository root:

```sh
docker build -f server/Dockerfile -t rayflow-server .
docker build -f pilot/Dockerfile -t rayflow-pilot .
```

The server image includes the embedded UI and an `init-db` utility. The pilot
image contains the consumer binary. Both run as UID/GID 10001 and read an
optional `/app/rayflow.yaml`; configuration and credentials are not baked in.

Initialize the database, then start the services with your configuration:

```sh
docker run --rm --entrypoint init-db \
  -v "$PWD/rayflow.yaml:/app/rayflow.yaml:ro" rayflow-server
docker run -d --name rayflow-server -p 8080:8080 \
  -v "$PWD/rayflow.yaml:/app/rayflow.yaml:ro" rayflow-server
docker run -d --name rayflow-pilot \
  -v "$PWD/rayflow.yaml:/app/rayflow.yaml:ro" rayflow-pilot
```

Make the mounted configuration readable by UID 10001. Use database and storage
addresses reachable from containers, and set `auth.public_url` to the browser's
server URL. The API is served under `/api/v1`. Environment overrides such as
`DATABASE_URL`, `RAYFLOW_STORAGE__ENDPOINT`, and `RAYFLOW_AUTH__PUBLIC_URL` also
work with these images. The pilot receives SIGINT on `docker stop` for graceful
shutdown.
