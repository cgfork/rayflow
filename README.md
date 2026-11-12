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
