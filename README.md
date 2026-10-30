# Rayflow

Rayflow is a Rust workspace (edition 2024) implementing a DAG-based task runtime.
A run walks graph nodes through a persisted state machine so work can be driven
forward, deferred, resumed, and recovered across processes.

## Documentation

- [Architecture](doc/architecture.md) — system boundaries, data flow, and state transitions.
- [Design](doc/design.md) — graph validation, event sourcing, transactions, scheduling, and job semantics.
- [Modules](doc/modules.md) — crate and binary responsibilities, APIs, and workspace layout.
- [Usage](doc/usage.md) — configuration, HTTP endpoints, deployment, migrations, and development commands.

## Quick Start

```sh
cargo check --workspace
cargo test --workspace
```

Apply `rayflow.sql` to a MySQL database before running the server or pilot. See
the [usage guide](doc/usage.md) for configuration and startup commands.
