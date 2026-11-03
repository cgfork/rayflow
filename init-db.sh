#!/usr/bin/env bash
set -euo pipefail

if (($#)); then
    if (($# == 1)) && [[ "$1" == '-h' || "$1" == '--help' ]]; then
        cat <<'EOF'
Usage: ./init-db.sh

Create the MySQL database and apply pending Toasty migrations.
Uses rayflow.yaml, RAYFLOW_CONFIG, and DATABASE_URL like the server.
Requires Cargo and database CREATE/ALTER permissions.
Stop Rayflow before migrating an existing database. Existing rows are preserved.
EOF
        exit 0
    fi
    printf 'Usage: ./init-db.sh [--help]\n' >&2
    exit 1
fi

root_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$root_dir"
exec cargo run -p server --bin init-db
