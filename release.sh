#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: ./release.sh [--embed-ui]

Build the UI and release server, pilot, and agent binaries.
By default, the UI assets and server executable are separate artifacts.

Options:
  --embed-ui  Also embed the built UI into the server executable.
  -h, --help  Show this help.

Requires Cargo, Node.js, and npm. Uses rust-toolchain.toml and the lockfiles.
Outputs: ui/dist/, target/release/server, target/release/init-db,
target/release/pilot, and target/release/agent.
Cargo target directory and build target overrides are respected.
EOF
}

fail() {
    printf 'Error: %s\n' "$*" >&2
    exit 1
}

embed_ui=false
for argument in "$@"; do
    case "$argument" in
        --embed-ui) embed_ui=true ;;
        -h|--help) usage; exit 0 ;;
        *) usage >&2; fail "Unknown argument: $argument" ;;
    esac
done

root_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$root_dir"

for dependency in cargo node npm; do
    command -v "$dependency" >/dev/null 2>&1 || fail "Install $dependency first."
done

printf 'Installing UI dependencies...\n'
npm ci --prefix ui --engine-strict --include=dev
printf 'Building UI...\n'
npm run build --prefix ui

server_arguments=(build --locked --release -p server --bin server --bin init-db)
if "$embed_ui"; then
    server_arguments+=(--features embed-ui)
    printf 'Building release server with embedded UI...\n'
else
    server_arguments+=(-p pilot -p agent)
    printf 'Building release server, pilot, and agent...\n'
fi

# Read artifact paths from Cargo so configured targets and output directories
# work without guessing their layout or executable suffixes.
extract_binaries() {
    node -e '
let input = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => { input += chunk; });
process.stdin.on("end", () => {
    for (const line of input.split("\n")) {
        if (!line.trim()) continue;
        const message = JSON.parse(line);
        if (message.reason === "compiler-artifact" && message.executable) {
            console.log(`  ${message.target.name}: ${message.executable}`);
        }
    }
});
';
}

binaries="$(cargo "${server_arguments[@]}" --message-format=json-render-diagnostics | extract_binaries)"
if "$embed_ui"; then
    printf 'Building release pilot and agent...\n'
    worker_binaries="$(cargo build --locked --release -p pilot -p agent --message-format=json-render-diagnostics | extract_binaries)"
    binaries="${binaries}"$'\n'"${worker_binaries}"
fi

printf '\nRelease build complete.\n  UI files: %s/ui/dist\n%s\n' "$root_dir" "$binaries"
if "$embed_ui"; then
    printf 'The server serves the embedded UI at / and the API at /api/v1.\n'
else
    printf 'Serve ui/dist/ with a web server and proxy /api/v1/ to the API server.\n'
fi
