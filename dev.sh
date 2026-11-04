#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: ./dev.sh [--agent]

Build and start the Rayflow server, pilot, and web UI locally.
Initialize MySQL with DATABASE_URL=... ./init-db.sh before starting.
The services reuse rayflow.yaml and the existing environment overrides.

Options:
  --agent   Also start the Docker agent, with a persisted local agent UUID.
  -h, --help

Environment:
  DATABASE_URL           Override the MySQL URL for server and pilot.
  RAYFLOW_CONFIG         Use a different configuration file.
  RAYFLOW_DEV_API_PORT   Local API port (default: 5172).
  RAYFLOW_DEV_UI_PORT    Local UI port (default: 5173).
  RAYFLOW_AGENT_ID       Override the local agent UUID when using --agent.

Logs: .rayflow/dev/{server,pilot,ui,agent}.log
With --agent, the agent's server URL is set to the local API URL automatically.
Press Ctrl-C to stop all processes started by this script.
EOF
}

fail() {
    printf 'Error: %s\n' "$*" >&2
    exit 1
}

with_agent=false
for argument in "$@"; do
    case "$argument" in
        --agent) with_agent=true ;;
        -h|--help) usage; exit 0 ;;
        *) usage >&2; fail "Unknown argument: $argument" ;;
    esac
done

root_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$root_dir"
log_dir="$root_dir/.rayflow/dev"
api_port="${RAYFLOW_DEV_API_PORT:-5172}"
ui_port="${RAYFLOW_DEV_UI_PORT:-5173}"
api_url="http://127.0.0.1:$api_port"
ui_url="http://127.0.0.1:$ui_port"

for dependency in cargo node npm curl; do
    command -v "$dependency" >/dev/null 2>&1 || fail "Install $dependency first."
done

node - "$api_port" "$ui_port" <<'JS'
const net = require('node:net');
const [major, minor] = process.versions.node.split('.').map(Number);
if (!(major === 20 && minor >= 19 || major === 22 && minor >= 12 || major > 22)) {
    console.error('Node.js 20.19+ (20.x) or 22.12+ is required.');
    process.exit(1);
}
const ports = process.argv.slice(2).map(Number);
if (ports.some((port) => !Number.isInteger(port) || port < 1 || port > 65535) || ports[0] === ports[1]) {
    console.error('API and UI ports must be different integers between 1 and 65535.');
    process.exit(1);
}
for (const port of ports) {
    const server = net.createServer();
    server.once('error', (error) => {
        console.error(`Cannot listen on 127.0.0.1:${port}: ${error.message}`);
        process.exitCode = 1;
    });
    server.listen(port, '127.0.0.1', () => server.close());
}
JS

if "$with_agent"; then
    command -v docker >/dev/null 2>&1 || fail 'Install Docker to use --agent.'
    docker info >/dev/null 2>&1 || fail 'Docker is not available. Start Docker before using --agent.'
fi

mkdir -p "$log_dir"
if [[ ! -x ui/node_modules/.bin/vite || ui/package-lock.json -nt ui/node_modules/.package-lock.json ]]; then
    printf 'Installing UI dependencies...\n'
    npm ci --prefix ui --engine-strict
fi

packages=(-p server -p pilot)
if "$with_agent"; then
    packages+=(-p agent)
    agent_id="${RAYFLOW_AGENT_ID:-}"
    if [[ -z "$agent_id" ]]; then
        if [[ ! -s "$log_dir/agent-id" ]]; then
            node -e 'console.log(require("node:crypto").randomUUID())' > "$log_dir/agent-id"
        fi
        agent_id="$(cat "$log_dir/agent-id")"
    fi
fi

printf 'Building Rayflow...\n'
cargo build "${packages[@]}"
target_dir="$(cargo metadata --no-deps --format-version 1 | node -e '
let input = "";
process.stdin.on("data", (chunk) => { input += chunk; });
process.stdin.on("end", () => { console.log(JSON.parse(input).target_directory); });
')"

process_ids=()
process_names=()

cleanup() {
    local exit_status=$?
    trap - EXIT INT TERM
    if ((${#process_ids[@]})); then
        printf '\nStopping Rayflow...\n'
        for child_pid in "${process_ids[@]}"; do
            kill -INT -- "-$child_pid" 2>/dev/null || true
        done
        for ((attempt = 0; attempt < 25; attempt++)); do
            local running=false
            for child_pid in "${process_ids[@]}"; do
                if kill -0 -- "-$child_pid" 2>/dev/null; then running=true; fi
            done
            if ! "$running"; then break; fi
            sleep 0.2
        done
        for child_pid in "${process_ids[@]}"; do
            kill -KILL -- "-$child_pid" 2>/dev/null || true
            wait "$child_pid" 2>/dev/null || true
        done
    fi
    exit "$exit_status"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
set -m

start() {
    local name=$1
    shift
    printf 'Starting %s (log: %s/%s.log)...\n' "$name" "$log_dir" "$name"
    "$@" > "$log_dir/$name.log" 2>&1 &
    process_ids+=("$!")
    process_names+=("$name")
}

check_processes() {
    for index in "${!process_ids[@]}"; do
        if ! kill -0 "${process_ids[$index]}" 2>/dev/null; then
            local exit_status=0
            wait "${process_ids[$index]}" || exit_status=$?
            fail "${process_names[$index]} exited ($exit_status). See $log_dir/${process_names[$index]}.log"
        fi
    done
}

wait_for_health() {
    local url=$1
    local deadline=$((SECONDS + 60))
    while ((SECONDS < deadline)); do
        check_processes
        if [[ "$(curl --noproxy '*' --fail --silent --max-time 1 "$url" || true)" == ok ]]; then
            return
        fi
        sleep 1
    done
    fail "Timed out waiting for $url. Check the logs in $log_dir."
}

start server env RAYFLOW_ADDR="127.0.0.1:$api_port" "$target_dir/debug/server"
wait_for_health "$api_url/health"
start pilot "$target_dir/debug/pilot"
start ui env RAYFLOW_API_URL="$api_url" npm run dev --prefix ui -- --port "$ui_port" --strictPort
wait_for_health "$ui_url/api/health"
if "$with_agent"; then
    start agent env RAYFLOW_AGENT_ID="$agent_id" RAYFLOW_AGENT_SERVER_URL="$api_url" "$target_dir/debug/agent"
fi
check_processes

printf '\nRayflow is running.\n  UI:  %s\n  API: %s\n  Logs: %s\nPress Ctrl-C to stop.\n\n' "$ui_url" "$api_url" "$log_dir"
while true; do
    check_processes
    sleep 1
done
