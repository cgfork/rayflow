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
  RAYFLOW_DEV_HOST       API and UI bind address (default: 0.0.0.0).
  RAYFLOW_DEV_API_PORT   Local API port (default: 5172).
  RAYFLOW_DEV_UI_PORT    Local UI port (default: 5173).
  RAYFLOW_AGENT_ID       Override the local agent UUID when using --agent.
  RAYFLOW_DEV_AGENT_TOKEN Override the shared local fleet token when using --agent.

Logs: .rayflow/dev/{server,pilot,ui,agent}.log
With authentication enabled, open the UI address configured in auth.public_url.
With --agent, the local API URL and matching fleet credentials are set automatically.
The generated fleet token is persisted in .rayflow/dev/agent-token.
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
bind_host="${RAYFLOW_DEV_HOST:-0.0.0.0}"
api_port="${RAYFLOW_DEV_API_PORT:-5172}"
ui_port="${RAYFLOW_DEV_UI_PORT:-5173}"
connect_host="$bind_host"
case "$connect_host" in
    0.0.0.0) connect_host=127.0.0.1 ;;
    ::) connect_host=::1 ;;
esac

# HTTP clients differ in which case they prefer. Preserve each existing list.
local_no_proxy=127.0.0.1,localhost,::1
case "$connect_host" in
    127.0.0.1|localhost|::1) ;;
    *) local_no_proxy+=",$connect_host" ;;
esac
upper_no_proxy="${NO_PROXY-${no_proxy:-}}"
lower_no_proxy="${no_proxy:-${NO_PROXY:-}}"
export NO_PROXY="${upper_no_proxy:+$upper_no_proxy,}$local_no_proxy"
# curl treats a wildcard as a bypass for every host only when it stands alone.
if [[ "$lower_no_proxy" != '*' ]]; then
    lower_no_proxy="${lower_no_proxy:+$lower_no_proxy,}$local_no_proxy"
fi
export no_proxy="$lower_no_proxy"

if [[ "$connect_host" == *:* ]]; then connect_host="[$connect_host]"; fi
api_url="http://$connect_host:$api_port"
ui_url="http://$connect_host:$ui_port"
api_bind="$bind_host:$api_port"
if [[ "$bind_host" == *:* ]]; then api_bind="[$bind_host]:$api_port"; fi

for dependency in cargo node npm curl; do
    command -v "$dependency" >/dev/null 2>&1 || fail "Install $dependency first."
done

node - "$bind_host" "$api_port" "$ui_port" <<'JS'
const net = require('node:net');
const [major, minor] = process.versions.node.split('.').map(Number);
if (!(major === 20 && minor >= 19 || major === 22 && minor >= 12 || major > 22)) {
    console.error('Node.js 20.19+ (20.x) or 22.12+ is required.');
    process.exit(1);
}
const host = process.argv[2];
const ports = process.argv.slice(3).map(Number);
if (ports.some((port) => !Number.isInteger(port) || port < 1 || port > 65535) || ports[0] === ports[1]) {
    console.error('API and UI ports must be different integers between 1 and 65535.');
    process.exit(1);
}
for (const port of ports) {
    const server = net.createServer();
    server.once('error', (error) => {
        console.error(`Cannot listen on ${host}:${port}: ${error.message}`);
        process.exitCode = 1;
    });
    server.listen(port, host, () => server.close());
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
    if [[ -n "${RAYFLOW_AUTH__AGENT_TOKEN:-}" && -n "${RAYFLOW_AGENT_AUTH_TOKEN:-}" && "$RAYFLOW_AUTH__AGENT_TOKEN" != "$RAYFLOW_AGENT_AUTH_TOKEN" ]]; then
        fail 'RAYFLOW_AUTH__AGENT_TOKEN and RAYFLOW_AGENT_AUTH_TOKEN must match.'
    fi
    fleet_token="${RAYFLOW_DEV_AGENT_TOKEN:-${RAYFLOW_AUTH__AGENT_TOKEN:-${RAYFLOW_AGENT_AUTH_TOKEN:-}}}"
    if [[ -z "$fleet_token" ]]; then
        if [[ ! -s "$log_dir/agent-token" ]]; then
            (umask 077; node -e 'console.log(require("node:crypto").randomBytes(32).toString("hex"))' > "$log_dir/agent-token")
        fi
        fleet_token="$(cat "$log_dir/agent-token")"
    fi
    export RAYFLOW_AUTH__AGENT_TOKEN="$fleet_token"
    export RAYFLOW_AGENT_AUTH_TOKEN="$fleet_token"
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

start server env RAYFLOW_ADDR="$api_bind" "$target_dir/debug/server"
wait_for_health "$api_url/health"
start pilot "$target_dir/debug/pilot"
start ui env RAYFLOW_API_URL="$api_url" npm run dev --prefix ui -- --host "$bind_host" --port "$ui_port" --strictPort
wait_for_health "$ui_url/api/v1/health"
browser_url="$(curl --noproxy '*' --fail --silent --show-error --max-time 5 "$ui_url/api/v1/auth/providers" | node -e '
const { enabled, public_url } = JSON.parse(require("node:fs").readFileSync(0, "utf8"));
process.stdout.write(enabled ? public_url : process.argv[1]);
' "$ui_url")" || fail 'Unable to read the sign-in settings from the local UI.'
if "$with_agent"; then
    start agent env RAYFLOW_AGENT_ID="$agent_id" RAYFLOW_AGENT_SERVER_URL="$api_url" "$target_dir/debug/agent"
fi
check_processes

printf 'API and UI bind address: %s\n' "$bind_host"
printf '\nRayflow is running.\n  UI:  %s\n  API: %s\n  Logs: %s\nPress Ctrl-C to stop.\n\n' "$browser_url" "$api_url" "$log_dir"
while true; do
    check_processes
    sleep 1
done
