#!/bin/sh
set -eu
input_dir=${RAYFLOW_INPUT_DIR:-/data/inputs}
output_dir=${RAYFLOW_OUTPUT_DIR:-/data/outputs}
fail() { printf '%s\n' "$*" >&2; exit 2; }
validate_path() {
    case "$1" in
        ''|/*|..|../*|*/..|*/../*|.|./*|*/.|*/./*|*//*|*\\*|*:*) fail "Invalid relative file path: $1" ;;
    esac
}

exercise_resources() (
    scratch=$(mktemp /dev/shm/rayflow-file-node.XXXXXX)
    trap 'rm -f -- "$scratch"' EXIT
    head -c "$((16 * 1024 * 1024))" /dev/zero > "$scratch"
    # Keep the buffer in memory while doing a small amount of work every second.
    remaining=10
    while [ "$remaining" -gt 0 ]; do
        sha256sum "$scratch" >/dev/null
        sleep 1
        remaining=$((remaining - 1))
    done
)
