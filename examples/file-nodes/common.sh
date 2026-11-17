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
