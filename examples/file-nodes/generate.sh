#!/bin/sh
. /opt/rayflow/common.sh
[ "$#" -gt 0 ] || fail 'Usage: generate OUTPUT [OUTPUT ...]'
size=${RAYFLOW_FILE_SIZE_BYTES:-1048576}
case "$size" in ''|*[!0-9]*) fail 'RAYFLOW_FILE_SIZE_BYTES must be a nonnegative integer' ;; esac
# Repeatable zero-filled content lets independent branches compare their outputs.
for output in "$@"; do
    validate_path "$output"
    destination="$output_dir/$output"
    mkdir -p "$(dirname "$destination")"
    head -c "$size" /dev/zero > "$destination"
    printf 'Generated %s (%s bytes)\n' "$output" "$size"
done
