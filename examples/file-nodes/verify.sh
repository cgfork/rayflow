#!/bin/sh
. /opt/rayflow/common.sh
[ "$#" -eq 2 ] || fail 'Usage: verify INPUT INPUT'
validate_path "$1"
validate_path "$2"
[ -f "$input_dir/$1" ] || fail "Input file is missing: $1"
[ -f "$input_dir/$2" ] || fail "Input file is missing: $2"
if cmp -- "$input_dir/$1" "$input_dir/$2"; then
    printf 'Files are identical: %s and %s\n' "$1" "$2"
else
    result=$?
    printf 'File comparison failed: %s and %s\n' "$1" "$2" >&2
    exit "$result"
fi
