#!/bin/sh
. /opt/rayflow/common.sh
[ "$#" -gt 0 ] && [ $(( $# % 2 )) -eq 0 ] || fail 'Usage: rename INPUT OUTPUT [INPUT OUTPUT ...]'
while [ "$#" -gt 0 ]; do
    input=$1
    output=$2
    shift 2
    validate_path "$input"
    validate_path "$output"
    [ -f "$input_dir/$input" ] || fail "Input file is missing: $input"
    destination="$output_dir/$output"
    mkdir -p "$(dirname "$destination")"
    cp -- "$input_dir/$input" "$destination"
    printf 'Renamed %s to %s\n' "$input" "$output"
done

exercise_resources
