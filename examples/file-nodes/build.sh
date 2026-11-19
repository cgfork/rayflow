#!/usr/bin/env bash
set -euo pipefail
context="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
for node in generate rename verify; do
    docker build --build-arg "BASE_IMAGE=${BASE_IMAGE:-ubuntu:24.04}" \
        -f "$context/$node.Dockerfile" -t "rayflow-file-$node:local" "$context"
done
