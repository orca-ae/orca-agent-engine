#!/usr/bin/env bash
# Copyright The Orca Authors
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail
repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
cd "$repo_root"
COMPOSE=(docker compose -p orca-transcript-avro -f services/dev/docker-compose.transcript-avro.yml)
if [[ "${1:-}" == down ]]; then
  "${COMPOSE[@]}" down --volumes --remove-orphans
  exit 0
fi
# Never touch the normal dev stack, global containers, or unowned volumes.
mkdir -p services/dev/logs/transcript-avro
cleanup() {
  rc=$?
  "${COMPOSE[@]}" logs --no-color > services/dev/logs/transcript-avro/containers.log 2>&1 || true
  "${COMPOSE[@]}" images > services/dev/logs/transcript-avro/images.txt 2>&1 || true
  if [[ "${KEEP_TRANSCRIPT_AVRO:-0}" != 1 ]]; then
    "${COMPOSE[@]}" down --volumes --remove-orphans || true
  fi
  exit "$rc"
}
trap cleanup EXIT
"${COMPOSE[@]}" up -d --wait --wait-timeout 180
node packages/transcript-store/test/integration/avro-schema-registry.mjs 2>&1 | tee services/dev/logs/transcript-avro/assertions.log
