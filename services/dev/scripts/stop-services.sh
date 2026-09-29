#!/usr/bin/env bash
# Copyright The Orca Authors
# SPDX-License-Identifier: Apache-2.0

# stop-services.sh — stop the ai-gateway compose service and kill the native
# registry/harness processes started by start-services.sh. Idempotent: safe to
# run even when nothing is running (missing pid files are skipped silently).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEV_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
RUN_DIR="${DEV_DIR}/run"
COMPOSE=(docker compose -f "${DEV_DIR}/docker-compose.yml")

stop_ai_gateway() {
  local container_ids
  container_ids="$("${COMPOSE[@]}" ps -q ai-gateway 2>/dev/null || true)"
  if [[ -z "${container_ids}" ]]; then
    echo "stop-services: no ai-gateway container; skipping"
    return 0
  fi

  echo "stop-services: stopping ai-gateway container"
  "${COMPOSE[@]}" stop ai-gateway >/dev/null
}

stop_service() {
  local name="$1"
  local pidfile="${RUN_DIR}/${name}.pid"

  if [[ ! -f "${pidfile}" ]]; then
    echo "stop-services: no pidfile for ${name}; skipping"
    return 0
  fi

  local pid
  pid="$(cat "${pidfile}" 2>/dev/null || true)"
  if [[ -z "${pid}" ]]; then
    rm -f "${pidfile}"
    echo "stop-services: ${name} pidfile was empty; cleaned"
    return 0
  fi

  if kill -0 "${pid}" 2>/dev/null; then
    echo "stop-services: stopping ${name} (pid ${pid})"
    # SIGTERM first; SIGKILL after 10s if still alive. The harness/registry
    # have explicit SIGTERM handlers that drain event consumers + close
    # connection pools, so the polite signal matters in dev too.
    kill -TERM "${pid}" 2>/dev/null || true
    local i
    for i in $(seq 1 20); do
      if ! kill -0 "${pid}" 2>/dev/null; then
        break
      fi
      sleep 0.5
    done
    if kill -0 "${pid}" 2>/dev/null; then
      echo "stop-services: ${name} did not exit on SIGTERM; sending SIGKILL"
      kill -KILL "${pid}" 2>/dev/null || true
    fi
  else
    echo "stop-services: ${name} (pid ${pid}) already stopped"
  fi

  rm -f "${pidfile}"
}

# Stop in reverse-dependency order: ai-gateway first (no upstream), then
# harness (depends on registry for /sessions, agents catalog), then registry last.
# Stopping registry while harness is mid-request causes spurious 5xx in
# harness logs at every shutdown.
stop_ai_gateway
stop_service harness
stop_service registry

echo "stop-services: done."
