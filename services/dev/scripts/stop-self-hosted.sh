#!/usr/bin/env bash
# Copyright The Orca Authors
# SPDX-License-Identifier: Apache-2.0

# stop-self-hosted.sh — stop the native environment-worker + registry started by
# start-self-hosted.sh and tear down the postgres + rustfs compose project.
# Idempotent: safe to run when nothing is running (missing pid files / compose
# project are skipped silently). Bash-3.2 compatible.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEV_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
RUN_DIR="${DEV_DIR}/run"
COMPOSE=(docker compose -p orca-self-hosted -f "${DEV_DIR}/docker-compose.self-hosted.yml")

stop_service() {
  name="$1"
  pidfile="${RUN_DIR}/${name}.pid"

  if [ ! -f "${pidfile}" ]; then
    echo "stop-self-hosted: no pidfile for ${name}; skipping"
    return 0
  fi

  pid="$(cat "${pidfile}" 2>/dev/null || true)"
  if [ -z "${pid}" ]; then
    rm -f "${pidfile}"
    echo "stop-self-hosted: ${name} pidfile was empty; cleaned"
    return 0
  fi

  if kill -0 "${pid}" 2>/dev/null; then
    echo "stop-self-hosted: stopping ${name} (pid ${pid})"
    # SIGTERM first; the registry drains its event consumer + closes pools and
    # the worker terminates its live runners on the way out, so the polite signal
    # matters even in dev. SIGKILL only if it is still alive after 10s.
    kill -TERM "${pid}" 2>/dev/null || true
    i=0
    while [ "${i}" -lt 20 ]; do
      if ! kill -0 "${pid}" 2>/dev/null; then
        break
      fi
      i=$((i + 1))
      sleep 0.5
    done
    if kill -0 "${pid}" 2>/dev/null; then
      echo "stop-self-hosted: ${name} did not exit on SIGTERM; sending SIGKILL"
      kill -KILL "${pid}" 2>/dev/null || true
    fi
  else
    echo "stop-self-hosted: ${name} (pid ${pid}) already stopped"
  fi

  rm -f "${pidfile}"
}

# Stop the worker FIRST (it owns the runners; stopping it terminates them), then
# the registry. Stopping the registry while the worker tunnel is mid-frame would
# only log spurious reconnect churn.
stop_service environment-worker
stop_service registry

# Tear down the compose project (postgres + rustfs). `down` removes the
# containers + the project network; the bind-mounted data dirs under
# services/dev/data/self-hosted persist so a restart keeps prior rows.
if docker compose -p orca-self-hosted -f "${DEV_DIR}/docker-compose.self-hosted.yml" ps -q >/dev/null 2>&1; then
  echo "stop-self-hosted: tearing down postgres + rustfs (project orca-self-hosted)"
  "${COMPOSE[@]}" down --remove-orphans >/dev/null 2>&1 || true
fi

echo "stop-self-hosted: done."
