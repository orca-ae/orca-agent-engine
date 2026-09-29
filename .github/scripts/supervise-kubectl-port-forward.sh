#!/usr/bin/env bash
# Copyright The Orca Authors
# SPDX-License-Identifier: Apache-2.0

# Keep a kubectl port-forward available for long-running CI suites. kubectl can
# terminate the whole process when one forwarded connection is reset even while
# the selected Pod remains healthy, so a one-shot background process is not
# sufficient for the real-agent matrix.

set -u

if [[ "$#" -lt 2 ]]; then
  echo "usage: $0 <log-file> <kubectl port-forward arguments...>" >&2
  exit 2
fi

log_file="$1"
shift
kubectl_bin="${KUBECTL_BIN:-kubectl}"
restart_delay_seconds="${PORT_FORWARD_RESTART_DELAY_SECONDS:-1}"

if ! [[ "${restart_delay_seconds}" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "PORT_FORWARD_RESTART_DELAY_SECONDS must be a non-negative number" >&2
  exit 2
fi

mkdir -p "$(dirname "${log_file}")"
touch "${log_file}"

stopping=0
child_pid=''

stop() {
  stopping=1
  if [[ -n "${child_pid}" ]] && kill -0 "${child_pid}" 2>/dev/null; then
    kill -TERM "${child_pid}" 2>/dev/null || true
  fi
}

trap stop TERM INT HUP

while [[ "${stopping}" -eq 0 ]]; do
  printf '[%s] starting kubectl %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >> "${log_file}"
  "${kubectl_bin}" "$@" >> "${log_file}" 2>&1 &
  child_pid="$!"
  wait "${child_pid}"
  exit_code="$?"
  child_pid=''

  if [[ "${stopping}" -ne 0 ]]; then
    break
  fi

  printf '[%s] kubectl port-forward exited with status %s; restarting in %ss\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    "${exit_code}" \
    "${restart_delay_seconds}" >> "${log_file}"

  sleep "${restart_delay_seconds}" &
  child_pid="$!"
  wait "${child_pid}" 2>/dev/null || true
  child_pid=''
done

exit 0
