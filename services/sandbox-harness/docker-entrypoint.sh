#!/bin/sh
# Copyright The Orca Authors
# SPDX-License-Identifier: Apache-2.0

set -eu

output_dir="${ORCA_OUTPUT_CAPTURE_DIRECTORY:-/mnt/session/outputs}"
if [ "$output_dir" != "/mnt/session/outputs" ]; then
  echo "unsupported ORCA_OUTPUT_CAPTURE_DIRECTORY: $output_dir" >&2
  exit 1
fi
if [ "$(id -u)" -ne 0 ]; then
  echo "sandbox harness entrypoint must start as root to create its mount namespace" >&2
  exit 1
fi

ready_path="/tmp/orca-sandbox-harness-ready"
attempt=0
while [ ! -e "$ready_path" ]; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 6000 ]; then
    echo "timed out waiting for session resources at $ready_path" >&2
    exit 1
  fi
  sleep 0.1
done

policy_file="$(mktemp)"
trap 'rm -f "$policy_file"' EXIT
node - <<'NODE' >"$policy_file"
const path = require('node:path').posix;
const raw = process.env.ORCA_SANDBOX_WRITE_POLICY;
if (!raw) throw new Error('ORCA_SANDBOX_WRITE_POLICY is required');
const policy = JSON.parse(raw);
if (!Array.isArray(policy.writablePaths) || !Array.isArray(policy.readonlyPaths)) {
  throw new Error('ORCA_SANDBOX_WRITE_POLICY must contain writablePaths and readonlyPaths arrays');
}
const emit = (mode, value) => {
  if (typeof value !== 'string' || !value.startsWith('/')) {
    throw new Error(`invalid sandbox write-policy path: ${String(value)}`);
  }
  const normalized = path.normalize(value).replace(/\/$/, '') || '/';
  if (normalized !== value.replace(/\/$/, '') || /[\t\r\n\0]/.test(value)) {
    throw new Error(`non-canonical sandbox write-policy path: ${value}`);
  }
  process.stdout.write(`${mode}\t${normalized}\n`);
};
for (const entry of policy.writablePaths) emit('bind', entry?.path);
for (const value of policy.readonlyPaths) emit('ro-bind', value);
NODE

set -- bwrap \
  --new-session \
  --die-with-parent \
  --unshare-pid \
  --ro-bind / / \
  --perms 1777 --tmpfs /tmp \
  --perms 0777 --tmpfs /home/node \
  --dev /dev \
  --proc /proc

tab="$(printf '\t')"
while IFS="$tab" read -r mode path; do
  if [ "$mode" = "bind" ]; then
    mkdir -p "$path"
    # OpenSandbox upload metadata gives materialized files and newly-created
    # parents to node. Hand over only the root as a constant-time fallback for
    # pre-existing/empty paths; never walk a large repository during startup.
    chown 1000:1000 "$path"
    set -- "$@" --bind "$path" "$path"
  elif [ "$mode" = "ro-bind" ]; then
    if [ ! -e "$path" ]; then
      echo "read-only session resource is missing: $path" >&2
      exit 1
    fi
    set -- "$@" --ro-bind "$path" "$path"
  else
    echo "invalid sandbox write-policy mount mode: $mode" >&2
    exit 1
  fi
done <"$policy_file"

rm -f "$policy_file"
trap - EXIT

set -- "$@" \
  --chdir "$output_dir" \
  --setenv HOME /home/node \
  --setenv USER node \
  --setenv LOGNAME node \
  --setenv TMPDIR /tmp \
  -- \
  setpriv --reuid 1000 --regid 1000 --clear-groups \
  --no-new-privs --bounding-set=-all \
  node /app/services/sandbox-harness/dist/index.js

exec "$@"
