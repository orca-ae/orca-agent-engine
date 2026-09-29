#!/usr/bin/env bash
# Copyright The Orca Authors
# SPDX-License-Identifier: Apache-2.0

# start-self-hosted.sh — bring up the MINIMAL self-hosted stack and the native
# registry + environment-worker, end to end, so the self-hosted path can be
# exercised live:
#
#   client → registry → (claim) → environment-worker → session-runner →
#   single-writer event bridge → client SSE
#
# Footprint (see docker-compose.self-hosted.yml): Postgres + RustFS only. NO
# Kafka/Pulsar, NO ai-gateway. The registry runs with
# TRANSCRIPT_STORE_BACKEND=postgres (durable transcript = a Postgres table); the
# session-runner needs no broker (its history is the in-memory tunnel-fed store);
# the e2e drives the LLM-free `mock` provider (no model credential, no gateway).
#
# Sequence:
#   1. compose up postgres + rustfs (+ bootstrap), wait for both to accept TCP.
#   2. pnpm -r build.
#   3. db:migrate for file-store + memory-store + registry.
#   4. generate the session-JWT keypair if missing (init-secrets.sh).
#   5. start the registry (postgres transcript backend), wait /healthz.
#   6. CREATE the self_hosted environment over the public API (POST
#      /v1/environments) — the registry returns the env_key exactly once — and
#      capture { environment id, env_key }.
#   7. start the environment-worker with that ENVIRONMENT_ID / ENVIRONMENT_KEY,
#      REGISTRY_TUNNEL_BASE_URL=http://localhost:8080, a WORKSPACE_DIR, and
#      RUNNER_LAUNCH_COMMAND=node <abs>/services/session-runner/dist/main.js.
#
# The created environment id + env_key are written to ${RUN_DIR}/self-hosted.env
# (KEY=value) so the e2e (and a human) can read which environment the worker is
# attached to. Re-running is safe: the worker is restarted against a freshly
# created environment each time (rotate-on-restart — the env_key is unrecoverable
# once issued, so we mint a new environment rather than try to reuse a key we
# never stored).
#
# Conventions (pid files in run/, logs in logs/, healthz waits, detached node
# launcher, EXIT-trap cleanup) mirror start-services.sh.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEV_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${DEV_DIR}/../.." && pwd)"
LOG_DIR="${DEV_DIR}/logs"
RUN_DIR="${DEV_DIR}/run"
SECRETS_DIR="${DEV_DIR}/secrets"
self_hosted_env_file="${RUN_DIR}/self-hosted.env"

mkdir -p "${LOG_DIR}" "${RUN_DIR}"

# --- prereq probe ------------------------------------------------------------

missing=()
for bin in pnpm node docker openssl curl lsof; do
  if ! command -v "${bin}" >/dev/null 2>&1; then
    missing+=("${bin}")
  fi
done
if [ "${#missing[@]}" -gt 0 ]; then
  echo "start-self-hosted: missing required binaries: ${missing[*]}" >&2
  echo "  install via your package manager (brew install ${missing[*]} or apt install ${missing[*]})" >&2
  exit 1
fi

# --- failure cleanup trap ----------------------------------------------------

# Mirror start-services.sh: on ANY non-zero exit after a service was
# backgrounded, reap the native processes via stop-self-hosted.sh (idempotent)
# so a failed start never leaks a registry/worker. EXIT (not ERR) fires for every
# exit path including explicit `exit N` and a wait-timeout `return 1`.
on_failure() {
  exit_code=$?
  if [ "${exit_code}" -ne 0 ]; then
    echo "start-self-hosted: failed (exit ${exit_code}) — cleaning up backgrounded services…" >&2
    "${SCRIPT_DIR}/stop-self-hosted.sh" 2>/dev/null || true
  fi
  exit "${exit_code}"
}
trap on_failure EXIT

# --- env wiring (postgres transcript backend; no kafka/pulsar/gateway) -------

# Host ports the compose publishes. Overridable so this stack can coexist with
# another Postgres or S3 server already bound to 5432/9000 (the compose maps
# these to the in-container 5432/9000). The DB URLs + S3 endpoint below derive
# from them, so a caller picks alternate ports by setting ONLY these two.
: "${SELF_HOSTED_POSTGRES_PORT:=5432}"
: "${SELF_HOSTED_RUSTFS_PORT:=9000}"
export SELF_HOSTED_POSTGRES_PORT SELF_HOSTED_RUSTFS_PORT
: "${SELF_HOSTED_RUSTFS_CONSOLE_PORT:=9001}"
export SELF_HOSTED_RUSTFS_CONSOLE_PORT

# Self-hosted defaults. The registry reads each of these per
# registry-service-ts/src/config.ts; the inline localhost values match the
# compose stack. A caller may export any of them first to override. The DB URLs +
# S3 endpoint default off the (overridable) host ports above.
: "${TRANSCRIPT_STORE_BACKEND:=postgres}"
: "${DATABASE_URL:=postgres://orca:orca@localhost:${SELF_HOSTED_POSTGRES_PORT}/registry}"
: "${TRANSCRIPT_STORE_DATABASE_URL:=postgres://orca:orca@localhost:${SELF_HOSTED_POSTGRES_PORT}/transcriptstore}"
: "${FILESTORE_DATABASE_URL:=postgres://orca:orca@localhost:${SELF_HOSTED_POSTGRES_PORT}/filestore}"
: "${MEMORYSTORE_DATABASE_URL:=postgres://orca:orca@localhost:${SELF_HOSTED_POSTGRES_PORT}/memorystore}"
: "${S3_ENDPOINT:=http://localhost:${SELF_HOSTED_RUSTFS_PORT}}"
: "${S3_BUCKET:=orca-files}"
: "${S3_ACCESS_KEY:=minioadmin}"
: "${S3_SECRET_KEY:=minioadmin}"
: "${S3_ACCESS_KEY_ID:=${S3_ACCESS_KEY}}"
: "${S3_SECRET_ACCESS_KEY:=${S3_SECRET_KEY}}"
: "${S3_REGION:=us-east-1}"
: "${S3_KEY_PREFIX:=blobs/}"
: "${OUTPUTS_KEY_PREFIX:=outputs/}"
: "${MEMORY_KEY_PREFIX:=memory/}"
: "${SESSION_JWT_ISSUER:=orca-registry}"
: "${SESSION_JWT_AUDIENCE:=ai-gateway}"
: "${SESSION_JWT_TTL_SECS:=300}"
# NODE_ENV and ORCA_SECRET_STORE_MODE are a PAIR, not two independent knobs. The
# registry's `parseSecretStoreMode` refuses `local` unless NODE_ENV is development or
# test — a deployed instance must not fall back to on-disk secrets — so setting the
# mode without the environment fails closed at boot with
# "ORCA_SECRET_STORE_MODE=local requires NODE_ENV=development or test". Set together,
# exported together, exactly as start-services.sh does.
: "${NODE_ENV:=development}"
: "${ORCA_SECRET_STORE_MODE:=local}"
: "${REGISTRY_HTTP_PORT:=8080}"
# The registry binds THREE listeners, not one. Only the public port is part of this
# stack's story, but a clash on either of the other two is just as fatal -- and used
# to surface as a 120-second health-check timeout with nothing in the message naming
# the port, because the pre-flight below checked only the public one.
: "${REGISTRY_INTERNAL_HTTP_PORT:=8081}"
: "${REGISTRY_ADMIN_HTTP_PORT:=8082}"
# The worker authenticates with an Env Key and dials the registry host tunnel at
# this base URL; the spawned runner inherits it (default registryRunnerUrl) and
# dials the runner tunnel at the same origin. http:// is upgraded to ws:// by the
# worker + runner URL builders. Tracks REGISTRY_HTTP_PORT so an alternate registry
# port works end to end (worker + runner dial the right origin).
: "${REGISTRY_TUNNEL_BASE_URL:=http://localhost:${REGISTRY_HTTP_PORT}}"
# Host directory under which the worker creates per-session runner workspaces.
: "${WORKSPACE_DIR:=/var/tmp/orca-self-hosted-workspaces}"
# A human-readable name for the worker (defaults to hostname in the worker if
# unset; we pin one so logs are recognizable on a shared box).
: "${ENVIRONMENT_WORKER_NAME:=self-hosted-dev}"
# Name of the environment the script creates over the API. UNIQUE per run, because
# the registry enforces name uniqueness per workspace and this script creates a NEW
# environment every time -- an Env Key is echoed exactly once and never stored, so a
# previous environment's key cannot be recovered and reusing the row is not an option.
# A fixed name therefore made the second run of the day fail with a bare `curl: (22)
# ... 409`, contradicting the "re-running is safe" promise at the top of this file.
# Random rather than a timestamp so two runs in the same second, or on two checkouts,
# also stay clear of each other.
: "${SELF_HOSTED_ENVIRONMENT_NAME:=self-hosted-$(openssl rand -hex 4)}"
# The api key this script uses to call the public API is seeded further down by
# @orca/e2e-tests' own `seedWorkspaceApiKey()`, so it is neither declared nor
# defaulted here: that helper mints the plaintext, and the key is stored only as an
# argon2 hash plus a fingerprint, so there is nothing to pin.
#
# What matters is the WORKSPACE, and the helper decides that too: it always seeds
# into `ws_e2e_tests`. The environment this script creates therefore belongs to the
# same workspace the e2e drives, which is what lets the e2e see and use the
# environment the worker attaches to. The e2e reads only the environment id from
# `self-hosted.env`; it seeds its own key by calling the same helper.

mkdir -p "${WORKSPACE_DIR}"

# Ensure the JWT keypair exists before the registry boots (the snapshot resolver
# mints scoped session JWTs with it). Idempotent.
if [ ! -s "${SECRETS_DIR}/session-jwt.pem" ]; then
  echo "start-self-hosted: secrets missing — running init-secrets.sh"
  "${SCRIPT_DIR}/init-secrets.sh"
fi
SESSION_JWT_PRIVATE_KEY_PEM="$(cat "${SECRETS_DIR}/session-jwt.pem")"

# The registry's internal listener requires EXACTLY ONE of INTERNAL_SERVICE_TOKEN or
# INTERNAL_SERVICE_TOKEN_FILE and refuses to boot otherwise -- an internal API that
# would otherwise come up unauthenticated. Nothing in this stack calls that listener
# (the colocated path has no harness-server and no ai-gateway), but the registry is
# one process and its config is validated as a whole, so a token is required to start
# at all.
#
# So mint one, file-based, exactly as start-services.sh does: a caller may supply
# either form, and the file is the canonical runtime copy either way. 0600 here,
# unlike start-services.sh's 0644 -- that file is bind-mounted into ai-gateway's
# non-root container, and this stack has no such reader.
INTERNAL_SERVICE_TOKEN_RUNTIME_FILE="${RUN_DIR}/internal-service-token"
if [ -n "${INTERNAL_SERVICE_TOKEN:-}" ] && [ -n "${INTERNAL_SERVICE_TOKEN_FILE:-}" ]; then
  echo "start-self-hosted: set only one of INTERNAL_SERVICE_TOKEN or INTERNAL_SERVICE_TOKEN_FILE" >&2
  exit 1
fi
if [ -n "${INTERNAL_SERVICE_TOKEN:-}" ]; then
  printf '%s\n' "${INTERNAL_SERVICE_TOKEN}" > "${INTERNAL_SERVICE_TOKEN_RUNTIME_FILE}"
elif [ -n "${INTERNAL_SERVICE_TOKEN_FILE:-}" ] &&
  [ "${INTERNAL_SERVICE_TOKEN_FILE}" != "${INTERNAL_SERVICE_TOKEN_RUNTIME_FILE}" ]; then
  cp "${INTERNAL_SERVICE_TOKEN_FILE}" "${INTERNAL_SERVICE_TOKEN_RUNTIME_FILE}"
elif [ ! -s "${INTERNAL_SERVICE_TOKEN_RUNTIME_FILE}" ]; then
  openssl rand -hex 32 > "${INTERNAL_SERVICE_TOKEN_RUNTIME_FILE}"
fi
chmod 600 "${INTERNAL_SERVICE_TOKEN_RUNTIME_FILE}"
INTERNAL_AUTH_MODE=static_token
INTERNAL_SERVICE_TOKEN_FILE="${INTERNAL_SERVICE_TOKEN_RUNTIME_FILE}"
unset INTERNAL_SERVICE_TOKEN
export INTERNAL_AUTH_MODE INTERNAL_SERVICE_TOKEN_FILE

export TRANSCRIPT_STORE_BACKEND DATABASE_URL TRANSCRIPT_STORE_DATABASE_URL
export FILESTORE_DATABASE_URL MEMORYSTORE_DATABASE_URL
export S3_ENDPOINT S3_BUCKET S3_ACCESS_KEY S3_SECRET_KEY S3_ACCESS_KEY_ID S3_SECRET_ACCESS_KEY
export S3_REGION S3_KEY_PREFIX OUTPUTS_KEY_PREFIX MEMORY_KEY_PREFIX
export SESSION_JWT_ISSUER SESSION_JWT_AUDIENCE SESSION_JWT_TTL_SECS SESSION_JWT_PRIVATE_KEY_PEM
export NODE_ENV ORCA_SECRET_STORE_MODE
export REGISTRY_HTTP_PORT REGISTRY_INTERNAL_HTTP_PORT REGISTRY_ADMIN_HTTP_PORT

# --- port pre-flight ---------------------------------------------------------

port_in_use() {
  lsof -nP -iTCP:"$1" -sTCP:LISTEN >/dev/null 2>&1
}

# All three registry listeners, each with the variable that moves it -- so the error
# tells you which port and which knob, instead of leaving you to read a stack trace
# out of the log after a two-minute wait.
check_port() {
  port="$1"; role="$2"; var="$3"
  if port_in_use "${port}"; then
    echo "start-self-hosted: TCP port ${port} (intended for the registry ${role} listener) is already in use." >&2
    echo "  stop the holder or set ${var} to a free port. Current holder:" >&2
    lsof -nP -iTCP:"${port}" -sTCP:LISTEN 2>/dev/null | sed 's/^/    /' >&2 || true
    exit 1
  fi
}

check_port "${REGISTRY_HTTP_PORT}" public REGISTRY_HTTP_PORT
check_port "${REGISTRY_INTERNAL_HTTP_PORT}" internal REGISTRY_INTERNAL_HTTP_PORT
check_port "${REGISTRY_ADMIN_HTTP_PORT}" admin REGISTRY_ADMIN_HTTP_PORT

# --- compose up (postgres + rustfs) -----------------------------------------

# A distinct compose project name keeps these containers isolated from the full
# `docker-compose.yml` stack (their own network + named containers), so the two
# can coexist on a dev box without name clashes.
COMPOSE=(docker compose -p orca-self-hosted -f "${DEV_DIR}/docker-compose.self-hosted.yml")

node_major="$(node -p "process.versions.node.split('.')[0]")"
if [ "${node_major}" -lt 22 ]; then
  echo "start-self-hosted: Node.js >=22 is required; current node is $(node -v) at $(command -v node)" >&2
  exit 1
fi

# FRESH=1 discards the volumes before starting. Everything this stack persists is
# derived — the registry re-runs its migrations and a new Environment is minted each
# run anyway — so the only thing a stale volume carries is a schema from an older
# checkout, which surfaces as a confusing migration failure rather than as an obvious
# "your database is old". Cheap to discard, expensive to debug.
if [ "${FRESH:-}" = "1" ]; then
  echo "start-self-hosted: FRESH=1 — discarding the postgres + rustfs volumes first"
  "${COMPOSE[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
fi

echo "start-self-hosted: bringing up postgres + rustfs (project orca-self-hosted)…"
"${COMPOSE[@]}" up -d postgres rustfs rustfs-bootstrap

wait_for_tcp() {
  host="$1"; port="$2"; name="$3"
  i=0
  while [ "${i}" -lt 120 ]; do
    if (echo > "/dev/tcp/${host}/${port}") >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  echo "start-self-hosted: timed out waiting for ${name} on ${host}:${port}" >&2
  return 1
}

echo "start-self-hosted: waiting for postgres + rustfs…"
wait_for_tcp localhost "${SELF_HOSTED_POSTGRES_PORT}" postgres
wait_for_tcp localhost "${SELF_HOSTED_RUSTFS_PORT}" rustfs

# --- build + migrate ---------------------------------------------------------

cd "${REPO_ROOT}"

echo "start-self-hosted: building TypeScript packages (pnpm -r build)"
pnpm -r build

echo "start-self-hosted: applying file-store + memory-store + registry migrations"
pnpm -F @orca/file-store db:migrate
pnpm -F @orca/memory-store db:migrate
pnpm -F @orca/registry-service-ts db:migrate

# --- detached service launcher (survives this script's process group) --------

start_service() {
  name="$1"; shift
  logfile="${LOG_DIR}/${name}.log"
  pidfile="${RUN_DIR}/${name}.pid"

  if [ -f "${pidfile}" ]; then
    existing="$(cat "${pidfile}" 2>/dev/null || true)"
    if [ -n "${existing}" ] && kill -0 "${existing}" 2>/dev/null; then
      echo "start-self-hosted: ${name} already running (pid ${existing}); stopping it first"
      kill -TERM "${existing}" 2>/dev/null || true
      sleep 1
    fi
    rm -f "${pidfile}"
  fi

  echo "start-self-hosted: launching ${name} (log: ${logfile})"
  pid="$(
    ORCA_DEV_LOGFILE="${logfile}" node -e '
      const fs = require("node:fs");
      const { spawn } = require("node:child_process");
      const logfile = process.env.ORCA_DEV_LOGFILE;
      const fd = fs.openSync(logfile, "a");
      const child = spawn(process.argv[1], process.argv.slice(2), {
        detached: true,
        env: process.env,
        stdio: ["ignore", fd, fd],
      });
      child.on("error", (err) => {
        console.error(err.stack || err.message || String(err));
        process.exit(1);
      });
      child.unref();
      console.log(child.pid);
    ' "$@"
  )"
  echo "${pid}" > "${pidfile}"
}

# --- start the registry ------------------------------------------------------

# All three, translated. The registry reads HTTP_PORT / INTERNAL_HTTP_PORT /
# ADMIN_HTTP_PORT; the REGISTRY_-prefixed names are the operator-facing spelling
# start-services.sh already uses. Passing only the public one left the other two
# pinned to 8081/8082 no matter what the caller set, so an alternate-port run still
# collided -- and the pre-flight above, which checks the REGISTRY_ names, would have
# been checking ports the registry never bound.
HTTP_PORT="${REGISTRY_HTTP_PORT}" \
INTERNAL_HTTP_PORT="${REGISTRY_INTERNAL_HTTP_PORT}" \
ADMIN_HTTP_PORT="${REGISTRY_ADMIN_HTTP_PORT}" \
  start_service registry node services/registry-service-ts/dist/main.js

wait_for_http() {
  url="$1"; name="$2"; log_hint="${3:-${LOG_DIR}/${name}.log}"
  i=0
  while [ "${i}" -lt 120 ]; do
    code="$(curl -fsS -o /dev/null -w '%{http_code}' --max-time 2 "${url}" 2>/dev/null || echo 000)"
    if [ "${code}" = "200" ]; then
      echo "start-self-hosted: ${name} healthy (${url})"
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  echo "start-self-hosted: ${name} did not become healthy at ${url} within 120s — see ${log_hint}" >&2
  return 1
}

echo "start-self-hosted: waiting for registry health…"
wait_for_http "http://localhost:${REGISTRY_HTTP_PORT}/healthz" registry

# --- seed a bootstrap api key + create the self_hosted environment -----------

# Seed the workspace api key by CALLING @orca/e2e-tests' `seedWorkspaceApiKey()`,
# not by re-issuing its INSERT. The copy this replaced had drifted: it wrote six
# columns while the helper now writes eleven, so the first run against this branch's
# schema died on `null value in column "key_fingerprint" violates not-null
# constraint` -- a schema change three PRs away silently invalidating a hand-copied
# statement. The helper also seeds the organization + workspace rows the key needs,
# which the copy never did.
#
# It returns the plaintext once (the key is stored only as an argon2 hash and a
# fingerprint), so capture it here. It is idempotent and always lands in the SAME
# workspace the e2e seeds into, which is what lets the e2e see and use the
# environment this script attaches the worker to.
echo "start-self-hosted: seeding bootstrap api key via @orca/e2e-tests seedWorkspaceApiKey"
bootstrap_api_key="$(
  cd "${REPO_ROOT}/packages/e2e-tests" &&
  DATABASE_URL="${DATABASE_URL}" \
  node --input-type=module -e '
    import { seedWorkspaceApiKey } from "./src/seed.ts";
    const seeded = await seedWorkspaceApiKey();
    process.stdout.write(seeded.apiKey);
  ' 2>/dev/null
)"

if [ -z "${bootstrap_api_key}" ]; then
  echo "start-self-hosted: failed to seed a bootstrap api key" >&2
  exit 1
fi

echo "start-self-hosted: creating self_hosted environment via POST /v1/environments"
# Parse the response body's `id` + `env_key` with node (no jq dependency). The
# env_key is returned exactly once on create and is never recoverable, so we capture
# it here and persist it (mode 600) for the worker + the e2e.
#
# `orca-beta: true` is REQUIRED, not decorative. The default wire for this route is
# Anthropic's `BetaEnvironment` projection, which has no `env_key` field at all --
# without the header the create succeeds and returns an environment whose key is
# unobtainable, since the raw key is never persisted and only `rotate-key` (also
# beta-gated) can mint another. That is a worker this script could never attach.
create_out="$(
  curl -fsS -X POST "http://localhost:${REGISTRY_HTTP_PORT}/v1/environments" \
    -H "x-api-key: ${bootstrap_api_key}" \
    -H 'orca-beta: true' \
    -H 'content-type: application/json' \
    -d "{\"name\":\"${SELF_HOSTED_ENVIRONMENT_NAME}\",\"target\":\"self_hosted\",\"egress_mode\":\"sidecar\"}"
)"

# `egress_mode=sidecar`: a sidecar session resolves the agent snapshot WITHOUT a
# gateway URL (the resolver only requires AI_GATEWAY_MCP_URL for gateway egress),
# so the mock-provider e2e needs no ai-gateway at all.

env_parsed="$(printf '%s' "${create_out}" | node -e '
  let s = "";
  process.stdin.on("data", (d) => (s += d));
  process.stdin.on("end", () => {
    const o = JSON.parse(s);
    if (!o.id || !o.env_key) {
      console.error("create-environment response missing id/env_key: " + s);
      process.exit(1);
    }
    process.stdout.write(o.id + "\n" + o.env_key + "\n");
  });
')"
ENVIRONMENT_ID="$(printf '%s' "${env_parsed}" | sed -n '1p')"
ENVIRONMENT_KEY="$(printf '%s' "${env_parsed}" | sed -n '2p')"

if [ -z "${ENVIRONMENT_ID}" ] || [ -z "${ENVIRONMENT_KEY}" ]; then
  echo "start-self-hosted: failed to create environment (empty id/env_key)" >&2
  exit 1
fi

# Persist the environment wiring for the e2e + humans (mode 600 — the env_key is
# a credential). The e2e reads ORCA_SELF_HOSTED_ENVIRONMENT_ID from here too.
umask_old="$(umask)"
umask 077
{
  echo "# Written by start-self-hosted.sh — the self_hosted environment the worker is attached to."
  echo "ORCA_SELF_HOSTED_ENVIRONMENT_ID=${ENVIRONMENT_ID}"
  echo "ORCA_SELF_HOSTED_ENVIRONMENT_KEY=${ENVIRONMENT_KEY}"
  echo "ORCA_SELF_HOSTED_ENVIRONMENT_NAME=${SELF_HOSTED_ENVIRONMENT_NAME}"
} > "${self_hosted_env_file}"
umask "${umask_old}"

echo "start-self-hosted: created environment ${ENVIRONMENT_ID} (env_key captured → ${self_hosted_env_file})"

# --- start the environment-worker --------------------------------------------

runner_main="${REPO_ROOT}/services/session-runner/dist/main.js"
if [ ! -s "${runner_main}" ]; then
  echo "start-self-hosted: session-runner build missing at ${runner_main}" >&2
  exit 1
fi

# Real-provider SDK path (opt-in via a model credential): a spawned runner inherits
# only an ALLOWLIST of the worker's env plus the operator-named passthrough extras
# (see services/environment-worker/src/runner-env.ts). For the real Claude SDK
# providers (`claude`, `claude-sdk-persistent`) to reach Anthropic, the worker must
# both HOLD the credential AND forward it to the runner:
#   - ANTHROPIC_API_KEY lands in the worker's env here (default empty — harmless; the
#     mock provider needs no key, so the CI-default self-hosted path stays green
#     without a secret). Empty just means "no real-provider egress".
#   - ORCA_RUNNER_ENV_PASSTHROUGH names the vars the worker passes worker→runner. The
#     default forwards the Anthropic key + the optional base-URL / default-model knobs
#     and OPENAI_API_KEY (for a codex-provider smoke); unset names are no-ops. An
#     operator may override the list. Only vars actually present in the worker env are
#     forwarded, so listing an unset one is harmless.
# Launched through `oeadm worker`, not by spawning the worker's entry directly.
# `packages/oeadm/src/commands/worker.ts` already owns the flag→env-var mapping and
# `pnpm docs:env-check` gates it against the worker's own contract table, so setting
# those names here would be a second hand-maintained copy of one contract — the exact
# drift that put a wrong route and a wrong field name in that CLI to begin with.
#
# ENVIRONMENT_KEY goes through the ENVIRONMENT and not `--env-key`: an Env Key on the
# command line is visible in `ps` to every user on the host. The credential and
# passthrough vars below need no flag at all — `runWorker` spawns the worker with
# `{ ...process.env, ...mapped }`, so anything exported here reaches it.
oeadm_bin="${REPO_ROOT}/packages/oeadm/dist/bin.js"
if [ ! -s "${oeadm_bin}" ]; then
  echo "start-self-hosted: oeadm build missing at ${oeadm_bin}" >&2
  exit 1
fi

ENVIRONMENT_KEY="${ENVIRONMENT_KEY}" \
ANTHROPIC_API_KEY="${ANTHROPIC_API_KEY:-}" \
ORCA_RUNNER_ENV_PASSTHROUGH="${ORCA_RUNNER_ENV_PASSTHROUGH:-ANTHROPIC_API_KEY,OPENAI_API_KEY,ANTHROPIC_BASE_URL,ANTHROPIC_MODEL_DEFAULT}" \
  start_service environment-worker node "${oeadm_bin}" worker \
    --environment "${ENVIRONMENT_ID}" \
    --registry "${REGISTRY_TUNNEL_BASE_URL}" \
    --workspace-dir "${WORKSPACE_DIR}" \
    --runner-command "node ${runner_main}" \
    --name "${ENVIRONMENT_WORKER_NAME}"

# Wait until the worker's worker-tunnel claim is live: the public work_stats route
# reports worker_connected once the durable claim heartbeat is fresh. Poll it so
# the script only returns when a session created now would actually be dispatched
# (not left pending). Uses the bootstrap api key (same workspace that owns the env).
#
# SELF_HOSTED_SKIP_WORKER_WAIT=1 brings the stack up and returns immediately
# without waiting (a fast "leave it up, I'll inspect it myself" mode for local
# debugging); the normal path waits so callers get a ready-to-use stack.
if [ "${SELF_HOSTED_SKIP_WORKER_WAIT:-0}" = "1" ]; then
  echo "start-self-hosted: SELF_HOSTED_SKIP_WORKER_WAIT=1 — not waiting for the worker to connect"
  trap - EXIT
  cat <<EOF
start-self-hosted: stack is up (worker connect NOT awaited).
  registry           http://localhost:${REGISTRY_HTTP_PORT}/healthz
  environment        ${ENVIRONMENT_ID}
  env file           ${self_hosted_env_file}
  logs               ${LOG_DIR}/{registry,environment-worker}.log
Stop with: bash ${SCRIPT_DIR}/stop-self-hosted.sh
EOF
  exit 0
fi

echo "start-self-hosted: waiting for the worker to connect (work_stats.worker_connected)…"
i=0
worker_ready=0
while [ "${i}" -lt 60 ]; do
  ws_out="$(
    curl -fsS --max-time 2 \
      -H "x-api-key: ${bootstrap_api_key}" \
      "http://localhost:${REGISTRY_HTTP_PORT}/v1/environments/${ENVIRONMENT_ID}/work_stats" 2>/dev/null || true
  )"
  if [ -n "${ws_out}" ]; then
    connected="$(printf '%s' "${ws_out}" | node -e '
      let s = ""; process.stdin.on("data", (d) => (s += d));
      process.stdin.on("end", () => {
        try { const o = JSON.parse(s); process.stdout.write(o.worker_connected ? "yes" : "no"); }
        catch { process.stdout.write("no"); }
      });
    ' 2>/dev/null || echo no)"
    if [ "${connected}" = "yes" ]; then
      worker_ready=1
      break
    fi
  fi
  i=$((i + 1))
  sleep 1
done

if [ "${worker_ready}" -ne 1 ]; then
  echo "start-self-hosted: worker did not connect within 60s — see ${LOG_DIR}/environment-worker.log" >&2
  exit 1
fi

# Success — disarm the failure-cleanup trap so a clean exit leaves the stack up.
trap - EXIT

cat <<EOF
start-self-hosted: stack is up.
  registry           http://localhost:${REGISTRY_HTTP_PORT}/healthz
  environment        ${ENVIRONMENT_ID} (worker connected)
  transcript backend postgres
  env file           ${self_hosted_env_file}
  logs               ${LOG_DIR}/{registry,environment-worker}.log
  runner logs        ${LOG_DIR}/session-runner-*.log (one per spawned runner, if the worker wires file logs)
  pids               ${RUN_DIR}/{registry,environment-worker}.pid
Stop with: bash ${SCRIPT_DIR}/stop-self-hosted.sh
EOF
