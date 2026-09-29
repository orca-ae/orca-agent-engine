#!/usr/bin/env bash
# Copyright The Orca Authors
# SPDX-License-Identifier: Apache-2.0

# start-services.sh — spawn registry-service-ts + harness-server natively on
# the host and ai-gateway as a docker-compose service, against the compose infra.
#
# Hybrid architecture: registry + harness run on the host (so harness can use
# `srt` without container-in-container) while Postgres/RustFS, the selected
# transcript broker, and the external ai-gateway image live in docker-compose.
# See services/dev/README.md
# "stack-up" section.
#
# Behavior:
#  - Sources `services/dev/.env` if present, else falls back to inline defaults
#    that match the compose stack.
#  - Verifies prerequisite binaries (pnpm, node, docker) up front and
#    bails out with a clear error if any are missing.
#  - Builds + applies DB migrations, then backgrounds registry/harness and
#    starts ai-gateway via docker compose (logs in services/dev/logs/, pids in
#    services/dev/run/ for the native services).
#  - Polls the three /healthz endpoints (with timeout) before returning so
#    `make stack-up` only exits once everything is reachable.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEV_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${DEV_DIR}/../.." && pwd)"
LOG_DIR="${DEV_DIR}/logs"
RUN_DIR="${DEV_DIR}/run"
SECRETS_DIR="${DEV_DIR}/secrets"

mkdir -p "${LOG_DIR}" "${RUN_DIR}"
chmod 700 "${RUN_DIR}"

# --- prereq probe ------------------------------------------------------------

# `lsof` is REQUIRED (used below for the port pre-flight); preserve consistent
# behavior across macOS + Linux instead of silently skipping the check when
# absent. macOS ships lsof; on Debian/Ubuntu it's `apt install lsof`.
missing=()
for bin in pnpm node docker openssl curl lsof; do
  if ! command -v "${bin}" >/dev/null 2>&1; then
    missing+=("${bin}")
  fi
done
if [[ "${#missing[@]}" -gt 0 ]]; then
  echo "start-services: missing required binaries: ${missing[*]}" >&2
  echo "  install via your package manager (brew install ${missing[*]} or apt install ${missing[*]})" >&2
  exit 1
fi

# --- failure cleanup trap ----------------------------------------------------

# If the script exits non-zero AFTER any service has been backgrounded, we'd
# otherwise leak harness/gateway processes (set -e exits the script but doesn't
# reap children). EXIT (not ERR) is the right hook: it fires for *every* exit
# path including explicit `exit N` calls, so we get cleanup whether the failure
# came from `set -e`, a wait_for_http timeout, or a non-zero `return 1`. ERR
# misses explicit exits and behaves inconsistently inside subshells. The
# cleanup is best-effort: stop-services.sh is idempotent and safe to invoke
# even before any services have started.
on_failure() {
  local exit_code=$?
  if [[ ${exit_code} -ne 0 ]]; then
    echo "start-services: failed (exit ${exit_code}) — cleaning up backgrounded services..." >&2
    "${SCRIPT_DIR}/stop-services.sh" 2>/dev/null || true
  fi
  exit ${exit_code}
}
trap on_failure EXIT

# --- env wiring --------------------------------------------------------------

CALLER_TRANSCRIPT_STORE_BACKEND="${TRANSCRIPT_STORE_BACKEND-}"
if [[ -f "${DEV_DIR}/.env" ]]; then
  echo "start-services: sourcing ${DEV_DIR}/.env"
  # shellcheck disable=SC1091
  set -a
  . "${DEV_DIR}/.env"
  set +a
fi
if [[ -n "${CALLER_TRANSCRIPT_STORE_BACKEND}" ]]; then
  TRANSCRIPT_STORE_BACKEND="${CALLER_TRANSCRIPT_STORE_BACKEND}"
fi

# Fallback defaults — match services/dev/docker-compose.yml. The caller's
# TRANSCRIPT_STORE_BACKEND overrides `.env`; other values follow `.env` first,
# then the inline localhost defaults below.
: "${TRANSCRIPT_STORE_BACKEND:=kafka}"
TRANSCRIPT_STORE_BACKEND="$(printf '%s' "${TRANSCRIPT_STORE_BACKEND}" | tr '[:upper:]' '[:lower:]')"
case "${TRANSCRIPT_STORE_BACKEND}" in
  kafka | postgres | pulsar) ;;
  *)
    echo "start-services: unsupported TRANSCRIPT_STORE_BACKEND=${TRANSCRIPT_STORE_BACKEND}" >&2
    echo "  expected one of: kafka, postgres, pulsar" >&2
    exit 1
    ;;
esac
: "${DATABASE_URL:=postgres://orca:orca@localhost:5432/registry}"
: "${TRANSCRIPT_STORE_DATABASE_URL:=postgres://orca:orca@localhost:5432/transcriptstore}"
: "${FILESTORE_DATABASE_URL:=postgres://orca:orca@localhost:5432/filestore}"
: "${MEMORYSTORE_DATABASE_URL:=postgres://orca:orca@localhost:5432/memorystore}"
: "${KAFKA_BROKERS:=localhost:9092}"
: "${KAFKA_CONNECTION_MODE:=plaintext}"
: "${KAFKA_SSL:=}"
: "${KAFKA_SSL_REJECT_UNAUTHORIZED:=}"
: "${KAFKA_SSL_CA_FILE:=}"
: "${KAFKA_SSL_CERT_FILE:=}"
: "${KAFKA_SSL_KEY_FILE:=}"
: "${KAFKA_SASL_MECHANISM:=}"
: "${KAFKA_SASL_USERNAME:=}"
: "${KAFKA_SASL_PASSWORD:=}"
: "${KAFKA_AUTH_TOKEN:=}"
if [[ "${TRANSCRIPT_STORE_BACKEND}" == "kafka" ]]; then
  # Local-stack default: poll every 5s for newly-created session topics and
  # replace the explicit canonical topic-list subscription. Operators can
  # override this via their .env or the secrets backend; Kafka rejects zero.
  : "${KAFKA_TOPIC_REDISCOVER_INTERVAL_MS:=5000}"
else
  : "${KAFKA_TOPIC_REDISCOVER_INTERVAL_MS:=0}"
fi
: "${PULSAR_SERVICE_URL:=pulsar://localhost:6650}"
: "${PULSAR_TENANT:=public}"
: "${PULSAR_NAMESPACE:=default}"
: "${PULSAR_TOPIC_PREFIX:=orca}"
: "${PULSAR_RECEIVE_TIMEOUT_MS:=500}"
: "${PULSAR_AUTH_TYPE:=}"
: "${PULSAR_AUTH_TOKEN:=}"
: "${PULSAR_OAUTH2_TYPE:=}"
: "${PULSAR_OAUTH2_ISSUER_URL:=}"
: "${PULSAR_OAUTH2_CLIENT_ID:=}"
: "${PULSAR_OAUTH2_CLIENT_SECRET:=}"
: "${PULSAR_OAUTH2_PRIVATE_KEY:=}"
: "${PULSAR_OAUTH2_AUDIENCE:=}"
: "${PULSAR_OAUTH2_SCOPE:=}"
: "${S3_ENDPOINT:=http://localhost:9000}"
# Preserve an explicit empty value so local runs can exercise the AWS SDK default.
: "${S3_STS_ENDPOINT=http://localhost:9000}"
: "${S3_BUCKET:=orca-files}"
: "${S3_ACCESS_KEY:=minioadmin}"
: "${S3_SECRET_KEY:=minioadmin}"
: "${S3_ACCESS_KEY_ID:=${S3_ACCESS_KEY}}"
: "${S3_SECRET_ACCESS_KEY:=${S3_SECRET_KEY}}"
: "${S3_REGION:=us-east-1}"
: "${S3_KEY_PREFIX:=managed-agents/}"
: "${ALLOW_INSECURE_STATIC_S3_CREDS:=true}"
: "${SESSION_JWT_ISSUER:=orca-registry}"
: "${SESSION_JWT_AUDIENCE:=ai-gateway}"
: "${SESSION_JWT_TTL_SECS:=300}"
: "${NODE_ENV:=development}"
: "${ORCA_SECRET_STORE_MODE:=local}"
: "${AI_GATEWAY_URL:=http://localhost:8090}"
: "${HARNESS_WORK_DIR:=/var/tmp/orca-harness}"
: "${SESSION_IDLE_TIMEOUT_MS:=60000}"
: "${SANDBOX_RUNTIME:=local}"
: "${ANTHROPIC_API_KEY:=}"
: "${OPENAI_API_KEY:=}"
: "${OPENAI_BASE_URL:=}"
: "${AI_GATEWAY_IMAGE:=ghcr.io/orca-ae/orca-ai-gateway:v0.4.3-rc.3}"
: "${REGISTRY_HTTP_PORT:=8080}"
: "${REGISTRY_INTERNAL_HTTP_PORT:=8081}"
: "${REGISTRY_ADMIN_HTTP_PORT:=8082}"
: "${REGISTRY_BASE_URL:=http://localhost:${REGISTRY_HTTP_PORT}}"
: "${REGISTRY_INTERNAL_BASE_URL:=http://localhost:${REGISTRY_INTERNAL_HTTP_PORT}}"
: "${REGISTRY_ADMIN_BASE_URL:=http://localhost:${REGISTRY_ADMIN_HTTP_PORT}}"
: "${HARNESS_HTTP_PORT:=9094}"
: "${AI_GATEWAY_ADMIN_PORT:=9099}"

# The non-Kubernetes stack uses one high-entropy service token. Normalize a
# caller-provided raw token or source file into a private runtime file so
# Registry, Harness, and the gateway container all consume the same value.
# Keeping file-based consumption also exercises rotation-safe request paths.
INTERNAL_SERVICE_TOKEN_RUNTIME_FILE="${RUN_DIR}/internal-service-token"
INTERNAL_SERVICE_TOKEN_SOURCE_FILE="${INTERNAL_SERVICE_TOKEN_FILE-}"
if [[ -n "${INTERNAL_SERVICE_TOKEN-}" && -n "${INTERNAL_SERVICE_TOKEN_SOURCE_FILE}" ]]; then
  echo "start-services: set only one of INTERNAL_SERVICE_TOKEN or INTERNAL_SERVICE_TOKEN_FILE" >&2
  exit 1
fi
if [[ -n "${INTERNAL_SERVICE_TOKEN-}" ]]; then
  printf '%s\n' "${INTERNAL_SERVICE_TOKEN}" > "${INTERNAL_SERVICE_TOKEN_RUNTIME_FILE}"
elif [[ -n "${INTERNAL_SERVICE_TOKEN_SOURCE_FILE}" && "${INTERNAL_SERVICE_TOKEN_SOURCE_FILE}" != "${INTERNAL_SERVICE_TOKEN_RUNTIME_FILE}" ]]; then
  cp "${INTERNAL_SERVICE_TOKEN_SOURCE_FILE}" "${INTERNAL_SERVICE_TOKEN_RUNTIME_FILE}"
elif [[ ! -s "${INTERNAL_SERVICE_TOKEN_RUNTIME_FILE}" ]]; then
  openssl rand -hex 32 > "${INTERNAL_SERVICE_TOKEN_RUNTIME_FILE}"
fi
# The runtime directory is 0700 on the host, while the file itself must be
# readable by ai-gateway's non-root UID after Docker bind-mounts it directly.
chmod 644 "${INTERNAL_SERVICE_TOKEN_RUNTIME_FILE}"
if [[ "$(tr -d '[:space:]' < "${INTERNAL_SERVICE_TOKEN_RUNTIME_FILE}" | wc -c | tr -d ' ')" -lt 32 ]]; then
  echo "start-services: internal service token must contain at least 32 non-whitespace characters" >&2
  exit 1
fi
INTERNAL_AUTH_MODE=static_token
INTERNAL_SERVICE_TOKEN_FILE="${INTERNAL_SERVICE_TOKEN_RUNTIME_FILE}"
unset INTERNAL_SERVICE_TOKEN

# Ensure the keypair exists before touching env values that reference it.
# Bash's noclobber rules + idempotent script make this safe to call from both
# `make stack-up` and a standalone `bash start-services.sh` run.
if [[ ! -s "${SECRETS_DIR}/session-jwt.pem" || ! -s "${SECRETS_DIR}/session-jwt-pub.pem" ]]; then
  echo "start-services: secrets missing — running init-secrets.sh"
  "${SCRIPT_DIR}/init-secrets.sh"
fi

# `SESSION_JWT_PRIVATE_KEY_PEM` accepts either an inline PEM (contains BEGIN)
# or a file path — registry-service-ts/src/config.ts handles both. We pass the
# PEM contents inline so the registry doesn't have to share the same fs path
# in container-in-host setups.
SESSION_JWT_PRIVATE_KEY_PEM="$(cat "${SECRETS_DIR}/session-jwt.pem")"
export SESSION_JWT_PRIVATE_KEY_PEM
export DATABASE_URL TRANSCRIPT_STORE_BACKEND TRANSCRIPT_STORE_DATABASE_URL FILESTORE_DATABASE_URL MEMORYSTORE_DATABASE_URL
export KAFKA_BROKERS KAFKA_CONNECTION_MODE KAFKA_SSL KAFKA_SSL_REJECT_UNAUTHORIZED
export KAFKA_SSL_CA_FILE KAFKA_SSL_CERT_FILE KAFKA_SSL_KEY_FILE
export KAFKA_SASL_MECHANISM KAFKA_SASL_USERNAME KAFKA_SASL_PASSWORD KAFKA_AUTH_TOKEN
# Keep Registry auth separate from broker auth; unset settings remain unset.
export KAFKA_TRANSCRIPT_ENCODING KAFKA_SCHEMA_REGISTRY_URL KAFKA_SCHEMA_REGISTRY_SUBJECT
export KAFKA_SCHEMA_REGISTRY_AUTO_REGISTER KAFKA_SCHEMA_REGISTRY_REQUEST_TIMEOUT_MS
export KAFKA_SCHEMA_REGISTRY_AUTH_MODE KAFKA_SCHEMA_REGISTRY_USERNAME KAFKA_SCHEMA_REGISTRY_PASSWORD
export KAFKA_SCHEMA_REGISTRY_CA_FILE KAFKA_SCHEMA_REGISTRY_CERT_FILE KAFKA_SCHEMA_REGISTRY_KEY_FILE
export PULSAR_SERVICE_URL PULSAR_TENANT PULSAR_NAMESPACE PULSAR_TOPIC_PREFIX PULSAR_RECEIVE_TIMEOUT_MS
export PULSAR_ACK_TIMEOUT_MS POSTGRES_EVENT_POLL_INTERVAL_MS POSTGRES_EVENT_LEASE_MS
export PULSAR_AUTH_TYPE PULSAR_AUTH_TOKEN PULSAR_OAUTH2_TYPE PULSAR_OAUTH2_ISSUER_URL
export PULSAR_OAUTH2_CLIENT_ID PULSAR_OAUTH2_CLIENT_SECRET PULSAR_OAUTH2_PRIVATE_KEY
export PULSAR_OAUTH2_AUDIENCE PULSAR_OAUTH2_SCOPE
export S3_ENDPOINT S3_STS_ENDPOINT S3_BUCKET S3_ACCESS_KEY S3_SECRET_KEY S3_ACCESS_KEY_ID S3_SECRET_ACCESS_KEY S3_REGION
export S3_KEY_PREFIX
export ALLOW_INSECURE_STATIC_S3_CREDS
export SESSION_JWT_ISSUER SESSION_JWT_AUDIENCE SESSION_JWT_TTL_SECS
export NODE_ENV ORCA_SECRET_STORE_MODE
export REGISTRY_BASE_URL REGISTRY_INTERNAL_BASE_URL REGISTRY_ADMIN_BASE_URL AI_GATEWAY_URL HARNESS_WORK_DIR SESSION_IDLE_TIMEOUT_MS
export LLM_GATEWAY_URL
export OPEN_SANDBOX_DOMAIN OPEN_SANDBOX_PROTOCOL OPEN_SANDBOX_API_KEY OPEN_SANDBOX_IMAGE OPEN_SANDBOX_ENTRYPOINT
export OPEN_SANDBOX_TIMEOUT_SECONDS OPEN_SANDBOX_USE_SERVER_PROXY OPEN_SANDBOX_REQUEST_TIMEOUT_SECONDS
export OPEN_SANDBOX_RESOURCE_CPU OPEN_SANDBOX_RESOURCE_MEMORY
export SANDBOX_RUNTIME ANTHROPIC_API_KEY OPENAI_API_KEY OPENAI_BASE_URL
export KAFKA_TOPIC_REDISCOVER_INTERVAL_MS AI_GATEWAY_IMAGE
export INTERNAL_AUTH_MODE INTERNAL_SERVICE_TOKEN_FILE

# --- check service ports are free -------------------------------------------

port_in_use() {
  # Return 0 if a process is already listening on the given TCP port.
  # Uses lsof (macOS + Linux) for portability — `ss` isn't on macOS, `netstat`
  # output format differs between BSD and GNU.
  local port="$1"
  lsof -nP -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1
}

url_port() {
  local url="$1"
  node -e 'const u = new URL(process.argv[1]); console.log(u.port || (u.protocol === "https:" ? "443" : "80"));' "${url}"
}

AI_GATEWAY_DATA_PORT="$(url_port "${AI_GATEWAY_URL}")"

for spec in \
  "registry:${REGISTRY_HTTP_PORT}" \
  "registry-internal:${REGISTRY_INTERNAL_HTTP_PORT}" \
  "registry-admin:${REGISTRY_ADMIN_HTTP_PORT}" \
  "harness:${HARNESS_HTTP_PORT}" \
  "ai-gateway-data:${AI_GATEWAY_DATA_PORT}" \
  "ai-gateway-admin:${AI_GATEWAY_ADMIN_PORT}"; do
  name="${spec%:*}"; port="${spec##*:}"
  if port_in_use "${port}"; then
    echo "start-services: TCP port ${port} (intended for ${name}) is already in use." >&2
    echo "  another process is listening — stop it first or override the port via" >&2
    echo "  REGISTRY_HTTP_PORT / REGISTRY_INTERNAL_HTTP_PORT / REGISTRY_ADMIN_HTTP_PORT / HARNESS_HTTP_PORT / AI_GATEWAY_URL / AI_GATEWAY_ADMIN_PORT." >&2
    echo "  current holder:" >&2
    lsof -nP -iTCP:"${port}" -sTCP:LISTEN 2>/dev/null | sed 's/^/    /' >&2 || true
    exit 1
  fi
done

# --- wait for infra healthcheck ---------------------------------------------

wait_for_tcp() {
  local host="$1" port="$2" name="$3"
  # 180s budget — Kafka/Pulsar cold starts on GH runners can take close to a
  # minute before the broker port accepts TCP. Local laptops usually settle
  # faster; CI is the bottleneck.
  local i
  for i in $(seq 1 180); do
    if (echo > "/dev/tcp/${host}/${port}") >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "start-services: timed out waiting for ${name} on ${host}:${port}" >&2
  return 1
}

case "${TRANSCRIPT_STORE_BACKEND}" in
  kafka) infra_names=(postgres kafka rustfs) ;;
  pulsar) infra_names=(postgres pulsar rustfs) ;;
  postgres) infra_names=(postgres rustfs) ;;
esac
echo "start-services: waiting for infra (${infra_names[*]}) for ${TRANSCRIPT_STORE_BACKEND} transcript backend…"
wait_for_tcp localhost 5432 "postgres"
wait_for_tcp localhost 9000 "rustfs"
case "${TRANSCRIPT_STORE_BACKEND}" in
  kafka) wait_for_tcp localhost 9092 "kafka" ;;
  pulsar) wait_for_tcp localhost 6650 "pulsar" ;;
  postgres) ;;
esac

# --- build + apply migrations -----------------------------------------------

cd "${REPO_ROOT}"

node_major="$(node -p "process.versions.node.split('.')[0]")"
if (( node_major < 22 )); then
  echo "start-services: Node.js >=22 is required; current node is $(node -v) at $(command -v node)" >&2
  echo "  Run \`nvm use 24\` or another Node >=22 runtime, then rerun \`make stack-up\`." >&2
  exit 1
fi

COMPOSE=(docker compose -f "${DEV_DIR}/docker-compose.yml")

echo "start-services: preparing ai-gateway image (${AI_GATEWAY_IMAGE})"
AI_GATEWAY_IMAGE="$("${SCRIPT_DIR}/prepare-ai-gateway-image.sh")"
export AI_GATEWAY_IMAGE

echo "start-services: building TypeScript packages (pnpm -r build)"
pnpm -r build

echo "start-services: applying file-store + memory-store + registry migrations"
pnpm -F @orca/file-store db:migrate
pnpm -F @orca/memory-store db:migrate
pnpm -F @orca/registry-service-ts db:migrate

# --- spawn services ---------------------------------------------------------

start_service() {
  local name="$1"; shift
  local logfile="${LOG_DIR}/${name}.log"
  local pidfile="${RUN_DIR}/${name}.pid"

  if [[ -f "${pidfile}" ]]; then
    local existing
    existing="$(cat "${pidfile}" 2>/dev/null || true)"
    if [[ -n "${existing}" ]] && kill -0 "${existing}" 2>/dev/null; then
      echo "start-services: ${name} already running (pid ${existing}); skipping"
      return 0
    fi
    rm -f "${pidfile}"
  fi

  echo "start-services: launching ${name} (log: ${logfile})"
  # Use a detached Node launcher instead of plain `cmd &`: Vitest/pnpm may
  # signal their process group during e2e runs, and native dev services must
  # survive as independent daemons after this script returns.
  #
  # Two-stage launch. The inline launcher below spawns a detached MONITOR
  # process (detached => new session, which is what shields it and the service
  # from process-group signals), reads a single pid line from its stdout pipe,
  # prints that pid for the command substitution, and exits. The monitor stays
  # behind: it spawns the actual service (NOT detached, so the service shares
  # the monitor session and keeps the signal isolation), reports the service
  # pid over the handshake pipe, then waits for the service to exit and appends
  # "start-services: <name> exited (code=... signal=...)" to the service
  # logfile. The monitor exists for silent-death diagnosis: the e2e-stack CI
  # saw harness-server disappear mid-run with the logfile just stopping — no
  # exit code, no signal, nothing to triage. After the handshake the monitor
  # must never write to stdout again: the launcher is gone and the pipe read
  # end is closed, so any further write would EPIPE.
  if [[ -n "${HTTP_PORT+x}" ]]; then export HTTP_PORT; fi
  local monitor_src
  monitor_src='
    const fs = require("node:fs");
    const { spawn } = require("node:child_process");
    const name = process.env.ORCA_DEV_SERVICE_NAME;
    // ORCA_DEV_* is launcher/monitor plumbing — strip it so the service env
    // stays clean.
    const env = { ...process.env };
    delete env.ORCA_DEV_MONITOR_SRC;
    delete env.ORCA_DEV_SERVICE_NAME;
    delete env.ORCA_DEV_LOGFILE;
    // fd 2 is the service logfile, inherited from the launcher (opened with
    // O_APPEND, so concurrent writes from monitor + service stay intact).
    const child = spawn(process.argv[1], process.argv.slice(2), {
      env,
      stdio: ["ignore", 2, 2],
    });
    // If the launcher is killed before reading the handshake, the pid write
    // below hits a closed pipe; swallow it so the monitor keeps supervising.
    process.stdout.on("error", () => {});
    child.on("error", (err) => {
      fs.writeSync(2, "start-services: " + name + " failed to spawn: " + (err.stack || String(err)) + "\n");
      process.exit(1);
    });
    child.on("spawn", () => {
      // pid handshake: exactly one line; stdout is off-limits afterwards.
      process.stdout.write(child.pid + "\n");
    });
    child.on("exit", (code, signal) => {
      const line = "start-services: " + name + " exited (code=" + code + " signal=" + signal + ") at " +
        new Date().toISOString() + "\n";
      fs.writeSync(2, line);
      process.exit(0);
    });
  '
  local pid
  pid="$(
    ORCA_DEV_LOGFILE="${logfile}" \
    ORCA_DEV_SERVICE_NAME="${name}" \
    ORCA_DEV_MONITOR_SRC="${monitor_src}" \
    node -e '
      const fs = require("node:fs");
      const { spawn } = require("node:child_process");
      const logfile = process.env.ORCA_DEV_LOGFILE;
      const name = process.env.ORCA_DEV_SERVICE_NAME;
      const fd = fs.openSync(logfile, "a");
      const monitor = spawn(process.execPath, ["-e", process.env.ORCA_DEV_MONITOR_SRC, ...process.argv.slice(1)], {
        detached: true,
        env: process.env,
        stdio: ["ignore", "pipe", fd],
      });
      monitor.on("error", (err) => {
        console.error(err.stack || err.message || String(err));
        process.exit(1);
      });
      let done = false;
      let buf = "";
      monitor.stdout.setEncoding("utf8");
      monitor.stdout.on("data", (chunk) => {
        if (done) return;
        buf += chunk;
        const nl = buf.indexOf("\n");
        if (nl === -1) return;
        done = true;
        monitor.unref();
        // Flush the pid to the command substitution before exiting; a bare
        // process.exit() can drop buffered pipe writes.
        process.stdout.write(buf.slice(0, nl + 1), () => process.exit(0));
      });
      monitor.stdout.on("end", () => {
        if (done) return;
        done = true;
        console.error("start-services: " + name + " monitor exited before reporting a pid — see " + logfile);
        process.exit(1);
      });
    ' "$@"
  )"
  echo "${pid}" > "${pidfile}"
}

# Registry: HTTP_PORT honors the env override. We set it explicitly so the
# inline default in config.ts (8080) and the wait-for-healthz loop below stay
# consistent under override.
HTTP_PORT="${REGISTRY_HTTP_PORT}" \
INTERNAL_HTTP_PORT="${REGISTRY_INTERNAL_HTTP_PORT}" \
ADMIN_HTTP_PORT="${REGISTRY_ADMIN_HTTP_PORT}" \
  start_service registry node services/registry-service-ts/dist/main.js

# Harness: separate HTTP_PORT so registry + harness don't collide.
HTTP_PORT="${HARNESS_HTTP_PORT}" \
  start_service harness node services/harness-server/dist/main.js

# ai-gateway: external image configured by services/dev/ai-gateway-config.yaml.
# The registry service is already listening on the host; docker-compose maps
# the `registry` hostname inside the container back to the Docker host.
echo "start-services: launching ai-gateway container (image: ${AI_GATEWAY_IMAGE})"
"${COMPOSE[@]}" up -d ai-gateway

# --- wait for service health ------------------------------------------------

wait_for_http() {
  local url="$1" name="$2"
  local log_hint="${3:-${LOG_DIR}/${name}.log}"
  # 300s budget leaves room for a first-time image pull on cold CI runners.
  local i
  for i in $(seq 1 300); do
    local code
    code="$(curl -fsS -o /dev/null -w '%{http_code}' --max-time 2 "${url}" 2>/dev/null || echo 000)"
    if [[ "${code}" == "200" ]]; then
      echo "start-services: ${name} healthy (${url})"
      return 0
    fi
    sleep 1
  done
  echo "start-services: ${name} did not become healthy at ${url} within 300s — see ${log_hint}" >&2
  return 1
}

echo "start-services: waiting for service health…"
wait_for_http "http://localhost:${REGISTRY_HTTP_PORT}/healthz" registry
wait_for_http "http://localhost:${REGISTRY_INTERNAL_HTTP_PORT}/healthz" registry-internal
wait_for_http "http://localhost:${REGISTRY_ADMIN_HTTP_PORT}/healthz" registry-admin
wait_for_http "http://localhost:${HARNESS_HTTP_PORT}/healthz" harness
wait_for_http "http://localhost:${AI_GATEWAY_ADMIN_PORT}/healthz" ai-gateway \
  "docker compose -f ${DEV_DIR}/docker-compose.yml logs ai-gateway"

cat <<EOF
start-services: stack is up.
  registry      http://localhost:${REGISTRY_HTTP_PORT}/healthz
  registry-int  http://localhost:${REGISTRY_INTERNAL_HTTP_PORT}/healthz
  registry-admin http://localhost:${REGISTRY_ADMIN_HTTP_PORT}/healthz
  harness       http://localhost:${HARNESS_HTTP_PORT}/healthz
  ai-gateway    http://localhost:${AI_GATEWAY_ADMIN_PORT}/healthz (data: ${AI_GATEWAY_URL})
  logs          ${LOG_DIR}/{registry,harness}.log; docker compose -f ${DEV_DIR}/docker-compose.yml logs ai-gateway
  pids          ${RUN_DIR}/{registry,harness}.pid
Stop with: make stack-down
EOF
