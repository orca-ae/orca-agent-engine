#!/usr/bin/env bash
# Copyright The Orca Authors
# SPDX-License-Identifier: Apache-2.0

# prepare-ai-gateway-image.sh — pull and smoke-test the configured external
# ai-gateway image. If its runtime is incompatible with the shipped binary
# (for example GLIBC_2.39 missing on linux/amd64), build a tiny compatibility
# wrapper from services/dev/ai-gateway-compat.Dockerfile.
#
# Output: prints the image tag that docker-compose should use to stdout. Logs
# go to stderr so callers can safely capture stdout into AI_GATEWAY_IMAGE.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEV_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
SOURCE_IMAGE="${AI_GATEWAY_IMAGE:-ghcr.io/orca-ae/orca-ai-gateway:v0.4.3-rc.3}"
COMPAT_IMAGE="${AI_GATEWAY_COMPAT_IMAGE:-orca-ai-gateway-compat:dev}"
DOCKERFILE="${DEV_DIR}/ai-gateway-compat.Dockerfile"
INTERNAL_SERVICE_TOKEN_FILE="${INTERNAL_SERVICE_TOKEN_FILE:-${DEV_DIR}/run/internal-service-token}"
gateway_config_path="${AI_GATEWAY_CONFIG_FILE:-${DEV_DIR}/ai-gateway-config.yaml}"
if [[ "${gateway_config_path}" != /* ]]; then
  gateway_config_path="${DEV_DIR}/${gateway_config_path}"
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "prepare-ai-gateway-image: docker is required" >&2
  exit 1
fi

if [[ ! -f "${DOCKERFILE}" ]]; then
  echo "prepare-ai-gateway-image: missing Dockerfile: ${DOCKERFILE}" >&2
  exit 1
fi

if [[ ! -s "${DEV_DIR}/secrets/session-jwt.pem" || ! -s "${DEV_DIR}/secrets/session-jwt-pub.pem" ]]; then
  echo "prepare-ai-gateway-image: secrets missing — running init-secrets.sh" >&2
  "${SCRIPT_DIR}/init-secrets.sh" >&2
fi

if [[ ! -s "${INTERNAL_SERVICE_TOKEN_FILE}" ]]; then
  echo "prepare-ai-gateway-image: missing internal service token file: ${INTERNAL_SERVICE_TOKEN_FILE}" >&2
  echo "  run through start-services.sh or set INTERNAL_SERVICE_TOKEN_FILE to a readable token file" >&2
  exit 1
fi

if [[ "${SOURCE_IMAGE}" != */* ]] && docker image inspect "${SOURCE_IMAGE}" >/dev/null 2>&1; then
  echo "prepare-ai-gateway-image: using local image ${SOURCE_IMAGE}" >&2
elif ! docker pull "${SOURCE_IMAGE}" >&2; then
  if docker image inspect "${SOURCE_IMAGE}" >/dev/null 2>&1; then
    echo "prepare-ai-gateway-image: pull failed; using local image ${SOURCE_IMAGE}" >&2
  else
    echo "prepare-ai-gateway-image: failed to pull ${SOURCE_IMAGE} and no local image exists" >&2
    exit 1
  fi
fi

probe_log="$(mktemp)"
capability_config="$(mktemp)"
resolver_capability_config="$(mktemp)"
cleanup() {
  rm -f "${probe_log}" "${capability_config}" "${resolver_capability_config}"
}
trap cleanup EXIT

check_config_compat() {
  local image="$1"

  # VaultConfig in older gateway releases accepts unknown fields, so a normal
  # config check cannot prove bearer_token_file support. Point the field at a
  # deliberately missing path: a compatible gateway must recognize the field,
  # fail validation, and name it in the diagnostic; an old gateway exits 0.
  sed \
    "s#^\([[:space:]]*bearer_token_file:\).*#\1 '/__orca_missing_bearer_token_capability_probe__'#" \
    "${gateway_config_path}" >"${capability_config}"
  chmod 644 "${capability_config}"
  if ! grep -q 'bearer_token_file:' "${capability_config}"; then
    echo "prepare-ai-gateway-image: capability probe could not find bearer_token_file in config" >&2
    exit 1
  fi
  if ! grep -q "^[[:space:]]*'\*':" "${capability_config}" ||
    ! grep -q 'destination_resolver:' "${capability_config}"; then
    echo "prepare-ai-gateway-image: capability probe could not find wildcard destination_resolver in config" >&2
    exit 1
  fi
  local capability_code=0
  docker run --rm \
    --entrypoint /usr/local/bin/orca-gateway \
    -v "${capability_config}:/tmp/ai-gateway-capability.yaml:ro" \
    -v "${DEV_DIR}/secrets/session-jwt-pub.pem:/etc/orca-gateway/session-jwt-pub.pem:ro" \
    "${image}" check /tmp/ai-gateway-capability.yaml \
    >"${probe_log}" 2>&1 || capability_code=$?
  if [[ "${capability_code}" == "0" ]] || ! grep -q 'bearer_token_file' "${probe_log}"; then
    echo "prepare-ai-gateway-image: ${image} does not support HTTP vault bearer_token_file" >&2
    echo "  choose an ai-gateway release that supports HTTP vault bearer_token_file" >&2
    exit 1
  fi

  # Probe wildcard destination_resolver independently from the optional boot
  # smoke. A supporting gateway must parse the nested MCP egress policy and
  # reject its deliberately invalid zero DNS timeout with the typed validator.
  # Older gateways that flatten/ignore destination_resolver either accept this
  # config or fail for another reason, neither of which proves compatibility.
  local resolver_dns_fields
  resolver_dns_fields="$(grep -c '^[[:space:]]*dns_timeout_ms:' "${gateway_config_path}" || true)"
  if [[ "${resolver_dns_fields}" != "1" ]]; then
    echo "prepare-ai-gateway-image: capability probe requires exactly one destination resolver dns_timeout_ms field" >&2
    exit 1
  fi
  sed \
    's/^\([[:space:]]*dns_timeout_ms:\)[[:space:]]*[0-9][0-9]*/\1 0/' \
    "${gateway_config_path}" >"${resolver_capability_config}"
  chmod 644 "${resolver_capability_config}"
  if ! grep -q '^[[:space:]]*dns_timeout_ms:[[:space:]]*0$' "${resolver_capability_config}"; then
    echo "prepare-ai-gateway-image: capability probe could not set destination resolver dns_timeout_ms to zero" >&2
    exit 1
  fi
  local resolver_capability_code=0
  docker run --rm \
    --entrypoint /usr/local/bin/orca-gateway \
    -v "${resolver_capability_config}:/tmp/ai-gateway-resolver-capability.yaml:ro" \
    -v "${DEV_DIR}/secrets/session-jwt-pub.pem:/etc/orca-gateway/session-jwt-pub.pem:ro" \
    -v "${INTERNAL_SERVICE_TOKEN_FILE}:/var/run/secrets/orca/registry-internal/token:ro" \
    "${image}" check /tmp/ai-gateway-resolver-capability.yaml \
    >"${probe_log}" 2>&1 || resolver_capability_code=$?
  if [[ "${resolver_capability_code}" == "0" ]] ||
    ! grep -q 'egress_policy\.dns_timeout_ms must be greater than 0' "${probe_log}"; then
    echo "prepare-ai-gateway-image: ${image} does not positively validate wildcard destination_resolver egress policy" >&2
    sed 's/^/  /' "${probe_log}" >&2 || true
    exit 1
  fi

  docker run --rm \
    --entrypoint /usr/local/bin/orca-gateway \
    -v "${gateway_config_path}:/tmp/ai-gateway-config.yaml:ro" \
    -v "${DEV_DIR}/secrets/session-jwt-pub.pem:/etc/orca-gateway/session-jwt-pub.pem:ro" \
    -v "${INTERNAL_SERVICE_TOKEN_FILE}:/var/run/secrets/orca/registry-internal/token:ro" \
    "${image}" check /tmp/ai-gateway-config.yaml \
    >/dev/null 2>"${probe_log}" || {
    echo "prepare-ai-gateway-image: ${image} lacks wildcard destination_resolver support or failed static config check" >&2
    sed 's/^/  /' "${probe_log}" >&2 || true
    exit 1
  }

  local timeout_bin=""
  if command -v timeout >/dev/null 2>&1; then
    timeout_bin="timeout"
  elif command -v gtimeout >/dev/null 2>&1; then
    timeout_bin="gtimeout"
  fi
  if [[ -z "${timeout_bin}" ]]; then
    echo "prepare-ai-gateway-image: timeout command not found; static bearer-token and wildcard resolver probes passed; skipping extra boot smoke test" >&2
    return 0
  fi

  local boot_code=0
  "${timeout_bin}" 8s docker run --rm \
    --entrypoint /usr/local/bin/orca-gateway \
    -v "${gateway_config_path}:/tmp/ai-gateway-config.yaml:ro" \
    -v "${DEV_DIR}/secrets/session-jwt-pub.pem:/etc/orca-gateway/session-jwt-pub.pem:ro" \
    -v "${INTERNAL_SERVICE_TOKEN_FILE}:/var/run/secrets/orca/registry-internal/token:ro" \
    "${image}" run --config /tmp/ai-gateway-config.yaml \
    >/dev/null 2>"${probe_log}" || boot_code=$?
  if [[ "${boot_code}" == "0" || "${boot_code}" == "124" ]]; then
    return 0
  fi

  echo "prepare-ai-gateway-image: ${image} does not support managed-agents wildcard destination_resolver config" >&2
  sed 's/^/  /' "${probe_log}" >&2 || true
  echo "prepare-ai-gateway-image: set AI_GATEWAY_IMAGE to a wildcard destination-resolver-capable gateway image/tag" >&2
  exit 1
}

if docker run --rm --entrypoint /usr/local/bin/orca-gateway "${SOURCE_IMAGE}" --version \
  >/dev/null 2>"${probe_log}"; then
  check_config_compat "${SOURCE_IMAGE}"
  echo "prepare-ai-gateway-image: ${SOURCE_IMAGE} passed runtime, bearer-token, and wildcard destination-resolver smoke tests" >&2
  printf '%s\n' "${SOURCE_IMAGE}"
  exit 0
fi

echo "prepare-ai-gateway-image: ${SOURCE_IMAGE} failed runtime smoke test; building ${COMPAT_IMAGE}" >&2
sed 's/^/  /' "${probe_log}" >&2 || true

docker build \
  --build-arg "AI_GATEWAY_SOURCE_IMAGE=${SOURCE_IMAGE}" \
  -t "${COMPAT_IMAGE}" \
  -f "${DOCKERFILE}" \
  "${DEV_DIR}" >&2

if ! docker run --rm --entrypoint /usr/local/bin/orca-gateway "${COMPAT_IMAGE}" --version \
  >/dev/null 2>"${probe_log}"; then
  echo "prepare-ai-gateway-image: ${COMPAT_IMAGE} failed runtime smoke test" >&2
  sed 's/^/  /' "${probe_log}" >&2 || true
  exit 1
fi

check_config_compat "${COMPAT_IMAGE}"
echo "prepare-ai-gateway-image: using compatibility image ${COMPAT_IMAGE}" >&2
printf '%s\n' "${COMPAT_IMAGE}"
