#!/usr/bin/env bash
# Copyright The Orca Authors
# SPDX-License-Identifier: Apache-2.0

# Print the compose services needed by the selected local transcript backend.

set -euo pipefail

mode="${1:-up}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEV_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

caller_backend="${TRANSCRIPT_STORE_BACKEND-}"
if [[ -f "${DEV_DIR}/.env" ]]; then
  # shellcheck disable=SC1091
  set -a
  . "${DEV_DIR}/.env"
  set +a
fi
if [[ -n "${caller_backend}" ]]; then
  TRANSCRIPT_STORE_BACKEND="${caller_backend}"
fi

backend="$(printf '%s' "${TRANSCRIPT_STORE_BACKEND:-kafka}" | tr '[:upper:]' '[:lower:]')"
case "${backend}" in
  kafka | postgres | pulsar) ;;
  *)
    echo "dev-infra-services: unsupported TRANSCRIPT_STORE_BACKEND=${TRANSCRIPT_STORE_BACKEND:-}" >&2
    echo "  expected one of: kafka, postgres, pulsar" >&2
    exit 1
    ;;
esac

case "${mode}" in
  backend)
    printf '%s\n' "${backend}"
    ;;
  up)
    services=(postgres rustfs rustfs-bootstrap)
    case "${backend}" in
      kafka) services+=(kafka) ;;
      pulsar) services+=(pulsar) ;;
      postgres) ;;
    esac
    printf '%s\n' "${services[*]}"
    ;;
  wait)
    services=(postgres rustfs)
    case "${backend}" in
      kafka) services+=(kafka) ;;
      pulsar) services+=(pulsar) ;;
      postgres) ;;
    esac
    printf '%s\n' "${services[*]}"
    ;;
  *)
    echo "dev-infra-services: unsupported mode ${mode}" >&2
    echo "  expected one of: backend, up, wait" >&2
    exit 1
    ;;
esac
