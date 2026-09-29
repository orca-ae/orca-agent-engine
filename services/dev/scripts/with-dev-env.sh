#!/usr/bin/env bash
# Copyright The Orca Authors
# SPDX-License-Identifier: Apache-2.0

# Run a command with services/dev/.env loaded, while preserving caller-provided
# environment variables as explicit overrides.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEV_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${DEV_DIR}/.env"

if [[ -f "${ENV_FILE}" ]]; then
  caller_keys=()
  caller_present=()
  caller_values=()

  while IFS= read -r line; do
    if [[ "${line}" =~ ^[[:space:]]*([A-Za-z_][A-Za-z0-9_]*)[[:space:]]*= ]]; then
      key="${BASH_REMATCH[1]}"
      caller_keys+=("${key}")
      if [[ -n "${!key+x}" ]]; then
        caller_present+=(1)
        caller_values+=("${!key}")
      else
        caller_present+=(0)
        caller_values+=("")
      fi
    fi
  done <"${ENV_FILE}"

  # shellcheck disable=SC1090
  set -a
  . "${ENV_FILE}"
  set +a

  for i in "${!caller_keys[@]}"; do
    key="${caller_keys[${i}]}"
    if [[ "${caller_present[${i}]}" == "1" ]]; then
      printf -v "${key}" '%s' "${caller_values[${i}]}"
      export "${key}"
    fi
  done
fi

exec "$@"
