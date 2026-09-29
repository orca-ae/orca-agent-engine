#!/usr/bin/env bash
# Copyright The Orca Authors
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

attempts="${PNPM_INSTALL_ATTEMPTS:-3}"
delay_seconds="${PNPM_INSTALL_RETRY_DELAY_SECONDS:-10}"

for attempt in $(seq 1 "${attempts}"); do
  if pnpm install --frozen-lockfile; then
    exit 0
  fi

  status="$?"
  if [[ "${attempt}" == "${attempts}" ]]; then
    exit "${status}"
  fi

  echo "pnpm install failed on attempt ${attempt}/${attempts}; retrying in ${delay_seconds}s..."
  rm -rf node_modules services/*/node_modules packages/*/node_modules
  sleep "${delay_seconds}"
done
