#!/usr/bin/env bash
# Copyright The Orca Authors
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

specs=(test/real-agent-loop.spec.ts test/trigger-agent-loop.spec.ts test/guardrails-agent.spec.ts)
case "${ORCA_E2E_AGENT_HARNESS:-claude_agent_sdk}" in
  claude_agent_sdk)
    specs+=(test/real-multiagent-session-threads.spec.ts test/guardrails-budget-agent.spec.ts)
    if [[ "${ORCA_E2E_SANDBOX_HARNESS:-}" == 1 ]]; then
      specs+=(test/sandbox-harness-agent.spec.ts test/sandbox-harness-multiagent.spec.ts)
    fi
    ;;
  codex_sdk|pi_sdk)
    specs+=(test/guardrails-budget-agent.spec.ts)
    if [[ "${ORCA_E2E_SANDBOX_HARNESS:-}" == 1 ]]; then
      specs+=(test/sandbox-harness-agent.spec.ts)
    fi
    ;;
  *)
    echo "Unsupported ORCA_E2E_AGENT_HARNESS: ${ORCA_E2E_AGENT_HARNESS}" >&2
    exit 1
    ;;
esac
# All SDKs exercise the same public custom-tool callback contract by default.
export ORCA_E2E_REAL_CUSTOM_TOOL="${ORCA_E2E_REAL_CUSTOM_TOOL:-1}"
# Surface the first exhausted scenario before the job's outer timeout masks it.
exec vitest run --reporter=verbose --bail=1 --no-file-parallelism --maxWorkers=1 "${specs[@]}" "$@"
