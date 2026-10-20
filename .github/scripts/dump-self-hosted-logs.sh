#!/usr/bin/env bash
# Copyright The Orca Authors
# SPDX-License-Identifier: Apache-2.0

# Dump everything needed to diagnose a self-hosted e2e failure, in collapsible groups.
#
# Factored out of `self-hosted-e2e.yml` because both suites need it, and two copies of a
# ten-line heredoc in one workflow is how the two copies drift.
#
# Never fails. This runs under `if: failure()`, so an error raised while dumping would
# replace the real failure with its own and hide what actually went wrong.
set -uo pipefail

group() {
  local title="$1"
  shift
  echo "::group::${title}"
  "$@" 2>&1 || echo "(unavailable)"
  echo "::endgroup::"
}

group "registry.log" cat services/dev/logs/registry.log
group "environment-worker.log" cat services/dev/logs/environment-worker.log

# One log per spawned runner, under the worker's per-runner workspace.
echo "::group::session-runner logs"
find /var/tmp/orca-self-hosted-workspaces -type f -name '*.log' \
  -exec sh -c 'echo "== $1 =="; cat "$1"' _ {} \; 2>/dev/null || echo "(no runner logs)"
echo "::endgroup::"

group "compose logs" docker compose -p orca-self-hosted \
  -f services/dev/docker-compose.self-hosted.yml logs --no-color --tail=200
group "docker ps -a" docker ps -a

exit 0
