#!/usr/bin/env bash
# Copyright The Orca Authors
# SPDX-License-Identifier: Apache-2.0

#
# Decide which CI suites a change can possibly affect.
#
# Every workflow used to run on every push, so a prose-only PR paid for four
# Kind clusters and six compose stacks. The filtering has to happen *inside* the
# workflows rather than on their `on:` blocks: branch protection requires checks
# by job name (`test`, `integration`, `lint-and-breaking`), and GitHub treats the
# two kinds of skip in opposite ways. A workflow skipped by a trigger-level
# `paths:` filter never creates its check, which then sits Pending forever and
# makes the PR unmergeable; a *job* skipped by an `if:` reports success. So the
# triggers stay open, this script runs first, and its outputs gate the jobs.
#
# It lives here rather than inline in each workflow so the mapping is written
# down once. Three copies of these globs would drift, and a filter that drifts
# fails silently in the direction of not testing.
#
# Outputs `ts`, `stack` and `kind` as `true`/`false` to `$GITHUB_OUTPUT` (or to
# stdout when run locally).
#
# Mapping, first match wins (`classify` below is authoritative; keep in step):
#   services/*.md, packages/*.md                          ts
#   other *.md (incl. root community docs and the PR
#     template), LICENSE, NOTICE, .gitignore,
#     .gitattributes                                      (none)
#   .github/ISSUE_TEMPLATE/*, .github/CODEOWNERS,
#     .claude/settings.json                               (none)
#   proposals/*                                           (none)
#   charts/orca-managed-agents/*                          ts, kind
#   charts/opensandbox-patches/*                          stack, kind
#   services/*, packages/*, scripts/*, .github/*, root
#     build and workspace config                          ts, stack, kind
#   anything else                                         ts, stack, kind
#
# Usage:
#   changed-areas.sh                 # derive the range from the GitHub event
#   changed-areas.sh <git-range>     # e.g. `main...HEAD`, for local checks
#   changed-areas.sh -               # classify a path list read from stdin
set -euo pipefail

ts=false
stack=false
kind=false

emit() {
  local out="${GITHUB_OUTPUT:-/dev/stdout}"
  {
    echo "ts=$ts"
    echo "stack=$stack"
    echo "kind=$kind"
  } >>"$out"
}

# Fail open, loudly. Every branch that cannot establish what changed lands here,
# and the safe answer is always "run everything" — a wasted run costs minutes, a
# wrongly skipped one ships the break.
run_everything() {
  echo "::notice::changed-areas: $1 — running all suites."
  ts=true
  stack=true
  kind=true
  emit
  exit 0
}

classify() {
  case "$1" in
    # `pnpm format:check` globs `services/**/*.{ts,tsx,json,md}`, so prose under
    # `services/` is an input to the required `test` job — but a README cannot
    # move either e2e suite. `packages/` markdown is not format-checked; it is
    # grouped here anyway rather than carved out, because one rule that is
    # slightly conservative beats two rules that can disagree.
    services/*.md | packages/*.md)
      ts=true
      ;;

    # All other prose, plus repo metadata. This is deliberately keyed on the
    # `.md` extension rather than on `docs/`: `docs/` also contains operator
    # values/manifests. Matching the directory would skip suites that consume
    # them. Anything non-markdown under `docs/` falls through to catch-all and
    # runs everything, which is safe default for an unclassified file.
    #
    # `docs/managed-agents/conformance-matrix.md` *is* inert by this rule even
    # though the `test` job drift-gates it, so a hand-edit will not be caught on
    # its own PR. That is a deliberate trade with two nets under it:
    # `nightly-anthropic-spec.yml` regenerates the matrix daily and opens a PR,
    # and `release-images.yml` re-runs the same drift gate at tag time,
    # unfiltered. It is also the only inert-classified file any workflow reads.
    *.md | LICENSE | NOTICE | .gitignore | .gitattributes) ;;

    # Community and contributor-tooling files that no workflow reads: issue
    # forms, code owners, and the shared Claude Code settings. (The PR template
    # is Markdown, so the arm above already covers it.) They must precede the
    # `.github/*` arm below, which runs everything.
    .github/ISSUE_TEMPLATE/* | .github/CODEOWNERS | .claude/settings.json) ;;

    # Design proposals are prose that no suite or gate reads, including any
    # non-markdown asset beside them (a diagram, a template). If a check ever
    # starts reading `proposals/`, this arm must name the suite that runs it.
    proposals/*) ;;

    # The managed chart is rendered by `pnpm test:chart:render` in the `test`
    # job and installed by the Kind Helm e2e. The compose stack never sees it.
    charts/orca-managed-agents/*)
      ts=true
      kind=true
      ;;

    # The OpenSandbox overlay is applied by both e2e suites — e2e-stack.yml
    # lints and templates it directly — but no unit suite reads it.
    charts/opensandbox-patches/*)
      stack=true
      kind=true
      ;;

    # Source, shared config, and CI's own definition. A workflow edit must
    # exercise the workflow it edits.
    services/* | packages/* | scripts/* | .github/* | \
      package.json | pnpm-lock.yaml | pnpm-workspace.yaml | tsconfig.base.json | \
      eslint.config.mjs | vitest.shared.mjs | coverage-thresholds.json | \
      Makefile | .npmrc | .prettierignore)
      ts=true
      stack=true
      kind=true
      ;;

    # Unrecognised. A new top-level directory must not go untested because
    # nobody remembered to teach this script about it.
    *)
      ts=true
      stack=true
      kind=true
      ;;
  esac
}

paths=""

if [[ "${1:-}" == "-" ]]; then
  paths="$(cat)"
elif [[ -n "${1:-}" ]]; then
  paths="$(git diff --name-only "$1")"
elif [[ "${GITHUB_EVENT_NAME:-}" == "pull_request" ]]; then
  base="origin/${GITHUB_BASE_REF:?GITHUB_BASE_REF unset on a pull_request event}"
  git rev-parse --verify --quiet "$base^{commit}" >/dev/null ||
    run_everything "base ref $base is not fetched"
  git merge-base "$base" HEAD >/dev/null 2>&1 ||
    run_everything "no merge base with $base (shallow clone?)"
  paths="$(git diff --name-only "$base...HEAD")"
elif [[ "${GITHUB_EVENT_NAME:-}" == "push" ]]; then
  before="${BEFORE_SHA:-}"
  # A new branch reports the all-zeros SHA, and a force-push can name a commit
  # that no longer exists.
  [[ -n "$before" && ! "$before" =~ ^0+$ ]] ||
    run_everything "push event has no usable before-SHA"
  git rev-parse --verify --quiet "$before^{commit}" >/dev/null ||
    run_everything "before-SHA $before is unreachable (force-push?)"
  paths="$(git diff --name-only "$before" HEAD)"
else
  run_everything "event ${GITHUB_EVENT_NAME:-unknown} has no diff range"
fi

# An empty diff is not "nothing changed" — it is far more likely that the range
# was wrong than that a run was triggered by no change at all.
[[ -n "$paths" ]] || run_everything "computed an empty diff"

while IFS= read -r path; do
  [[ -n "$path" ]] && classify "$path"
done <<<"$paths"

echo "changed-areas: ts=$ts stack=$stack kind=$kind"
emit
