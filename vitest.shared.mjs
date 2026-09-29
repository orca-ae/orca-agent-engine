// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Shared Vitest coverage settings for the workspace.
//
// This file is plain ESM (not TypeScript) on purpose: it is imported both by
// the per-package `vitest.config.ts` / `vitest.integration.config.ts` files and
// by `scripts/merge-coverage.mjs`, which runs under bare Node and cannot load
// TypeScript without a loader. Keeping one source of truth avoids the package
// list and the exclude list drifting apart.

/**
 * Coverage is opt-in via `COVERAGE=1` so ordinary `pnpm test` runs stay fast.
 * The `--coverage` CLI flag still forces it on independently of this.
 */
export const COVERAGE_ENABLED = process.env.COVERAGE === '1';

/**
 * Excluded from coverage denominators. `include` below already restricts to
 * `src/**`, so this list only needs to carve out files inside `src/`.
 *
 * Globs are `**`-prefixed so a single list works from every package root.
 */
export const coverageExclude = [
  // Generated protobuf stubs. CI already verifies these are committed and in
  // sync (`git diff --exit-code services/*/src/generated`).
  '**/src/generated/**',

  // Generated Unicode case-folding table (1,593 lines of data, no logic).
  '**/src/unicode-case-fold-data.ts',

  // Process and CLI entrypoints: I/O wiring (Pool/Kafka/S3 construction,
  // listen(), signal handlers). Exercised by the e2e-stack workflow, which is
  // not part of this merge, so counting them here would only depress the
  // number without telling us anything actionable.
  //
  // Keep in sync with the `tsup` entrypoints in each service's `build` script.
  // This list names files individually, so a new entrypoint added upstream
  // starts being counted silently — `create-platform-key.ts` arrived that way
  // in #98 and was caught only because the file count moved.
  '**/src/main.ts',
  '**/src/migrate.ts',
  '**/src/bootstrap-admin.ts',
  '**/src/create-admin-key.ts',
  '**/src/create-platform-key.ts',

  // Orphaned declarative contracts: zero references anywhere in src or test
  // (verified by symbol search). Excluded so they do not distort the baseline
  // while their disposition — delete, or add conformance tests binding them to
  // the hand-written routes — is decided separately.
  '**/src/contracts/memory-stores.contract.ts',
  '**/src/contracts/files.contract.ts',
  '**/src/contracts/environments.contract.ts',
  '**/src/contracts/git-creds.contract.ts',
  '**/src/contracts/outcomes.contract.ts',

  // Ambient type declarations carry no executable statements.
  '**/src/**/*.d.ts',
];

/**
 * Base coverage config. Spread into each package config, adding a distinct
 * `reportsDirectory` so unit and integration runs never clobber each other.
 */
export const coverageBase = {
  provider: /** @type {'v8'} */ ('v8'),
  enabled: COVERAGE_ENABLED,

  // `all: true` is load-bearing. Without it v8 reports only files a test
  // actually imported, so never-loaded source would be invisible rather than
  // scoring 0% — inflating the baseline and defeating the ratchet.
  all: true,

  // Coverage is scoped to the Vitest `root` (the package directory) by
  // default, which silently dropped cross-package coverage: harness-server
  // integration specs import `registry-service-ts/src/server.ts` directly, yet
  // every file in that report was still under `services/harness-server`.
  // `allowExternal` lifts that restriction.
  allowExternal: true,

  // MUST stay `**/`-anchored, and MUST change together with `allowExternal`.
  // The provider builds `TestExclude` with `relativePath: !allowExternal`, so
  // enabling `allowExternal` switches these globs to match ABSOLUTE paths — a
  // root-relative `src/**/*.ts` then matches nothing and coverage silently
  // drops to zero entries with no error. Verified all four combinations.
  //
  // `all: true` still globs from `root`, so this only enumerates the package's
  // own files as 0%; an external file appears only when a test actually loads
  // it. That distinction matters — a synthesized empty branch map merged
  // against a real one is what makes branch percentages non-monotonic.
  include: ['**/src/**/*.ts'],
  exclude: coverageExclude,

  // `json` emits coverage-final.json, which is what the merge step consumes.
  reporter: /** @type {string[]} */ (['json', 'text-summary']),

  // Still emit a report when a suite fails, so the merge job has something to
  // work with instead of failing on a missing file.
  reportOnFailure: true,
};

/**
 * Workspace packages that produce coverage, as repo-relative directories.
 * `packages/e2e-tests` is excluded: it is a test-only package whose suites run
 * in the separate e2e-stack workflow against a live stack.
 * `packages/transcript-store-types` is excluded: pure type declarations with
 * zero runtime statements (its test script is `--passWithNoTests`), so there
 * is nothing for v8 to instrument.
 *
 * Used by the merge script to group merged file entries per package.
 */
export const COVERED_PACKAGES = [
  'packages/agent-event-contract',
  'packages/cloud-sandbox',
  'packages/codex-harness',
  'packages/sdk-harness',
  'packages/pi-harness',
  'packages/file-store',
  'packages/guardrails',
  'packages/harness-catalog',
  'packages/harness-tunnel',
  'packages/memory-store',
  'packages/oeadm',
  'packages/sandbox-runtime',
  'packages/skill-store',
  'packages/transcript-store',
  'services/environment-worker',
  'services/harness-server',
  'services/observability-exporter',
  'services/registry-service-ts',
  'services/sandbox-harness',
  'services/session-runner',
];
