// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { defineConfig } from 'vitest/config';
import { coverageBase } from '../../vitest.shared.mjs';

// This package has no `test/integration/` tree, so Vitest's default excludes
// are sufficient here — the same shape as `services/sandbox-harness`.
//
// The config exists for coverage, not for discovery: without it the package
// never spreads `coverageBase`, so `COVERAGE=1` was a silent no-op and the
// runner's source did not appear on a single row of the merged report. Keep
// `services/session-runner` in `COVERED_PACKAGES` (`vitest.shared.mjs`) and in
// `coverage-thresholds.json` alongside it; the three are one mechanism.
export default defineConfig({
  test: {
    coverage: { ...coverageBase, reportsDirectory: 'coverage/unit' },
  },
});
