// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { defineConfig } from 'vitest/config';
import { coverageBase } from '../../vitest.shared.mjs';

// This package is pure and has no `test/integration/` tree, so Vitest's default
// excludes are sufficient. The config exists so `COVERAGE=1 pnpm test` collects
// coverage for the package at all — without it the largest and most
// security-critical package contributes nothing to the merged report.
export default defineConfig({
  test: {
    coverage: { ...coverageBase, reportsDirectory: 'coverage/unit' },
  },
});
