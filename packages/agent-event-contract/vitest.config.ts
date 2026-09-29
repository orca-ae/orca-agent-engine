// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { defineConfig } from 'vitest/config';
import { coverageBase } from '../../vitest.shared.mjs';

// This package is pure and has no integration tree. Keep its unit report
// separate so the workspace coverage merge can attribute it correctly.
export default defineConfig({
  test: {
    coverage: { ...coverageBase, reportsDirectory: 'coverage/unit' },
  },
});
