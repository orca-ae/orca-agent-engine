// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { defineConfig } from 'vitest/config';
import { coverageBase } from '../../vitest.shared.mjs';

// This package has no `test/integration/` tree, so Vitest's default excludes
// are sufficient here.
export default defineConfig({
  test: {
    coverage: { ...coverageBase, reportsDirectory: 'coverage/unit' },
  },
});
