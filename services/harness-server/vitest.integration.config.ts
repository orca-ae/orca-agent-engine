// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { defineConfig } from 'vitest/config';
import { coverageBase } from '../../vitest.shared.mjs';

export default defineConfig({
  test: {
    include: ['test/integration/**/*.spec.ts'],
    pool: 'forks',
    poolOptions: { forks: { singleFork: true } },
    testTimeout: 60000,
    coverage: { ...coverageBase, reportsDirectory: 'coverage/integration' },
  },
});
