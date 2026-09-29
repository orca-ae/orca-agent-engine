// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { defineConfig } from 'vitest/config';
import { coverageBase } from '../../vitest.shared.mjs';

/**
 * Integration-test vitest config — singleFork so all integration specs share
 * the same RustFS + Postgres connection pool and we don't thrash the dev
 * containers between specs.
 */
export default defineConfig({
  test: {
    include: ['test/integration/**/*.spec.ts'],
    pool: 'forks',
    poolOptions: { forks: { singleFork: true } },
    coverage: { ...coverageBase, reportsDirectory: 'coverage/integration' },
  },
});
