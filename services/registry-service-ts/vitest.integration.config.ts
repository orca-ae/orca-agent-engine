// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { defineConfig } from 'vitest/config';
import { coverageBase } from '../../vitest.shared.mjs';

export default defineConfig({
  test: {
    // Made explicit rather than relying on the CLI path argument, so a bare
    // `vitest run -c vitest.integration.config.ts` cannot pick up unit specs.
    include: ['test/integration/**/*.spec.ts'],
    pool: 'forks',
    poolOptions: {
      forks: {
        singleFork: true,
      },
    },
    // Integration specs run serially (singleFork) against real Postgres + the
    // ai-gateway image. Cold/legacy keys and proof-cache rollback scenarios
    // still perform memory-hard argon2id verification; retain headroom for
    // these request-heavy specs and external I/O under machine load.
    testTimeout: 60000,
    coverage: { ...coverageBase, reportsDirectory: 'coverage/integration' },
  },
});
