// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { defineConfig } from 'vitest/config';
import { coverageBase } from '../../vitest.shared.mjs';

/**
 * Integration-test vitest config.
 * Runs all integration specs in a single forked process so a single shared
 * Kafka client / `KafkaTranscriptStore` instance can be reused across specs
 * and connection setup/teardown only happens once per run.
 */
export default defineConfig({
  test: {
    include: ['test/integration/**/*.spec.ts'],
    pool: 'forks',
    poolOptions: {
      forks: {
        singleFork: true,
      },
    },
    coverage: { ...coverageBase, reportsDirectory: 'coverage/integration' },
  },
});
