// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['test/e2e/**/*.e2e.ts'],
    fileParallelism: false,
    testTimeout: 1_200_000,
    hookTimeout: 120_000,
  },
});
