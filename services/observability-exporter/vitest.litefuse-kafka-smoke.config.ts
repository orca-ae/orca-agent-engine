// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['test/smoke/litefuse-kafka.smoke.spec.ts'],
    testTimeout: 320_000,
  },
});
