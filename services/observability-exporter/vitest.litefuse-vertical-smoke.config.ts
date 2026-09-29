// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['test/smoke/litefuse-vertical.smoke.spec.ts'],
    pool: 'forks',
    poolOptions: { forks: { singleFork: true } },
    // Max remote delivery + query deadlines, one in-flight OTLP request,
    // bounded local setup, and ordered cleanup all need separate headroom.
    testTimeout: 1_000_000,
  },
});
