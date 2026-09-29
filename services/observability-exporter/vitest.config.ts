// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { configDefaults, defineConfig } from 'vitest/config';
import { coverageBase } from '../../vitest.shared.mjs';

export default defineConfig({
  test: {
    exclude: [...configDefaults.exclude, 'test/integration/**', 'test/smoke/**'],
    coverage: {
      ...coverageBase,
      // Contract parity imports Registry source, but Registry coverage belongs
      // to its own suites. Only exporter parser execution is attributed here.
      exclude: [...coverageBase.exclude, '**/services/registry-service-ts/src/**'],
      reportsDirectory: 'coverage/unit',
    },
  },
});
