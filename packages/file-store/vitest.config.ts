// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { defineConfig, configDefaults } from 'vitest/config';
import { coverageBase } from '../../vitest.shared.mjs';

export default defineConfig({
  test: {
    // See the note in `services/registry-service-ts/vitest.config.ts`: this
    // must append to `configDefaults.exclude`, not replace it.
    exclude: [...configDefaults.exclude, 'test/integration/**'],
    coverage: { ...coverageBase, reportsDirectory: 'coverage/unit' },
  },
});
