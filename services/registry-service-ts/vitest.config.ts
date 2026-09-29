// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { defineConfig, configDefaults } from 'vitest/config';
import { coverageBase } from '../../vitest.shared.mjs';

export default defineConfig({
  test: {
    // Integration specs need the dev compose stack and run via
    // `vitest.integration.config.ts`. This appends to `configDefaults.exclude`
    // rather than replacing it: the previous `--exclude` CLI flag *replaced*
    // Vitest's default exclude array, which silently dropped the built-in
    // `node_modules/` and `dist/` exclusions from test discovery.
    exclude: [...configDefaults.exclude, 'test/integration/**'],
    coverage: { ...coverageBase, reportsDirectory: 'coverage/unit' },
  },
});
