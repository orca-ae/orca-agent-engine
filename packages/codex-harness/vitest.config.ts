// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { defineConfig } from 'vitest/config';
import { coverageBase } from '../../vitest.shared.mjs';

export default defineConfig({
  test: { coverage: { ...coverageBase, reportsDirectory: 'coverage/unit' } },
});
