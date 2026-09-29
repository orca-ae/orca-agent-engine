// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { expect, it } from 'vitest';
import { assertSdkTerminalUsage } from '../../src/index.js';
it.each([
  {},
  { input_tokens: 2, cached_input_tokens: 3, output_tokens: 1 },
  { input_tokens: -1, cached_input_tokens: 0, output_tokens: 1 },
  { input_tokens: Number.MAX_SAFE_INTEGER, cached_input_tokens: 0, output_tokens: 1 },
])('rejects malformed or overflowing counters %j', (usage) => {
  expect(() => assertSdkTerminalUsage(usage)).toThrow();
});
