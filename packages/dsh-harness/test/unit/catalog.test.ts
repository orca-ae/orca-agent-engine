// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { expect, it } from 'vitest';
import { DSH_SDK_MODELS, dshModel, dshLlmRoute, validateHarnessModel } from '@orca/harness-catalog';
import { assertDshProtocol } from '../../src/transport.js';

it('matches every chat model in the official plugin dependency catalog', () => {
  expect(() =>
    execFileSync(process.execPath, [
      fileURLToPath(new URL('../../scripts/generate-catalog.mjs', import.meta.url)),
      '--check',
    ]),
  ).not.toThrow();
  for (const [provider, models] of Object.entries(DSH_SDK_MODELS))
    for (const id of Object.keys(models)) {
      expect(validateHarnessModel({ harness: 'dsh_sdk' }, { provider, id })).toBeNull();
      expect(dshModel(provider, id)?.api).toEqual(expect.any(String));
    }
});

it('separates catalog identity, protocol capability and deployment grants', () => {
  expect(dshLlmRoute('anthropic', 'claude-sonnet-4-6')).toBe('llm-pi-anthropic-anthropic-messages');
  expect(dshLlmRoute('deepseek', 'deepseek-v4-flash')).toBe('llm-pi-deepseek-anthropic-messages');
  expect(() => assertDshProtocol('bedrock-converse-stream')).toThrow(
    'protocol bedrock-converse-stream',
  );
  expect(dshModel('__proto__', 'constructor')).toBeUndefined();
  expect(dshModel('anthropic', 'unknown')).toBeUndefined();
});
