// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { expect, it } from 'vitest';
import { dshToolManifestSha256, parseDshToolManifest } from '../../src/dsh-tool-manifest.js';

it('bounds and validates private native tool schemas and hashes canonical schema data', () => {
  const tool = {
    name: 'probe',
    description: 'fixture',
    inputSchema: { type: 'object' as const, properties: { x: { type: 'integer' } } },
  };
  expect(parseDshToolManifest([tool])).toEqual([tool]);
  expect(() => parseDshToolManifest([tool, tool])).toThrow('colliding');
  expect(() => parseDshToolManifest([{ ...tool, name: '*' }])).toThrow();
  expect(() => parseDshToolManifest([{ ...tool, inputSchema: { type: 'array' } }])).toThrow();
  expect(() => parseDshToolManifest([{ ...tool, execute: 'supplied code' }])).toThrow();
  expect(() => parseDshToolManifest(Array.from({ length: 257 }, () => tool))).toThrow('bounded');
  expect(() => parseDshToolManifest([{ ...tool, description: 'x'.repeat(1024 * 1024) }])).toThrow(
    'bounded',
  );
  expect(dshToolManifestSha256([tool])).toBe(
    dshToolManifestSha256([
      {
        inputSchema: { properties: { x: { type: 'integer' } }, type: 'object' },
        description: 'fixture',
        name: 'probe',
      },
    ]),
  );
  expect(dshToolManifestSha256([tool])).not.toBe(
    dshToolManifestSha256([{ ...tool, deferLoading: true }]),
  );
});
