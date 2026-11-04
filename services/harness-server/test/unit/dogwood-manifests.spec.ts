// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import { manifestDrift } from '../../src/harness/dogwood-manifests.js';

describe('Dogwood manifest identity', () => {
  it('ignores catalog/object order without using the process locale', () => {
    const compare = vi.spyOn(String.prototype, 'localeCompare').mockImplementation(() => {
      throw new Error('locale-dependent comparison');
    });
    try {
      expect(
        manifestDrift(
          [
            {
              name: 'ä',
              inputSchema: {
                properties: { z: { type: 'number' }, A: { type: 'string' } },
                type: 'object',
              },
            },
            { name: 'Z', inputSchema: { type: 'object' } },
          ],
          [
            { inputSchema: { type: 'object' }, name: 'Z' },
            {
              inputSchema: {
                type: 'object',
                properties: { A: { type: 'string' }, z: { type: 'number' } },
              },
              name: 'ä',
            },
          ],
        ),
      ).toBe(false);
    } finally {
      compare.mockRestore();
    }
  });
  it('still reports real schema changes', () => {
    expect(
      manifestDrift(
        [{ name: 'charge', inputSchema: { type: 'object' } }],
        [{ name: 'charge', inputSchema: { type: 'string' } }],
      ),
    ).toBe(true);
  });
});
