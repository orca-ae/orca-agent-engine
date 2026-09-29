// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { decodeCanonicalBase64 } from '../src/base64.js';

describe('decodeCanonicalBase64', () => {
  it('decodes the canonical padded form emitted by session-manager', () => {
    const value = 'system prompt with unicode: 你好';
    const encoded = Buffer.from(value, 'utf8').toString('base64');
    expect(decodeCanonicalBase64(encoded).toString('utf8')).toBe(value);
    expect(decodeCanonicalBase64('')).toEqual(Buffer.alloc(0));
  });

  it.each(['not base64!', 'YQ', 'YQ===', 'AB==', 'YWJj\n'])(
    'rejects malformed or non-canonical input %j',
    (raw) => {
      expect(() => decodeCanonicalBase64(raw)).toThrow('expected canonical base64');
    },
  );
});
