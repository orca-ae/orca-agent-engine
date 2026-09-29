// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import { extract } from '../../src/secrets/payloads.js';

describe('SecretPayloads.extract', () => {
  it('returns payload verbatim if no jsonKey', () => {
    expect(extract('plain', null)).toBe('plain');
  });
  it('extracts a top-level key from JSON', () => {
    expect(extract('{"k":"v"}', 'k')).toBe('v');
  });
  it('throws if key missing', () => {
    expect(() => extract('{"k":"v"}', 'other')).toThrow(/does not contain key/);
  });
  it('throws if key is present but null', () => {
    expect(() => extract('{"k":null}', 'k')).toThrow(/does not contain key/);
  });
  it('throws if not valid JSON', () => {
    expect(() => extract('not json', 'k')).toThrow(/not valid JSON/);
  });
});
