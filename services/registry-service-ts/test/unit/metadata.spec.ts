// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  applyMetadataPatch,
  normalizeStoredMetadata,
  parseMetadata,
  parseMetadataPatch,
  toJsonMetadata,
  validateMetadataLimits,
} from '../../src/api/metadata.js';
import { Metadata, MetadataPatch } from '../../src/contracts/metadata.js';

describe('metadata helpers', () => {
  it('accepts 16 metadata pairs and rejects 17', () => {
    expect(parseMetadata(entries(16)).ok).toBe(true);

    const parsed = parseMetadata(entries(17));
    expect(parsed).toEqual({
      ok: false,
      error: 'metadata must contain at most 16 pairs',
    });
  });

  it('enforces key and value length limits', () => {
    expect(parseMetadata({ '': 'value' })).toEqual({
      ok: false,
      error: 'metadata keys must contain at least 1 character',
    });
    expect(parseMetadata({ [keyOfLength(64)]: valueOfLength(512) }).ok).toBe(true);
    expect(parseMetadata({ [keyOfLength(65)]: 'value' })).toEqual({
      ok: false,
      error: 'metadata keys must be at most 64 characters',
    });
    expect(parseMetadata({ ok: valueOfLength(513) })).toEqual({
      ok: false,
      error: 'metadata.ok must be at most 512 characters',
    });
  });

  it('validates patch entries and merged metadata size', () => {
    expect(parseMetadataPatch({ keep: 'value', remove: null }).ok).toBe(true);
    expect(parseMetadataPatch({ [keyOfLength(65)]: null })).toEqual({
      ok: false,
      error: 'metadata keys must be at most 64 characters',
    });
    expect(parseMetadataPatch(entries(17)).ok).toBe(true);

    const patch = parseMetadataPatch({ extra: 'value' });
    expect(patch.ok).toBe(true);
    if (!patch.ok) return;

    const merged = applyMetadataPatch(entries(16), patch.value);
    expect(validateMetadataLimits(merged)).toBe('metadata must contain at most 16 pairs');
  });

  it('allows large delete-and-replace patches when the merged metadata is within limits', () => {
    const patch = parseMetadataPatch(replaceAllPatch(16));
    expect(patch.ok).toBe(true);
    if (!patch.ok) return;

    const merged = applyMetadataPatch(entries(16, 'a'), patch.value);
    expect(validateMetadataLimits(merged)).toBeNull();
    expect(merged).toEqual(entries(16, 'b'));
  });

  it('round-trips reserved keys without prototype pollution', () => {
    const input = JSON.parse('{"__proto__":"proto-value","constructor":"ctor-value"}');
    const parsed = parseMetadata(input);
    expect(parsed.ok).toBe(true);
    if (!parsed.ok) return;

    expect(Object.getPrototypeOf(parsed.value)).toBeNull();
    expect(Object.prototype.hasOwnProperty.call(parsed.value, '__proto__')).toBe(true);
    expect(parsed.value.__proto__).toBe('proto-value');
    expect(parsed.value.constructor).toBe('ctor-value');

    const json = toJsonMetadata(parsed.value);
    expect(Object.getPrototypeOf(json)).toBe(Object.prototype);
    expect(Object.getOwnPropertyDescriptor(json, '__proto__')?.value).toBe('proto-value');
    expect(({} as Record<string, unknown>).polluted).toBeUndefined();
  });

  it('normalizes stored metadata into null-prototype containers', () => {
    const stored = JSON.parse('{"__proto__":"proto-value","skip":7,"ok":"yes"}');
    const normalized = normalizeStoredMetadata(stored);

    expect(Object.getPrototypeOf(normalized)).toBeNull();
    expect(normalized.__proto__).toBe('proto-value');
    expect(normalized.ok).toBe('yes');
    expect(Object.prototype.hasOwnProperty.call(normalized, 'skip')).toBe(false);
  });
});

describe('metadata contract schemas', () => {
  it('encode managed metadata limits for full metadata objects', () => {
    expect(Metadata.safeParse({ '': 'value' }).success).toBe(false);
    expect(Metadata.safeParse(entries(16)).success).toBe(true);
    expect(Metadata.safeParse(entries(17)).success).toBe(false);
    expect(Metadata.safeParse({ [keyOfLength(65)]: 'value' }).success).toBe(false);
    expect(Metadata.safeParse({ ok: valueOfLength(513) }).success).toBe(false);
  });

  it('encode managed metadata limits for patch objects', () => {
    expect(MetadataPatch.safeParse({ '': null }).success).toBe(false);
    expect(MetadataPatch.safeParse({ keep: 'value', remove: null }).success).toBe(true);
    expect(MetadataPatch.safeParse(entries(17)).success).toBe(true);
    expect(MetadataPatch.safeParse({ [keyOfLength(65)]: null }).success).toBe(false);
    expect(MetadataPatch.safeParse({ ok: valueOfLength(513) }).success).toBe(false);
  });
});

function entries(count: number, prefix = 'k'): Record<string, string> {
  return Object.fromEntries(
    Array.from({ length: count }, (_, index) => [`${prefix}${index}`, `v${index}`]),
  );
}

function replaceAllPatch(count: number): Record<string, string | null> {
  return {
    ...Object.fromEntries(Array.from({ length: count }, (_, index) => [`a${index}`, null])),
    ...entries(count, 'b'),
  };
}

function keyOfLength(length: number): string {
  return 'k'.repeat(length);
}

function valueOfLength(length: number): string {
  return 'v'.repeat(length);
}
