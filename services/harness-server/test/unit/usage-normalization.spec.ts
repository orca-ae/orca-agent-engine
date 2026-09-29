// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';

import {
  cacheCreationTotalFromRaw,
  cacheCreationUsageFromRaw,
} from '../../src/usage-normalization.js';

describe('cacheCreationUsageFromRaw', () => {
  it('prefers a non-zero TTL breakdown over a conflicting flat total', () => {
    const usage = cacheCreationUsageFromRaw({
      cache_creation_input_tokens: 12,
      cache_creation: {
        ephemeral_1h_input_tokens: 5,
        ephemeral_5m_input_tokens: 20,
      },
    });

    expect(usage).toEqual({
      ephemeral_1h_input_tokens: 5,
      ephemeral_5m_input_tokens: 20,
    });
  });

  it('falls back to the flat total when the SDK nested default is all zero', () => {
    const usage = cacheCreationUsageFromRaw({
      cache_creation_input_tokens: 30,
      cache_creation: {
        ephemeral_1h_input_tokens: 0,
        ephemeral_5m_input_tokens: 0,
      },
    });

    expect(usage).toEqual({
      ephemeral_1h_input_tokens: 0,
      ephemeral_5m_input_tokens: 30,
    });
  });

  it.each([
    ['null', null],
    ['array', []],
  ])('falls back to the flat total for a %s nested value', (_label, cacheCreation) => {
    expect(
      cacheCreationUsageFromRaw({
        cache_creation_input_tokens: 11,
        cache_creation: cacheCreation,
      }),
    ).toEqual({
      ephemeral_1h_input_tokens: 0,
      ephemeral_5m_input_tokens: 11,
    });
  });

  it('accepts a partial nested breakdown and floors valid counts', () => {
    expect(
      cacheCreationUsageFromRaw({
        cache_creation: { ephemeral_5m_input_tokens: 17.9 },
      }),
    ).toEqual({
      ephemeral_1h_input_tokens: 0,
      ephemeral_5m_input_tokens: 17,
    });
  });

  it.each([
    ['negative', -1],
    ['NaN', Number.NaN],
  ])('treats a %s flat count as zero', (_label, cacheCreationInputTokens) => {
    expect(
      cacheCreationUsageFromRaw({ cache_creation_input_tokens: cacheCreationInputTokens }),
    ).toEqual({
      ephemeral_1h_input_tokens: 0,
      ephemeral_5m_input_tokens: 0,
    });
  });
});

describe('cacheCreationTotalFromRaw', () => {
  it('sums the normalized TTL counters', () => {
    expect(
      cacheCreationTotalFromRaw({
        cache_creation: {
          ephemeral_1h_input_tokens: 5.9,
          ephemeral_5m_input_tokens: 20.9,
        },
      }),
    ).toBe(25);
  });
});
