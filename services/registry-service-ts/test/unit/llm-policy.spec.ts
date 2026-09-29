// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { intersectLlmModels, llmModelPatternMatches } from '../../src/domain/llm-policy.js';

describe('LLM model policy', () => {
  it('matches the Gateway simple-star glob forms', () => {
    expect(llmModelPatternMatches('*', 'gpt-4o')).toBe(true);
    expect(llmModelPatternMatches('claude-*', 'claude-sonnet-4-5')).toBe(true);
    expect(llmModelPatternMatches('*-mini', 'gpt-4o-mini')).toBe(true);
    expect(llmModelPatternMatches('*sonnet*', 'claude-sonnet-4-5')).toBe(true);
    expect(llmModelPatternMatches('gpt-4o', 'gpt-4o-mini')).toBe(false);
  });

  it('returns deduplicated concrete session models allowed by deployment policy', () => {
    expect(
      intersectLlmModels(
        ['claude-*', 'gpt-4o-mini'],
        ['claude-sonnet-4-5', 'gpt-4o', 'gpt-4o-mini', 'claude-sonnet-4-5'],
      ),
    ).toEqual(['claude-sonnet-4-5', 'gpt-4o-mini']);
  });

  it('returns an empty list when deployment policy permits none of the session models', () => {
    expect(intersectLlmModels(['gpt-*'], ['claude-sonnet-4-5'])).toEqual([]);
  });

  it('does not propagate wildcard session model ids into the JWT allowlist', () => {
    expect(intersectLlmModels(['gpt-*'], ['gpt-*'])).toEqual([]);
    expect(intersectLlmModels(['gpt-*'], ['gpt-*', 'gpt-4o'])).toEqual(['gpt-4o']);
  });

  it('does not copy oversized legacy model ids into the JWT allowlist', () => {
    expect(intersectLlmModels(['m*'], ['m'.repeat(257)])).toEqual([]);
  });
});
