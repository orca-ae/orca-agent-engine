// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  dshLlmGrants,
  intersectLlmModels,
  llmModelPatternMatches,
} from '../../src/domain/llm-policy.js';

describe('LLM model policy', () => {
  it('narrows official DSH provider routes and rejects another provider protocol grant', () => {
    const model = { provider: 'anthropic', id: 'claude-sonnet-4-6' };
    const route = 'llm-pi-anthropic-anthropic-messages';
    expect(
      dshLlmGrants(
        { routes: [route, 'llm-pi-openai-openai-responses'], models: ['claude-*'] },
        model,
      ),
    ).toEqual({ llm_routes: [route], llm_models: [model.id] });
    expect(
      dshLlmGrants({ routes: ['llm-pi-deepseek-anthropic-messages'], models: ['*'] }, model),
    ).toBeNull();
    expect(dshLlmGrants({ routes: [route], models: ['gpt-*'] }, model)).toBeNull();
  });
  it('narrows DSH grants to the fixed Messages route/model and fails closed', () => {
    const model = { provider: 'deepseek', id: 'deepseek-v4-flash' };
    const route = 'llm-pi-deepseek-anthropic-messages';
    expect(dshLlmGrants({ routes: [route, 'llm-responses'], models: ['*'] }, model)).toEqual({
      llm_routes: [route],
      llm_models: [model.id],
    });
    expect(dshLlmGrants(undefined, model)).toBeNull();
    expect(
      dshLlmGrants({ routes: ['llm-pi-deepseek-openai-completions'], models: ['*'] }, model),
    ).toBeNull();
    expect(dshLlmGrants({ routes: [route], models: ['deepseek-flash'] }, model)).toBeNull();
    expect(
      dshLlmGrants({ routes: [route], models: ['*'] }, { ...model, id: 'deepseek-flash' }),
    ).toBeNull();
    expect(
      dshLlmGrants({ routes: [route], models: ['*'] }, { ...model, provider: 'openai' }),
    ).toBeNull();
  });
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
