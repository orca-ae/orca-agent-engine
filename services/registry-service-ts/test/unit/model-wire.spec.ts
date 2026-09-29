// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  ModelInput,
  ModelOutput,
  MAX_MODEL_ID_LENGTH,
  mergeModelOverrideForStorage,
  modelToApi,
  normalizeModelForStorage,
  validateModelControlsForStorage,
  validateModelRosterForStorage,
  type StoredModel,
} from '../../src/contracts/model-wire.js';

describe('normalizeModelForStorage', () => {
  it('expands a bare string to {provider:anthropic, id}', () => {
    expect(normalizeModelForStorage('claude-3-5-sonnet')).toEqual({
      provider: 'anthropic',
      id: 'claude-3-5-sonnet',
    });
  });

  it('accepts the Claude {id, speed} shape and fills the default provider', () => {
    expect(normalizeModelForStorage({ id: 'claude-opus-5', speed: 'fast' })).toEqual({
      provider: 'anthropic',
      id: 'claude-opus-5',
      speed: 'fast',
    });
  });

  it('preserves an explicit {provider, id, speed}', () => {
    expect(
      normalizeModelForStorage({ provider: 'openai', id: 'gpt-5', speed: 'standard' }),
    ).toEqual({
      provider: 'openai',
      id: 'gpt-5',
      speed: 'standard',
    });
  });

  it('rejects an empty string, a missing id, and an invalid speed', () => {
    expect(normalizeModelForStorage('')).toEqual({ error: expect.any(String) });
    expect(normalizeModelForStorage({})).toEqual({ error: expect.any(String) });
    expect(normalizeModelForStorage({ id: 'm', speed: 'turbo' })).toEqual({
      error: expect.any(String),
    });
    const unsupportedFast = normalizeModelForStorage({ id: 'claude-opus-4-6', speed: 'fast' });
    expect('error' in unsupportedFast).toBe(false);
    if (!('error' in unsupportedFast)) {
      expect(validateModelControlsForStorage(unsupportedFast)).toEqual({
        error: expect.stringContaining('claude-opus-5'),
      });
    }
  });

  it('rejects wildcard model ids', () => {
    expect(normalizeModelForStorage('gpt-*')).toEqual({
      error: expect.stringContaining('wildcard'),
    });
    expect(normalizeModelForStorage({ provider: 'openai', id: 'gpt-*' })).toEqual({
      error: expect.stringContaining('wildcard'),
    });
  });

  it('rejects oversized model ids', () => {
    const oversized = 'm'.repeat(MAX_MODEL_ID_LENGTH + 1);
    expect(normalizeModelForStorage(oversized)).toEqual({
      error: expect.stringContaining(`${MAX_MODEL_ID_LENGTH}`),
    });
    expect(normalizeModelForStorage({ id: oversized })).toEqual({
      error: expect.stringContaining(`${MAX_MODEL_ID_LENGTH}`),
    });
  });

  it('bounds ModelInput model ids', () => {
    const oversized = 'm'.repeat(MAX_MODEL_ID_LENGTH + 1);
    expect(() => ModelInput.parse(oversized)).toThrow();
    expect(() => ModelInput.parse({ id: oversized })).toThrow();
  });
});

describe('mergeModelOverrideForStorage', () => {
  const base: StoredModel = {
    provider: 'anthropic',
    id: 'claude-opus-5',
    speed: 'fast',
    effort: 'high',
  };

  it('replaces the pinned model without inheriting omitted controls', () => {
    expect(
      mergeModelOverrideForStorage(base, {
        id: 'claude-opus-4-8',
        effort: { type: 'low' },
      }),
    ).toEqual({
      provider: 'anthropic',
      id: 'claude-opus-4-8',
      effort: 'low',
    });
    expect(mergeModelOverrideForStorage(base, 'claude-opus-4-8')).toEqual({
      provider: 'anthropic',
      id: 'claude-opus-4-8',
      effort: 'high',
    });
    expect(mergeModelOverrideForStorage(base, 'claude-sonnet-4-6')).toEqual({
      provider: 'anthropic',
      id: 'claude-sonnet-4-6',
      effort: 'high',
    });
  });

  it('treats explicit null controls like omitted replacement controls', () => {
    expect(
      mergeModelOverrideForStorage(base, {
        id: 'claude-opus-4-8',
        speed: null,
        effort: null,
      }),
    ).toEqual({ provider: 'anthropic', id: 'claude-opus-4-8', effort: 'high' });
  });

  it('rejects wildcard replacement model ids', () => {
    expect(mergeModelOverrideForStorage(base, 'gpt-*')).toEqual({
      error: expect.stringContaining('wildcard'),
    });
  });
});

describe('model control validation', () => {
  it('admits controls from pinned native SDK catalogs and rejects unknown providers', () => {
    expect(
      validateModelControlsForStorage({ provider: 'openai', id: 'gpt-5', speed: 'standard' }),
    ).toEqual({ provider: 'openai', id: 'gpt-5', speed: 'standard' });
    expect(
      validateModelControlsForStorage({ provider: 'openai', id: 'gpt-5', effort: 'high' }),
    ).toEqual({ provider: 'openai', id: 'gpt-5', effort: 'high' });
    expect(
      validateModelControlsForStorage({ provider: 'unknown', id: 'gpt-5', effort: 'high' }),
    ).toEqual({ error: expect.stringContaining('only for Anthropic') });
    expect(validateModelControlsForStorage({ provider: 'openai', id: 'gpt-5' })).toEqual({
      provider: 'openai',
      id: 'gpt-5',
    });
  });

  it('resolves known defaults and rejects unsupported effort combinations', () => {
    expect(
      validateModelControlsForStorage({
        provider: 'anthropic',
        id: 'claude-opus-5-5',
        speed: 'fast',
      }),
    ).toEqual({
      provider: 'anthropic',
      id: 'claude-opus-5-5',
      speed: 'fast',
      effort: 'medium',
    });
    expect(validateModelControlsForStorage({ provider: 'anthropic', id: 'claude-opus-5' })).toEqual(
      {
        provider: 'anthropic',
        id: 'claude-opus-5',
        effort: 'high',
      },
    );
    expect(
      validateModelControlsForStorage({
        provider: 'anthropic',
        id: 'claude-sonnet-4-6',
        effort: 'max',
      }),
    ).toEqual({
      provider: 'anthropic',
      id: 'claude-sonnet-4-6',
      effort: 'max',
    });
    expect(
      validateModelControlsForStorage({
        provider: 'anthropic',
        id: 'claude-sonnet-4-6',
        effort: 'xhigh',
      }),
    ).toEqual({ error: expect.stringContaining('supported levels are low, medium, high, max') });
    expect(
      validateModelControlsForStorage({
        provider: 'anthropic',
        id: 'claude-sonnet-4-5',
        effort: 'high',
      }),
    ).toEqual({ provider: 'anthropic', id: 'claude-sonnet-4-5', effort: 'high' });
  });

  it('rejects mixed coordinator speeds', () => {
    expect(
      validateModelRosterForStorage([
        {
          label: 'primary agent',
          model: { provider: 'anthropic', id: 'claude-opus-5', speed: 'fast' },
        },
        {
          label: 'subagent child',
          model: { provider: 'anthropic', id: 'claude-opus-4-8' },
        },
      ]),
    ).toMatch(/mixed model\.speed.*primary agent=fast, subagent child=standard/);
  });
});

describe('modelToApi', () => {
  const stored: StoredModel = { provider: 'anthropic', id: 'claude-opus-5', speed: 'fast' };

  it('emits resolved Claude model controls for default clients', () => {
    expect(modelToApi(stored, false)).toEqual({
      id: 'claude-opus-5',
      speed: 'fast',
      effort: { type: 'high' },
    });
  });

  it('defaults speed to standard when unset for default clients', () => {
    expect(modelToApi({ provider: 'anthropic', id: 'claude-x' }, false)).toEqual({
      id: 'claude-x',
      speed: 'standard',
    });
  });

  it('keeps the legacy {provider, id} shape for orca-beta clients', () => {
    expect(modelToApi(stored, true)).toEqual({ provider: 'anthropic', id: 'claude-opus-5' });
  });

  it('preserves provider when parsing the legacy response union branch', () => {
    expect(ModelOutput.parse({ provider: 'anthropic', id: 'claude-x' })).toEqual({
      provider: 'anthropic',
      id: 'claude-x',
    });
  });
});

describe('Pi model controls', () => {
  const model = { provider: 'anthropic', id: 'claude-sonnet-4-6' };
  it('preserves omitted and cleared effort through storage and wire normalization', () => {
    expect(validateModelControlsForStorage(model, 'pi_sdk')).toEqual(model);
    expect(
      mergeModelOverrideForStorage(
        { ...model, effort: 'high' },
        { ...model, effort: null },
        'pi_sdk',
      ),
    ).toEqual(model);
    expect(modelToApi(model, false, 'pi_sdk')).toEqual({ id: model.id, speed: 'standard' });
    expect(validateModelControlsForStorage(model)).toEqual({ ...model, effort: 'high' });
  });
  it('uses the Pi catalog for overlapping models and rejects fast mode', () => {
    expect(
      validateModelControlsForStorage(
        { provider: 'anthropic', id: 'claude-opus-5-5', effort: 'xhigh' },
        'pi_sdk',
      ),
    ).toEqual({ provider: 'anthropic', id: 'claude-opus-5-5', effort: 'xhigh' });
    expect(
      validateModelControlsForStorage(
        { provider: 'anthropic', id: 'claude-opus-4-6', speed: 'fast' },
        'pi_sdk',
      ),
    ).toHaveProperty('error');
    expect(validateModelControlsForStorage({ ...model, effort: 'xhigh' }, 'pi_sdk')).toHaveProperty(
      'error',
    );
    expect(validateModelControlsForStorage({ ...model, effort: 'medium' }, 'pi_sdk')).toEqual({
      ...model,
      effort: 'medium',
    });
  });
});
