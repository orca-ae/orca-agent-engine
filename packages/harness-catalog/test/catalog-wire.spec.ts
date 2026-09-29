// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import {
  MAX_CATALOG_MODEL_ID_LENGTH,
  parseModelPriceCatalog as parseCatalogPayload,
  resolveModelPricing as resolvePricing,
  type ModelPriceCatalogParseOptions,
  type ModelPriceEntry,
} from '../src/index.js';

const PER_MILLION = 1_000_000;
const PROVIDER = 'anthropic';

function parseModelPriceCatalog(
  value: unknown,
  options: Partial<ModelPriceCatalogParseOptions> = {},
) {
  return parseCatalogPayload(value, { provider: PROVIDER, ...options });
}

function resolveModelPricing(modelId: string, entries: readonly ModelPriceEntry[]) {
  return resolvePricing(PROVIDER, modelId, entries);
}

function payload(models: Record<string, unknown>, schemaVersion: unknown = '1.0'): unknown {
  return { schema_version: schemaVersion, models };
}

describe('parseModelPriceCatalog', () => {
  it('parses a valid payload into upstream-sourced entries', () => {
    const result = parseModelPriceCatalog(
      payload({
        'claude-opus-4-8': {
          pricing: {
            input_per_million_tokens: 15.0,
            output_per_million_tokens: 75.0,
            cache_read_per_million_tokens: 1.5,
            cache_write_per_million_tokens: 18.75,
          },
        },
      }),
    );

    expect(result).toEqual({
      ok: true,
      skipped: 0,
      entries: [
        {
          provider: PROVIDER,
          modelId: 'claude-opus-4-8',
          source: 'upstream',
          inputPerMillionTokens: 15.0,
          outputPerMillionTokens: 75.0,
          cacheReadPerMillionTokens: 1.5,
          cacheWritePerMillionTokens: 18.75,
        },
      ],
    });
  });

  it('produces entries the resolver can consume', () => {
    const result = parseModelPriceCatalog(
      payload({
        'claude-opus-4-8': {
          pricing: { input_per_million_tokens: 15.0, output_per_million_tokens: 75.0 },
        },
      }),
    );

    expect(result.ok).toBe(true);
    if (!result.ok) return;

    expect(resolveModelPricing('claude-opus-4-8', result.entries)).toEqual({
      inputPerToken: 15.0 / PER_MILLION,
      outputPerToken: 75.0 / PER_MILLION,
    });
  });

  it('accepts a bare numeric major version', () => {
    const result = parseModelPriceCatalog(
      payload(
        { 'm-1': { pricing: { input_per_million_tokens: 1, output_per_million_tokens: 2 } } },
        1,
      ),
    );

    expect(result.ok).toBe(true);
  });

  it('accepts any minor version within major 1', () => {
    for (const version of ['1.0', '1.4', '1.10', 1]) {
      expect(
        parseModelPriceCatalog(
          payload(
            { 'm-1': { pricing: { input_per_million_tokens: 1, output_per_million_tokens: 2 } } },
            version,
          ),
        ).ok,
      ).toBe(true);
    }
  });

  it('rejects a missing or unreadable provider context', () => {
    const catalog = payload({
      'm-1': { pricing: { input_per_million_tokens: 1, output_per_million_tokens: 2 } },
    });

    for (const provider of ['', 'not a provider', 'x'.repeat(MAX_CATALOG_MODEL_ID_LENGTH + 1)]) {
      const result = parseModelPriceCatalog(catalog, { provider });
      expect(result.ok).toBe(false);
      if (!result.ok) expect(result.error).toMatch(/provider/i);
    }
  });

  it('rejects an invalid upstream price-delta factor', () => {
    const catalog = payload({
      'm-1': { pricing: { input_per_million_tokens: 1, output_per_million_tokens: 2 } },
    });

    for (const maxPriceDeltaFactor of [0.5, Number.NaN, Number.POSITIVE_INFINITY]) {
      const result = parseModelPriceCatalog(catalog, { maxPriceDeltaFactor });
      expect(result.ok).toBe(false);
      if (!result.ok) expect(result.error).toMatch(/maxPriceDeltaFactor/i);
    }
  });

  it('rejects a payload whose major schema version is not 1', () => {
    for (const version of ['2.0', 2, '0.9', 0]) {
      const result = parseModelPriceCatalog(
        payload(
          { 'm-1': { pricing: { input_per_million_tokens: 1, output_per_million_tokens: 2 } } },
          version,
        ),
      );
      expect(result.ok).toBe(false);
      if (result.ok) continue;
      expect(result.error).toMatch(/schema_version/i);
    }
  });

  it('rejects an unparseable schema version', () => {
    for (const version of [null, 'banana', '', {}, [], true, Number.NaN]) {
      expect(
        parseModelPriceCatalog(
          payload(
            { 'm-1': { pricing: { input_per_million_tokens: 1, output_per_million_tokens: 2 } } },
            version,
          ),
        ).ok,
      ).toBe(false);
    }
  });

  it('rejects a payload with no schema_version key at all', () => {
    // Built inline: routing `undefined` through payload()'s default would
    // silently substitute a valid version and never exercise the parser.
    const result = parseModelPriceCatalog({
      models: { 'm-1': { pricing: { input_per_million_tokens: 1, output_per_million_tokens: 2 } } },
    });

    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.error).toMatch(/schema_version/i);
  });

  it('rejects an explicitly undefined schema version', () => {
    const result = parseModelPriceCatalog({
      schema_version: undefined,
      models: { 'm-1': { pricing: { input_per_million_tokens: 1, output_per_million_tokens: 2 } } },
    });

    expect(result.ok).toBe(false);
  });

  it('rejects a non-object payload', () => {
    for (const bad of [null, undefined, 'string', 42, [], true]) {
      const result = parseModelPriceCatalog(bad);
      expect(result.ok).toBe(false);
    }
  });

  it('rejects a missing or empty models map', () => {
    expect(parseModelPriceCatalog({ schema_version: '1.0' }).ok).toBe(false);
    expect(parseModelPriceCatalog(payload({})).ok).toBe(false);
    expect(parseModelPriceCatalog({ schema_version: '1.0', models: null }).ok).toBe(false);
    expect(parseModelPriceCatalog({ schema_version: '1.0', models: [] }).ok).toBe(false);
  });

  it('skips an entry with no pricing rather than defaulting it to zero', () => {
    const result = parseModelPriceCatalog(
      payload({
        'priced-model': {
          pricing: { input_per_million_tokens: 1, output_per_million_tokens: 2 },
        },
        'unpriced-model': {},
      }),
    );

    expect(result.ok).toBe(true);
    if (!result.ok) return;

    expect(result.entries.map((e) => e.modelId)).toEqual(['priced-model']);
    expect(resolveModelPricing('unpriced-model', result.entries)).toBeNull();
  });

  it('skips an entry missing the input or output rate', () => {
    const result = parseModelPriceCatalog(
      payload({
        'no-output': { pricing: { input_per_million_tokens: 1 } },
        'no-input': { pricing: { output_per_million_tokens: 2 } },
        both: { pricing: { input_per_million_tokens: 1, output_per_million_tokens: 2 } },
      }),
    );

    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.entries.map((e) => e.modelId)).toEqual(['both']);
  });

  it('skips an entry whose rates are not finite non-negative numbers', () => {
    const result = parseModelPriceCatalog(
      payload({
        'string-rate': {
          pricing: { input_per_million_tokens: '1', output_per_million_tokens: 2 },
        },
        'negative-rate': {
          pricing: { input_per_million_tokens: -1, output_per_million_tokens: 2 },
        },
        'ok-model': { pricing: { input_per_million_tokens: 1, output_per_million_tokens: 2 } },
      }),
    );

    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.entries.map((e) => e.modelId)).toEqual(['ok-model']);
  });

  it('skips a zero input or output rate — zero is not a real published token price', () => {
    // A zero rate computes a $0 cost that reads as real and disables the budget,
    // so a zero input/output rate is UNPRICED rather than priced at nothing.
    const result = parseModelPriceCatalog(
      payload({
        'zero-input': { pricing: { input_per_million_tokens: 0, output_per_million_tokens: 2 } },
        'zero-output': { pricing: { input_per_million_tokens: 1, output_per_million_tokens: 0 } },
        'ok-model': { pricing: { input_per_million_tokens: 1, output_per_million_tokens: 2 } },
      }),
    );

    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.entries.map((e) => e.modelId)).toEqual(['ok-model']);
    expect(resolveModelPricing('zero-input', result.entries)).toBeNull();
    expect(resolveModelPricing('zero-output', result.entries)).toBeNull();
  });

  it('keeps a published zero cache rate — unlike input/output, a zero cache rate is real', () => {
    const result = parseModelPriceCatalog(
      payload({
        'm-1': {
          pricing: {
            input_per_million_tokens: 1,
            output_per_million_tokens: 2,
            cache_read_per_million_tokens: 0,
          },
        },
      }),
    );

    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.entries[0]?.cacheReadPerMillionTokens).toBe(0);
  });

  it('counts skipped models on an otherwise successful parse', () => {
    const result = parseModelPriceCatalog(
      payload({
        good: { pricing: { input_per_million_tokens: 1, output_per_million_tokens: 2 } },
        'no-pricing': {},
        'zero-input': { pricing: { input_per_million_tokens: 0, output_per_million_tokens: 2 } },
      }),
    );

    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.entries.map((e) => e.modelId)).toEqual(['good']);
    expect(result.skipped).toBe(2);
  });

  it('skips upstream rates more than 10x above or below the persisted baseline', () => {
    const baseline: ModelPriceEntry[] = [
      {
        provider: PROVIDER,
        modelId: 'too-high',
        source: 'upstream',
        inputPerMillionTokens: 10,
        outputPerMillionTokens: 20,
      },
      {
        provider: PROVIDER,
        modelId: 'too-low',
        source: 'seed',
        inputPerMillionTokens: 10,
        outputPerMillionTokens: 20,
      },
    ];
    const result = parseModelPriceCatalog(
      payload({
        'too-high': {
          pricing: { input_per_million_tokens: 100.001, output_per_million_tokens: 20 },
        },
        'too-low': {
          pricing: { input_per_million_tokens: 0.999, output_per_million_tokens: 20 },
        },
        bounded: {
          pricing: { input_per_million_tokens: 1, output_per_million_tokens: 2 },
        },
      }),
      { baseline },
    );

    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.entries.map((entry) => entry.modelId)).toEqual(['bounded']);
    expect(result.skipped).toBe(2);
  });

  it('prefers a persisted upstream baseline over the seed fallback', () => {
    const baseline: ModelPriceEntry[] = [
      {
        provider: PROVIDER,
        modelId: 'm-1',
        source: 'seed',
        inputPerMillionTokens: 1,
        outputPerMillionTokens: 2,
      },
      {
        provider: PROVIDER,
        modelId: 'm-1',
        source: 'upstream',
        inputPerMillionTokens: 2,
        outputPerMillionTokens: 4,
      },
    ];
    const result = parseModelPriceCatalog(
      payload({
        'm-1': {
          pricing: { input_per_million_tokens: 15, output_per_million_tokens: 30 },
        },
      }),
      { baseline },
    );

    expect(result.ok).toBe(true);
  });

  it('scopes baselines and emitted entries to the configured provider', () => {
    const result = parseModelPriceCatalog(
      payload({
        'm-1': {
          pricing: { input_per_million_tokens: 20, output_per_million_tokens: 40 },
        },
      }),
      {
        provider: 'bedrock',
        baseline: [
          {
            provider: PROVIDER,
            modelId: 'm-1',
            source: 'upstream',
            inputPerMillionTokens: 1,
            outputPerMillionTokens: 2,
          },
        ],
      },
    );

    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.entries[0]?.provider).toBe('bedrock');
  });

  it('skips model ids the public model contract cannot address', () => {
    const result = parseModelPriceCatalog(
      payload({
        ['m'.repeat(MAX_CATALOG_MODEL_ID_LENGTH + 1)]: {
          pricing: { input_per_million_tokens: 1, output_per_million_tokens: 2 },
        },
        'wildcard-*': {
          pricing: { input_per_million_tokens: 1, output_per_million_tokens: 2 },
        },
        good: { pricing: { input_per_million_tokens: 1, output_per_million_tokens: 2 } },
      }),
    );

    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.entries.map((entry) => entry.modelId)).toEqual(['good']);
    expect(result.skipped).toBe(2);
  });

  it('skips rates with more than three decimal places', () => {
    const result = parseModelPriceCatalog(
      payload({
        overprecise: {
          pricing: { input_per_million_tokens: 1.0001, output_per_million_tokens: 2 },
        },
        good: { pricing: { input_per_million_tokens: 1.001, output_per_million_tokens: 2.125 } },
      }),
    );

    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.entries.map((entry) => entry.modelId)).toEqual(['good']);
    expect(result.skipped).toBe(1);
  });

  it('skips entries that explicitly publish an invalid cache rate', () => {
    const result = parseModelPriceCatalog(
      payload({
        'string-cache': {
          pricing: {
            input_per_million_tokens: 1,
            output_per_million_tokens: 2,
            cache_read_per_million_tokens: 'nope',
          },
        },
        'negative-cache': {
          pricing: {
            input_per_million_tokens: 1,
            output_per_million_tokens: 2,
            cache_write_per_million_tokens: -1,
          },
        },
        'overprecise-cache': {
          pricing: {
            input_per_million_tokens: 1,
            output_per_million_tokens: 2,
            cache_read_per_million_tokens: 0.0001,
          },
        },
        good: {
          pricing: { input_per_million_tokens: 1, output_per_million_tokens: 2 },
        },
      }),
    );

    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.entries.map((entry) => entry.modelId)).toEqual(['good']);
    expect(result.skipped).toBe(3);
  });

  it('fails when every model in a non-empty map is skipped as unpriced', () => {
    // An upstream rename that makes every row unreadable (e.g. renaming
    // input_per_million_tokens) must not silently replace the whole price set
    // with nothing — that reads as "every model is free". Treat it like the
    // empty-models rejection.
    const result = parseModelPriceCatalog(payload({ a: {}, b: { pricing: {} } }));

    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.error).toMatch(/skip|unpriced|usable/i);
  });

  it('never throws on hostile input', () => {
    for (const bad of [
      undefined,
      null,
      Symbol('x'),
      { schema_version: '1.0', models: { m: null } },
      { schema_version: '1.0', models: { m: { pricing: null } } },
      { schema_version: '1.0', models: { m: { pricing: 7 } } },
    ]) {
      expect(() => parseModelPriceCatalog(bad)).not.toThrow();
    }
  });

  it('reports an error rather than throwing when reading the payload throws', () => {
    // A plain object whose property access throws reaches the catch: the parser
    // must return a failed result, not propagate the exception to the caller.
    const hostile = Object.defineProperty({}, 'schema_version', {
      enumerable: true,
      get() {
        throw new Error('boom');
      },
    });
    const result = parseModelPriceCatalog(hostile);
    expect(result.ok).toBe(false);
    if (!result.ok) expect(result.error).toContain('could not be read');
  });
});
