// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import {
  computeCostNanoUsd,
  resolveModelPricing as resolvePricing,
  stripDatedSnapshotSuffix,
  toSeedEntry,
  SEED_GENERATED_AT,
  SEED_MODEL_PRICES,
  CACHE_WRITE_1H_FROM_5M_RATIO,
  CACHE_WRITE_1H_INPUT_RATIO,
  type ModelPriceEntry,
  type ModelPricing,
} from '../src/index.js';

/** Rates in the doc are quoted per million tokens; the engine works per token. */
const PER_MILLION = 1_000_000;
const PROVIDER = 'anthropic';

function resolveModelPricing(
  modelId: string,
  entries: readonly ModelPriceEntry[],
  organizationId?: string,
) {
  return resolvePricing(PROVIDER, modelId, entries, organizationId);
}

function entry(
  modelId: string,
  source: ModelPriceEntry['source'],
  inputPerMillionTokens: number,
  outputPerMillionTokens: number,
  cache?: { read?: number; write?: number },
  organizationId = 'org-1',
  provider = PROVIDER,
): ModelPriceEntry {
  return {
    provider,
    modelId,
    source,
    ...(source === 'operator' ? { organizationId } : {}),
    inputPerMillionTokens,
    outputPerMillionTokens,
    ...(cache?.read !== undefined ? { cacheReadPerMillionTokens: cache.read } : {}),
    ...(cache?.write !== undefined ? { cacheWritePerMillionTokens: cache.write } : {}),
  };
}

describe('computeCostNanoUsd', () => {
  it('sums all five token buckets at their own rates', () => {
    const pricing: ModelPricing = {
      inputPerToken: 10 / PER_MILLION,
      outputPerToken: 50 / PER_MILLION,
      cacheReadPerToken: 1 / PER_MILLION,
      cacheWritePerToken: 12.5 / PER_MILLION,
    };

    const cost = computeCostNanoUsd(
      {
        inputTokens: 1_000,
        outputTokens: 100,
        cacheReadInputTokens: 10_000,
        cacheCreationEphemeral5mInputTokens: 2_000,
        cacheCreationEphemeral1hInputTokens: 500,
      },
      pricing,
    );

    // 0.01 input + 0.005 output + 0.01 cache read + 0.025 5m write + 0.01 1h write
    expect(cost).toBe(60_000_000n);
  });

  it('derives cache rates from the input rate when the catalog publishes none', () => {
    const pricing: ModelPricing = {
      inputPerToken: 10 / PER_MILLION,
      outputPerToken: 50 / PER_MILLION,
    };

    const cost = computeCostNanoUsd(
      {
        cacheReadInputTokens: 10_000,
        cacheCreationEphemeral5mInputTokens: 2_000,
        cacheCreationEphemeral1hInputTokens: 500,
      },
      pricing,
    );

    expect(cost).toBe(45_000_000n);
  });

  it('prefers published cache rates over the fallback ratios', () => {
    const base = { inputPerToken: 10 / PER_MILLION, outputPerToken: 50 / PER_MILLION };
    const usage = { cacheReadInputTokens: 10_000, cacheCreationEphemeral5mInputTokens: 2_000 };

    const derived = computeCostNanoUsd(usage, base);
    const published = computeCostNanoUsd(usage, {
      ...base,
      // Deliberately unlike the 0.10 / 1.25 ratios, so the published rate is observable.
      cacheReadPerToken: 5 / PER_MILLION,
      cacheWritePerToken: 30 / PER_MILLION,
    });

    expect(published).toBe(110_000_000n);
    expect(published).not.toBe(derived);
  });

  it('derives the 1h cache-creation bucket from a published 5m rate', () => {
    const cost = computeCostNanoUsd(
      { cacheCreationEphemeral1hInputTokens: 1_000 },
      {
        inputPerToken: 10 / PER_MILLION,
        outputPerToken: 50 / PER_MILLION,
        cacheWritePerToken: 30 / PER_MILLION,
      },
    );

    expect(CACHE_WRITE_1H_INPUT_RATIO).toBe(2.0);
    expect(CACHE_WRITE_1H_FROM_5M_RATIO).toBe(1.6);
    expect(cost).toBe(48_000_000n);
  });

  it('treats missing buckets as zero, reducing to input + output', () => {
    const pricing: ModelPricing = {
      inputPerToken: 10 / PER_MILLION,
      outputPerToken: 50 / PER_MILLION,
      cacheReadPerToken: 1 / PER_MILLION,
      cacheWritePerToken: 12.5 / PER_MILLION,
    };

    expect(computeCostNanoUsd({ inputTokens: 1_000, outputTokens: 100 }, pricing)).toBe(
      15_000_000n,
    );
    expect(computeCostNanoUsd({}, pricing)).toBe(0n);
  });

  it('never subtracts cached tokens from the total', () => {
    const pricing: ModelPricing = {
      inputPerToken: 10 / PER_MILLION,
      outputPerToken: 50 / PER_MILLION,
    };

    const withoutCache = computeCostNanoUsd({ inputTokens: 1_000 }, pricing);
    const withCache = computeCostNanoUsd(
      { inputTokens: 1_000, cacheReadInputTokens: 5_000 },
      pricing,
    );

    expect(withCache).toBeGreaterThan(withoutCache);
  });

  it('is independent of how one model call is partitioned into deltas', () => {
    const pricing: ModelPricing = {
      inputPerToken: 2.5 / PER_MILLION,
      outputPerToken: 12.5 / PER_MILLION,
    };
    const whole = computeCostNanoUsd({ inputTokens: 3, outputTokens: 2 }, pricing);
    const split =
      computeCostNanoUsd({ inputTokens: 1 }, pricing) +
      computeCostNanoUsd({ inputTokens: 2, outputTokens: 1 }, pricing) +
      computeCostNanoUsd({ outputTokens: 1 }, pricing);
    expect(split).toBe(whole);
  });

  it('rejects token counts that cannot be accumulated exactly', () => {
    const pricing: ModelPricing = {
      inputPerToken: 10 / PER_MILLION,
      outputPerToken: 50 / PER_MILLION,
    };

    expect(() => computeCostNanoUsd({ inputTokens: -1 }, pricing)).toThrow(
      'inputTokens must be a non-negative safe integer',
    );
  });

  it('rejects prices that cannot be represented in nano-USD', () => {
    const pricing: ModelPricing = {
      inputPerToken: Number.POSITIVE_INFINITY,
      outputPerToken: 50 / PER_MILLION,
    };

    expect(() => computeCostNanoUsd({ inputTokens: 1 }, pricing)).toThrow(
      'input price cannot be represented in nano-USD',
    );
  });
});

describe('resolveModelPricing', () => {
  it('converts per-million rates to per-token rates', () => {
    const pricing = resolveModelPricing('m-1', [
      entry('m-1', 'seed', 15, 75, { read: 1.5, write: 18.75 }),
    ]);

    expect(pricing).toEqual({
      inputPerToken: 15 / PER_MILLION,
      outputPerToken: 75 / PER_MILLION,
      cacheReadPerToken: 1.5 / PER_MILLION,
      cacheWritePerToken: 18.75 / PER_MILLION,
    });
  });

  it('omits cache rates the entry does not carry', () => {
    const pricing = resolveModelPricing('m-1', [entry('m-1', 'seed', 15, 75)]);

    expect(pricing).toEqual({
      inputPerToken: 15 / PER_MILLION,
      outputPerToken: 75 / PER_MILLION,
    });
  });

  it('drops an entry with a non-positive or non-finite base rate rather than pricing at zero', () => {
    // A computed 0/NaN cost reads as a real number and silently disables every
    // budget, so a zero, negative, or non-finite input/output rate is UNPRICED.
    expect(resolveModelPricing('m', [entry('m', 'operator', 0, 25)])).toBeNull();
    expect(resolveModelPricing('m', [entry('m', 'operator', 5, 0)])).toBeNull();
    expect(resolveModelPricing('m', [entry('m', 'operator', -5, 25)])).toBeNull();
    expect(resolveModelPricing('m', [entry('m', 'operator', Number.NaN, 25)])).toBeNull();
    expect(
      resolveModelPricing('m', [entry('m', 'operator', 5, Number.POSITIVE_INFINITY)]),
    ).toBeNull();
  });

  it('falls back past an invalid higher-precedence entry to a valid lower one', () => {
    // An operator zero (a budget-bypass attempt) must neither win over a valid
    // seed rate nor yield a $0 price.
    const entries = [entry('m', 'operator', 0, 0), entry('m', 'seed', 5, 25)];

    expect(resolveModelPricing('m', entries)?.inputPerToken).toBeCloseTo(5 / PER_MILLION, 12);
  });

  it('omits a corrupt cache rate but keeps the valid input/output entry', () => {
    // A bad cache rate is derived from the input rate, exactly as an absent one
    // is — never a NaN cost — without discarding an otherwise-priced entry.
    const pricing = resolveModelPricing('m', [
      entry('m', 'seed', 10, 50, { read: -1, write: Number.NaN }),
    ]);

    expect(pricing).toEqual({ inputPerToken: 10 / PER_MILLION, outputPerToken: 50 / PER_MILLION });
  });

  it('matches an exact model id', () => {
    const entries = [
      entry('claude-opus-4-8', 'seed', 5, 25),
      entry('claude-sonnet-5', 'seed', 3, 15),
    ];

    expect(resolveModelPricing('claude-sonnet-5', entries)?.inputPerToken).toBeCloseTo(
      3 / PER_MILLION,
      12,
    );
  });

  it('resolves the same model id independently within each provider', () => {
    const entries = [
      entry('shared-model', 'seed', 2, 10, undefined, 'org-1', 'anthropic'),
      entry('shared-model', 'seed', 8, 40, undefined, 'org-1', 'bedrock'),
    ];

    expect(resolvePricing('anthropic', 'shared-model', entries)?.inputPerToken).toBe(
      2 / PER_MILLION,
    );
    expect(resolvePricing('bedrock', 'shared-model', entries)?.inputPerToken).toBe(8 / PER_MILLION);
    expect(resolvePricing('vertex', 'shared-model', entries)).toBeNull();
  });

  it('lets a dated snapshot inherit its base model price', () => {
    const entries = [entry('claude-opus-4-8', 'seed', 5, 25)];

    expect(resolveModelPricing('claude-opus-4-8-20251101', entries)).toEqual(
      resolveModelPricing('claude-opus-4-8', entries),
    );
  });

  it('lets an unlisted point revision inherit an unambiguous family price', () => {
    const entries = [
      entry('claude-opus-4-8', 'seed', 5, 25),
      entry('claude-opus-4-7', 'seed', 5, 25),
      entry('claude-opus-4-6', 'seed', 5, 25),
      entry('claude-opus-4-5', 'seed', 5, 25),
    ];

    expect(resolveModelPricing('claude-opus-4-1', entries)).toEqual(
      resolveModelPricing('claude-opus-4-8', entries),
    );
    expect(resolveModelPricing('claude-opus-4-9', entries)).toEqual(
      resolveModelPricing('claude-opus-4-8', entries),
    );
  });

  it('does not let a bare generation id inherit a different generation price', () => {
    // claude-opus-6 / claude-opus must not pull claude-opus-4-8 across the
    // generation boundary.
    const entries = [entry('claude-opus-4-8', 'seed', 5, 25)];

    expect(resolveModelPricing('claude-opus-6', entries)).toBeNull();
    expect(resolveModelPricing('claude-opus', entries)).toBeNull();
  });

  it('lets a dated snapshot of an unlisted point revision use family fallback', () => {
    const entries = [
      entry('claude-opus-4-8', 'seed', 5, 25),
      entry('claude-opus-4-7', 'seed', 5, 25),
    ];

    expect(resolveModelPricing('claude-opus-4-9-20260101', entries)).toEqual(
      resolveModelPricing('claude-opus-4-8', entries),
    );
  });

  it('leaves an ambiguous family unpriced', () => {
    const entries = [
      entry('claude-opus-4-8', 'seed', 5, 25),
      entry('claude-opus-4-7', 'seed', 15, 75),
    ];
    expect(resolveModelPricing('claude-opus-4-9', entries)).toBeNull();
  });

  it('returns null when same-precedence entries for one id disagree on a rate', () => {
    // Two seed rows for the same model that disagree are ambiguous; guessing one
    // would mis-bill, so the tier collapses to unpriced.
    const entries = [entry('m', 'seed', 1, 2), entry('m', 'seed', 3, 4)];

    expect(resolveModelPricing('m', entries)).toBeNull();
  });

  it('treats a same-id disagreement on a cache rate alone as ambiguous', () => {
    const entries = [
      entry('m', 'seed', 1, 2, { read: 0.1 }),
      entry('m', 'seed', 1, 2, { read: 0.5 }),
    ];

    expect(resolveModelPricing('m', entries)).toBeNull();
  });

  it('resolves same-id entries that all agree', () => {
    const entries = [
      entry('m', 'seed', 1, 2, { read: 0.1 }),
      entry('m', 'seed', 1, 2, { read: 0.1 }),
    ];

    expect(resolveModelPricing('m', entries)?.inputPerToken).toBeCloseTo(1 / PER_MILLION, 12);
  });

  it('returns null — never 0 — for an unknown model', () => {
    const entries = [entry('claude-opus-4-8', 'seed', 5, 25)];

    const pricing = resolveModelPricing('some-other-vendor-model', entries);

    expect(pricing).toBeNull();
    expect(pricing).not.toEqual({ inputPerToken: 0, outputPerToken: 0 });
  });

  it('returns null against an empty catalog', () => {
    expect(resolveModelPricing('claude-opus-4-8', [])).toBeNull();
  });

  it('returns null for a single-segment id with no exact match', () => {
    // Nothing to drop, so there is no family to fall back to.
    expect(resolveModelPricing('claude', [entry('claude-opus-4-8', 'seed', 5, 25)])).toBeNull();
  });

  it('prefers operator over upstream over seed for the same model id', () => {
    const seed = entry('claude-opus-4-8', 'seed', 5, 25);
    const upstream = entry('claude-opus-4-8', 'upstream', 6, 30);
    const operator = entry('claude-opus-4-8', 'operator', 7, 35);

    expect(resolveModelPricing('claude-opus-4-8', [seed])?.inputPerToken).toBeCloseTo(
      5 / PER_MILLION,
      12,
    );
    expect(resolveModelPricing('claude-opus-4-8', [seed, upstream])?.inputPerToken).toBeCloseTo(
      6 / PER_MILLION,
      12,
    );
    expect(
      resolveModelPricing('claude-opus-4-8', [seed, upstream, operator], 'org-1')?.inputPerToken,
    ).toBeCloseTo(7 / PER_MILLION, 12);
  });

  it('applies precedence regardless of entry order', () => {
    const entries = [
      entry('claude-opus-4-8', 'operator', 7, 35),
      entry('claude-opus-4-8', 'seed', 5, 25),
      entry('claude-opus-4-8', 'upstream', 6, 30),
    ];

    expect(resolveModelPricing('claude-opus-4-8', entries, 'org-1')?.inputPerToken).toBeCloseTo(
      7 / PER_MILLION,
      12,
    );
  });

  it('lets a higher-precedence entry win even when a lower one disagrees', () => {
    const entries = [entry('m', 'seed', 1, 2), entry('m', 'operator', 9, 18)];

    // Only the operator entry survives precedence narrowing, so the seed
    // disagreement never makes the id ambiguous.
    expect(resolveModelPricing('m', entries, 'org-1')?.inputPerToken).toBeCloseTo(
      9 / PER_MILLION,
      12,
    );
  });

  it('applies an operator row only to its organization', () => {
    const entries = [entry('m', 'seed', 1, 2), entry('m', 'operator', 9, 18, undefined, 'org-1')];
    expect(resolveModelPricing('m', entries, 'org-1')?.inputPerToken).toBe(9 / PER_MILLION);
    expect(resolveModelPricing('m', entries, 'org-2')?.inputPerToken).toBe(1 / PER_MILLION);
    expect(resolveModelPricing('m', entries)?.inputPerToken).toBe(1 / PER_MILLION);
  });
});

describe('stripDatedSnapshotSuffix', () => {
  it('drops a trailing 8-digit date', () => {
    expect(stripDatedSnapshotSuffix('claude-opus-4-8-20251101')).toBe('claude-opus-4-8');
  });

  it('leaves a bare alias untouched', () => {
    expect(stripDatedSnapshotSuffix('claude-opus-4-8')).toBe('claude-opus-4-8');
  });
});

describe('SEED_MODEL_PRICES', () => {
  it('parses and carries only seed-sourced entries', () => {
    expect(SEED_MODEL_PRICES.length).toBeGreaterThan(0);
    for (const priceEntry of SEED_MODEL_PRICES) {
      expect(priceEntry.source).toBe('seed');
    }
  });

  it('gives every entry a positive input and output rate', () => {
    for (const priceEntry of SEED_MODEL_PRICES) {
      expect(priceEntry.inputPerMillionTokens).toBeGreaterThan(0);
      expect(priceEntry.outputPerMillionTokens).toBeGreaterThan(0);
    }
  });

  it('gives every published cache rate a positive value', () => {
    for (const priceEntry of SEED_MODEL_PRICES) {
      if (priceEntry.cacheReadPerMillionTokens !== undefined) {
        expect(priceEntry.cacheReadPerMillionTokens).toBeGreaterThan(0);
      }
      if (priceEntry.cacheWritePerMillionTokens !== undefined) {
        expect(priceEntry.cacheWritePerMillionTokens).toBeGreaterThan(0);
      }
    }
  });

  it('has no duplicate model ids', () => {
    const ids = SEED_MODEL_PRICES.map((e) => e.modelId);
    expect(new Set(ids).size).toBe(ids.length);
  });

  it('prices the models this repo already knows about', () => {
    const ids = new Set(SEED_MODEL_PRICES.map((e) => e.modelId));
    for (const modelId of [
      'claude-opus-5-5',
      'claude-opus-5',
      'claude-opus-4-8',
      'claude-opus-4-7',
      'claude-opus-4-6',
      'claude-opus-4-5',
      'claude-sonnet-5',
      'claude-sonnet-4-6',
      'claude-fable-5',
      'claude-haiku-4-5',
    ]) {
      expect(ids).toContain(modelId);
    }
  });

  it('resolves a known model to its published rate', () => {
    expect(resolveModelPricing('claude-opus-5-5', SEED_MODEL_PRICES)).toEqual({
      inputPerToken: 4 / PER_MILLION,
      outputPerToken: 20 / PER_MILLION,
      cacheReadPerToken: 0.2 / PER_MILLION,
      cacheWritePerToken: 5 / PER_MILLION,
    });
    expect(resolveModelPricing('claude-opus-5', SEED_MODEL_PRICES)).toEqual({
      inputPerToken: 5 / PER_MILLION,
      outputPerToken: 25 / PER_MILLION,
      cacheReadPerToken: 0.5 / PER_MILLION,
      cacheWritePerToken: 6.25 / PER_MILLION,
    });
  });

  it('seeds the current permanent Claude Sonnet 5 price', () => {
    expect(resolveModelPricing('claude-sonnet-5', SEED_MODEL_PRICES)).toEqual({
      inputPerToken: 2 / PER_MILLION,
      outputPerToken: 10 / PER_MILLION,
      cacheReadPerToken: 0.2 / PER_MILLION,
      cacheWritePerToken: 2.5 / PER_MILLION,
    });
  });

  it('fails once the checked-in seed is older than 180 days', () => {
    const generatedAt = new Date(`${SEED_GENERATED_AT}T00:00:00Z`);
    expect(Number.isNaN(generatedAt.getTime())).toBe(false);
    const ageDays = (Date.now() - generatedAt.getTime()) / (24 * 60 * 60 * 1_000);
    expect(ageDays).toBeLessThanOrEqual(180);
  });

  it('resolves a dated snapshot of a seeded model', () => {
    expect(
      resolveModelPricing('claude-opus-4-5-20251101', SEED_MODEL_PRICES)?.inputPerToken,
    ).toBeCloseTo(5 / PER_MILLION, 12);
  });

  it('leaves a model it has never heard of unpriced', () => {
    expect(resolveModelPricing('gpt-nonexistent', SEED_MODEL_PRICES)).toBeNull();
  });

  it('resolves a seed-sourced entry, ranking it above an unpriced default', () => {
    const pricing = resolveModelPricing('m-seed', [
      toSeedEntry('m-seed', { input_per_million_tokens: 3, output_per_million_tokens: 15 }),
    ]);
    expect(pricing).toEqual({ inputPerToken: 3 / 1_000_000, outputPerToken: 15 / 1_000_000 });
  });

  it('still resolves an entry whose source is unrecognised, ranking it lowest', () => {
    // A malformed `source` from untrusted catalog data reaches the defensive
    // default rank rather than throwing; the price still resolves.
    const entry = {
      provider: PROVIDER,
      modelId: 'm',
      source: 'mystery',
      inputPerMillionTokens: 3,
      outputPerMillionTokens: 15,
    } as unknown as ModelPriceEntry;
    expect(resolveModelPricing('m', [entry])).toEqual({
      inputPerToken: 3 / 1_000_000,
      outputPerToken: 15 / 1_000_000,
    });
  });
});

describe('toSeedEntry', () => {
  it('carries cache rates when the row has them', () => {
    const entry = toSeedEntry('m', {
      input_per_million_tokens: 3,
      output_per_million_tokens: 15,
      cache_read_per_million_tokens: 0.3,
      cache_write_per_million_tokens: 3.75,
    });
    expect(entry).toMatchObject({
      provider: PROVIDER,
      source: 'seed',
      cacheReadPerMillionTokens: 0.3,
      cacheWritePerMillionTokens: 3.75,
    });
  });

  it('omits cache fields when the row has none', () => {
    const entry = toSeedEntry('m', { input_per_million_tokens: 3, output_per_million_tokens: 15 });
    expect('cacheReadPerMillionTokens' in entry).toBe(false);
    expect('cacheWritePerMillionTokens' in entry).toBe(false);
  });
});
