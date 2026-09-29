// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  MODEL_PRICE_ID_MAX_LENGTH,
  ModelPriceCreate,
  ModelPriceUpdate,
  PRICING_API_PREFIX,
  modelPriceEntryToWire,
  modelPriceWriteFromCreate,
  modelPriceWriteFromUpdate,
  resolvedModelPriceToWire,
} from '../../src/contracts/model-prices.contract.js';

const T = new Date('2026-07-01T00:00:00.000Z');

describe('the pricing API group', () => {
  it('is versioned independently of core and of the policy group', () => {
    expect(PRICING_API_PREFIX).toBe('/apis/pricing.runorca.ai/v1');
  });
});

describe('source is read-only on the wire', () => {
  it('ignores a source supplied on create rather than honouring it', () => {
    const parsed = ModelPriceCreate.parse({
      model_id: 'zz-testmodel-1',
      input_per_million_tokens: 1,
      output_per_million_tokens: 2,
      source: 'seed',
    });

    expect(modelPriceWriteFromCreate(parsed)).toEqual({
      provider: 'anthropic',
      modelId: 'zz-testmodel-1',
      inputPerMillionTokens: 1,
      outputPerMillionTokens: 2,
      cacheReadPerMillionTokens: null,
      cacheWritePerMillionTokens: null,
    });
    expect(Object.keys(modelPriceWriteFromCreate(parsed))).not.toContain('source');
  });

  it('ignores a source supplied on update', () => {
    const parsed = ModelPriceUpdate.parse({ input_per_million_tokens: 9, source: 'upstream' });
    const applied = modelPriceWriteFromUpdate(parsed, {
      provider: 'anthropic',
      organizationId: 'org_model_price_test',
      modelId: 'zz-testmodel-1',
      source: 'operator',
      inputPerMillionTokens: 1,
      outputPerMillionTokens: 2,
      cacheReadPerMillionTokens: null,
      cacheWritePerMillionTokens: null,
      fetchedAt: null,
      createdAt: T,
      updatedAt: T,
    });

    expect(applied).toEqual({
      provider: 'anthropic',
      modelId: 'zz-testmodel-1',
      inputPerMillionTokens: 9,
      outputPerMillionTokens: 2,
      cacheReadPerMillionTokens: null,
      cacheWritePerMillionTokens: null,
    });
  });

  it('accepts rather than rejects it, so a read object round-trips', () => {
    // A client that PATCHes back the object it just read would otherwise get a
    // 400 for echoing a field the server itself sent.
    expect(
      ModelPriceCreate.safeParse({
        model_id: 'zz-testmodel-1',
        input_per_million_tokens: 1,
        output_per_million_tokens: 2,
        source: 'operator',
      }).success,
    ).toBe(true);
  });
});

describe('rate validation', () => {
  const base = { model_id: 'zz-testmodel-1', output_per_million_tokens: 2 };

  it('rejects a zero base rate because the resolver treats it as unpriced', () => {
    expect(ModelPriceCreate.safeParse({ ...base, input_per_million_tokens: 0 }).success).toBe(
      false,
    );
  });

  it('accepts a zero cache rate because an individual cache bucket may be free', () => {
    expect(
      ModelPriceCreate.safeParse({
        ...base,
        input_per_million_tokens: 1,
        cache_read_per_million_tokens: 0,
      }).success,
    ).toBe(true);
  });

  it('rejects rates beyond the fixed three-decimal precision', () => {
    expect(ModelPriceCreate.safeParse({ ...base, input_per_million_tokens: 1.0001 }).success).toBe(
      false,
    );
  });

  it('rejects a negative rate', () => {
    expect(ModelPriceCreate.safeParse({ ...base, input_per_million_tokens: -1 }).success).toBe(
      false,
    );
  });

  it('rejects a non-finite rate', () => {
    for (const value of [Number.NaN, Number.POSITIVE_INFINITY]) {
      expect(ModelPriceCreate.safeParse({ ...base, input_per_million_tokens: value }).success).toBe(
        false,
      );
    }
  });

  it('requires both of the rates every catalog publishes', () => {
    expect(ModelPriceCreate.safeParse({ model_id: 'zz-testmodel-1' }).success).toBe(false);
    expect(
      ModelPriceCreate.safeParse({ model_id: 'zz-testmodel-1', input_per_million_tokens: 1 })
        .success,
    ).toBe(false);
  });

  it('accepts an explicit null cache rate, which clears it back to derived', () => {
    const parsed = ModelPriceCreate.parse({
      ...base,
      input_per_million_tokens: 1,
      cache_read_per_million_tokens: null,
    });
    expect(modelPriceWriteFromCreate(parsed).cacheReadPerMillionTokens).toBeNull();
  });
});

describe('model_id validation', () => {
  const rates = { input_per_million_tokens: 1, output_per_million_tokens: 2 };

  it('rejects an empty or whitespace-padded id', () => {
    for (const model_id of ['', '   ', ' zz-testmodel-1', 'zz-testmodel-1 ']) {
      expect(ModelPriceCreate.safeParse({ ...rates, model_id }).success).toBe(false);
    }
  });

  it('rejects an id past the storage bound', () => {
    expect(
      ModelPriceCreate.safeParse({ ...rates, model_id: 'z'.repeat(MODEL_PRICE_ID_MAX_LENGTH + 1) })
        .success,
    ).toBe(false);
  });

  it('accepts the punctuation vendors actually publish', () => {
    for (const model_id of ['claude-opus-4-8', 'zz.testmodel.1', 'zz_testmodel:1']) {
      expect(ModelPriceCreate.safeParse({ ...rates, model_id }).success).toBe(true);
    }
  });

  it('rejects an id that could not be addressed as a path segment', () => {
    // An entry the API cannot name is an entry no operator can update or
    // delete, so it is refused at the door rather than stored and stranded.
    for (const model_id of ['zz/testmodel', 'zz testmodel', 'zz\ntestmodel', 'zz%2Ftestmodel']) {
      expect(ModelPriceCreate.safeParse({ ...rates, model_id }).success).toBe(false);
    }
  });

  it('refuses to move an entry to another model on update', () => {
    expect(ModelPriceUpdate.safeParse({ model_id: 'zz-othermodel-1' }).success).toBe(false);
  });
});

describe('update requires a change', () => {
  it('rejects a body that would change nothing', () => {
    expect(ModelPriceUpdate.safeParse({}).success).toBe(false);
    expect(ModelPriceUpdate.safeParse({ source: 'operator' }).success).toBe(false);
  });
});

describe('the stored entry on the wire', () => {
  it('reports its source and its stored rates verbatim', () => {
    expect(
      modelPriceEntryToWire({
        provider: 'anthropic',
        organizationId: 'org_model_price_test',
        modelId: 'zz-testmodel-1',
        source: 'operator',
        inputPerMillionTokens: 15,
        outputPerMillionTokens: 75,
        cacheReadPerMillionTokens: 1.5,
        cacheWritePerMillionTokens: 18.75,
        fetchedAt: null,
        createdAt: T,
        updatedAt: T,
      }),
    ).toEqual({
      type: 'model_price_entry',
      provider: 'anthropic',
      model_id: 'zz-testmodel-1',
      source: 'operator',
      input_per_million_tokens: 15,
      output_per_million_tokens: 75,
      cache_read_per_million_tokens: 1.5,
      cache_write_per_million_tokens: 18.75,
      fetched_at: null,
      created_at: '2026-07-01T00:00:00.000Z',
      updated_at: '2026-07-01T00:00:00.000Z',
    });
  });

  it('reports an unpublished cache rate as null, never as zero', () => {
    const wire = modelPriceEntryToWire({
      provider: 'anthropic',
      organizationId: '',
      modelId: 'zz-testmodel-1',
      source: 'upstream',
      inputPerMillionTokens: 1,
      outputPerMillionTokens: 2,
      cacheReadPerMillionTokens: null,
      cacheWritePerMillionTokens: null,
      fetchedAt: T,
      createdAt: T,
      updatedAt: T,
    });

    expect(wire.cache_read_per_million_tokens).toBeNull();
    expect(wire.cache_write_per_million_tokens).toBeNull();
    expect(wire.fetched_at).toBe('2026-07-01T00:00:00.000Z');
  });
});

describe('the resolved price on the wire', () => {
  it('quotes per million tokens, matching how vendors publish', () => {
    expect(
      resolvedModelPriceToWire('anthropic', 'zz-testmodel-1', {
        inputPerToken: 15 / 1_000_000,
        outputPerToken: 75 / 1_000_000,
        cacheReadPerToken: 1.5 / 1_000_000,
        cacheWritePerToken: 18.75 / 1_000_000,
      }),
    ).toEqual({
      type: 'model_price',
      provider: 'anthropic',
      model_id: 'zz-testmodel-1',
      input_per_million_tokens: 15,
      output_per_million_tokens: 75,
      cache_read_per_million_tokens: 1.5,
      cache_write_per_million_tokens: 18.75,
    });
  });

  it('reports the derived cache rates a session is actually billed at', () => {
    // The engine derives an absent cache rate from the input rate rather than
    // dropping the bucket, so the effective rate is what an operator needs to
    // see — not a null that reads as "free".
    expect(
      resolvedModelPriceToWire('anthropic', 'zz-testmodel-1', {
        inputPerToken: 0.8 / 1_000_000,
        outputPerToken: 4 / 1_000_000,
      }),
    ).toEqual({
      type: 'model_price',
      provider: 'anthropic',
      model_id: 'zz-testmodel-1',
      input_per_million_tokens: 0.8,
      output_per_million_tokens: 4,
      cache_read_per_million_tokens: 0.08,
      cache_write_per_million_tokens: 1,
    });
  });
});
