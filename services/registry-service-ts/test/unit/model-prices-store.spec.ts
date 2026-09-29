// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { SEED_MODEL_PRICES, SEED_PRICE_PROVIDER } from '@orca/harness-catalog';
import {
  InMemoryModelPriceStore,
  loadSeedModelPrices,
  resolveStoredModelPricing,
  toPriceEntries,
} from '../../src/pricing/store.js';

const T0 = new Date('2026-07-01T00:00:00.000Z');
const T1 = new Date('2026-07-02T00:00:00.000Z');

/** Deliberately outside every seeded family so a test never matches one. */
const MODEL = 'zz-testmodel-1';
const OTHER = 'zz-othermodel-1';
const ORG = 'org_model_price_test';
/** The seed's provider, so a seeded model and a written one share an identity. */
const PROVIDER = SEED_PRICE_PROVIDER;
const GLOBAL_SCOPE = '';

describe('source precedence', () => {
  it('resolves operator over upstream over seed for the same model', async () => {
    const store = new InMemoryModelPriceStore();

    await store.upsert(
      'seed',
      [{ provider: PROVIDER, modelId: MODEL, inputPerMillionTokens: 1, outputPerMillionTokens: 2 }],
      T0,
      GLOBAL_SCOPE,
    );
    expect((await resolveStoredModelPricing(store, PROVIDER, MODEL, ORG))?.inputPerToken).toBe(
      1 / 1_000_000,
    );

    await store.upsert(
      'upstream',
      [
        {
          provider: PROVIDER,
          modelId: MODEL,
          inputPerMillionTokens: 10,
          outputPerMillionTokens: 20,
        },
      ],
      T0,
      GLOBAL_SCOPE,
    );
    expect((await resolveStoredModelPricing(store, PROVIDER, MODEL, ORG))?.inputPerToken).toBe(
      10 / 1_000_000,
    );

    await store.upsert(
      'operator',
      [
        {
          provider: PROVIDER,
          modelId: MODEL,
          inputPerMillionTokens: 100,
          outputPerMillionTokens: 200,
        },
      ],
      T0,
      ORG,
    );
    expect((await resolveStoredModelPricing(store, PROVIDER, MODEL, ORG))?.inputPerToken).toBe(
      100 / 1_000_000,
    );
  });

  it('falls back to the next source when the operator entry is deleted', async () => {
    const store = new InMemoryModelPriceStore();
    await store.upsert(
      'seed',
      [{ provider: PROVIDER, modelId: MODEL, inputPerMillionTokens: 1, outputPerMillionTokens: 2 }],
      T0,
      GLOBAL_SCOPE,
    );
    await store.upsert(
      'upstream',
      [
        {
          provider: PROVIDER,
          modelId: MODEL,
          inputPerMillionTokens: 10,
          outputPerMillionTokens: 20,
        },
      ],
      T0,
      GLOBAL_SCOPE,
    );
    await store.upsert(
      'operator',
      [
        {
          provider: PROVIDER,
          modelId: MODEL,
          inputPerMillionTokens: 100,
          outputPerMillionTokens: 200,
        },
      ],
      T0,
      ORG,
    );

    expect(await store.delete(PROVIDER, MODEL, 'operator', ORG)).toBe(true);
    expect((await resolveStoredModelPricing(store, PROVIDER, MODEL, ORG))?.inputPerToken).toBe(
      10 / 1_000_000,
    );

    expect(await store.delete(PROVIDER, MODEL, 'upstream', GLOBAL_SCOPE)).toBe(true);
    expect((await resolveStoredModelPricing(store, PROVIDER, MODEL, ORG))?.inputPerToken).toBe(
      1 / 1_000_000,
    );

    // Never unpriced by the removal of an override alone: the model stays
    // priced for as long as any source still carries it.
    expect(await resolveStoredModelPricing(store, PROVIDER, MODEL, ORG)).not.toBeNull();
  });

  it('reports deleting an absent entry rather than claiming it removed one', async () => {
    const store = new InMemoryModelPriceStore();
    expect(await store.delete(PROVIDER, MODEL, 'operator', ORG)).toBe(false);
  });

  it('isolates operator overrides by organization while sharing global prices', async () => {
    const store = new InMemoryModelPriceStore();
    const otherOrg = 'org_model_price_other';
    await store.upsert(
      'seed',
      [{ provider: PROVIDER, modelId: MODEL, inputPerMillionTokens: 1, outputPerMillionTokens: 2 }],
      T0,
      GLOBAL_SCOPE,
    );
    await store.upsert(
      'operator',
      [
        {
          provider: PROVIDER,
          modelId: MODEL,
          inputPerMillionTokens: 10,
          outputPerMillionTokens: 20,
        },
      ],
      T0,
      ORG,
    );
    await store.upsert(
      'operator',
      [
        {
          provider: PROVIDER,
          modelId: MODEL,
          inputPerMillionTokens: 30,
          outputPerMillionTokens: 40,
        },
      ],
      T0,
      otherOrg,
    );

    expect((await resolveStoredModelPricing(store, PROVIDER, MODEL, ORG))?.inputPerToken).toBe(
      10 / 1_000_000,
    );
    expect((await resolveStoredModelPricing(store, PROVIDER, MODEL, otherOrg))?.inputPerToken).toBe(
      30 / 1_000_000,
    );
    expect((await store.list(ORG)).some((row) => row.organizationId === otherOrg)).toBe(false);
  });

  it('refuses an operator row without an organization scope', async () => {
    const store = new InMemoryModelPriceStore();
    await expect(
      store.upsert(
        'operator',
        [
          {
            provider: PROVIDER,
            modelId: MODEL,
            inputPerMillionTokens: 1,
            outputPerMillionTokens: 2,
          },
        ],
        T0,
        GLOBAL_SCOPE,
      ),
    ).rejects.toThrow('require an organization scope');
  });
});

describe('unpriced is not zero', () => {
  it('resolves an unknown model to null', async () => {
    const store = new InMemoryModelPriceStore();
    await store.upsert(
      'seed',
      [{ provider: PROVIDER, modelId: MODEL, inputPerMillionTokens: 1, outputPerMillionTokens: 2 }],
      T0,
      GLOBAL_SCOPE,
    );

    expect(await resolveStoredModelPricing(store, PROVIDER, OTHER, ORG)).toBeNull();
  });

  it('resolves an empty catalog to null for every model', async () => {
    const store = new InMemoryModelPriceStore();
    expect(await resolveStoredModelPricing(store, PROVIDER, MODEL, ORG)).toBeNull();
  });

  it('resolves a zero-rate row to unpriced rather than to a measured $0', async () => {
    // `@orca/harness-catalog` treats a zero, negative, or non-finite base rate as
    // no price at all: pricing a token at zero computes a cost that reads as
    // measured and silently disables every budget, which is strictly worse than
    // the unpriced state the budgets already know how to ask about.
    const store = new InMemoryModelPriceStore();
    await store.upsert(
      'operator',
      [{ provider: PROVIDER, modelId: MODEL, inputPerMillionTokens: 0, outputPerMillionTokens: 0 }],
      T0,
      ORG,
    );

    expect(await resolveStoredModelPricing(store, PROVIDER, MODEL, ORG)).toBeNull();
  });
});

describe('stored rows project onto library entries', () => {
  it('omits absent cache rates rather than sending them as zero', async () => {
    const store = new InMemoryModelPriceStore();
    await store.upsert(
      'seed',
      [{ provider: PROVIDER, modelId: MODEL, inputPerMillionTokens: 1, outputPerMillionTokens: 2 }],
      T0,
      GLOBAL_SCOPE,
    );

    const entries = toPriceEntries(await store.list(ORG));
    expect(entries).toEqual([
      {
        provider: PROVIDER,
        organizationId: GLOBAL_SCOPE,
        modelId: MODEL,
        source: 'seed',
        inputPerMillionTokens: 1,
        outputPerMillionTokens: 2,
      },
    ]);
  });

  it('carries published cache rates through', async () => {
    const store = new InMemoryModelPriceStore();
    await store.upsert(
      'operator',
      [
        {
          provider: PROVIDER,
          modelId: MODEL,
          inputPerMillionTokens: 1,
          outputPerMillionTokens: 2,
          cacheReadPerMillionTokens: 0.1,
          cacheWritePerMillionTokens: 1.25,
        },
      ],
      T0,
      ORG,
    );

    expect(toPriceEntries(await store.list(ORG))[0]).toMatchObject({
      cacheReadPerMillionTokens: 0.1,
      cacheWritePerMillionTokens: 1.25,
    });
  });
});

describe('the provider dimension', () => {
  const OTHER_PROVIDER = 'zz-other-vendor';

  it('prices the same model id independently under two providers', async () => {
    // The whole reason provider is part of the key: a vendor reselling an id
    // at its own rate must not inherit the other vendor's price.
    const store = new InMemoryModelPriceStore();
    await store.upsert(
      'seed',
      [{ provider: PROVIDER, modelId: MODEL, inputPerMillionTokens: 1, outputPerMillionTokens: 2 }],
      T0,
      GLOBAL_SCOPE,
    );
    await store.upsert(
      'seed',
      [
        {
          provider: OTHER_PROVIDER,
          modelId: MODEL,
          inputPerMillionTokens: 50,
          outputPerMillionTokens: 60,
        },
      ],
      T0,
      GLOBAL_SCOPE,
    );

    expect((await resolveStoredModelPricing(store, PROVIDER, MODEL, ORG))?.inputPerToken).toBe(
      1 / 1_000_000,
    );
    expect(
      (await resolveStoredModelPricing(store, OTHER_PROVIDER, MODEL, ORG))?.inputPerToken,
    ).toBe(50 / 1_000_000);
  });

  it('leaves a model unpriced under a provider that does not price it', async () => {
    // Not a fallback to whoever does price the id. A wrong price is worse than
    // no price, and resolution never crosses providers to find one.
    const store = new InMemoryModelPriceStore();
    await store.upsert(
      'seed',
      [{ provider: PROVIDER, modelId: MODEL, inputPerMillionTokens: 1, outputPerMillionTokens: 2 }],
      T0,
      GLOBAL_SCOPE,
    );

    expect(await resolveStoredModelPricing(store, OTHER_PROVIDER, MODEL, ORG)).toBeNull();
  });

  it('confines a full replace to the provider it refreshed', async () => {
    // `replace` withdraws rows the new catalog omitted. Scoped to one provider,
    // because a refresh fetches one vendor's catalog: spanning providers would
    // read every other vendor's rows as withdrawn and delete them.
    const store = new InMemoryModelPriceStore();
    await store.upsert(
      'upstream',
      [{ provider: PROVIDER, modelId: MODEL, inputPerMillionTokens: 1, outputPerMillionTokens: 2 }],
      T0,
      GLOBAL_SCOPE,
    );
    await store.upsert(
      'upstream',
      [
        {
          provider: OTHER_PROVIDER,
          modelId: OTHER,
          inputPerMillionTokens: 3,
          outputPerMillionTokens: 4,
        },
      ],
      T0,
      GLOBAL_SCOPE,
    );

    // A refresh of one provider that no longer carries its old model.
    await store.replace(
      PROVIDER,
      'upstream',
      [
        {
          provider: PROVIDER,
          modelId: OTHER,
          inputPerMillionTokens: 9,
          outputPerMillionTokens: 9,
        },
      ],
      T1,
      GLOBAL_SCOPE,
    );

    expect(await store.get(PROVIDER, MODEL, 'upstream', GLOBAL_SCOPE)).toBeNull();
    expect(await store.get(OTHER_PROVIDER, OTHER, 'upstream', GLOBAL_SCOPE)).not.toBeNull();
  });
});

describe('seed load', () => {
  it('loads every seeded model at seed precedence', async () => {
    const store = new InMemoryModelPriceStore();
    await loadSeedModelPrices(store, T0);

    const rows = await store.list(ORG);
    expect(rows).toHaveLength(SEED_MODEL_PRICES.length);
    expect(rows.every((row) => row.source === 'seed')).toBe(true);
    // Seed rows were never fetched, so they carry no fetch timestamp.
    expect(rows.every((row) => row.fetchedAt === null)).toBe(true);
  });

  it('is idempotent — a second load neither duplicates nor multiplies rows', async () => {
    const store = new InMemoryModelPriceStore();
    await loadSeedModelPrices(store, T0);
    await loadSeedModelPrices(store, T1);

    expect(await store.list(ORG)).toHaveLength(SEED_MODEL_PRICES.length);
  });

  it('does not clobber an operator override of a seeded model', async () => {
    const seeded = SEED_MODEL_PRICES[0]!;
    const store = new InMemoryModelPriceStore();
    await store.upsert(
      'operator',
      [
        {
          provider: PROVIDER,
          modelId: seeded.modelId,
          inputPerMillionTokens: 999,
          outputPerMillionTokens: 888,
        },
      ],
      T0,
      ORG,
    );

    await loadSeedModelPrices(store, T1);

    const operator = await store.get(PROVIDER, seeded.modelId, 'operator', ORG);
    expect(operator).toMatchObject({
      inputPerMillionTokens: 999,
      outputPerMillionTokens: 888,
      updatedAt: T0,
    });
    // …and the override still wins the resolution it was written to win.
    expect(
      (await resolveStoredModelPricing(store, PROVIDER, seeded.modelId, ORG))?.inputPerToken,
    ).toBe(999 / 1_000_000);
  });

  it('does not clobber an upstream entry for a seeded model', async () => {
    const seeded = SEED_MODEL_PRICES[0]!;
    const store = new InMemoryModelPriceStore();
    await store.upsert(
      'upstream',
      [
        {
          provider: PROVIDER,
          modelId: seeded.modelId,
          inputPerMillionTokens: 42,
          outputPerMillionTokens: 43,
        },
      ],
      T0,
      GLOBAL_SCOPE,
    );

    await loadSeedModelPrices(store, T1);

    expect(await store.get(PROVIDER, seeded.modelId, 'upstream', GLOBAL_SCOPE)).toMatchObject({
      inputPerMillionTokens: 42,
      updatedAt: T0,
    });
  });

  it('refreshes a seed row whose checked-in rates have changed', async () => {
    const seeded = SEED_MODEL_PRICES[0]!;
    const store = new InMemoryModelPriceStore();
    await store.upsert(
      'seed',
      [
        {
          provider: PROVIDER,
          modelId: seeded.modelId,
          inputPerMillionTokens: 0.01,
          outputPerMillionTokens: 0.02,
        },
      ],
      T0,
      GLOBAL_SCOPE,
    );

    await loadSeedModelPrices(store, T1);

    expect(await store.get(PROVIDER, seeded.modelId, 'seed', GLOBAL_SCOPE)).toMatchObject({
      inputPerMillionTokens: seeded.inputPerMillionTokens,
      createdAt: T0,
      updatedAt: T1,
    });
  });
});
