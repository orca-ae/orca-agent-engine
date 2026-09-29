// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { SEED_PRICE_PROVIDER } from '@orca/harness-catalog';
import { createModelPriceRefresher } from '../../src/pricing/refresher.js';
import { InMemoryModelPriceStore, resolveStoredModelPricing } from '../../src/pricing/store.js';

const URL_ = 'https://prices.test/catalog.json';
const MODEL = 'zz-testmodel-1';
const ORG = 'org_model_price_test';
const GLOBAL_SCOPE = '';
/** The refresher's default provider, which is what an unconfigured feed writes under. */
const PROVIDER = SEED_PRICE_PROVIDER;

const CATALOG = {
  schema_version: 1,
  models: {
    [MODEL]: {
      pricing: {
        input_per_million_tokens: 7,
        output_per_million_tokens: 21,
        cache_read_per_million_tokens: 0.7,
        cache_write_per_million_tokens: 8.75,
      },
    },
  },
};

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}

/** A fetch stub that records its calls so "did not touch the network" is testable. */
function recordingFetch(handler: () => Promise<Response>): {
  impl: typeof fetch;
  calls: string[];
} {
  const calls: string[] = [];
  const impl = (async (input: RequestInfo | URL) => {
    calls.push(String(input));
    return handler();
  }) as typeof fetch;
  return { impl, calls };
}

describe('no refresh URL configured', () => {
  it('is a supported configuration, not an error, and performs no fetch', async () => {
    const store = new InMemoryModelPriceStore();
    const fetchStub = recordingFetch(async () => {
      throw new Error('the refresher must not reach the network');
    });
    const refresher = createModelPriceRefresher({
      store,
      url: undefined,
      intervalMs: 60_000,
      fetchImpl: fetchStub.impl,
    });

    await expect(refresher.refreshNow()).resolves.toEqual({
      status: 'skipped',
      reason: 'not_configured',
    });
    expect(fetchStub.calls).toEqual([]);
    expect(refresher.lastSuccessAt()).toBeNull();
    expect(refresher.lastSuccessAgeMs()).toBeNull();
  });

  it('starts and stops without scheduling anything', async () => {
    const store = new InMemoryModelPriceStore();
    const fetchStub = recordingFetch(async () => {
      throw new Error('the refresher must not reach the network');
    });
    const refresher = createModelPriceRefresher({
      store,
      url: undefined,
      intervalMs: 60_000,
      fetchImpl: fetchStub.impl,
    });

    refresher.start();
    refresher.stop();
    expect(fetchStub.calls).toEqual([]);
  });
});

describe('successful refresh', () => {
  it('upserts fetched entries at upstream precedence', async () => {
    const store = new InMemoryModelPriceStore();
    const now = new Date('2026-07-01T00:00:00.000Z');
    const refresher = createModelPriceRefresher({
      store,
      url: URL_,
      intervalMs: 60_000,
      fetchImpl: recordingFetch(async () => jsonResponse(CATALOG)).impl,
      now: () => now,
    });

    await expect(refresher.refreshNow()).resolves.toEqual({ status: 'succeeded', entries: 1 });

    expect(await store.get(PROVIDER, MODEL, 'upstream', GLOBAL_SCOPE)).toMatchObject({
      source: 'upstream',
      inputPerMillionTokens: 7,
      outputPerMillionTokens: 21,
      cacheReadPerMillionTokens: 0.7,
      cacheWritePerMillionTokens: 8.75,
      fetchedAt: now,
    });
  });

  it('does not outrank an operator override', async () => {
    const store = new InMemoryModelPriceStore();
    await store.upsert(
      'operator',
      [{ provider: PROVIDER, modelId: MODEL, inputPerMillionTokens: 1, outputPerMillionTokens: 2 }],
      new Date('2026-06-01T00:00:00.000Z'),
      ORG,
    );
    const refresher = createModelPriceRefresher({
      store,
      url: URL_,
      intervalMs: 60_000,
      fetchImpl: recordingFetch(async () => jsonResponse(CATALOG)).impl,
    });

    await refresher.refreshNow();

    expect((await resolveStoredModelPricing(store, PROVIDER, MODEL, ORG))?.inputPerToken).toBe(
      1 / 1_000_000,
    );
  });

  it('records the age of the last successful refresh', async () => {
    const store = new InMemoryModelPriceStore();
    let clock = new Date('2026-07-01T00:00:00.000Z');
    const refresher = createModelPriceRefresher({
      store,
      url: URL_,
      intervalMs: 60_000,
      fetchImpl: recordingFetch(async () => jsonResponse(CATALOG)).impl,
      now: () => clock,
    });

    await refresher.refreshNow();
    expect(refresher.lastSuccessAt()).toEqual(new Date('2026-07-01T00:00:00.000Z'));
    expect(refresher.lastSuccessAgeMs()).toBe(0);

    clock = new Date('2026-07-01T02:00:00.000Z');
    expect(refresher.lastSuccessAgeMs()).toBe(2 * 60 * 60 * 1000);
  });

  it('removes upstream rows omitted from the next complete catalog', async () => {
    const store = new InMemoryModelPriceStore();
    const staleModel = 'zz-stale-model-1';
    let catalog: unknown = {
      ...CATALOG,
      models: {
        ...CATALOG.models,
        [staleModel]: {
          pricing: { input_per_million_tokens: 1, output_per_million_tokens: 2 },
        },
      },
    };
    const refresher = createModelPriceRefresher({
      store,
      url: URL_,
      intervalMs: 60_000,
      fetchImpl: recordingFetch(async () => jsonResponse(catalog)).impl,
    });

    await refresher.refreshNow();
    expect(await store.get(PROVIDER, staleModel, 'upstream', GLOBAL_SCOPE)).not.toBeNull();

    catalog = CATALOG;
    await refresher.refreshNow();
    expect(await store.get(PROVIDER, staleModel, 'upstream', GLOBAL_SCOPE)).toBeNull();
    expect(await store.get(PROVIDER, MODEL, 'upstream', GLOBAL_SCOPE)).not.toBeNull();
  });
});

describe('failed refresh', () => {
  const failures: Array<[string, () => Promise<Response>]> = [
    [
      'a transport error',
      async () => {
        throw new Error('econnrefused');
      },
    ],
    ['a non-2xx status', async () => jsonResponse({ error: 'nope' }, 503)],
    ['a body that is not JSON', async () => new Response('<html>captive portal</html>')],
    ['an unsupported schema version', async () => jsonResponse({ schema_version: 2, models: {} })],
    ['a payload with no models', async () => jsonResponse({ schema_version: 1, models: {} })],
  ];

  for (const [label, handler] of failures) {
    it(`leaves the last successful rows authoritative after ${label}`, async () => {
      const store = new InMemoryModelPriceStore();
      const good = createModelPriceRefresher({
        store,
        url: URL_,
        intervalMs: 60_000,
        fetchImpl: recordingFetch(async () => jsonResponse(CATALOG)).impl,
        now: () => new Date('2026-07-01T00:00:00.000Z'),
      });
      await good.refreshNow();
      const before = await store.list(ORG);

      const bad = createModelPriceRefresher({
        store,
        url: URL_,
        intervalMs: 60_000,
        fetchImpl: recordingFetch(handler).impl,
        now: () => new Date('2026-07-02T00:00:00.000Z'),
      });
      const outcome = await bad.refreshNow();

      expect(outcome.status).toBe('failed');
      expect(await store.list(ORG)).toEqual(before);
      // A refresher that has never succeeded reports no age rather than a
      // reassuring zero.
      expect(bad.lastSuccessAt()).toBeNull();
    });
  }

  it('never throws out of a scheduled refresh', async () => {
    const store = new InMemoryModelPriceStore();
    const refresher = createModelPriceRefresher({
      store,
      url: URL_,
      intervalMs: 60_000,
      fetchImpl: recordingFetch(async () => {
        throw new Error('boom');
      }).impl,
    });

    await expect(refresher.refreshNow()).resolves.toMatchObject({ status: 'failed' });
  });

  it('keeps the earlier success timestamp when a later refresh fails', async () => {
    const store = new InMemoryModelPriceStore();
    let clock = new Date('2026-07-01T00:00:00.000Z');
    let fail = false;
    const refresher = createModelPriceRefresher({
      store,
      url: URL_,
      intervalMs: 60_000,
      fetchImpl: recordingFetch(async () => {
        if (fail) throw new Error('egress lost');
        return jsonResponse(CATALOG);
      }).impl,
      now: () => clock,
    });

    await refresher.refreshNow();
    fail = true;
    clock = new Date('2026-07-03T00:00:00.000Z');
    await refresher.refreshNow();

    expect(refresher.lastSuccessAt()).toEqual(new Date('2026-07-01T00:00:00.000Z'));
    expect(refresher.lastSuccessAgeMs()).toBe(2 * 24 * 60 * 60 * 1000);
  });
});

describe('overlapping runs', () => {
  it('does not start a second fetch while one is in flight', async () => {
    const store = new InMemoryModelPriceStore();
    let release: (() => void) | undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const fetchStub = recordingFetch(async () => {
      await gate;
      return jsonResponse(CATALOG);
    });
    const refresher = createModelPriceRefresher({
      store,
      url: URL_,
      intervalMs: 60_000,
      fetchImpl: fetchStub.impl,
    });

    const first = refresher.refreshNow();
    const second = await refresher.refreshNow();
    expect(second).toEqual({ status: 'skipped', reason: 'in_flight' });

    release!();
    await expect(first).resolves.toMatchObject({ status: 'succeeded' });
    expect(fetchStub.calls).toEqual([URL_]);
  });
});
