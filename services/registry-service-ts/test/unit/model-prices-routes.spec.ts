// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import Fastify, { type FastifyInstance } from 'fastify';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { registerModelPricesRoutes } from '../../src/api/model-prices.routes.js';
import { PRICING_API_PREFIX } from '../../src/contracts/model-prices.contract.js';
import { InMemoryModelPriceStore } from '../../src/pricing/store.js';

const T0 = new Date('2026-07-01T00:00:00.000Z');
const MODEL = 'zz-testmodel-1';
const OTHER = 'zz-othermodel-1';
const ORG = 'org_model_price_test';
const GLOBAL_SCOPE = '';
/** The default provider, which is what a request omitting `?provider=` resolves against. */
const PROVIDER = 'anthropic';

describe('public model price routes', () => {
  let app: FastifyInstance;
  let store: InMemoryModelPriceStore;

  beforeEach(async () => {
    app = Fastify();
    app.addHook('onRequest', async (request) => {
      request.auth = {
        workspaceId: 'ws_model_price_route_test',
        principal: 'test',
        scopes: [],
        authMethod: 'api-key',
        apiKeyId: 'key_test',
      };
    });
    store = new InMemoryModelPriceStore();
    registerModelPricesRoutes(app, store, async () => ORG);
    await app.ready();
  });

  afterEach(async () => {
    await app.close();
  });

  it('serves the resolved price for a model', async () => {
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
          inputPerMillionTokens: 15,
          outputPerMillionTokens: 75,
          cacheReadPerMillionTokens: 1.5,
          cacheWritePerMillionTokens: 18.75,
        },
      ],
      T0,
      ORG,
    );

    const res = await app.inject({
      method: 'GET',
      url: `${PRICING_API_PREFIX}/modelprices/${MODEL}`,
    });

    expect(res.statusCode).toBe(200);
    expect(res.json()).toEqual({
      type: 'model_price',
      provider: PROVIDER,
      model_id: MODEL,
      input_per_million_tokens: 15,
      output_per_million_tokens: 75,
      cache_read_per_million_tokens: 1.5,
      cache_write_per_million_tokens: 18.75,
    });
  });

  it('answers unpriced with a 404 rather than a zero-rate object', async () => {
    await store.upsert(
      'seed',
      [{ provider: PROVIDER, modelId: MODEL, inputPerMillionTokens: 1, outputPerMillionTokens: 2 }],
      T0,
      GLOBAL_SCOPE,
    );

    const res = await app.inject({
      method: 'GET',
      url: `${PRICING_API_PREFIX}/modelprices/${OTHER}`,
    });

    expect(res.statusCode).toBe(404);
    expect(res.json()).toMatchObject({
      type: 'error',
      error: { type: 'not_found_error' },
    });
    expect(JSON.stringify(res.json())).not.toContain('per_million_tokens');
  });

  it('lists one resolved price per model, not one per stored row', async () => {
    await store.upsert(
      'seed',
      [
        { provider: PROVIDER, modelId: MODEL, inputPerMillionTokens: 1, outputPerMillionTokens: 2 },
        { provider: PROVIDER, modelId: OTHER, inputPerMillionTokens: 3, outputPerMillionTokens: 4 },
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

    const res = await app.inject({ method: 'GET', url: `${PRICING_API_PREFIX}/modelprices` });

    expect(res.statusCode).toBe(200);
    const body = res.json() as {
      data: Array<{ model_id: string; input_per_million_tokens: number }>;
    };
    expect(body.data.map((entry) => entry.model_id)).toEqual([OTHER, MODEL]);
    expect(body.data.find((entry) => entry.model_id === MODEL)?.input_per_million_tokens).toBe(100);
  });

  it('pages the list with a cursor that resumes where it stopped', async () => {
    await store.upsert(
      'seed',
      [
        {
          provider: PROVIDER,
          modelId: 'zz-a-1',
          inputPerMillionTokens: 1,
          outputPerMillionTokens: 1,
        },
        {
          provider: PROVIDER,
          modelId: 'zz-b-1',
          inputPerMillionTokens: 2,
          outputPerMillionTokens: 2,
        },
        {
          provider: PROVIDER,
          modelId: 'zz-c-1',
          inputPerMillionTokens: 3,
          outputPerMillionTokens: 3,
        },
      ],
      T0,
      GLOBAL_SCOPE,
    );

    const first = await app.inject({
      method: 'GET',
      url: `${PRICING_API_PREFIX}/modelprices?limit=2`,
    });
    const firstBody = first.json() as {
      data: Array<{ model_id: string }>;
      next_page: string | null;
    };
    expect(firstBody.data.map((e) => e.model_id)).toEqual(['zz-a-1', 'zz-b-1']);
    expect(firstBody.next_page).not.toBeNull();

    const second = await app.inject({
      method: 'GET',
      url: `${PRICING_API_PREFIX}/modelprices?limit=2&page=${encodeURIComponent(firstBody.next_page!)}`,
    });
    const secondBody = second.json() as {
      data: Array<{ model_id: string }>;
      next_page: string | null;
    };
    expect(secondBody.data.map((e) => e.model_id)).toEqual(['zz-c-1']);
    expect(secondBody.next_page).toBeNull();
  });

  it('rejects a malformed limit', async () => {
    const res = await app.inject({
      method: 'GET',
      url: `${PRICING_API_PREFIX}/modelprices?limit=0`,
    });
    expect(res.statusCode).toBe(400);
    expect(res.json()).toMatchObject({ type: 'error', error: { type: 'invalid_request_error' } });
  });

  it('omits a model that is stored nowhere from the list', async () => {
    const res = await app.inject({ method: 'GET', url: `${PRICING_API_PREFIX}/modelprices` });
    expect(res.json()).toEqual({ data: [], next_page: null });
  });
});

describe('the public listener is read-only', () => {
  let app: FastifyInstance;
  let store: InMemoryModelPriceStore;

  beforeEach(async () => {
    app = Fastify();
    app.addHook('onRequest', async (request) => {
      request.auth = {
        workspaceId: 'ws_model_price_route_test',
        principal: 'test',
        scopes: [],
        authMethod: 'api-key',
        apiKeyId: 'key_test',
      };
    });
    store = new InMemoryModelPriceStore();
    registerModelPricesRoutes(app, store, async () => ORG);
    await app.ready();
  });

  afterEach(async () => {
    await app.close();
  });

  const writes: Array<[string, string]> = [
    ['POST', `${PRICING_API_PREFIX}/modelprices`],
    ['PUT', `${PRICING_API_PREFIX}/modelprices/${MODEL}`],
    ['PATCH', `${PRICING_API_PREFIX}/modelprices/${MODEL}`],
    ['POST', `${PRICING_API_PREFIX}/modelprices/${MODEL}`],
    ['DELETE', `${PRICING_API_PREFIX}/modelprices/${MODEL}`],
  ];

  for (const [method, url] of writes) {
    it(`does not serve ${method} ${url}`, async () => {
      const res = await app.inject({
        method: method as 'POST',
        url,
        payload: { model_id: MODEL, input_per_million_tokens: 0, output_per_million_tokens: 0 },
      });

      // The invariant is that no write mutates through this listener. An
      // earlier revision answered 403 with a message naming the admin
      // listener, which registered eight routes no contract declared — see
      // `route-contract-parity.spec.ts`. The status is now whatever Fastify
      // gives an unregistered method; what matters, and what is asserted, is
      // that it is not a success and the store is untouched.
      expect(res.statusCode, `${method} ${url}`).toBeGreaterThanOrEqual(400);
      expect(await store.list(ORG)).toEqual([]);
    });
  }

  it('serves reads on the same paths, so the split is by method and not by path', async () => {
    // Guards against the write methods disappearing because the whole route
    // family stopped registering: reads must still work here.
    await store.upsert(
      'operator',
      [{ provider: PROVIDER, modelId: MODEL, inputPerMillionTokens: 1, outputPerMillionTokens: 1 }],
      T0,
      ORG,
    );

    const res = await app.inject({ method: 'GET', url: `${PRICING_API_PREFIX}/modelprices` });
    expect(res.statusCode).toBe(200);
  });
});
