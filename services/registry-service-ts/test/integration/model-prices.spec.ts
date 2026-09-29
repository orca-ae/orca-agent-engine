// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { and, eq, inArray, like, sql } from 'drizzle-orm';
import { SEED_MODEL_PRICES, SEED_PRICE_PROVIDER } from '@orca/harness-catalog';
import {
  fingerprintAdminApiKey,
  generateAdminApiKey,
  hashAdminApiKey,
  partialAdminApiKeyHint,
} from '../../src/auth/admin-api-key.js';
import {
  adminApiKeys,
  adminAuditEvents,
  modelPrices,
} from '../../src/persistence/postgres/schema.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import { buildAdminApp, buildCombinedTestApp } from '../../src/server.js';
import { PRICING_API_PREFIX } from '../../src/contracts/model-prices.contract.js';
import {
  createPostgresModelPriceStore,
  loadSeedModelPrices,
  resolveStoredModelPricing,
  type ModelPriceStore,
} from '../../src/pricing/store.js';
import { createModelPriceRefresher } from '../../src/pricing/refresher.js';
import {
  buildStubFileStore,
  buildStubStore,
  buildTestJwtMinter,
  closeTestDb,
  getTestDb,
  STUB_SSE_CONFIG,
} from './setup.js';
import { createTestApiKey, TEST_ORGANIZATION_ID, uniqueWorkspace } from './fixtures.js';

const ADMIN_BASE = '/v1/organizations/modelprices';

/**
 * Test model ids share this prefix so the suite can clean up after itself
 * without disturbing the seed rows a parallel spec may have loaded.
 */
const PREFIX = 'zzint';
const MODEL = `${PREFIX}-testmodel-1`;
const ORG = TEST_ORGANIZATION_ID;
const GLOBAL_SCOPE = '';
/** The default provider, which every route here resolves against when none is named. */
const PROVIDER = SEED_PRICE_PROVIDER;

describe('Model prices (integration)', () => {
  let db: DbClient;
  let store: ModelPriceStore;
  let app: FastifyInstance;
  let adminApp: FastifyInstance;
  let apiKey: string;
  let adminKey: string;
  let readOnlyAdminKey: string;
  let unscopedAdminKey: string;

  beforeAll(async () => {
    ({ db } = await getTestDb());
    store = createPostgresModelPriceStore(db);

    app = buildCombinedTestApp({
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store: buildStubStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
    });
    // Routes come from `buildCombinedTestApp`, which now wires them, so this
    // suite exercises the same registration path a running server uses. `store`
    // is kept for seeding rows directly.
    adminApp = buildAdminApp({
      db,
      oidc: { allowedIssuers: [], audience: 'admin-test' },
      platformOidc: { allowedIssuers: [], audience: 'platform-test' },
      store: buildStubStore(),
    });
    await Promise.all([app.ready(), adminApp.ready()]);

    apiKey = await createTestApiKey(db, uniqueWorkspace('mpr'));
    adminKey = await seedAdminKey(db, ['model_prices:read', 'model_prices:write']);
    readOnlyAdminKey = await seedAdminKey(db, ['model_prices:read']);
    unscopedAdminKey = await seedAdminKey(db, ['workspaces:read']);
  });

  afterAll(async () => {
    await db.delete(modelPrices).where(like(modelPrices.modelId, `${PREFIX}-%`));
    await db
      .delete(adminAuditEvents)
      .where(
        and(
          eq(adminAuditEvents.targetType, 'model_price'),
          like(adminAuditEvents.targetId, `${PREFIX}-%`),
        ),
      );
    await Promise.all([app.close(), adminApp.close()]);
    await closeTestDb();
  });

  beforeEach(async () => {
    await db.delete(modelPrices).where(like(modelPrices.modelId, `${PREFIX}-%`));
    // Audit rows are append-only, so they outlive the price rows they describe.
    // Clearing this suite's own entries keeps the audit assertion exact rather
    // than "at least one of each".
    await db
      .delete(adminAuditEvents)
      .where(
        and(
          eq(adminAuditEvents.targetType, 'model_price'),
          like(adminAuditEvents.targetId, `${PREFIX}-%`),
        ),
      );
  });

  const adminReq = (
    method: 'GET' | 'POST' | 'PATCH' | 'DELETE',
    url: string,
    key: string,
    payload?: unknown,
  ) =>
    adminApp.inject({
      method,
      url,
      headers: { 'x-api-key': key },
      ...(payload === undefined ? {} : { payload: payload as object }),
    });

  describe('operator writes', () => {
    it('provisions and removes an unseeded OpenAI price for the organization', async () => {
      const model = `${PREFIX}-openai-e2e`;
      const identity = `${model}?provider=openai`;
      expect((await publicGet(identity)).statusCode).toBe(404);

      const created = await adminReq('POST', ADMIN_BASE, adminKey, {
        provider: 'openai',
        model_id: model,
        input_per_million_tokens: 1,
        output_per_million_tokens: 2,
      });
      expect(created.statusCode).toBe(201);
      expect(await store.get('openai', model, 'operator', ORG)).not.toBeNull();
      const globalRows = await db
        .select()
        .from(modelPrices)
        .where(
          and(
            eq(modelPrices.provider, 'openai'),
            eq(modelPrices.modelId, model),
            eq(modelPrices.organizationId, GLOBAL_SCOPE),
          ),
        );
      expect(globalRows).toHaveLength(0);

      const quote = await publicGet(identity);
      expect(quote.statusCode).toBe(200);
      expect(quote.json()).toMatchObject({
        provider: 'openai',
        model_id: model,
        input_per_million_tokens: 1,
        output_per_million_tokens: 2,
      });
      // Provider identity is part of the key: this does not add a Claude price.
      expect((await publicGet(model)).statusCode).toBe(404);

      expect((await adminReq('DELETE', `${ADMIN_BASE}/${identity}`, adminKey)).statusCode).toBe(
        200,
      );
      expect((await publicGet(identity)).statusCode).toBe(404);
    });

    it('rolls the price back when its audit event cannot be written', async () => {
      // A price change is a privileged mutation; one that lands with no
      // immutable record of who made it is worse than one that fails outright.
      // The only way to observe that they commit together is to make the audit
      // insert fail after the row write has already happened, which a temporary
      // check constraint does precisely.
      const model = `zzint-audit-atomic-${Date.now()}`;
      await db.execute(
        sql`ALTER TABLE admin_audit_events ADD CONSTRAINT audit_fail_probe CHECK (action <> 'model_price.created')`,
      );
      try {
        const res = await adminReq('POST', ADMIN_BASE, adminKey, {
          model_id: model,
          input_per_million_tokens: 15,
          output_per_million_tokens: 75,
        });
        expect(res.statusCode).toBe(500);
        // The row must not survive the failed audit.
        expect(await store.get(PROVIDER, model, 'operator', ORG)).toBeNull();
      } finally {
        await db.execute(sql`ALTER TABLE admin_audit_events DROP CONSTRAINT audit_fail_probe`);
      }

      // With the audit path healthy again, the same write succeeds — proving the
      // constraint was what blocked it, not the payload.
      const ok = await adminReq('POST', ADMIN_BASE, adminKey, {
        model_id: model,
        input_per_million_tokens: 15,
        output_per_million_tokens: 75,
      });
      expect(ok.statusCode).toBe(201);
      expect(await store.get(PROVIDER, model, 'operator', ORG)).not.toBeNull();
      await store.delete(PROVIDER, model, 'operator', ORG);
    });

    it('creates an entry and stores it at operator precedence', async () => {
      const res = await adminReq('POST', ADMIN_BASE, adminKey, {
        model_id: MODEL,
        input_per_million_tokens: 15,
        output_per_million_tokens: 75,
        cache_read_per_million_tokens: 1.5,
        cache_write_per_million_tokens: 18.75,
      });

      expect(res.statusCode).toBe(201);
      expect(res.json()).toMatchObject({
        type: 'model_price_entry',
        provider: PROVIDER,
        model_id: MODEL,
        source: 'operator',
        input_per_million_tokens: 15,
        output_per_million_tokens: 75,
      });
      expect(await store.get(PROVIDER, MODEL, 'operator', ORG)).toMatchObject({
        inputPerMillionTokens: 15,
      });
    });

    it('ignores a source supplied in the body rather than honouring it', async () => {
      const res = await adminReq('POST', ADMIN_BASE, adminKey, {
        model_id: MODEL,
        input_per_million_tokens: 1,
        output_per_million_tokens: 2,
        source: 'seed',
      });

      expect(res.statusCode).toBe(201);
      expect((res.json() as { source: string }).source).toBe('operator');
      expect(await store.get(PROVIDER, MODEL, 'seed', GLOBAL_SCOPE)).toBeNull();
    });

    it('refuses to create a second entry for the same model', async () => {
      const body = {
        model_id: MODEL,
        input_per_million_tokens: 1,
        output_per_million_tokens: 2,
      };
      expect((await adminReq('POST', ADMIN_BASE, adminKey, body)).statusCode).toBe(201);
      const again = await adminReq('POST', ADMIN_BASE, adminKey, body);
      expect(again.statusCode).toBe(409);
    });

    it('patches only the rates the body names', async () => {
      await adminReq('POST', ADMIN_BASE, adminKey, {
        model_id: MODEL,
        input_per_million_tokens: 1,
        output_per_million_tokens: 2,
        cache_read_per_million_tokens: 0.1,
      });

      const res = await adminReq('PATCH', `${ADMIN_BASE}/${MODEL}`, adminKey, {
        output_per_million_tokens: 20,
        source: 'upstream',
      });

      expect(res.statusCode).toBe(200);
      expect(res.json()).toMatchObject({
        source: 'operator',
        input_per_million_tokens: 1,
        output_per_million_tokens: 20,
        cache_read_per_million_tokens: 0.1,
      });
    });

    it('clears a cache rate back to derived when the body sends null', async () => {
      await adminReq('POST', ADMIN_BASE, adminKey, {
        model_id: MODEL,
        input_per_million_tokens: 1,
        output_per_million_tokens: 2,
        cache_read_per_million_tokens: 0.1,
      });

      const res = await adminReq('PATCH', `${ADMIN_BASE}/${MODEL}`, adminKey, {
        cache_read_per_million_tokens: null,
      });

      expect(
        (res.json() as { cache_read_per_million_tokens: number | null })
          .cache_read_per_million_tokens,
      ).toBeNull();
    });

    it('404s a patch or delete for a model with no operator entry', async () => {
      await store.upsert(
        'seed',
        [
          {
            provider: PROVIDER,
            modelId: MODEL,
            inputPerMillionTokens: 1,
            outputPerMillionTokens: 2,
          },
        ],
        new Date(),
        GLOBAL_SCOPE,
      );

      expect(
        (
          await adminReq('PATCH', `${ADMIN_BASE}/${MODEL}`, adminKey, {
            input_per_million_tokens: 5,
          })
        ).statusCode,
      ).toBe(404);
      expect((await adminReq('DELETE', `${ADMIN_BASE}/${MODEL}`, adminKey)).statusCode).toBe(404);
      // …and the seed row it is not allowed to address is still there.
      expect(await store.get(PROVIDER, MODEL, 'seed', GLOBAL_SCOPE)).not.toBeNull();
    });

    it('records an audit event for every write', async () => {
      await adminReq('POST', ADMIN_BASE, adminKey, {
        model_id: MODEL,
        input_per_million_tokens: 1,
        output_per_million_tokens: 2,
      });
      await adminReq('PATCH', `${ADMIN_BASE}/${MODEL}`, adminKey, {
        input_per_million_tokens: 3,
      });
      await adminReq('DELETE', `${ADMIN_BASE}/${MODEL}`, adminKey);

      const events = await db
        .select({ action: adminAuditEvents.action })
        .from(adminAuditEvents)
        .where(
          and(
            eq(adminAuditEvents.targetType, 'model_price'),
            eq(adminAuditEvents.targetId, MODEL),
            inArray(adminAuditEvents.action, [
              'model_price.created',
              'model_price.updated',
              'model_price.deleted',
            ]),
          ),
        );
      expect(events.map((event) => event.action).sort()).toEqual([
        'model_price.created',
        'model_price.deleted',
        'model_price.updated',
      ]);
    });
  });

  describe('deleting an override is reversible', () => {
    it('falls back to the remaining sources rather than making the model unpriced', async () => {
      await store.upsert(
        'seed',
        [
          {
            provider: PROVIDER,
            modelId: MODEL,
            inputPerMillionTokens: 1,
            outputPerMillionTokens: 2,
          },
        ],
        new Date(),
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
        new Date(),
        GLOBAL_SCOPE,
      );
      await adminReq('POST', ADMIN_BASE, adminKey, {
        model_id: MODEL,
        input_per_million_tokens: 100,
        output_per_million_tokens: 200,
      });

      const before = await publicGet(MODEL);
      expect(before.json()).toMatchObject({ input_per_million_tokens: 100 });

      const deleted = await adminReq('DELETE', `${ADMIN_BASE}/${MODEL}`, adminKey);
      expect(deleted.statusCode).toBe(200);
      expect(deleted.json()).toEqual({
        provider: PROVIDER,
        model_id: MODEL,
        type: 'model_price_entry_deleted',
      });

      const after = await publicGet(MODEL);
      expect(after.statusCode).toBe(200);
      expect(after.json()).toMatchObject({ input_per_million_tokens: 10 });
    });
  });

  describe('scopes', () => {
    it('refuses a write without model_prices:write', async () => {
      for (const key of [readOnlyAdminKey, unscopedAdminKey]) {
        const res = await adminReq('POST', ADMIN_BASE, key, {
          model_id: MODEL,
          input_per_million_tokens: 1,
          output_per_million_tokens: 2,
        });
        expect(res.statusCode).toBe(403);
      }
      expect(await store.get(PROVIDER, MODEL, 'operator', ORG)).toBeNull();
    });

    it('refuses a read without model_prices:read', async () => {
      expect((await adminReq('GET', ADMIN_BASE, unscopedAdminKey)).statusCode).toBe(403);
    });

    it('allows a read with model_prices:read alone', async () => {
      expect((await adminReq('GET', ADMIN_BASE, readOnlyAdminKey)).statusCode).toBe(200);
    });

    it('refuses an unauthenticated write', async () => {
      const res = await adminApp.inject({
        method: 'POST',
        url: ADMIN_BASE,
        payload: { model_id: MODEL, input_per_million_tokens: 1, output_per_million_tokens: 2 },
      });
      expect(res.statusCode).toBe(401);
    });
  });

  describe('admin reads', () => {
    it('lists every source so precedence is diagnosable', async () => {
      await store.upsert(
        'seed',
        [
          {
            provider: PROVIDER,
            modelId: MODEL,
            inputPerMillionTokens: 1,
            outputPerMillionTokens: 2,
          },
        ],
        new Date(),
        GLOBAL_SCOPE,
      );
      await adminReq('POST', ADMIN_BASE, adminKey, {
        model_id: MODEL,
        input_per_million_tokens: 100,
        output_per_million_tokens: 200,
      });

      const res = await adminReq('GET', `${ADMIN_BASE}?limit=1000`, adminKey);
      const rows = (
        res.json() as { data: Array<{ model_id: string; source: string }> }
      ).data.filter((row) => row.model_id === MODEL);
      expect(rows.map((row) => row.source).sort()).toEqual(['operator', 'seed']);
    });

    it('filters the list by source', async () => {
      await store.upsert(
        'seed',
        [
          {
            provider: PROVIDER,
            modelId: MODEL,
            inputPerMillionTokens: 1,
            outputPerMillionTokens: 2,
          },
        ],
        new Date(),
        GLOBAL_SCOPE,
      );
      await adminReq('POST', ADMIN_BASE, adminKey, {
        model_id: MODEL,
        input_per_million_tokens: 100,
        output_per_million_tokens: 200,
      });

      const res = await adminReq('GET', `${ADMIN_BASE}?source=operator&limit=1000`, adminKey);
      const rows = (res.json() as { data: Array<{ model_id: string; source: string }> }).data;
      expect(rows.every((row) => row.source === 'operator')).toBe(true);
      expect(rows.some((row) => row.model_id === MODEL)).toBe(true);
    });

    it('rejects an unknown source filter', async () => {
      expect((await adminReq('GET', `${ADMIN_BASE}?source=guess`, adminKey)).statusCode).toBe(400);
    });

    it('reads back a single operator entry', async () => {
      await adminReq('POST', ADMIN_BASE, adminKey, {
        model_id: MODEL,
        input_per_million_tokens: 1,
        output_per_million_tokens: 2,
      });

      const res = await adminReq('GET', `${ADMIN_BASE}/${MODEL}`, adminKey);
      expect(res.statusCode).toBe(200);
      expect(res.json()).toMatchObject({ model_id: MODEL, source: 'operator' });
    });
  });

  describe('the public listener', () => {
    it('serves resolved prices to a workspace principal', async () => {
      await store.upsert(
        'seed',
        [
          {
            provider: PROVIDER,
            modelId: MODEL,
            inputPerMillionTokens: 1,
            outputPerMillionTokens: 2,
          },
        ],
        new Date(),
        GLOBAL_SCOPE,
      );

      const res = await publicGet(MODEL);
      expect(res.statusCode).toBe(200);
      expect(res.json()).toEqual({
        type: 'model_price',
        provider: PROVIDER,
        model_id: MODEL,
        input_per_million_tokens: 1,
        output_per_million_tokens: 2,
        cache_read_per_million_tokens: 0.1,
        cache_write_per_million_tokens: 1.25,
      });
    });

    it('refuses a workspace write', async () => {
      const res = await app.inject({
        method: 'POST',
        url: `${PRICING_API_PREFIX}/modelprices`,
        headers: { 'x-api-key': apiKey },
        payload: {
          model_id: MODEL,
          input_per_million_tokens: 0,
          output_per_million_tokens: 0,
        },
      });

      // Write methods are not registered on this listener, so the refusal is a
      // 404 rather than the 403 an earlier revision returned. Registering eight
      // routes only to reject them put served operations outside the published
      // contract — see `test/unit/route-contract-parity.spec.ts`. What has to
      // hold either way is that nothing was written.
      expect(res.statusCode).toBeGreaterThanOrEqual(400);
      expect(await store.get(PROVIDER, MODEL, 'operator', ORG)).toBeNull();
    });

    it('requires authentication', async () => {
      const res = await app.inject({
        method: 'GET',
        url: `${PRICING_API_PREFIX}/modelprices`,
      });
      expect(res.statusCode).toBe(401);
    });
  });

  describe('seed load against Postgres', () => {
    it('is idempotent and leaves an operator override alone', async () => {
      const seeded = SEED_MODEL_PRICES[0]!;
      await db.delete(modelPrices).where(eq(modelPrices.modelId, seeded.modelId));
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
        new Date('2026-01-01T00:00:00.000Z'),
        ORG,
      );

      await loadSeedModelPrices(store);
      await loadSeedModelPrices(store);

      expect(await store.get(PROVIDER, seeded.modelId, 'operator', ORG)).toMatchObject({
        inputPerMillionTokens: 999,
        updatedAt: new Date('2026-01-01T00:00:00.000Z'),
      });
      expect(await store.get(PROVIDER, seeded.modelId, 'seed', GLOBAL_SCOPE)).toMatchObject({
        inputPerMillionTokens: seeded.inputPerMillionTokens,
      });
      expect(
        (await resolveStoredModelPricing(store, PROVIDER, seeded.modelId, ORG))?.inputPerToken,
      ).toBe(999 / 1_000_000);

      await db
        .delete(modelPrices)
        .where(and(eq(modelPrices.modelId, seeded.modelId), eq(modelPrices.source, 'operator')));
    });
  });

  describe('the refresher against Postgres', () => {
    it('upserts fetched entries at upstream precedence and leaves them on failure', async () => {
      const catalog = {
        schema_version: 1,
        models: {
          [MODEL]: {
            pricing: { input_per_million_tokens: 7, output_per_million_tokens: 21 },
          },
        },
      };
      const succeeding = createModelPriceRefresher({
        store,
        url: 'https://prices.test/catalog.json',
        intervalMs: 3_600_000,
        fetchImpl: (async () =>
          new Response(JSON.stringify(catalog), {
            headers: { 'content-type': 'application/json' },
          })) as typeof fetch,
      });
      expect(await succeeding.refreshNow()).toMatchObject({ status: 'succeeded' });
      expect(await store.get(PROVIDER, MODEL, 'upstream', GLOBAL_SCOPE)).toMatchObject({
        inputPerMillionTokens: 7,
      });

      const failing = createModelPriceRefresher({
        store,
        url: 'https://prices.test/catalog.json',
        intervalMs: 3_600_000,
        fetchImpl: (async () => {
          throw new Error('egress lost');
        }) as typeof fetch,
      });
      expect(await failing.refreshNow()).toMatchObject({ status: 'failed' });
      expect(await store.get(PROVIDER, MODEL, 'upstream', GLOBAL_SCOPE)).toMatchObject({
        inputPerMillionTokens: 7,
      });
    });
  });

  function publicGet(modelId: string) {
    return app.inject({
      method: 'GET',
      url: `${PRICING_API_PREFIX}/modelprices/${modelId}`,
      headers: { 'x-api-key': apiKey },
    });
  }
});

async function seedAdminKey(db: DbClient, scopes: string[]): Promise<string> {
  const plaintext = generateAdminApiKey();
  await db.insert(adminApiKeys).values({
    id: `adminkey_${Math.random().toString(36).slice(2, 12)}`,
    organizationId: TEST_ORGANIZATION_ID,
    hashedKey: await hashAdminApiKey(plaintext),
    keyFingerprint: fingerprintAdminApiKey(plaintext),
    name: `model-prices-${scopes.join('-') || 'none'}`,
    partialKeyHint: partialAdminApiKeyHint(plaintext),
    scopes,
    status: 'active',
    createdBy: 'integration-test',
  });
  return plaintext;
}
