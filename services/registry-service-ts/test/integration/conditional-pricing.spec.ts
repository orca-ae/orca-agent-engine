// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { beforeAll, afterAll, describe, it, expect } from 'vitest';
import { and, eq } from 'drizzle-orm';
import type { FastifyInstance } from 'fastify';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import {
  agents,
  agentVersions,
  sessions,
  sessionUsageEvents,
  pricingCatalogHeads,
  pricingCatalogVersions,
  pricingFetchSnapshots,
  pricingSourceLeases,
  pricingAdminIdempotency,
  adminApiKeys,
  adminAuditEvents,
} from '../../src/persistence/postgres/schema.js';
import { buildAdminApp, buildCombinedTestApp } from '../../src/server.js';
import {
  generateAdminApiKey,
  hashAdminApiKey,
  fingerprintAdminApiKey,
  partialAdminApiKeyHint,
} from '../../src/auth/admin-api-key.js';
import { createPostgresPricingCatalogStore } from '../../src/pricing/catalog-store.js';
import { publishOperatorRuleSet } from '../../src/pricing/rule-sets.js';
import { PriceRuleSetCreateSchema } from '../../src/contracts/model-price-rules.contract.js';
import { persistUsage } from '../../src/domain/usage-accounting.js';
import { createTestApiKey, TEST_ORGANIZATION_ID as org, uniqueWorkspace } from './fixtures.js';
import {
  getTestDb,
  closeTestDb,
  buildStubStore,
  buildStubFileStore,
  buildTestJwtMinter,
  STUB_SSE_CONFIG,
} from './setup.js';

const route = '/v1/organizations/modelpricerulesets';
const modelId = 'oip035-model';
const rate = {
  input_per_million_tokens: 2,
  output_per_million_tokens: 8,
  cache_read_per_million_tokens: 0.2,
  cache_write_per_million_tokens: 2.5,
  cache_write_1h_per_million_tokens: 4,
};
const body = PriceRuleSetCreateSchema.parse({
  provider: 'anthropic',
  model_id: modelId,
  activate: true,
  usage_profile: 'anthropic-cache-v1',
  profile_version: 'anthropic-v1',
  provider_effective_at: null,
  rules: [
    {
      conditions: { service_tier: 'standard', request_input_tokens: { min: 0, max: 100001 } },
      rates: rate,
    },
    {
      conditions: { service_tier: 'standard', request_input_tokens: { min: 100001, max: null } },
      rates: { ...rate, input_per_million_tokens: 4 },
    },
  ],
});

describe('conditional pricing PostgreSQL/API/accounting', () => {
  let db: DbClient, admin: FastifyInstance, publicApp: FastifyInstance;
  let workspace: string, key: string, adminKey: string, readKey: string;
  let setId: string;
  const auth = () => ({ 'x-api-key': adminKey });
  beforeAll(async () => {
    ({ db } = await getTestDb());
    workspace = uniqueWorkspace('conditional');
    key = await createTestApiKey(db, workspace);
    for (const scopes of [['model_prices:read', 'model_prices:write'], ['model_prices:read']]) {
      const plaintext = generateAdminApiKey();
      await db.insert(adminApiKeys).values({
        id: uniqueWorkspace('adminkey'),
        organizationId: org,
        hashedKey: await hashAdminApiKey(plaintext),
        keyFingerprint: fingerprintAdminApiKey(plaintext),
        name: uniqueWorkspace('price-key'),
        partialKeyHint: partialAdminApiKeyHint(plaintext),
        scopes,
        status: 'active',
        createdBy: 'integration-test',
      });
      if (scopes.length === 2) adminKey = plaintext;
      else readKey = plaintext;
    }
    admin = buildAdminApp({
      db,
      oidc: { allowedIssuers: [], audience: 'test' },
      platformOidc: { allowedIssuers: [], audience: 'test-platform' },
      store: buildStubStore(),
      conditionalPricingEnabled: true,
    });
    publicApp = buildCombinedTestApp({
      db,
      oidc: { allowedIssuers: [], audience: 'test' },
      store: buildStubStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
    });
    await Promise.all([admin.ready(), publicApp.ready()]);
  });
  afterAll(async () => {
    await Promise.all([admin.close(), publicApp.close()]);
    for (const table of [
      pricingAdminIdempotency,
      pricingFetchSnapshots,
      pricingCatalogHeads,
      pricingCatalogVersions,
    ])
      await db.delete(table).where(eq(table.organizationId, org));
    await db.delete(pricingSourceLeases).where(eq(pricingSourceLeases.sourceId, 'oip035-test'));
    await db
      .delete(adminAuditEvents)
      .where(eq(adminAuditEvents.targetType, 'model_price_rule_set'));
    await closeTestDb();
  });
  it('publishes scoped rules with atomic idempotency/audits, rejects unauthorized writes and stale If-Match', async () => {
    expect(
      (
        await admin.inject({
          method: 'POST',
          url: route,
          headers: { 'x-api-key': readKey },
          payload: body,
        })
      ).statusCode,
    ).toBe(403);
    const request = {
      method: 'POST' as const,
      url: route,
      headers: { ...auth(), 'idempotency-key': 'create-conditional' },
      payload: body,
    };
    const created = await admin.inject(request);
    expect(created.statusCode, created.body).toBe(201);
    setId = created.json().id;
    const repeated = await admin.inject(request);
    expect(repeated.statusCode).toBe(201);
    expect(repeated.json()).toEqual(created.json());
    expect(
      (await admin.inject({ ...request, payload: { ...body, activate: false } })).statusCode,
    ).toBe(409);
    const patch = {
      method: 'PATCH' as const,
      url: `${route}/${setId}`,
      headers: { ...auth(), 'if-match': '"99"' },
      payload: body,
    };
    expect((await admin.inject(patch)).statusCode).toBe(409);
    const doc = await createPostgresPricingCatalogStore(db).read(org);
    expect(doc.generation).toBe(1);
    const audits = await db
      .select()
      .from(adminAuditEvents)
      .where(eq(adminAuditEvents.targetId, setId));
    expect(audits).toHaveLength(1);
    expect(
      (
        await admin.inject({
          method: 'POST',
          url: '/v1/organizations/modelprices',
          headers: auth(),
          payload: {
            provider: 'anthropic',
            model_id: modelId,
            input_per_million_tokens: 2,
            output_per_million_tokens: 8,
          },
        })
      ).statusCode,
    ).toBe(400);
  });
  it('pins pages across publication and returns safe v1 projection/unknown cursor outcomes', async () => {
    const first = await publicApp.inject({
      url: '/apis/pricing.runorca.ai/v2/modelpricerules?limit=1',
      headers: { 'x-api-key': key },
    });
    expect(first.statusCode, first.body).toBe(200);
    const store = createPostgresPricingCatalogStore(db);
    await store.publish(org, new Date(), (doc) =>
      publishOperatorRuleSet(doc, org, { ...body, activate: false }, new Date(), true, setId, 1),
    );
    const second = await publicApp.inject({
      url: `/apis/pricing.runorca.ai/v2/modelpricerules?page=${first.json().next_page}`,
      headers: { 'x-api-key': key },
    });
    expect(second.statusCode, second.body).toBe(200);
    expect(second.json().catalog_id).toBe(first.json().catalog_id);
    expect(second.json().data[0].revision).toBe(1);
    expect(
      (
        await publicApp.inject({
          url: '/apis/pricing.runorca.ai/v2/modelpricerules?page=pcat_UNKNOWN:1',
          headers: { 'x-api-key': key },
        })
      ).statusCode,
    ).toBe(410);
    const v1 = await publicApp.inject({
      url: '/apis/pricing.runorca.ai/v1/modelprices',
      headers: { 'x-api-key': key },
    });
    expect(v1.json().data.some((m: { model_id: string }) => m.model_id === modelId)).toBe(false);
  });
  it('serializes competing publishers, fences expired leases and retains immutable versions', async () => {
    const store = createPostgresPricingCatalogStore(db);
    const before = await store.read(org);
    const results = await Promise.allSettled(
      [1, 2].map(() =>
        store.publish(org, new Date(), (doc) =>
          publishOperatorRuleSet(
            doc,
            org,
            body,
            new Date(),
            true,
            setId,
            before.rule_sets[0]!.revision,
          ),
        ),
      ),
    );
    expect(results.filter((r) => r.status === 'fulfilled')).toHaveLength(1);
    expect(results.filter((r) => r.status === 'rejected')).toHaveLength(1);
    expect(
      (
        await db
          .select()
          .from(pricingCatalogVersions)
          .where(eq(pricingCatalogVersions.organizationId, org))
      ).length,
    ).toBe(before.generation + 1);
    const at = new Date();
    const lease = (await store.acquireLease('anthropic', 'oip035-test', at, 100))!;
    const after = new Date(at.getTime() + 101);
    await store.acquireLease('anthropic', 'oip035-test', after, 100);
    await expect(store.publish('', after, () => {}, lease)).rejects.toThrow(/lease/);
  });
  it('records one immutable receipt and one budget delta on concurrent replay after repricing', async () => {
    const agentId = `agt_${workspace}`,
      sessionId = `ses_${workspace}`;
    await db.insert(agents).values({
      id: agentId,
      workspaceId: workspace,
      name: uniqueWorkspace('agent'),
      modelProvider: 'anthropic',
      modelId,
    });
    await db.insert(agentVersions).values({
      id: `version_${workspace}`,
      workspaceId: workspace,
      agentId,
      version: 1,
      snapshot: {},
    });
    await db
      .insert(sessions)
      .values({ id: sessionId, workspaceId: workspace, agentId, agentVersion: 1 });
    const usage = {
      inputTokens: 100001,
      outputTokens: 100,
      cacheReadInputTokens: 0,
      cacheCreationEphemeral5mInputTokens: 0,
      cacheCreationEphemeral1hInputTokens: 0,
    };
    const input = {
      workspaceId: workspace,
      sessionId,
      usage,
      costNanoUsd: null,
      pricingContext: {
        provider: 'anthropic',
        modelId,
        facts: {
          version: 1 as const,
          request_id: 'response-1',
          service_tier: 'standard',
          request_input_tokens: 100001,
          usage_profile: 'anthropic-cache-v1',
          evidence_source: 'provider_response' as const,
        },
      },
      now: new Date(),
      turnSubject: 'user',
      usageEventId: 'evt_conditional_1',
    };
    const store = createPostgresPricingCatalogStore(db);
    const [globalDoc, orgDoc] = await Promise.all([store.read(''), store.read(org)]);
    const results = await Promise.all(
      [1, 2].map(() => db.transaction((tx) => persistUsage(tx, input))),
    );
    expect(results.sort()).toEqual(['applied', 'replayed']);
    const [before] = await db
      .select()
      .from(sessionUsageEvents)
      .where(
        and(
          eq(sessionUsageEvents.workspaceId, workspace),
          eq(sessionUsageEvents.eventId, input.usageEventId),
        ),
      );
    expect(before!.pricingReceipt).toMatchObject({
      cost_nano_usd: '400804000',
      rule_revision: expect.any(Number),
      global_catalog_id: globalDoc.catalog_id || null,
      org_catalog_id: orgDoc.catalog_id,
    });
    const rollback = new Error('rollback global receipt fixture');
    await expect(
      db.transaction(async (tx) => {
        const scopedStore = createPostgresPricingCatalogStore(tx);
        await scopedStore.publish('', new Date(), () => {});
        const globalCatalog = await scopedStore.read('');
        expect(globalCatalog.catalog_id).not.toBe('');
        const usageEventId = 'evt_conditional_global_scope';
        await persistUsage(tx, {
          ...input,
          usageEventId,
          pricingContext: {
            ...input.pricingContext,
            facts: { ...input.pricingContext.facts, request_id: 'global-scope-response' },
          },
        });
        const [withGlobal] = await tx
          .select()
          .from(sessionUsageEvents)
          .where(
            and(
              eq(sessionUsageEvents.workspaceId, workspace),
              eq(sessionUsageEvents.eventId, usageEventId),
            ),
          );
        expect(withGlobal?.pricingReceipt).toMatchObject({
          global_catalog_id: globalCatalog.catalog_id,
          org_catalog_id: orgDoc.catalog_id,
        });
        throw rollback;
      }),
    ).rejects.toBe(rollback);
    const doc = await store.read(org);
    await store.publish(org, new Date(), (draft) =>
      publishOperatorRuleSet(
        draft,
        org,
        {
          ...body,
          rules: body.rules.map((r) => ({
            ...r,
            rates: { ...r.rates, input_per_million_tokens: 20 },
          })),
        },
        new Date(),
        true,
        setId,
        doc.rule_sets[0]!.revision,
      ),
    );
    expect(await db.transaction((tx) => persistUsage(tx, { ...input, now: new Date() }))).toBe(
      'replayed',
    );
    const [after] = await db
      .select()
      .from(sessionUsageEvents)
      .where(
        and(
          eq(sessionUsageEvents.workspaceId, workspace),
          eq(sessionUsageEvents.eventId, input.usageEventId),
        ),
      );
    expect(after!.pricingReceipt).toEqual(before!.pricingReceipt);
    const [session] = await db.select().from(sessions).where(eq(sessions.id, sessionId));
    expect(session!.usageCostNanoUsd).toBe(400804000n);
    expect(session!.usageInputTokens).toBe(100001);
    await db.transaction((tx) =>
      persistUsage(tx, {
        ...input,
        pricingContext: { ...input.pricingContext, facts: null },
        usageEventId: 'evt_conditional_missing',
      }),
    );
    const [unpriced] = await db
      .select()
      .from(sessionUsageEvents)
      .where(
        and(
          eq(sessionUsageEvents.workspaceId, workspace),
          eq(sessionUsageEvents.eventId, 'evt_conditional_missing'),
        ),
      );
    expect(unpriced!.pricingReceipt).toMatchObject({
      cost_nano_usd: null,
      reason: 'unknown_service_tier',
      global_catalog_id: globalDoc.catalog_id || null,
      org_catalog_id: (await store.read(org)).catalog_id,
    });
  });
  it('negotiates HTTP request facts, rejects incomplete counters and acknowledges one charge', async () => {
    const sessionId = `ses_http_${workspace}`,
      agentId = `agt_http_${workspace}`;
    await db.insert(agents).values({
      id: agentId,
      workspaceId: workspace,
      name: uniqueWorkspace('http-agent'),
      modelProvider: 'anthropic',
      modelId,
    });
    await db.insert(agentVersions).values({
      id: `version_http_${workspace}`,
      workspaceId: workspace,
      agentId,
      version: 1,
      snapshot: {},
    });
    await db
      .insert(sessions)
      .values({ id: sessionId, workspaceId: workspace, agentId, agentVersion: 1 });
    const payload = {
      model: modelId,
      provider: 'anthropic',
      usage_event_id: 'evt_http_facts',
      pricing_facts: {
        version: 1,
        request_id: 'response-http',
        service_tier: 'standard',
        request_input_tokens: 1,
        usage_profile: 'anthropic-cache-v1',
        evidence_source: 'provider_response',
      },
      usage: {
        input_tokens: 1,
        output_tokens: 0,
        cache_read_input_tokens: 0,
        cache_creation: { ephemeral_5m_input_tokens: 0, ephemeral_1h_input_tokens: 0 },
      },
    };
    const request = {
      method: 'POST' as const,
      url: `/internal/v1/workspaces/${workspace}/sessions/${sessionId}/usage`,
      payload,
    };
    expect((await publicApp.inject(request)).statusCode).toBe(400);
    const headers = { 'x-orca-pricing-capability': 'request-facts-v1' };
    expect(
      (
        await publicApp.inject({
          ...request,
          headers,
          payload: { ...payload, usage: { input_tokens: 1 } },
        })
      ).statusCode,
    ).toBe(400);
    const accepted = await publicApp.inject({ ...request, headers });
    expect(accepted.statusCode, accepted.body).toBe(200);
    expect(accepted.headers['x-orca-pricing-capability']).toBe('request-facts-v1');
    expect((await publicApp.inject({ ...request, headers })).statusCode).toBe(200);
    const [session] = await db.select().from(sessions).where(eq(sessions.id, sessionId));
    expect(session!.usageCostNanoUsd).toBe(20000n);
    expect(session!.usageInputTokens).toBe(1);
  });
});
