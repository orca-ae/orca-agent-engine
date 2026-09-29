// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Layer A: model pricing through deployed public/admin Registry listeners.
 * No model calls or sandbox are needed: these assertions cover price authority
 * and effective quotes, not retrospective Session usage or accounting.
 */
import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  apiCall,
  buildClientFromConfig,
  ensureStackReachable,
  type ApiResponse,
  type OrcaClientConfig,
} from '../src/client.js';
import { provisionWorkspace, type ProvisionedWorkspace } from './environment-helpers.js';
import {
  seedIsolatedOrganizationAdmin,
  type IsolatedOrganizationAdmin,
} from './organization-admin-helpers.js';

const PRICES = '/apis/pricing.runorca.ai/v1/modelprices';
const ADMIN_PRICES = '/v1/organizations/modelprices';
const publicBaseURL = process.env['ORCA_BASE_URL'] ?? 'http://localhost:8080';
const adminBaseURL = process.env['ORCA_ADMIN_BASE_URL'] ?? 'http://localhost:8082';

interface ModelPrice {
  type: 'model_price';
  provider: string;
  model_id: string;
  input_per_million_tokens: number;
  output_per_million_tokens: number;
  cache_read_per_million_tokens: number;
  cache_write_per_million_tokens: number;
}

interface ModelPriceEntry extends Omit<
  ModelPrice,
  'type' | 'cache_read_per_million_tokens' | 'cache_write_per_million_tokens'
> {
  type: 'model_price_entry';
  source: 'operator' | 'upstream' | 'seed';
  cache_read_per_million_tokens: number | null;
  cache_write_per_million_tokens: number | null;
  fetched_at: string | null;
  created_at: string;
  updated_at: string;
}

interface AdminPricePage {
  data: ModelPriceEntry[];
  has_more: boolean;
  first_id: string | null;
  last_id: string | null;
}

interface PublicPricePage {
  data: ModelPrice[];
  next_page: string | null;
}

function itemPath(base: string, modelId: string, provider = 'anthropic'): string {
  return `${base}/${encodeURIComponent(modelId)}?provider=${encodeURIComponent(provider)}`;
}

function assertPublicPrice(response: ApiResponse): ModelPrice {
  expect(response.status, response.text).toBe(200);
  const price = response.json<ModelPrice>();
  expect(Object.keys(price).sort()).toEqual(
    [
      'type',
      'provider',
      'model_id',
      'input_per_million_tokens',
      'output_per_million_tokens',
      'cache_read_per_million_tokens',
      'cache_write_per_million_tokens',
    ].sort(),
  );
  expect(price.type).toBe('model_price');
  return price;
}

describe('Layer A: model prices (live Registry, isolated organizations)', () => {
  const runId = randomUUID().replaceAll('-', '').slice(0, 16);
  const organizations: IsolatedOrganizationAdmin[] = [];
  const workspaces: Array<{ admin: OrcaClientConfig; workspace: ProvisionedWorkspace }> = [];
  const createdPrices = new Map<
    string,
    { admin: OrcaClientConfig; modelId: string; provider: string }
  >();
  let adminA: OrcaClientConfig;
  let adminB: OrcaClientConfig;
  let limitedAdmin: OrcaClientConfig;
  let workspaceA: ProvisionedWorkspace;
  let workspaceB: ProvisionedWorkspace;

  function priceKey(admin: OrcaClientConfig, provider: string, modelId: string): string {
    // Avoid putting credentials into assertion messages or cleanup keys.
    return `${admin === adminA ? 'a' : 'b'}/${provider}/${modelId}`;
  }

  async function createPrice(
    admin: OrcaClientConfig,
    body: Record<string, unknown>,
  ): Promise<ApiResponse> {
    const response = await apiCall(admin, ADMIN_PRICES, {
      method: 'POST',
      body: JSON.stringify(body),
    });
    if (response.status === 201) {
      const price = response.json<ModelPriceEntry>();
      createdPrices.set(priceKey(admin, price.provider, price.model_id), {
        admin,
        modelId: price.model_id,
        provider: price.provider,
      });
    }
    return response;
  }

  async function deletePrice(admin: OrcaClientConfig, modelId: string, provider = 'anthropic') {
    const response = await apiCall(admin, itemPath(ADMIN_PRICES, modelId, provider), {
      method: 'DELETE',
    });
    expect(response.status, response.text).toBe(200);
    expect(response.json()).toEqual({
      provider,
      model_id: modelId,
      type: 'model_price_entry_deleted',
    });
    createdPrices.delete(priceKey(admin, provider, modelId));
  }

  beforeAll(async () => {
    // Probe before seeding so an absent stack fails clearly without leaving a tenant.
    const probe = buildClientFromConfig({ baseURL: publicBaseURL, apiKey: 'health-probe' });
    await ensureStackReachable(probe);
    await ensureStackReachable({ ...probe, baseURL: adminBaseURL });
    const a = await seedIsolatedOrganizationAdmin(`Price A ${runId}`, {
      withoutPrices: ['workspaces:read'],
    });
    organizations.push(a);
    adminA = buildClientFromConfig({ baseURL: adminBaseURL, apiKey: a.apiKey });
    limitedAdmin = buildClientFromConfig({
      baseURL: adminBaseURL,
      apiKey: a.additionalApiKeys['withoutPrices']!,
    });
    workspaceA = await provisionWorkspace(adminA, `Price workspace A ${runId}`);
    workspaces.push({ admin: adminA, workspace: workspaceA });
    const b = await seedIsolatedOrganizationAdmin(`Price B ${runId}`);
    organizations.push(b);
    adminB = buildClientFromConfig({ baseURL: adminBaseURL, apiKey: b.apiKey });
    workspaceB = await provisionWorkspace(adminB, `Price workspace B ${runId}`);
    workspaces.push({ admin: adminB, workspace: workspaceB });
    expect(a.organizationId).not.toBe(b.organizationId);
    expect(workspaceA.id).not.toBe(workspaceB.id);
    for (const [admin, workspace, organization] of [
      [adminA, workspaceA, a],
      [adminB, workspaceB, b],
    ] as const) {
      const me = await apiCall(admin, '/v1/organizations/me');
      expect(me.status, me.text).toBe(200);
      expect(me.json<{ id: string }>().id).toBe(organization.organizationId);
      const ownWorkspace = await apiCall(admin, `/v1/organizations/workspaces/${workspace.id}`);
      expect(ownWorkspace.status, ownWorkspace.text).toBe(200);
    }
  });

  afterAll(async () => {
    // Report cleanup as an additional hook failure; never replace the original
    // test error or stop cleanup of the other resources after one failed delete.
    const errors: unknown[] = [];
    async function cleanup(action: () => Promise<void>) {
      try {
        await action();
      } catch (error) {
        errors.push(error);
      }
    }
    for (const price of [...createdPrices.values()]) {
      await cleanup(() => deletePrice(price.admin, price.modelId, price.provider));
    }
    for (const { admin, workspace } of workspaces) {
      await cleanup(async () => {
        const response = await apiCall(
          admin,
          `/v1/organizations/workspaces/${workspace.id}/archive`,
          {
            method: 'POST',
            body: JSON.stringify({}),
          },
        );
        expect(response.status, response.text).toBe(200);
        expect(response.json<{ archived_at: string | null }>().archived_at).toEqual(
          expect.any(String),
        );
        // Read the retired key through admin HTTP. Repeatedly presenting a
        // revoked key invokes the public legacy-key fallback rate limiter and
        // can legitimately return 429 when this suite is rerun within a minute.
        const revoked = await apiCall(admin, `/v1/organizations/api_keys/${workspace.keyId}`);
        expect(revoked.status, revoked.text).toBe(200);
        expect(revoked.json<{ status: string }>().status).toBe('archived');
      });
    }
    for (const organization of organizations) await cleanup(organization.archive);
    if (errors.length > 0) {
      const details = errors.map((error) =>
        error instanceof Error ? error.message : String(error),
      );
      throw new AggregateError(errors, `Model-price E2E cleanup failed: ${details.join('; ')}`);
    }
  });

  it('discovers pricing and serves its reads only on the public listener', async () => {
    const groups = await apiCall(workspaceA.cfg, '/apis');
    expect(groups.status, groups.text).toBe(200);
    expect(groups.json<{ groups: unknown[] }>().groups).toContainEqual({
      name: 'pricing.runorca.ai',
      versions: [{ group_version: 'pricing.runorca.ai/v1', version: 'v1' }],
      preferred_version: { group_version: 'pricing.runorca.ai/v1', version: 'v1' },
    });
    const resources = await apiCall(workspaceA.cfg, '/apis/pricing.runorca.ai/v1');
    expect(resources.status, resources.text).toBe(200);
    expect(resources.json()).toEqual({
      kind: 'APIResourceList',
      group_version: 'pricing.runorca.ai/v1',
      resources: [{ name: 'modelprices', namespaced: false, kind: 'ModelPrice' }],
    });
    const prices = await apiCall(workspaceA.cfg, PRICES);
    expect(prices.status, prices.text).toBe(200);
    expect(prices.json<PublicPricePage>().data.length).toBeGreaterThan(0);
    const publicAdminRoute = await apiCall(workspaceA.cfg, ADMIN_PRICES);
    expect(publicAdminRoute.status, publicAdminRoute.text).toBe(404);
    const adminPublicRoute = await apiCall(adminA, PRICES);
    expect(adminPublicRoute.status, adminPublicRoute.text).toBe(404);
  });

  it('keeps admin credentials, workspace credentials, and missing credentials separate', async () => {
    const adminOnPublic = buildClientFromConfig({ baseURL: publicBaseURL, apiKey: adminA.apiKey });
    const workspaceOnAdmin = buildClientFromConfig({
      baseURL: adminBaseURL,
      apiKey: workspaceA.cfg.apiKey,
    });
    for (const [cfg, path] of [
      [adminOnPublic, PRICES],
      [workspaceOnAdmin, ADMIN_PRICES],
    ] as const) {
      const response = await apiCall(cfg, path);
      expect(response.status, response.text).toBe(401);
    }
    for (const [cfg, path] of [
      [workspaceA.cfg, PRICES],
      [adminA, ADMIN_PRICES],
    ] as const) {
      const response = await fetch(`${cfg.baseURL}${path}`);
      expect(response.status, await response.text()).toBe(401);
    }
  });

  it('requires pricing scopes for every admin read and mutation', async () => {
    const modelId = `scope-${runId}`;
    const created = await createPrice(adminA, {
      model_id: modelId,
      input_per_million_tokens: 8,
      output_per_million_tokens: 24,
    });
    expect(created.status, created.text).toBe(201);
    const identity = await apiCall(limitedAdmin, '/v1/organizations/me');
    expect(identity.status, identity.text).toBe(200);
    for (const [method, path, scope, body] of [
      ['GET', ADMIN_PRICES, 'read', undefined],
      ['GET', itemPath(ADMIN_PRICES, modelId), 'read', undefined],
      [
        'POST',
        ADMIN_PRICES,
        'write',
        {
          model_id: `forbidden-${runId}`,
          input_per_million_tokens: 1,
          output_per_million_tokens: 2,
        },
      ],
      ['PATCH', itemPath(ADMIN_PRICES, modelId), 'write', { input_per_million_tokens: 99 }],
      ['DELETE', itemPath(ADMIN_PRICES, modelId), 'write', undefined],
    ] as const) {
      const response = await apiCall(limitedAdmin, path, {
        method,
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      });
      expect(response.status, response.text).toBe(403);
      expect(response.json()).toEqual({ error: `missing required scope: model_prices:${scope}` });
    }
    expect(
      assertPublicPrice(await apiCall(workspaceA.cfg, itemPath(PRICES, modelId)))
        .input_per_million_tokens,
    ).toBe(8);
    const deniedCreate = await apiCall(adminA, itemPath(ADMIN_PRICES, `forbidden-${runId}`));
    expect(deniedCreate.status, deniedCreate.text).toBe(404);
  });

  it('round-trips an operator entry while exposing only the effective public quote', async () => {
    const modelId = `crud-${runId}`;
    const created = await createPrice(adminA, {
      model_id: modelId,
      source: 'seed',
      input_per_million_tokens: 8,
      output_per_million_tokens: 24,
    });
    expect(created.status, created.text).toBe(201);
    const entry = created.json<ModelPriceEntry>();
    expect(entry).toEqual({
      type: 'model_price_entry',
      provider: 'anthropic',
      model_id: modelId,
      source: 'operator',
      input_per_million_tokens: 8,
      output_per_million_tokens: 24,
      cache_read_per_million_tokens: null,
      cache_write_per_million_tokens: null,
      fetched_at: null,
      created_at: expect.any(String),
      updated_at: expect.any(String),
    });
    expect(Number.isFinite(Date.parse(entry.created_at))).toBe(true);
    expect(Number.isFinite(Date.parse(entry.updated_at))).toBe(true);
    const read = await apiCall(adminA, itemPath(ADMIN_PRICES, modelId));
    expect(read.status, read.text).toBe(200);
    expect(read.json()).toEqual(entry);
    expect(assertPublicPrice(await apiCall(workspaceA.cfg, `${PRICES}/${modelId}`))).toEqual({
      type: 'model_price',
      provider: 'anthropic',
      model_id: modelId,
      input_per_million_tokens: 8,
      output_per_million_tokens: 24,
      cache_read_per_million_tokens: 0.8,
      cache_write_per_million_tokens: 10,
    });
    const patched = await apiCall(adminA, itemPath(ADMIN_PRICES, modelId), {
      method: 'PATCH',
      body: JSON.stringify({
        input_per_million_tokens: 12,
        provider: 'ignored-provider',
        source: 'upstream',
      }),
    });
    expect(patched.status, patched.text).toBe(200);
    expect(patched.json<ModelPriceEntry>()).toMatchObject({
      provider: 'anthropic',
      source: 'operator',
      input_per_million_tokens: 12,
      output_per_million_tokens: 24,
    });
    expect(
      assertPublicPrice(await apiCall(workspaceA.cfg, itemPath(PRICES, modelId))),
    ).toMatchObject({
      input_per_million_tokens: 12,
      output_per_million_tokens: 24,
      cache_read_per_million_tokens: 1.2,
      cache_write_per_million_tokens: 15,
    });
    await deletePrice(adminA, modelId);
    expect((await apiCall(adminA, itemPath(ADMIN_PRICES, modelId))).status).toBe(404);
    const unpriced = await apiCall(workspaceA.cfg, itemPath(PRICES, modelId));
    expect(unpriced.status, unpriced.text).toBe(404);
    expect(unpriced.json()).toMatchObject({ type: 'error', error: { type: 'not_found_error' } });
  });

  it('leaves POST, PATCH, and DELETE unmounted on the public pricing API', async () => {
    const modelId = `readonly-${runId}`;
    const created = await createPrice(adminA, {
      model_id: modelId,
      input_per_million_tokens: 8,
      output_per_million_tokens: 24,
    });
    expect(created.status, created.text).toBe(201);
    for (const [method, path, body] of [
      [
        'POST',
        PRICES,
        {
          model_id: `public-write-${runId}`,
          input_per_million_tokens: 1,
          output_per_million_tokens: 2,
        },
      ],
      ['PATCH', itemPath(PRICES, modelId), { input_per_million_tokens: 1 }],
      ['DELETE', itemPath(PRICES, modelId), undefined],
    ] as const) {
      const response = await apiCall(workspaceA.cfg, path, {
        method,
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      });
      expect(response.status, response.text).toBe(404);
    }
    expect(
      assertPublicPrice(await apiCall(workspaceA.cfg, itemPath(PRICES, modelId)))
        .input_per_million_tokens,
    ).toBe(8);
    expect((await apiCall(adminA, itemPath(ADMIN_PRICES, `public-write-${runId}`))).status).toBe(
      404,
    );
  });

  it('rejects duplicate creates without changing the first operator entry', async () => {
    const body = {
      model_id: `duplicate-${runId}`,
      input_per_million_tokens: 8,
      output_per_million_tokens: 24,
    };
    const first = await createPrice(adminA, body);
    expect(first.status, first.text).toBe(201);
    // Pricing has no admin idempotency middleware. A duplicate is a conflict,
    // so pin that actual contract instead of expecting cached replay behavior.
    for (const input of [8, 99]) {
      const duplicate = await createPrice(adminA, { ...body, input_per_million_tokens: input });
      expect(duplicate.status, duplicate.text).toBe(409);
    }
    const read = await apiCall(adminA, itemPath(ADMIN_PRICES, body.model_id));
    expect(read.status, read.text).toBe(200);
    expect(read.json()).toEqual(first.json());
  });

  it('isolates operator prices across two actual organizations', async () => {
    const modelId = `isolation-${runId}`;
    const a = await createPrice(adminA, {
      model_id: modelId,
      input_per_million_tokens: 8,
      output_per_million_tokens: 24,
    });
    expect(a.status, a.text).toBe(201);
    for (const [method, body] of [
      ['GET', undefined],
      ['PATCH', { input_per_million_tokens: 99 }],
      ['DELETE', undefined],
    ] as const) {
      const response = await apiCall(adminB, itemPath(ADMIN_PRICES, modelId), {
        method,
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      });
      expect(response.status, response.text).toBe(404);
    }
    expect((await apiCall(workspaceB.cfg, itemPath(PRICES, modelId))).status).toBe(404);
    const foreignCursor = await apiCall(
      workspaceB.cfg,
      `${PRICES}?limit=1&page=${encodeURIComponent(`anthropic/${modelId}`)}`,
    );
    expect(foreignCursor.status, foreignCursor.text).toBe(400);
    const listed = await apiCall(adminB, `${ADMIN_PRICES}?source=operator`);
    expect(listed.status, listed.text).toBe(200);
    expect(listed.json<AdminPricePage>().data).toEqual([]);
    const b = await createPrice(adminB, {
      model_id: modelId,
      input_per_million_tokens: 3,
      output_per_million_tokens: 9,
    });
    expect(b.status, b.text).toBe(201);
    expect(
      assertPublicPrice(await apiCall(workspaceA.cfg, itemPath(PRICES, modelId)))
        .input_per_million_tokens,
    ).toBe(8);
    expect(
      assertPublicPrice(await apiCall(workspaceB.cfg, itemPath(PRICES, modelId)))
        .input_per_million_tokens,
    ).toBe(3);
    await deletePrice(adminB, modelId);
    expect((await apiCall(workspaceB.cfg, itemPath(PRICES, modelId))).status).toBe(404);
    expect(
      assertPublicPrice(await apiCall(workspaceA.cfg, itemPath(PRICES, modelId)))
        .input_per_million_tokens,
    ).toBe(8);
  });

  it('paginates full provider/model identities without collapsing the same model id', async () => {
    const modelId = `same-model-${runId}`;
    const providers = [`e2e-${runId}-a`, `e2e-${runId}-b`, `e2e-${runId}-c`];
    for (const [index, provider] of providers.entries()) {
      const created = await createPrice(adminA, {
        provider,
        model_id: modelId,
        input_per_million_tokens: index + 1,
        output_per_million_tokens: (index + 1) * 3,
      });
      expect(created.status, created.text).toBe(201);
      expect(
        assertPublicPrice(await apiCall(workspaceA.cfg, itemPath(PRICES, modelId, provider)))
          .input_per_million_tokens,
      ).toBe(index + 1);
    }
    // Anchor at a row created by this test; deployment catalogs may contain
    // additional global models. Adjacent unique provider prefixes stay together.
    let publicCursor = `${providers[0]}/${modelId}`;
    let adminCursor = `${providers[0]} ${modelId} operator`;
    for (const [index, provider] of providers.slice(1).entries()) {
      const publicPage = await apiCall(
        workspaceA.cfg,
        `${PRICES}?limit=1&page=${encodeURIComponent(publicCursor)}`,
      );
      expect(publicPage.status, publicPage.text).toBe(200);
      const effective = publicPage.json<PublicPricePage>();
      expect(effective.data).toHaveLength(1);
      expect(effective.data[0]).toMatchObject({
        provider,
        model_id: modelId,
        input_per_million_tokens: index + 2,
      });
      if (index === 0) expect(effective.next_page).toBe(`${provider}/${modelId}`);
      publicCursor = `${provider}/${modelId}`;
      const adminPage = await apiCall(
        adminA,
        `${ADMIN_PRICES}?source=operator&limit=1&after_id=${encodeURIComponent(adminCursor)}`,
      );
      expect(adminPage.status, adminPage.text).toBe(200);
      const entries = adminPage.json<AdminPricePage>();
      expect(entries.data).toHaveLength(1);
      expect(entries.data[0]).toMatchObject({ provider, model_id: modelId, source: 'operator' });
      expect(entries.first_id).toBe(`${provider} ${modelId} operator`);
      expect(entries.last_id).toBe(entries.first_id);
      if (index === 0) expect(entries.has_more).toBe(true);
      adminCursor = entries.last_id!;
    }
    expect((await apiCall(workspaceA.cfg, itemPath(PRICES, modelId))).status).toBe(404);
    const invalidPage = await apiCall(
      workspaceA.cfg,
      `${PRICES}?page=${encodeURIComponent(`missing-${runId}`)}`,
    );
    expect(invalidPage.status, invalidPage.text).toBe(400);
  });

  it('restores the existing catalog price after deleting a known model override', async () => {
    const modelId = 'claude-sonnet-4-6';
    const seeds = await apiCall(adminA, `${ADMIN_PRICES}?source=seed&limit=1000`);
    expect(seeds.status, seeds.text).toBe(200);
    expect(seeds.json<AdminPricePage>().data).toContainEqual(
      expect.objectContaining({
        provider: 'anthropic',
        model_id: modelId,
        source: 'seed',
      }),
    );
    const baseline = assertPublicPrice(await apiCall(workspaceA.cfg, itemPath(PRICES, modelId)));
    const created = await createPrice(adminA, {
      model_id: modelId,
      input_per_million_tokens: 123,
      output_per_million_tokens: 456,
    });
    expect(created.status, created.text).toBe(201);
    expect(
      assertPublicPrice(await apiCall(workspaceA.cfg, itemPath(PRICES, modelId)))
        .input_per_million_tokens,
    ).toBe(123);
    expect(assertPublicPrice(await apiCall(workspaceB.cfg, itemPath(PRICES, modelId)))).toEqual(
      baseline,
    );
    await deletePrice(adminA, modelId);
    expect(assertPublicPrice(await apiCall(workspaceA.cfg, itemPath(PRICES, modelId)))).toEqual(
      baseline,
    );
    expect((await apiCall(adminA, itemPath(ADMIN_PRICES, modelId))).status).toBe(404);
  });

  it('rejects invalid base rates and unaddressable model/provider identifiers', async () => {
    for (const [index, invalid] of [
      { input_per_million_tokens: 0 },
      { output_per_million_tokens: -1 },
      { input_per_million_tokens: 0.0001 },
      { model_id: 'bad/model' },
      { provider: 'bad/provider' },
    ].entries()) {
      const response = await createPrice(adminA, {
        model_id: `invalid-${index}-${runId}`,
        input_per_million_tokens: 8,
        output_per_million_tokens: 24,
        ...invalid,
      });
      expect(response.status, response.text).toBe(400);
    }
    const modelId = `validation-${runId}`;
    const created = await createPrice(adminA, {
      model_id: modelId,
      input_per_million_tokens: 8,
      output_per_million_tokens: 24,
    });
    expect(created.status, created.text).toBe(201);
    for (const rate of [0, -1, 0.0001]) {
      const response = await apiCall(adminA, itemPath(ADMIN_PRICES, modelId), {
        method: 'PATCH',
        body: JSON.stringify({ input_per_million_tokens: rate }),
      });
      expect(response.status, response.text).toBe(400);
    }
    const invalidProvider = await apiCall(
      workspaceA.cfg,
      itemPath(PRICES, modelId, 'bad/provider'),
    );
    expect(invalidProvider.status, invalidProvider.text).toBe(400);
    expect(invalidProvider.json()).toMatchObject({
      type: 'error',
      error: { type: 'invalid_request_error' },
    });
    const invalidDelete = await apiCall(adminA, itemPath(ADMIN_PRICES, modelId, 'bad/provider'), {
      method: 'DELETE',
    });
    expect(invalidDelete.status, invalidDelete.text).toBe(400);
    const invalidPath = await apiCall(workspaceA.cfg, `${PRICES}/bad/model`);
    expect(invalidPath.status, invalidPath.text).toBe(404);
    expect(
      assertPublicPrice(await apiCall(workspaceA.cfg, itemPath(PRICES, modelId)))
        .input_per_million_tokens,
    ).toBe(8);
  });

  it('preserves explicit free cache buckets until null resets them to derived rates', async () => {
    const modelId = `cache-${runId}`;
    const created = await createPrice(adminA, {
      model_id: modelId,
      input_per_million_tokens: 8,
      output_per_million_tokens: 24,
      cache_read_per_million_tokens: 0,
      cache_write_per_million_tokens: 0,
    });
    expect(created.status, created.text).toBe(201);
    expect(created.json<ModelPriceEntry>()).toMatchObject({
      cache_read_per_million_tokens: 0,
      cache_write_per_million_tokens: 0,
    });
    expect(
      assertPublicPrice(await apiCall(workspaceA.cfg, itemPath(PRICES, modelId))),
    ).toMatchObject({ cache_read_per_million_tokens: 0, cache_write_per_million_tokens: 0 });
    const omitted = await apiCall(adminA, itemPath(ADMIN_PRICES, modelId), {
      method: 'PATCH',
      body: JSON.stringify({ input_per_million_tokens: 12 }),
    });
    expect(omitted.status, omitted.text).toBe(200);
    expect(
      assertPublicPrice(await apiCall(workspaceA.cfg, itemPath(PRICES, modelId))),
    ).toMatchObject({ cache_read_per_million_tokens: 0, cache_write_per_million_tokens: 0 });
    const cleared = await apiCall(adminA, itemPath(ADMIN_PRICES, modelId), {
      method: 'PATCH',
      body: JSON.stringify({
        cache_read_per_million_tokens: null,
        cache_write_per_million_tokens: null,
      }),
    });
    expect(cleared.status, cleared.text).toBe(200);
    expect(cleared.json<ModelPriceEntry>()).toMatchObject({
      cache_read_per_million_tokens: null,
      cache_write_per_million_tokens: null,
    });
    expect(
      assertPublicPrice(await apiCall(workspaceA.cfg, itemPath(PRICES, modelId))),
    ).toMatchObject({
      input_per_million_tokens: 12,
      output_per_million_tokens: 24,
      cache_read_per_million_tokens: 1.2,
      cache_write_per_million_tokens: 15,
    });
  });
});
