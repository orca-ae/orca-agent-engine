// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll, vi } from 'vitest';
import { eq } from 'drizzle-orm';
import { newId } from '../../src/domain/versioning.js';
import { createSessionInTransaction } from '../../src/domain/session-creation.js';
import { environments, sessions } from '../../src/persistence/postgres/schema.js';
import type { FastifyInstance } from 'fastify';
import {
  getTestDb,
  closeTestDb,
  buildStubStore,
  STUB_SSE_CONFIG,
  buildTestJwtMinter,
  buildStubFileStore,
} from './setup.js';
import { uniqueWorkspace, createTestApiKey } from './fixtures.js';
import { buildCombinedTestApp } from '../../src/server.js';

const apiPackages = (
  packages: Partial<Record<'apt' | 'cargo' | 'gem' | 'go' | 'npm' | 'pip', string[]>>,
) => ({
  type: 'packages',
  apt: packages.apt ?? [],
  cargo: packages.cargo ?? [],
  gem: packages.gem ?? [],
  go: packages.go ?? [],
  npm: packages.npm ?? [],
  pip: packages.pip ?? [],
});

describe('Environments CRUD (integration)', () => {
  let app: FastifyInstance;
  let apiKey: string;
  let routeDb: Awaited<ReturnType<typeof getTestDb>>['db'];
  const workspaceId = uniqueWorkspace('env');

  beforeAll(async () => {
    const { db } = await getTestDb();
    routeDb = db;
    app = buildCombinedTestApp({
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store: buildStubStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
    });
    await app.ready();
    apiKey = await createTestApiKey(db, workspaceId);
  });
  afterAll(async () => {
    await app.close();
    await closeTestDb();
  });

  /**
   * `orcaBeta` is not decoration. The default (non-`orca-beta`) environment
   * response is Anthropic's `BetaEnvironment` projection and nothing else —
   * pinned by the very next test, which is the compatibility contract for
   * this resource. Every Orca-only field (`env_key`, `env_key_set`,
   * `env_key_expires_at`, `egress_mode`, `llm`, and
   * the legacy flat `packages`/`networking`/`image`/`target`) is emitted by
   * `toApi` only on the beta branch, so a test asserting on one must opt in
   * here.
   */
  const jsonHeaders = (orcaBeta = false) => ({
    'x-api-key': apiKey,
    'content-type': 'application/json',
    ...(orcaBeta ? { 'orca-beta': '1' } : {}),
  });

  async function codexEnvironment() {
    const agent = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: jsonHeaders(),
      payload: {
        name: `codex-${Date.now()}`,
        model: { provider: 'openai', id: 'gpt-5.4' },
        metadata: { harness: 'codex_sdk' },
        tools: [],
      },
    });
    expect(agent.statusCode).toBe(200);
    const environment = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: jsonHeaders(),
      payload: { name: `codex-env-${Date.now()}`, config: { type: 'cloud' } },
    });
    expect(environment.statusCode).toBe(200);
    return { agentId: agent.json().id as string, environmentId: environment.json().id as string };
  }

  it('serializes a target update behind an in-flight Session creation', async () => {
    const { db } = await getTestDb();
    const { agentId, environmentId } = await codexEnvironment();
    let release!: () => void;
    const held = new Promise<void>((resolve) => {
      release = resolve;
    });
    let inserted = false;
    const creation = db.transaction(async (tx) => {
      const result = await createSessionInTransaction(tx, {
        id: newId('ses'),
        workspaceId,
        agentId,
        agentVersion: 1,
        environmentId,
        title: null,
        metadata: {},
        vaultIds: [],
        tools: null,
        mcpServers: null,
        agentOverrides: null,
        initialEvents: [],
        now: new Date(),
      });
      expect(result.ok).toBe(true);
      inserted = true;
      await held;
    });
    let update: Promise<Awaited<ReturnType<typeof app.inject>>> | undefined;
    try {
      await vi.waitFor(() => expect(inserted).toBe(true));
      let settled = false;
      update = app
        .inject({
          method: 'POST',
          url: `/v1/environments/${environmentId}`,
          headers: jsonHeaders(),
          payload: { config: { type: 'self_hosted' } },
        })
        .then((response) => {
          settled = true;
          return response;
        });
      await new Promise((resolve) => setTimeout(resolve, 25));
      expect(settled).toBe(false);
      release();
      await creation;
      expect((await update).statusCode).toBe(400);
      const [row] = await db.select().from(environments).where(eq(environments.id, environmentId));
      expect(row!.target).toBe('cloud');
    } finally {
      release();
      await creation;
      await update;
    }
  });

  it('rechecks target admission when an Environment update commits after Session preflight', async () => {
    const { db, pool } = await getTestDb();
    const { agentId, environmentId } = await codexEnvironment();
    const connection = await pool.connect();
    const transaction = vi.spyOn(routeDb, 'transaction');
    let pending: Promise<unknown> | undefined;
    try {
      await connection.query('BEGIN');
      await connection.query("UPDATE environments SET target = 'self_hosted' WHERE id = $1", [
        environmentId,
      ]);
      const request = app
        .inject({
          method: 'POST',
          url: '/v1/sessions',
          headers: jsonHeaders(),
          payload: { agent_id: agentId, environment_id: environmentId },
        })
        .then((response) => response);
      pending = request;
      await vi.waitFor(() => expect(transaction).toHaveBeenCalled());
      await connection.query('COMMIT');
      expect((await request).statusCode).toBe(400);
      expect(
        await db.select().from(sessions).where(eq(sessions.environmentId, environmentId)),
      ).toEqual([]);
    } finally {
      await connection.query('ROLLBACK');
      connection.release();
      await pending;
      transaction.mockRestore();
    }
  });

  it('uses the canonical Claude projection for a minimal create', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: jsonHeaders(),
      payload: { name: `claude-minimal-${Date.now()}` },
    });
    expect(res.statusCode).toBe(200);
    expect(res.json()).toEqual({
      id: expect.stringMatching(/^env_/),
      type: 'environment',
      name: expect.stringMatching(/^claude-minimal-/),
      description: '',
      metadata: {},
      config: {
        type: 'cloud',
        networking: { type: 'unrestricted' },
        packages: apiPackages({}),
      },
      archived_at: null,
      created_at: expect.any(String),
      updated_at: expect.any(String),
    });
  });

  it('supports Claude config null/reset, scope, metadata deletion, and nullable package managers', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        name: `claude-self-hosted-${Date.now()}`,
        config: { type: 'self_hosted' },
        scope: 'account',
        metadata: { remove_null: 'x', remove_empty: 'y', keep: 'old' },
      },
    });
    expect(create.statusCode).toBe(200);
    expect(create.json()).toMatchObject({
      config: { type: 'self_hosted' },
      scope: 'account',
      description: '',
    });

    const update = await app.inject({
      method: 'POST',
      url: `/v1/environments/${create.json().id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        config: null,
        scope: null,
        metadata: { remove_null: null, remove_empty: '', keep: 'new' },
      },
    });
    expect(update.statusCode).toBe(200);
    expect(update.json()).toMatchObject({
      config: {
        type: 'cloud',
        networking: { type: 'unrestricted' },
        packages: apiPackages({}),
      },
      metadata: { keep: 'new' },
      description: '',
    });
    expect(update.json()).not.toHaveProperty('scope');

    const packageReset = await app.inject({
      method: 'POST',
      url: `/v1/environments/${create.json().id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { config: { type: 'cloud', packages: { apt: null, npm: ['tsx'] } } },
    });
    expect(packageReset.statusCode).toBe(200);
    expect(packageReset.json().config.packages).toEqual(apiPackages({ npm: ['tsx'] }));
  });

  it('POST /v1/environments creates an env', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: jsonHeaders(true),
      payload: {
        name: `e-${Date.now()}`,
        packages: ['curl', 'jq'],
        networking: { allowed_hosts: ['*'] },
      },
    });
    expect(res.statusCode).toBe(200);
    const env = res.json();
    expect(env.id).toMatch(/^env_/);
    expect(env.type).toBe('environment');
    expect(env.packages).toEqual(['curl', 'jq']);
    expect(env.config).toEqual({
      type: 'cloud',
      networking: { type: 'unrestricted' },
      packages: apiPackages({ apt: ['curl', 'jq'] }),
    });
  });

  it('returns conflict_error for duplicate create and rename', async () => {
    const firstName = `duplicate-env-${Date.now()}`;
    const secondName = `${firstName}-other`;
    const first = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: jsonHeaders(),
      payload: { name: firstName },
    });
    const second = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: jsonHeaders(),
      payload: { name: secondName },
    });
    expect(first.statusCode).toBe(200);
    expect(second.statusCode).toBe(200);

    const duplicate = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: jsonHeaders(),
      payload: { name: firstName },
    });
    expect(duplicate.statusCode).toBe(409);
    expect(duplicate.json()).toMatchObject({
      type: 'error',
      error: {
        type: 'conflict_error',
        message: 'environment name is already in use',
      },
      request_id: expect.any(String),
    });

    const rename = await app.inject({
      method: 'POST',
      url: `/v1/environments/${second.json().id}`,
      headers: jsonHeaders(),
      payload: { name: firstName },
    });
    expect(rename.statusCode).toBe(409);
    expect(rename.json().error).toEqual({
      type: 'conflict_error',
      message: 'environment name is already in use',
    });

    const getSecond = await app.inject({
      method: 'GET',
      url: `/v1/environments/${second.json().id}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(getSecond.statusCode).toBe(200);
    expect(getSecond.json().name).toBe(secondName);
  });

  it('POST /v1/environments accepts config.packages and keeps legacy packages apt-only', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: jsonHeaders(true),
      payload: {
        name: `cfg-${Date.now()}`,
        config: { packages: { type: 'packages', apt: ['curl'], npm: ['typescript'], pip: [] } },
        packages: ['legacy-ignored'],
      },
    });
    expect(res.statusCode).toBe(200);
    const env = res.json();
    expect(env.config).toEqual({
      type: 'cloud',
      networking: { type: 'unrestricted' },
      packages: apiPackages({ apt: ['curl'], npm: ['typescript'] }),
    });
    expect(env.packages).toEqual(['curl']);
  });

  it('POST /v1/environments accepts cloud config packages without networking', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        name: `cfg-cloud-${Date.now()}`,
        config: { type: 'cloud', packages: { pip: ['pandas'] } },
      },
    });
    expect(res.statusCode).toBe(200);
    expect(res.json().config).toEqual({
      type: 'cloud',
      networking: { type: 'unrestricted' },
      packages: apiPackages({ pip: ['pandas'] }),
    });
  });

  it('POST /v1/environments rejects self-hosted package config', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        name: `cfg-self-hosted-pkg-${Date.now()}`,
        target: 'self_hosted',
        packages: ['curl'],
      },
    });
    expect(res.statusCode).toBe(400);
    expect(res.json().error.message).toMatch(/cloud environments/);
  });

  it('POST /v1/environments rejects unknown package managers', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { name: `bad-pkg-${Date.now()}`, config: { packages: { brew: ['curl'] } } },
    });
    expect(res.statusCode).toBe(400);
    expect(res.json().error.message).toMatch(/Unrecognized key/);
  });

  it('POST /v1/environments persists description and metadata', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        name: `meta-${Date.now()}`,
        description: 'Build environment',
        metadata: { owner: 'platform' },
      },
    });
    expect(create.statusCode).toBe(200);
    expect(create.json().description).toBe('Build environment');
    expect(create.json().metadata).toEqual({ owner: 'platform' });

    const update = await app.inject({
      method: 'POST',
      url: `/v1/environments/${create.json().id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        description: null,
        metadata: { owner: 'runtime', added: 'yes' },
      },
    });
    expect(update.statusCode).toBe(200);
    expect(update.json().description).toBe('');
    expect(update.json().metadata).toEqual({ owner: 'runtime', added: 'yes' });
  });

  it('POST /v1/environments rejects descriptions beyond the managed limit', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        name: `desc-${Date.now()}`,
        description: 'd'.repeat(1025),
      },
    });
    expect(create.statusCode).toBe(400);
    expect(create.json().error.message).toMatch(/at most 1024 characters/);

    const valid = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { name: `desc-valid-${Date.now()}` },
    });
    expect(valid.statusCode).toBe(200);
    const update = await app.inject({
      method: 'POST',
      url: `/v1/environments/${valid.json().id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { description: 'd'.repeat(1025) },
    });
    expect(update.statusCode).toBe(400);
    expect(update.json().error.message).toMatch(/at most 1024 characters/);
  });

  it('POST /v1/environments rejects metadata beyond managed limits', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { name: `meta-limit-${Date.now()}`, metadata: metadataPairs(17) },
    });
    expect(create.statusCode).toBe(400);
    expect(create.json().error.message).toMatch(/at most 16 pairs/);

    const valid = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { name: `meta-valid-${Date.now()}`, metadata: metadataPairs(16) },
    });
    expect(valid.statusCode).toBe(200);
    const update = await app.inject({
      method: 'POST',
      url: `/v1/environments/${valid.json().id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { metadata: { extra: 'value' } },
    });
    expect(update.statusCode).toBe(400);
    expect(update.json().error.message).toMatch(/at most 16 pairs/);
  });

  it('GET /v1/environments/:id', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { name: `g-${Date.now()}` },
    });
    const id = create.json().id;
    const get = await app.inject({
      method: 'GET',
      url: `/v1/environments/${id}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(get.statusCode).toBe(200);
    expect(get.json().id).toBe(id);
  });

  it('GET /v1/environments returns the Claude list envelope only', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { name: `list-${Date.now()}` },
    });
    expect(create.statusCode).toBe(200);

    const list = await app.inject({
      method: 'GET',
      url: '/v1/environments',
      headers: { 'x-api-key': apiKey },
    });
    expect(list.statusCode).toBe(200);
    const body = list.json() as { data: Array<{ id: string }>; next_page: string | null };
    expect(body.data.map((env) => env.id)).toContain(create.json().id);
    expect(body.next_page).toBeNull();
    expect(body).not.toHaveProperty('environments');
    expect(body).not.toHaveProperty('page_info');
  });

  it('GET /v1/environments honors limit and page cursors', async () => {
    const first = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { name: `page-first-${Date.now()}` },
    });
    expect(first.statusCode).toBe(200);
    const second = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { name: `page-second-${Date.now()}` },
    });
    expect(second.statusCode).toBe(200);

    const firstPage = await app.inject({
      method: 'GET',
      url: '/v1/environments?limit=1',
      headers: { 'x-api-key': apiKey },
    });
    expect(firstPage.statusCode).toBe(200);
    const firstPageBody = firstPage.json() as {
      data: Array<{ id: string }>;
      next_page: string | null;
    };
    expect(firstPageBody.data).toHaveLength(1);
    expect(firstPageBody.next_page).toBeTruthy();

    const secondPage = await app.inject({
      method: 'GET',
      url: `/v1/environments?limit=1&page=${encodeURIComponent(firstPageBody.next_page!)}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(secondPage.statusCode).toBe(200);
    const secondPageBody = secondPage.json() as {
      data: Array<{ id: string }>;
      next_page: string | null;
    };
    expect(secondPageBody.data).toHaveLength(1);
    expect(secondPageBody.data[0]!.id).not.toBe(firstPageBody.data[0]!.id);
  });

  it('POST /v1/environments/:id updates in place (no version bump)', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: jsonHeaders(true),
      payload: { name: `u-${Date.now()}`, packages: ['a'] },
    });
    const id = create.json().id;
    const update = await app.inject({
      method: 'POST',
      url: `/v1/environments/${id}`,
      headers: jsonHeaders(true),
      payload: { config: { packages: { npm: ['typescript'], apt: [] } } },
    });
    expect(update.statusCode).toBe(200);
    expect(update.json().config.packages).toEqual(apiPackages({ npm: ['typescript'] }));
    expect(update.json().packages).toEqual([]);
  });

  it('POST /v1/environments/:id/archive sets archived_at', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { name: `a-${Date.now()}` },
    });
    const id = create.json().id;
    const archive = await app.inject({
      method: 'POST',
      url: `/v1/environments/${id}/archive`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {},
    });
    expect(archive.statusCode).toBe(200);
    expect(archive.json().archived_at).toBeTruthy();

    const hidden = await app.inject({
      method: 'GET',
      url: '/v1/environments',
      headers: { 'x-api-key': apiKey },
    });
    expect(hidden.json().data.map((item: { id: string }) => item.id)).not.toContain(id);
    const included = await app.inject({
      method: 'GET',
      url: '/v1/environments?include_archived=true',
      headers: { 'x-api-key': apiKey },
    });
    expect(included.json().data.map((item: { id: string }) => item.id)).toContain(id);
  });

  it('POST /v1/environments persists image + target and GET round-trips them', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: jsonHeaders(true),
      payload: {
        name: `img-${Date.now()}`,
        image: 'ghcr.io/x:1',
        target: 'cloud',
      },
    });
    expect(res.statusCode).toBe(200);
    const env = res.json();
    expect(env.image).toBe('ghcr.io/x:1');
    expect(env.target).toBe('cloud');

    const get = await app.inject({
      method: 'GET',
      url: `/v1/environments/${env.id}`,
      headers: { 'x-api-key': apiKey, 'orca-beta': '1' },
    });
    expect(get.statusCode).toBe(200);
    expect(get.json().image).toBe('ghcr.io/x:1');
    expect(get.json().target).toBe('cloud');
  });

  it('POST /v1/environments defaults the legacy target to cloud', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: jsonHeaders(true),
      payload: { name: `no-img-${Date.now()}` },
    });
    expect(res.statusCode).toBe(200);
    const env = res.json();
    expect(env.image).toBeNull();
    expect(env.target).toBe('cloud');
  });

  it('POST /v1/environments/:id preserves image/target when updating an unrelated field', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: jsonHeaders(true),
      payload: { name: `pres-${Date.now()}`, image: 'ghcr.io/x:1', target: 'cloud' },
    });
    expect(create.statusCode).toBe(200);
    const id = create.json().id;

    // Update only `packages` — image/target must be preserved.
    const update1 = await app.inject({
      method: 'POST',
      url: `/v1/environments/${id}`,
      headers: jsonHeaders(true),
      payload: { packages: ['curl'] },
    });
    expect(update1.statusCode).toBe(200);
    expect(update1.json().packages).toEqual(['curl']);
    expect(update1.json().image).toBe('ghcr.io/x:1');
    expect(update1.json().target).toBe('cloud');

    // Now update `image` to a new value — it must change.
    const update2 = await app.inject({
      method: 'POST',
      url: `/v1/environments/${id}`,
      headers: jsonHeaders(true),
      payload: { image: 'ghcr.io/x:2' },
    });
    expect(update2.statusCode).toBe(200);
    expect(update2.json().image).toBe('ghcr.io/x:2');
    expect(update2.json().target).toBe('cloud');
  });

  it('POST /v1/environments rejects an invalid target enum with 400', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { name: `bad-target-${Date.now()}`, target: 'CLOUD' },
    });
    expect(res.statusCode).toBe(400);
    expect(res.json().error.message).toMatch(/target/);
  });

  it('POST /v1/environments returns an sk- env key once, never echoed on reads', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: jsonHeaders(true),
      payload: { name: `key-${Date.now()}` },
    });
    expect(create.statusCode).toBe(200);
    const created = create.json();
    // The raw key is returned exactly once, prefixed `sk-`.
    expect(created.env_key).toMatch(/^sk-[A-Za-z0-9_-]{32,}$/);
    // Its presence + expiry are reported, but never the raw key itself.
    expect(created.env_key_set).toBe(true);
    expect(created.env_key_expires_at).toBeTruthy();
    expect(new Date(created.env_key_expires_at).getTime()).toBeGreaterThan(Date.now());

    // GET must not leak the raw key, but must report it is armed.
    const get = await app.inject({
      method: 'GET',
      url: `/v1/environments/${created.id}`,
      headers: jsonHeaders(true),
    });
    expect(get.statusCode).toBe(200);
    const fetched = get.json();
    expect(fetched.env_key).toBeUndefined();
    expect(fetched.env_key_set).toBe(true);
    expect(fetched.env_key_expires_at).toBe(created.env_key_expires_at);
  });

  it('POST /v1/environments/:id/rotate-key issues a fresh key and bumps the expiry', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: jsonHeaders(true),
      payload: { name: `rot-${Date.now()}` },
    });
    const created = create.json();
    const firstKey = created.env_key as string;

    const rotate = await app.inject({
      method: 'POST',
      url: `/v1/environments/${created.id}/rotate-key`,
      headers: jsonHeaders(true),
      payload: {},
    });
    expect(rotate.statusCode).toBe(200);
    const rotated = rotate.json();
    // A genuinely new key, still `sk-` shaped, with a future expiry.
    expect(rotated.env_key).toMatch(/^sk-[A-Za-z0-9_-]{32,}$/);
    expect(rotated.env_key).not.toBe(firstKey);
    expect(new Date(rotated.env_key_expires_at).getTime()).toBeGreaterThan(Date.now());

    // The rotated key's expiry is reflected on subsequent reads.
    const get = await app.inject({
      method: 'GET',
      url: `/v1/environments/${created.id}`,
      headers: jsonHeaders(true),
    });
    expect(get.json().env_key_set).toBe(true);
    expect(get.json().env_key_expires_at).toBe(rotated.env_key_expires_at);
  });

  it('POST /v1/environments/:id/rotate-key 404s for an unknown environment', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/environments/env_doesnotexist00000/rotate-key',
      headers: jsonHeaders(true),
      payload: {},
    });
    expect(res.statusCode).toBe(404);
  });

  it('POST /v1/environments/:id/revoke-key clears the key (env_key_set false) and is idempotent', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: jsonHeaders(true),
      payload: { name: `rev-${Date.now()}` },
    });
    const created = create.json();
    expect(created.env_key_set).toBe(true);

    const revoke = await app.inject({
      method: 'POST',
      url: `/v1/environments/${created.id}/revoke-key`,
      headers: jsonHeaders(true),
      payload: {},
    });
    expect(revoke.statusCode).toBe(200);
    const revoked = revoke.json();
    // The credential is cleared: no raw key echoed, presence flips off, expiry gone.
    expect(revoked.env_key).toBeUndefined();
    expect(revoked.env_key_set).toBe(false);
    expect(revoked.env_key_expires_at).toBeNull();

    // Reads reflect the unarmed state.
    const get = await app.inject({
      method: 'GET',
      url: `/v1/environments/${created.id}`,
      headers: jsonHeaders(true),
    });
    expect(get.json().env_key_set).toBe(false);
    expect(get.json().env_key_expires_at).toBeNull();

    // Revoking again is a safe no-op that still reports unarmed.
    const revokeAgain = await app.inject({
      method: 'POST',
      url: `/v1/environments/${created.id}/revoke-key`,
      headers: jsonHeaders(true),
      payload: {},
    });
    expect(revokeAgain.statusCode).toBe(200);
    expect(revokeAgain.json().env_key_set).toBe(false);
  });

  it('POST /v1/environments/:id/revoke-key 404s for an unknown environment', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/environments/env_doesnotexist00000/revoke-key',
      headers: jsonHeaders(true),
      payload: {},
    });
    expect(res.statusCode).toBe(404);
  });

  it('rotate-key after revoke re-arms a fresh key', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: jsonHeaders(true),
      payload: { name: `rev-rot-${Date.now()}` },
    });
    const created = create.json();

    await app.inject({
      method: 'POST',
      url: `/v1/environments/${created.id}/revoke-key`,
      headers: jsonHeaders(true),
      payload: {},
    });

    const rotate = await app.inject({
      method: 'POST',
      url: `/v1/environments/${created.id}/rotate-key`,
      headers: jsonHeaders(true),
      payload: {},
    });
    expect(rotate.statusCode).toBe(200);
    expect(rotate.json().env_key).toMatch(/^sk-[A-Za-z0-9_-]{32,}$/);

    const get = await app.inject({
      method: 'GET',
      url: `/v1/environments/${created.id}`,
      headers: jsonHeaders(true),
    });
    expect(get.json().env_key_set).toBe(true);
  });

  it('archive clears the env key, and rotate/revoke on an archived env 404', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: jsonHeaders(true),
      payload: { name: `arch-key-${Date.now()}` },
    });
    const created = create.json();
    expect(created.env_key_set).toBe(true);

    const archive = await app.inject({
      method: 'POST',
      url: `/v1/environments/${created.id}/archive`,
      headers: jsonHeaders(true),
      payload: {},
    });
    expect(archive.statusCode).toBe(200);
    // Teardown revokes the credential: an archived env never retains a live key.
    expect(archive.json().archived_at).toBeTruthy();
    expect(archive.json().env_key_set).toBe(false);
    expect(archive.json().env_key_expires_at).toBeNull();

    // rotate-key / revoke-key treat an archived env as gone.
    const rotate = await app.inject({
      method: 'POST',
      url: `/v1/environments/${created.id}/rotate-key`,
      headers: jsonHeaders(true),
      payload: {},
    });
    expect(rotate.statusCode).toBe(404);

    const revoke = await app.inject({
      method: 'POST',
      url: `/v1/environments/${created.id}/revoke-key`,
      headers: jsonHeaders(true),
      payload: {},
    });
    expect(revoke.statusCode).toBe(404);
  });

  it('POST /v1/environments persists egress_mode + llm and GET round-trips them', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: jsonHeaders(true),
      payload: {
        name: `egr-${Date.now()}`,
        egress_mode: 'sidecar',
        llm: { provider: 'anthropic', model: 'claude' },
      },
    });
    expect(res.statusCode).toBe(200);
    const env = res.json();
    expect(env.egress_mode).toBe('sidecar');
    expect(env.llm).toEqual({ provider: 'anthropic', model: 'claude' });

    const get = await app.inject({
      method: 'GET',
      url: `/v1/environments/${env.id}`,
      headers: jsonHeaders(true),
    });
    expect(get.json().egress_mode).toBe('sidecar');
    expect(get.json().llm).toEqual({ provider: 'anthropic', model: 'claude' });
  });

  it('POST /v1/environments without egress_mode/llm yields null', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: jsonHeaders(true),
      payload: { name: `no-egr-${Date.now()}` },
    });
    expect(res.statusCode).toBe(200);
    const env = res.json();
    expect(env.egress_mode).toBeNull();
    expect(env.llm).toBeNull();
  });

  it('POST /v1/environments/:id preserves egress_mode/llm when updating an unrelated field', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: jsonHeaders(true),
      payload: { name: `egr-pres-${Date.now()}`, egress_mode: 'gateway', llm: { a: 1 } },
    });
    const id = create.json().id;

    const update1 = await app.inject({
      method: 'POST',
      url: `/v1/environments/${id}`,
      headers: jsonHeaders(true),
      payload: { packages: ['curl'] },
    });
    expect(update1.statusCode).toBe(200);
    expect(update1.json().egress_mode).toBe('gateway');
    expect(update1.json().llm).toEqual({ a: 1 });

    const update2 = await app.inject({
      method: 'POST',
      url: `/v1/environments/${id}`,
      headers: jsonHeaders(true),
      payload: { egress_mode: 'sidecar' },
    });
    expect(update2.statusCode).toBe(200);
    expect(update2.json().egress_mode).toBe('sidecar');
    expect(update2.json().llm).toEqual({ a: 1 });
  });

  it('POST /v1/environments rejects an invalid egress_mode with 400', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { name: `bad-egr-${Date.now()}`, egress_mode: 'proxy' },
    });
    expect(res.statusCode).toBe(400);
    expect(res.json().error.message).toMatch(/egress_mode/);
  });

  it('accepts a nested cloud config{type, networking:limited} and round-trips config + flat fields', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: jsonHeaders(true),
      payload: {
        name: `cfg-${Date.now()}`,
        config: {
          type: 'cloud',
          networking: { type: 'limited', allow_package_managers: true, allowed_hosts: ['a.test'] },
        },
      },
    });
    expect(create.statusCode).toBe(200);
    const env = create.json();
    expect(env.target).toBe('cloud');
    expect(env.config).toEqual({
      type: 'cloud',
      networking: {
        type: 'limited',
        allow_mcp_servers: false,
        allow_package_managers: true,
        allowed_hosts: ['a.test'],
      },
      packages: apiPackages({}),
    });

    const get = await app.inject({
      method: 'GET',
      url: `/v1/environments/${env.id}`,
      headers: { 'x-api-key': apiKey, 'orca-beta': '1' },
    });
    expect(get.json().config).toEqual(env.config);
  });

  it('round-trips Claude cloud config through create/get/list/update and preserves omitted config fields', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        name: `cfg-roundtrip-${Date.now()}`,
        config: {
          type: 'cloud',
          networking: { type: 'limited', allow_package_managers: true, allowed_hosts: ['a.test'] },
          packages: { type: 'packages', npm: ['typescript'], pip: ['pandas'] },
        },
      },
    });
    expect(create.statusCode).toBe(200);
    const id = create.json().id;
    const initialConfig = {
      type: 'cloud',
      networking: {
        type: 'limited',
        allow_mcp_servers: false,
        allow_package_managers: true,
        allowed_hosts: ['a.test'],
      },
      packages: apiPackages({ npm: ['typescript'], pip: ['pandas'] }),
    };
    expect(create.json().config).toEqual(initialConfig);

    const get = await app.inject({
      method: 'GET',
      url: `/v1/environments/${id}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(get.statusCode).toBe(200);
    expect(get.json().config).toEqual(initialConfig);

    const list = await app.inject({
      method: 'GET',
      url: '/v1/environments',
      headers: { 'x-api-key': apiKey },
    });
    expect(list.statusCode).toBe(200);
    const listed = list.json().data.find((env: { id: string }) => env.id === id);
    expect(listed?.config).toEqual(initialConfig);

    const updatePackages = await app.inject({
      method: 'POST',
      url: `/v1/environments/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        config: {
          type: 'cloud',
          packages: { type: 'packages', apt: ['curl'] },
        },
      },
    });
    expect(updatePackages.statusCode).toBe(200);
    expect(updatePackages.json().config).toEqual({
      type: 'cloud',
      networking: initialConfig.networking,
      packages: apiPackages({ apt: ['curl'] }),
    });

    const updateNetworking = await app.inject({
      method: 'POST',
      url: `/v1/environments/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        config: {
          type: 'cloud',
          networking: { type: 'limited', allow_package_managers: false, allowed_hosts: ['b.test'] },
        },
      },
    });
    expect(updateNetworking.statusCode).toBe(200);
    expect(updateNetworking.json().config).toEqual({
      type: 'cloud',
      networking: {
        type: 'limited',
        allow_mcp_servers: false,
        allow_package_managers: false,
        allowed_hosts: ['b.test'],
      },
      packages: apiPackages({ apt: ['curl'] }),
    });
  });

  it('round-trips limited networking with allow_mcp_servers: true', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        name: `cfg-mcp-${Date.now()}`,
        config: {
          type: 'cloud',
          networking: { type: 'limited', allow_mcp_servers: true, allowed_hosts: ['mcp.test'] },
        },
      },
    });
    expect(create.statusCode).toBe(200);
    const expectedNetworking = {
      type: 'limited',
      allow_mcp_servers: true,
      allow_package_managers: false,
      allowed_hosts: ['mcp.test'],
    };
    expect(create.json().config.networking).toEqual(expectedNetworking);

    const get = await app.inject({
      method: 'GET',
      url: `/v1/environments/${create.json().id}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(get.json().config.networking).toEqual(expectedNetworking);
  });

  it('update omitting allow_mcp_servers/allow_package_managers preserves existing values and replaces allowed_hosts', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        name: `cfg-merge-${Date.now()}`,
        config: {
          type: 'cloud',
          networking: {
            type: 'limited',
            allow_mcp_servers: true,
            allow_package_managers: true,
            allowed_hosts: ['a.test'],
          },
        },
      },
    });
    const id = create.json().id;

    const update = await app.inject({
      method: 'POST',
      url: `/v1/environments/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        config: { type: 'cloud', networking: { type: 'limited', allowed_hosts: ['b.test'] } },
      },
    });
    expect(update.statusCode).toBe(200);
    expect(update.json().config.networking).toEqual({
      type: 'limited',
      allow_mcp_servers: true,
      allow_package_managers: true,
      allowed_hosts: ['b.test'],
    });
  });

  it('update with {type: "unrestricted"} fully replaces limited networking', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        name: `cfg-replace-${Date.now()}`,
        config: {
          type: 'cloud',
          networking: { type: 'limited', allow_mcp_servers: true, allowed_hosts: ['a.test'] },
        },
      },
    });
    const id = create.json().id;

    const update = await app.inject({
      method: 'POST',
      url: `/v1/environments/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { config: { type: 'cloud', networking: { type: 'unrestricted' } } },
    });
    expect(update.statusCode).toBe(200);
    expect(update.json().config.networking).toEqual({ type: 'unrestricted' });
  });

  it('accepts self-hosted config and emits the self-hosted union arm only', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { name: `cfg-self-hosted-${Date.now()}`, config: { type: 'self_hosted' } },
    });
    expect(create.statusCode).toBe(200);
    expect(create.json().config).toEqual({ type: 'self_hosted' });
  });

  it('drops fields from the previous config variant when switching variants', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: jsonHeaders(),
      payload: {
        name: `cfg-switch-${Date.now()}`,
        config: {
          type: 'cloud',
          packages: { npm: ['typescript'] },
        },
      },
    });
    expect(create.statusCode).toBe(200);
    expect(create.json().config.packages).toEqual(apiPackages({ npm: ['typescript'] }));

    const toSelfHosted = await app.inject({
      method: 'POST',
      url: `/v1/environments/${create.json().id}`,
      headers: jsonHeaders(),
      payload: {
        config: { type: 'self_hosted' },
        scope: 'account',
      },
    });
    expect(toSelfHosted.statusCode).toBe(200);
    expect(toSelfHosted.json()).toMatchObject({
      config: { type: 'self_hosted' },
      scope: 'account',
    });

    const toCloud = await app.inject({
      method: 'POST',
      url: `/v1/environments/${create.json().id}`,
      headers: jsonHeaders(),
      payload: { config: { type: 'cloud' } },
    });
    expect(toCloud.statusCode).toBe(200);
    expect(toCloud.json().config).toEqual({
      type: 'cloud',
      networking: { type: 'unrestricted' },
      packages: apiPackages({}),
    });
    expect(toCloud.json()).not.toHaveProperty('scope');
  });

  it('rejects a config with invalid networking with 400', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        name: `cfg-bad-${Date.now()}`,
        config: { type: 'cloud', networking: { type: 'open' } },
      },
    });
    expect(res.statusCode).toBe(400);
  });

  it('projects legacy untyped networking through the non-null Claude config default', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { name: `cfg-legacy-${Date.now()}`, networking: { allowed_hosts: ['*'] } },
    });
    expect(res.statusCode).toBe(200);
    expect(res.json().config).toEqual({
      type: 'cloud',
      networking: { type: 'unrestricted' },
      packages: apiPackages({}),
    });
  });

  it('preserves legacy flat networking on a null-target row when updating config without networking', async () => {
    // Legacy create path: no config, no target -> row persists with target=null,
    // but with a typed legacy flat `networking` blob.
    const create = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: jsonHeaders(true),
      payload: {
        name: `legacy-null-target-${Date.now()}`,
        networking: { type: 'limited', allow_package_managers: true, allowed_hosts: ['a.test'] },
      },
    });
    expect(create.statusCode).toBe(200);
    const env = create.json();
    expect(env.target).toBe('cloud');

    // Update config.type=cloud + packages, omitting config.networking entirely.
    const update = await app.inject({
      method: 'POST',
      url: `/v1/environments/${env.id}`,
      headers: jsonHeaders(true),
      payload: {
        config: { type: 'cloud', packages: { type: 'packages', npm: ['typescript'] } },
      },
    });
    expect(update.statusCode).toBe(200);
    expect(update.json().target).toBe('cloud');
    expect(update.json().config).toEqual({
      type: 'cloud',
      networking: {
        type: 'limited',
        allow_mcp_servers: false,
        allow_package_managers: true,
        allowed_hosts: ['a.test'],
      },
      packages: apiPackages({ npm: ['typescript'] }),
    });
  });

  it('resets a limited sub-field to its default via explicit null, preserving the rest', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        name: `cfg-null-subfield-${Date.now()}`,
        config: {
          type: 'cloud',
          networking: {
            type: 'limited',
            allow_mcp_servers: true,
            allow_package_managers: true,
            allowed_hosts: ['a.test'],
          },
        },
      },
    });
    const id = create.json().id;

    const update = await app.inject({
      method: 'POST',
      url: `/v1/environments/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        config: { type: 'cloud', networking: { type: 'limited', allow_mcp_servers: null } },
      },
    });
    expect(update.statusCode).toBe(200);
    expect(update.json().config.networking).toEqual({
      type: 'limited',
      allow_mcp_servers: false,
      allow_package_managers: true,
      allowed_hosts: ['a.test'],
    });
  });

  it('resets networking to unrestricted via explicit config.networking: null', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        name: `cfg-null-networking-${Date.now()}`,
        config: {
          type: 'cloud',
          networking: { type: 'limited', allow_mcp_servers: true, allowed_hosts: ['a.test'] },
        },
      },
    });
    const id = create.json().id;

    const update = await app.inject({
      method: 'POST',
      url: `/v1/environments/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { config: { type: 'cloud', networking: null } },
    });
    expect(update.statusCode).toBe(200);
    expect(update.json().config.networking).toEqual({ type: 'unrestricted' });
  });

  it('resets packages to empty via explicit config.packages: null', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: jsonHeaders(true),
      payload: {
        name: `cfg-null-packages-${Date.now()}`,
        config: {
          type: 'cloud',
          packages: { type: 'packages', npm: ['typescript'] },
        },
      },
    });
    const id = create.json().id;

    const update = await app.inject({
      method: 'POST',
      url: `/v1/environments/${id}`,
      headers: jsonHeaders(true),
      payload: { config: { type: 'cloud', packages: null } },
    });
    expect(update.statusCode).toBe(200);
    expect(update.json().packages).toEqual([]);
    expect(update.json().config.packages).toEqual(apiPackages({}));
  });

  it('DELETE /v1/environments/:id returns a tombstone then 404', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { name: `d-${Date.now()}` },
    });
    const id = create.json().id;
    const del = await app.inject({
      method: 'DELETE',
      url: `/v1/environments/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {},
    });
    expect(del.statusCode).toBe(200);
    expect(del.json()).toEqual({ id, type: 'environment_deleted' });
    const get = await app.inject({
      method: 'GET',
      url: `/v1/environments/${id}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(get.statusCode).toBe(404);
  });

  it('DELETE /v1/environments/:id rejects environments referenced by sessions', async () => {
    const agent = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        name: `delete-env-agent-${Date.now()}`,
        model: { provider: 'anthropic', id: 'claude-3-5-sonnet-20240620' },
        system: '',
        tools: [],
        mcp_servers: [],
        skills: [],
        metadata: {},
      },
    });
    expect(agent.statusCode).toBe(200);

    const environment = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { name: `delete-env-in-use-${Date.now()}` },
    });
    expect(environment.statusCode).toBe(200);
    const environmentId = environment.json().id as string;

    const session = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { agent: agent.json().id, environment_id: environmentId },
    });
    expect(session.statusCode).toBe(200);

    const rejected = await app.inject({
      method: 'DELETE',
      url: `/v1/environments/${environmentId}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {},
    });
    expect(rejected.statusCode).toBe(409);
    expect(rejected.json()).toEqual({
      type: 'error',
      error: {
        type: 'conflict_error',
        message: 'environment is referenced by one or more sessions',
      },
      request_id: expect.any(String),
    });

    const deleteSession = await app.inject({
      method: 'DELETE',
      url: `/v1/sessions/${session.json().id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {},
    });
    expect(deleteSession.statusCode).toBe(200);

    const deleted = await app.inject({
      method: 'DELETE',
      url: `/v1/environments/${environmentId}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {},
    });
    expect(deleted.statusCode).toBe(200);
    expect(deleted.json()).toEqual({ id: environmentId, type: 'environment_deleted' });
  });
});

function metadataPairs(count: number): Record<string, string> {
  return Object.fromEntries(
    Array.from({ length: count }, (_, index) => [`key_${index}`, `value_${index}`]),
  );
}
