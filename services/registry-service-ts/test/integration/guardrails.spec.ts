// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { eq } from 'drizzle-orm';
import {
  fingerprintAdminApiKey,
  generateAdminApiKey,
  hashAdminApiKey,
  partialAdminApiKeyHint,
} from '../../src/auth/admin-api-key.js';
import { adminApiKeys, agents, guardrails } from '../../src/persistence/postgres/schema.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import { buildAdminApp, buildCombinedTestApp } from '../../src/server.js';
import {
  buildStubFileStore,
  buildStubStore,
  buildTestJwtMinter,
  closeTestDb,
  getTestDb,
  STUB_SSE_CONFIG,
} from './setup.js';
import { createTestApiKey, TEST_ORGANIZATION_ID, uniqueWorkspace } from './fixtures.js';

const BASE = '/apis/policy.runorca.ai/v1';

describe('Guardrails CRUD (integration)', () => {
  let db: DbClient;
  let app: FastifyInstance;
  let adminApp: FastifyInstance;
  let apiKey: string;
  let otherApiKey: string;
  let workspaceId: string;
  let adminKey: string;
  let unscopedAdminKey: string;

  beforeAll(async () => {
    ({ db } = await getTestDb());
    app = buildCombinedTestApp({
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store: buildStubStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
    });
    adminApp = buildAdminApp({
      db,
      oidc: { allowedIssuers: [], audience: 'admin-test' },
      platformOidc: { allowedIssuers: [], audience: 'platform-test' },
      store: buildStubStore(),
    });
    await Promise.all([app.ready(), adminApp.ready()]);

    workspaceId = uniqueWorkspace('grd');
    apiKey = await createTestApiKey(db, workspaceId);
    otherApiKey = await createTestApiKey(db, uniqueWorkspace('grdother'));

    adminKey = await seedAdminKey(db, ['guardrails:read', 'guardrails:write']);
    unscopedAdminKey = await seedAdminKey(db, ['workspaces:read']);
  });

  afterAll(async () => {
    await Promise.all([app.close(), adminApp.close()]);
    await closeTestDb();
  });

  const headers = (key = apiKey) => ({ 'x-api-key': key, 'content-type': 'application/json' });
  const adminHeaders = (key = adminKey) => ({
    'x-api-key': key,
    'content-type': 'application/json',
  });

  const create = (payload: Record<string, unknown>, key = apiKey) =>
    app.inject({ method: 'POST', url: `${BASE}/guardrails`, headers: headers(key), payload });

  // ------------------------------------------------------------------ create

  it('creates a guardrail and defaults phases from the builtin catalog', async () => {
    const res = await create({
      name: 'no-shell',
      rule: { kind: 'builtin', builtin: 'block_tools', params: { tools: ['Bash'] } },
    });
    expect(res.statusCode).toBe(201);
    expect(res.json()).toEqual({
      id: expect.stringMatching(/^grd_/),
      type: 'guardrail',
      name: 'no-shell',
      description: '',
      enabled: true,
      // `block_tools` fires only at tool_call, and the catalog is the single
      // description of that, so an author who omits phases still lands a
      // guardrail that fires where the rule actually runs.
      phases: ['tool_call'],
      scope: 'workspace',
      rule: { kind: 'builtin', builtin: 'block_tools', params: { tools: ['Bash'] } },
      metadata: {},
      archived_at: null,
      created_at: expect.any(String),
      updated_at: expect.any(String),
    });
  });

  it('creates an explicit-scope expression guardrail with every field supplied', async () => {
    const res = await create({
      name: 'bounded-calls',
      description: 'stop after ten calls',
      enabled: false,
      phases: ['tool_call'],
      scope: 'explicit',
      rule: {
        kind: 'expression',
        expression: 'event.state.calls < 10',
        on_false: 'ask',
        reason: 'too many calls',
      },
      metadata: { owner: 'platform' },
    });
    expect(res.statusCode).toBe(201);
    expect(res.json()).toEqual({
      id: expect.stringMatching(/^grd_/),
      type: 'guardrail',
      name: 'bounded-calls',
      description: 'stop after ten calls',
      enabled: false,
      phases: ['tool_call'],
      scope: 'explicit',
      rule: {
        kind: 'expression',
        expression: 'event.state.calls < 10',
        on_false: 'ask',
        reason: 'too many calls',
      },
      metadata: { owner: 'platform' },
      archived_at: null,
      created_at: expect.any(String),
      updated_at: expect.any(String),
    });
  });

  it('refuses to mint an organization-scoped guardrail from the workspace API', async () => {
    const res = await create({
      name: 'org-from-workspace',
      scope: 'organization',
      rule: { kind: 'builtin', builtin: 'ask_on_os_tools' },
    });
    expect(res.statusCode).toBe(403);
    expect(res.json()).toEqual({
      type: 'error',
      error: {
        type: 'permission_error',
        message: 'organization-scoped guardrails are managed by the organization',
      },
      request_id: expect.any(String),
    });
  });

  it('replays a create under the same idempotency key instead of writing twice', async () => {
    const payload = {
      name: `idem-${Date.now()}`,
      rule: { kind: 'builtin', builtin: 'ask_on_os_tools' },
    };
    const key = `guardrail-idem-${Date.now()}`;
    const first = await app.inject({
      method: 'POST',
      url: `${BASE}/guardrails`,
      headers: { ...headers(), 'idempotency-key': key },
      payload,
    });
    const second = await app.inject({
      method: 'POST',
      url: `${BASE}/guardrails`,
      headers: { ...headers(), 'idempotency-key': key },
      payload,
    });
    expect(first.statusCode).toBe(201);
    expect(second.statusCode).toBe(201);
    expect(second.json()).toEqual(first.json());
  });

  // -------------------------------------------------------------- validation

  it('rejects an unknown builtin at write time', async () => {
    const res = await create({ name: 'nope', rule: { kind: 'builtin', builtin: 'no_such_rule' } });
    expect(res.statusCode).toBe(400);
    expect(res.json()).toEqual({
      type: 'error',
      error: {
        type: 'invalid_request_error',
        message: 'unknown guardrail type: no_such_rule',
      },
      request_id: expect.any(String),
    });
  });

  it('rejects invalid builtin parameters at write time, listing every problem', async () => {
    const res = await create({
      name: 'bad-params',
      rule: { kind: 'builtin', builtin: 'cost_budget', params: { max_cost_usd: 0, bogus: 1 } },
    });
    expect(res.statusCode).toBe(400);
    const message = res.json().error.message as string;
    expect(message).toContain('bogus: unknown parameter: bogus');
    expect(message).toContain('max_cost_usd: max_cost_usd must be greater than 0');
  });

  it('rejects an expression that does not compile', async () => {
    const res = await create({
      name: 'bad-expression',
      phases: ['tool_call'],
      rule: { kind: 'expression', expression: 'event.tool.name ===', on_false: 'deny' },
    });
    expect(res.statusCode).toBe(400);
    expect(res.json().error.message).toContain('expression:');
  });

  it('rejects a phase the builtin never fires on', async () => {
    const res = await create({
      name: 'wrong-phase',
      phases: ['response'],
      rule: { kind: 'builtin', builtin: 'block_tools', params: { tools: ['Bash'] } },
    });
    expect(res.statusCode).toBe(400);
    expect(res.json().error.message).toContain('does not fire at response');
  });

  it('rejects a malformed body without touching the store', async () => {
    const res = await create({ name: '', rule: { kind: 'builtin' } });
    expect(res.statusCode).toBe(400);
    expect(res.json().error.type).toBe('invalid_request_error');
  });

  // -------------------------------------------------------------- read paths

  it('reads one guardrail back by id', async () => {
    const created = await create({
      name: `readback-${Date.now()}`,
      rule: { kind: 'builtin', builtin: 'read_only_os' },
    });
    const id = created.json().id as string;
    const res = await app.inject({
      method: 'GET',
      url: `${BASE}/guardrails/${id}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(res.statusCode).toBe(200);
    expect(res.json()).toEqual(created.json());
  });

  it('lists guardrails in the Claude envelope and honours keyset pages', async () => {
    const first = await create({
      name: `page-a-${Date.now()}`,
      rule: { kind: 'builtin', builtin: 'ask_on_os_tools' },
    });
    const second = await create({
      name: `page-b-${Date.now()}`,
      rule: { kind: 'builtin', builtin: 'ask_on_os_tools' },
    });
    expect(first.statusCode).toBe(201);
    expect(second.statusCode).toBe(201);

    const list = await app.inject({
      method: 'GET',
      url: `${BASE}/guardrails`,
      headers: { 'x-api-key': apiKey },
    });
    expect(list.statusCode).toBe(200);
    const body = list.json() as { data: Array<{ id: string }>; next_page: string | null };
    expect(Object.keys(body).sort()).toEqual(['data', 'next_page']);
    expect(body.data.map((row) => row.id)).toEqual(
      expect.arrayContaining([first.json().id, second.json().id]),
    );

    const page1 = await app.inject({
      method: 'GET',
      url: `${BASE}/guardrails?limit=1`,
      headers: { 'x-api-key': apiKey },
    });
    expect(page1.statusCode).toBe(200);
    expect(page1.json().data).toHaveLength(1);
    expect(page1.json().next_page).toBeTruthy();

    const page2 = await app.inject({
      method: 'GET',
      url: `${BASE}/guardrails?limit=1&page=${encodeURIComponent(page1.json().next_page)}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(page2.statusCode).toBe(200);
    expect(page2.json().data).toHaveLength(1);
    expect(page2.json().data[0].id).not.toBe(page1.json().data[0].id);
  });

  it('rejects an unusable page cursor', async () => {
    const res = await app.inject({
      method: 'GET',
      url: `${BASE}/guardrails?page=grd_NOTAREALCURSOR`,
      headers: { 'x-api-key': apiKey },
    });
    expect(res.statusCode).toBe(400);
    expect(res.json().error.type).toBe('invalid_request_error');
  });

  it('keeps one workspace out of another workspace guardrails', async () => {
    const mine = await create({
      name: `isolated-${Date.now()}`,
      rule: { kind: 'builtin', builtin: 'ask_on_os_tools' },
    });
    const id = mine.json().id as string;

    const theirList = await app.inject({
      method: 'GET',
      url: `${BASE}/guardrails`,
      headers: { 'x-api-key': otherApiKey },
    });
    expect(theirList.statusCode).toBe(200);
    expect(theirList.json().data.map((row: { id: string }) => row.id)).not.toContain(id);

    const theirGet = await app.inject({
      method: 'GET',
      url: `${BASE}/guardrails/${id}`,
      headers: { 'x-api-key': otherApiKey },
    });
    expect(theirGet.statusCode).toBe(404);

    const theirUpdate = await app.inject({
      method: 'POST',
      url: `${BASE}/guardrails/${id}`,
      headers: headers(otherApiKey),
      payload: { enabled: false },
    });
    expect(theirUpdate.statusCode).toBe(404);
  });

  // ------------------------------------------------------------------ update

  it('updates a guardrail in place', async () => {
    const created = await create({
      name: `update-${Date.now()}`,
      description: 'first',
      metadata: { owner: 'platform', drop: 'me' },
      rule: { kind: 'builtin', builtin: 'block_tools', params: { tools: ['Bash'] } },
    });
    const id = created.json().id as string;

    const res = await app.inject({
      method: 'POST',
      url: `${BASE}/guardrails/${id}`,
      headers: headers(),
      payload: {
        name: 'renamed',
        description: null,
        enabled: false,
        scope: 'explicit',
        rule: { kind: 'builtin', builtin: 'block_tools', params: { tools: ['Bash', 'Write'] } },
        metadata: { owner: 'runtime', drop: null },
      },
    });
    expect(res.statusCode).toBe(200);
    expect(res.json()).toEqual({
      ...created.json(),
      name: 'renamed',
      description: '',
      enabled: false,
      scope: 'explicit',
      rule: { kind: 'builtin', builtin: 'block_tools', params: { tools: ['Bash', 'Write'] } },
      metadata: { owner: 'runtime' },
      updated_at: expect.any(String),
    });
  });

  it('rejects an update whose stored phases the new rule never fires on', async () => {
    // Phases are never silently re-derived: a rule swap that would move where a
    // guardrail fires has to say so, or enforcement would widen or vanish
    // without the author asking for either.
    const created = await create({
      name: `rule-swap-${Date.now()}`,
      phases: ['tool_result'],
      rule: { kind: 'builtin', builtin: 'detect_thrashing' },
    });
    expect(created.statusCode).toBe(201);

    const res = await app.inject({
      method: 'POST',
      url: `${BASE}/guardrails/${created.json().id}`,
      headers: headers(),
      payload: { rule: { kind: 'builtin', builtin: 'block_tools', params: { tools: ['Bash'] } } },
    });
    expect(res.statusCode).toBe(400);
    expect(res.json().error.message).toContain('does not fire at tool_result');

    const withPhases = await app.inject({
      method: 'POST',
      url: `${BASE}/guardrails/${created.json().id}`,
      headers: headers(),
      payload: {
        phases: ['tool_call'],
        rule: { kind: 'builtin', builtin: 'block_tools', params: { tools: ['Bash'] } },
      },
    });
    expect(withPhases.statusCode).toBe(200);
    expect(withPhases.json().phases).toEqual(['tool_call']);
  });

  it('404s an update against a guardrail that does not exist', async () => {
    const res = await app.inject({
      method: 'POST',
      url: `${BASE}/guardrails/grd_MISSING`,
      headers: headers(),
      payload: { enabled: false },
    });
    expect(res.statusCode).toBe(404);
    expect(res.json().error.type).toBe('not_found_error');
  });

  // ------------------------------------------------------------ archive+delete

  it('archives a guardrail and hides it unless asked for', async () => {
    const created = await create({
      name: `archive-${Date.now()}`,
      rule: { kind: 'builtin', builtin: 'ask_on_os_tools' },
    });
    const id = created.json().id as string;

    const archive = await app.inject({
      method: 'POST',
      url: `${BASE}/guardrails/${id}/archive`,
      headers: headers(),
      payload: {},
    });
    expect(archive.statusCode).toBe(200);
    expect(archive.json().archived_at).toBeTruthy();

    const hidden = await app.inject({
      method: 'GET',
      url: `${BASE}/guardrails`,
      headers: { 'x-api-key': apiKey },
    });
    expect(hidden.json().data.map((row: { id: string }) => row.id)).not.toContain(id);

    const included = await app.inject({
      method: 'GET',
      url: `${BASE}/guardrails?include_archived=true`,
      headers: { 'x-api-key': apiKey },
    });
    expect(included.json().data.map((row: { id: string }) => row.id)).toContain(id);
  });

  it('deletes a guardrail and returns a tombstone', async () => {
    const created = await create({
      name: `delete-${Date.now()}`,
      rule: { kind: 'builtin', builtin: 'ask_on_os_tools' },
    });
    const id = created.json().id as string;

    const del = await app.inject({
      method: 'DELETE',
      url: `${BASE}/guardrails/${id}`,
      headers: headers(),
      payload: {},
    });
    expect(del.statusCode).toBe(200);
    expect(del.json()).toEqual({ id, type: 'guardrail_deleted' });

    const get = await app.inject({
      method: 'GET',
      url: `${BASE}/guardrails/${id}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(get.statusCode).toBe(404);
  });

  it('refuses to delete a guardrail an agent still names', async () => {
    const created = await create({
      name: `in-use-${Date.now()}`,
      scope: 'explicit',
      rule: { kind: 'builtin', builtin: 'ask_on_os_tools' },
    });
    const id = created.json().id as string;

    const agent = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: headers(),
      payload: {
        name: `guardrail-agent-${Date.now()}`,
        model: { provider: 'anthropic', id: 'claude-3-5-sonnet-20240620' },
        system: '',
        tools: [],
        mcp_servers: [],
        skills: [],
        metadata: {},
      },
    });
    // 200, not 201: `POST /v1/agents` is a core route, and its create codes
    // are aligned with Anthropic's. The group's own routes below still
    // answer 201 — they are Orca surface and answer to nobody's published spec.
    expect(agent.statusCode).toBe(200);
    const agentId = agent.json().id as string;
    await db
      .update(agents)
      .set({ guardrailIds: [id] })
      .where(eq(agents.id, agentId));

    const blocked = await app.inject({
      method: 'DELETE',
      url: `${BASE}/guardrails/${id}`,
      headers: headers(),
      payload: {},
    });
    expect(blocked.statusCode).toBe(409);
    expect(blocked.json()).toEqual({
      type: 'error',
      error: {
        type: 'conflict_error',
        message: 'guardrail is referenced by one or more agents',
      },
      request_id: expect.any(String),
    });

    await db.update(agents).set({ guardrailIds: [] }).where(eq(agents.id, agentId));
    const deleted = await app.inject({
      method: 'DELETE',
      url: `${BASE}/guardrails/${id}`,
      headers: headers(),
      payload: {},
    });
    expect(deleted.statusCode).toBe(200);
  });

  // ------------------------------------------------------------------ catalog

  it('serves the builtin catalog the server itself enforces', async () => {
    const res = await app.inject({
      method: 'GET',
      url: `${BASE}/guardrailtypes`,
      headers: { 'x-api-key': apiKey },
    });
    expect(res.statusCode).toBe(200);
    const body = res.json() as { data: Array<Record<string, unknown>> };
    expect(Object.keys(body)).toEqual(['data']);

    const blockTools = body.data.find((entry) => entry.name === 'block_tools');
    expect(blockTools).toMatchObject({
      name: 'block_tools',
      title: 'Block tools',
      phases: ['tool_call'],
      stateful: false,
      verdicts: ['deny'],
      paramsSchema: expect.objectContaining({ type: 'object', additionalProperties: false }),
    });

    // Machinery is not authorable, so it is not advertised.
    expect(body.data.map((entry) => entry.name)).not.toContain('tool_permission_policy');
    // A type the catalog advertises must be one the server will accept.
    const authored = await create({
      name: `catalog-roundtrip-${Date.now()}`,
      rule: { kind: 'builtin', builtin: String(blockTools!.name), params: { tools: ['Bash'] } },
    });
    expect(authored.statusCode).toBe(201);

    // The counter-only budgets are served and authorable again: this delivery
    // is the one that advances `daily_cost_usd` and `subagent_cost_<id>`, which
    // is what the registry withheld them for.
    for (const builtin of ['user_daily_cost_budget', 'subagent_cost_budget']) {
      expect(body.data.map((entry) => entry.name)).toContain(builtin);
    }
    const dailyBudget = await create({
      name: `daily-budget-${Date.now()}`,
      scope: 'workspace',
      rule: { kind: 'builtin', builtin: 'user_daily_cost_budget', params: { max_cost_usd: 25 } },
    });
    expect(dailyBudget.statusCode).toBe(201);
  });

  // ------------------------------------------------------------- organization

  describe('organization tier', () => {
    let orgGuardrailId: string;

    it('mints an organization-scoped guardrail on the admin listener', async () => {
      const res = await adminApp.inject({
        method: 'POST',
        url: '/v1/organizations/guardrails',
        headers: adminHeaders(),
        payload: {
          name: `org-wide-${Date.now()}`,
          description: 'no shell anywhere',
          rule: { kind: 'builtin', builtin: 'block_tools', params: { tools: ['Bash'] } },
        },
      });
      expect(res.statusCode).toBe(201);
      expect(res.json()).toEqual({
        id: expect.stringMatching(/^grd_/),
        type: 'guardrail',
        name: expect.stringMatching(/^org-wide-/),
        description: 'no shell anywhere',
        enabled: true,
        phases: ['tool_call'],
        scope: 'organization',
        rule: { kind: 'builtin', builtin: 'block_tools', params: { tools: ['Bash'] } },
        metadata: {},
        archived_at: null,
        created_at: expect.any(String),
        updated_at: expect.any(String),
      });
      orgGuardrailId = res.json().id as string;
    });

    it('shows the organization guardrail to every workspace it applies to', async () => {
      const list = await app.inject({
        method: 'GET',
        url: `${BASE}/guardrails`,
        headers: { 'x-api-key': apiKey },
      });
      expect(list.json().data.map((row: { id: string }) => row.id)).toContain(orgGuardrailId);

      const otherList = await app.inject({
        method: 'GET',
        url: `${BASE}/guardrails`,
        headers: { 'x-api-key': otherApiKey },
      });
      expect(otherList.json().data.map((row: { id: string }) => row.id)).toContain(orgGuardrailId);

      const get = await app.inject({
        method: 'GET',
        url: `${BASE}/guardrails/${orgGuardrailId}`,
        headers: { 'x-api-key': apiKey },
      });
      expect(get.statusCode).toBe(200);
      expect(get.json().scope).toBe('organization');
    });

    it('refuses every workspace-plane mutation of an organization guardrail', async () => {
      const forbidden = {
        type: 'error',
        error: {
          type: 'permission_error',
          message: 'organization-scoped guardrails are managed by the organization',
        },
        request_id: expect.any(String),
      };

      const update = await app.inject({
        method: 'POST',
        url: `${BASE}/guardrails/${orgGuardrailId}`,
        headers: headers(),
        payload: { enabled: false },
      });
      expect(update.statusCode).toBe(403);
      expect(update.json()).toEqual(forbidden);

      const archive = await app.inject({
        method: 'POST',
        url: `${BASE}/guardrails/${orgGuardrailId}/archive`,
        headers: headers(),
        payload: {},
      });
      expect(archive.statusCode).toBe(403);
      expect(archive.json()).toEqual(forbidden);

      const del = await app.inject({
        method: 'DELETE',
        url: `${BASE}/guardrails/${orgGuardrailId}`,
        headers: headers(),
        payload: {},
      });
      expect(del.statusCode).toBe(403);
      expect(del.json()).toEqual(forbidden);

      // Still there, still enabled: a refusal that half-applied would be worse
      // than one that never ran.
      const after = await app.inject({
        method: 'GET',
        url: `${BASE}/guardrails/${orgGuardrailId}`,
        headers: { 'x-api-key': apiKey },
      });
      expect(after.statusCode).toBe(200);
      expect(after.json().enabled).toBe(true);
      expect(after.json().archived_at).toBeNull();
    });

    it('lists, reads, updates and deletes organization guardrails on the admin listener', async () => {
      const list = await adminApp.inject({
        method: 'GET',
        url: '/v1/organizations/guardrails',
        headers: adminHeaders(),
      });
      expect(list.statusCode).toBe(200);
      expect(list.json().data.map((row: { id: string }) => row.id)).toContain(orgGuardrailId);
      expect(list.json()).toMatchObject({ has_more: expect.any(Boolean) });

      const get = await adminApp.inject({
        method: 'GET',
        url: `/v1/organizations/guardrails/${orgGuardrailId}`,
        headers: adminHeaders(),
      });
      expect(get.statusCode).toBe(200);
      expect(get.json().id).toBe(orgGuardrailId);

      const patch = await adminApp.inject({
        method: 'PATCH',
        url: `/v1/organizations/guardrails/${orgGuardrailId}`,
        headers: adminHeaders(),
        payload: { enabled: false },
      });
      expect(patch.statusCode).toBe(200);
      expect(patch.json().enabled).toBe(false);

      const del = await adminApp.inject({
        method: 'DELETE',
        url: `/v1/organizations/guardrails/${orgGuardrailId}`,
        headers: adminHeaders(),
      });
      expect(del.statusCode).toBe(200);
      expect(del.json()).toEqual({ id: orgGuardrailId, type: 'guardrail_deleted' });

      const gone = await app.inject({
        method: 'GET',
        url: `${BASE}/guardrails/${orgGuardrailId}`,
        headers: { 'x-api-key': apiKey },
      });
      expect(gone.statusCode).toBe(404);
    });

    it('refuses to delete an organization guardrail an agent in any workspace names', async () => {
      // The org tier's reach is the whole organization, so the reference that
      // blocks deletion can come from a workspace other than the one the
      // deleting admin happens to think about. `otherApiKey` is a second
      // workspace in the same organization, which is the case the workspace-tier
      // check cannot cover and the one that made this gap invisible.
      const orgRule = await adminApp.inject({
        method: 'POST',
        url: '/v1/organizations/guardrails',
        headers: adminHeaders(),
        payload: {
          name: `org-in-use-${Date.now()}`,
          rule: { kind: 'builtin', builtin: 'ask_on_os_tools' },
        },
      });
      expect(orgRule.statusCode).toBe(201);
      const orgRuleId = orgRule.json().id as string;

      const agent = await app.inject({
        method: 'POST',
        url: '/v1/agents',
        headers: { 'x-api-key': otherApiKey, 'content-type': 'application/json' },
        payload: {
          name: `org-guardrail-agent-${Date.now()}`,
          model: { provider: 'anthropic', id: 'claude-3-5-sonnet-20240620' },
          system: '',
          tools: [],
          mcp_servers: [],
          skills: [],
          metadata: {},
        },
      });
      expect(agent.statusCode).toBe(200);
      const agentId = agent.json().id as string;
      await db
        .update(agents)
        .set({ guardrailIds: [orgRuleId] })
        .where(eq(agents.id, agentId));

      const blocked = await adminApp.inject({
        method: 'DELETE',
        url: `/v1/organizations/guardrails/${orgRuleId}`,
        headers: adminHeaders(),
      });
      expect(blocked.statusCode).toBe(409);
      expect(blocked.json()).toEqual({
        error: 'guardrail is referenced by one or more agents in this organization',
      });

      // Still there: a refused delete must not have half-applied.
      const stillThere = await adminApp.inject({
        method: 'GET',
        url: `/v1/organizations/guardrails/${orgRuleId}`,
        headers: adminHeaders(),
      });
      expect(stillThere.statusCode).toBe(200);

      // And the reference is what blocked it, not something incidental.
      await db.update(agents).set({ guardrailIds: [] }).where(eq(agents.id, agentId));
      const deleted = await adminApp.inject({
        method: 'DELETE',
        url: `/v1/organizations/guardrails/${orgRuleId}`,
        headers: adminHeaders(),
      });
      expect(deleted.statusCode).toBe(200);
    });

    it('validates organization guardrails with the same compiler as the workspace plane', async () => {
      const res = await adminApp.inject({
        method: 'POST',
        url: '/v1/organizations/guardrails',
        headers: adminHeaders(),
        payload: {
          name: 'bad-org-rule',
          rule: { kind: 'builtin', builtin: 'cost_budget', params: { max_cost_usd: 0 } },
        },
      });
      expect(res.statusCode).toBe(400);
      expect(res.json().error).toContain('max_cost_usd must be greater than 0');
    });

    it('refuses to mint a workspace-scoped guardrail from the organization listener', async () => {
      const res = await adminApp.inject({
        method: 'POST',
        url: '/v1/organizations/guardrails',
        headers: adminHeaders(),
        payload: {
          name: 'wrong-scope',
          scope: 'workspace',
          rule: { kind: 'builtin', builtin: 'ask_on_os_tools' },
        },
      });
      expect(res.statusCode).toBe(400);
      expect(res.json().error).toContain('organization');
    });

    it('refuses an admin key without the guardrail scope', async () => {
      const write = await adminApp.inject({
        method: 'POST',
        url: '/v1/organizations/guardrails',
        headers: adminHeaders(unscopedAdminKey),
        payload: { name: 'nope', rule: { kind: 'builtin', builtin: 'ask_on_os_tools' } },
      });
      expect(write.statusCode).toBe(403);
      expect(write.json()).toEqual({ error: 'missing required scope: guardrails:write' });

      const read = await adminApp.inject({
        method: 'GET',
        url: '/v1/organizations/guardrails',
        headers: adminHeaders(unscopedAdminKey),
      });
      expect(read.statusCode).toBe(403);
      expect(read.json()).toEqual({ error: 'missing required scope: guardrails:read' });
    });

    it('leaves no organization row owned by a workspace', async () => {
      const rows = await db
        .select({ workspaceId: guardrails.workspaceId })
        .from(guardrails)
        .where(eq(guardrails.scope, 'organization'));
      for (const row of rows) expect(row.workspaceId).toBeNull();
    });
  });
});

async function seedAdminKey(db: DbClient, scopes: string[]): Promise<string> {
  const plaintext = generateAdminApiKey();
  await db.insert(adminApiKeys).values({
    id: `adminkey_grd_${Math.random().toString(36).slice(2, 10)}`,
    organizationId: TEST_ORGANIZATION_ID,
    name: 'guardrail integration',
    hashedKey: await hashAdminApiKey(plaintext),
    keyFingerprint: fingerprintAdminApiKey(plaintext),
    partialKeyHint: partialAdminApiKeyHint(plaintext),
    scopes,
    status: 'active',
    createdBy: 'integration-test',
  });
  return plaintext;
}
