// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { and, eq, sql } from 'drizzle-orm';
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
import type { DbClient } from '../../src/persistence/postgres/client.js';
import {
  agents,
  agentVersions,
  skills,
  skillVersions,
  sessionSkillBindings,
} from '../../src/persistence/postgres/schema.js';
import { newId } from '../../src/domain/versioning.js';

describe('Agents CRUD (integration)', () => {
  let app: FastifyInstance;
  let db: DbClient;
  let workspaceId: string;
  let apiKey: string;

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
    await app.ready();
    workspaceId = uniqueWorkspace('agents');
    apiKey = await createTestApiKey(db, workspaceId);
  });
  afterAll(async () => {
    await app.close();
    await closeTestDb();
  });

  const baseAgent = {
    name: 'Test agent',
    model: { provider: 'anthropic', id: 'claude-3-5-sonnet-20240620' },
    system: 'You are helpful.',
    tools: [{ type: 'agent_toolset_20260401' }], // dated form on input
    mcp_servers: [],
    skills: [],
    metadata: {},
  };

  const errorMessage = (response: { json(): unknown }): string =>
    (response.json() as { error: { message: string } }).error.message;

  async function seedCustomSkill(
    prefix: string,
    options: { name?: string; packageSha256?: string } = {},
  ): Promise<{ id: string; name: string; versionIdentifier: string }> {
    const now = new Date();
    const skillId = newId('skill');
    const skillVersionId = newId('skillver');
    const name = options.name ?? `${prefix}-${Date.now()}`.toLowerCase();
    await db.insert(skills).values({
      id: skillId,
      workspaceId,
      type: 'custom',
      name,
      slug: name,
      version: 1,
      latestVersionId: null,
      description: `${prefix} test skill`,
      displayTitle: null,
      archivedAt: null,
      createdAt: now,
      updatedAt: now,
    });
    const versionIdentifier = String(now.getTime() * 1000 + 1);
    await db.insert(skillVersions).values({
      id: skillVersionId,
      workspaceId,
      skillId,
      version: 1,
      versionIdentifier,
      name,
      description: `${prefix} test skill`,
      directory: name,
      entrypoint: 'SKILL.md',
      packageSha256: options.packageSha256 ?? 'a'.repeat(64),
      packageSizeBytes: 1,
      packageManifest: [
        {
          path: 'SKILL.md',
          sizeBytes: 1,
          sha256: 'b'.repeat(64),
          mode: 0o644,
          mimeType: 'text/markdown',
        },
      ],
      archivedAt: null,
      createdAt: now,
    });
    await db
      .update(skills)
      .set({ latestVersionId: skillVersionId })
      .where(and(eq(skills.workspaceId, workspaceId), eq(skills.id, skillId)));
    return { id: skillId, name, versionIdentifier };
  }

  async function seedNextCustomSkillVersion(
    skill: { id: string; name: string; versionIdentifier: string },
    packageSha256: string,
  ): Promise<void> {
    const now = new Date();
    const skillVersionId = newId('skillver');
    await db.insert(skillVersions).values({
      id: skillVersionId,
      workspaceId,
      skillId: skill.id,
      version: 2,
      versionIdentifier: (BigInt(skill.versionIdentifier) + 1n).toString(),
      name: skill.name,
      description: 'updated test skill',
      directory: skill.name,
      entrypoint: 'SKILL.md',
      packageSha256,
      packageSizeBytes: 1,
      packageManifest: [
        {
          path: 'SKILL.md',
          sizeBytes: 1,
          sha256: 'c'.repeat(64),
          mode: 0o644,
          mimeType: 'text/markdown',
        },
      ],
      archivedAt: null,
      createdAt: now,
    });
    await db
      .update(skills)
      .set({ version: 2, latestVersionId: skillVersionId, updatedAt: now })
      .where(and(eq(skills.workspaceId, workspaceId), eq(skills.id, skill.id)));
  }

  it('POST /v1/agents creates v1 with normalized tool names (dated → canonical in storage)', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { ...baseAgent, name: `agt-${Date.now()}` },
    });
    expect(res.statusCode).toBe(200);
    const agent = res.json();
    expect(agent).toMatchObject({ type: 'agent', description: null, multiagent: null });
    expect(agent.version).toBe(1);
    // Without orca-beta header, output is the dated form (Anthropic-SDK compat)
    expect(agent.tools[0].type).toBe('agent_toolset_20260401');
  });

  it('POST /v1/agents rejects missing model with 400', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        name: `missing-model-${Date.now()}`,
        system: baseAgent.system,
        tools: baseAgent.tools,
        mcp_servers: baseAgent.mcp_servers,
        skills: baseAgent.skills,
        metadata: baseAgent.metadata,
      },
    });
    expect(res.statusCode).toBe(400);
    expect(errorMessage(res)).toMatch(/model/i);
  });

  it('POST /v1/agents rejects missing name with 400', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        model: baseAgent.model,
        system: baseAgent.system,
        tools: baseAgent.tools,
        mcp_servers: baseAgent.mcp_servers,
        skills: baseAgent.skills,
        metadata: baseAgent.metadata,
      },
    });
    expect(res.statusCode).toBe(400);
    expect(errorMessage(res)).toMatch(/name/i);
  });

  it('rejects custom tools that reuse reserved agent_toolset names', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        ...baseAgent,
        name: `reserved-custom-create-${Date.now()}`,
        tools: [{ type: 'custom', name: 'bash' }],
      },
    });
    expect(create.statusCode).toBe(400);
    expect(errorMessage(create)).toMatch(/tools\.0\.name: custom tool name is reserved/);

    const created = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { ...baseAgent, name: `reserved-custom-update-${Date.now()}` },
    });
    expect(created.statusCode).toBe(200);

    const update = await app.inject({
      method: 'POST',
      url: `/v1/agents/${created.json().id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        version: created.json().version,
        tools: [{ type: 'custom', name: 'read' }],
      },
    });
    expect(update.statusCode).toBe(400);
    expect(errorMessage(update)).toMatch(/tools\.0\.name: custom tool name is reserved/);
  });

  it('GET /v1/agents and versions include Claude compatibility aliases', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        ...baseAgent,
        name: `aliases-${Date.now()}`,
        description: 'test description',
        metadata: { description: 'metadata description' },
      },
    });
    expect(create.statusCode).toBe(200);
    const created = create.json();
    expect(created).toMatchObject({
      type: 'agent',
      description: 'test description',
      metadata: { description: 'metadata description' },
      multiagent: null,
    });

    const list = await app.inject({
      method: 'GET',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey },
    });
    expect(list.statusCode).toBe(200);
    const page = list.json() as {
      data: Array<{ id: string; type: string }>;
      next_page: string | null;
    };
    expect(page.data.find((a) => a.id === created.id)).toMatchObject({ type: 'agent' });
    expect(page.next_page).toBeNull();
    expect(page).not.toHaveProperty('agents');
    expect(page).not.toHaveProperty('page_info');

    const versions = await app.inject({
      method: 'GET',
      url: `/v1/agents/${created.id}/versions`,
      headers: { 'x-api-key': apiKey },
    });
    expect(versions.statusCode).toBe(200);
    const versionsBody = versions.json();
    expect(versionsBody.data[0]).toMatchObject({
      type: 'agent',
      description: 'test description',
      metadata: { description: 'metadata description' },
      multiagent: null,
    });
    expect(versionsBody).not.toHaveProperty('versions');
    expect(versionsBody).not.toHaveProperty('page_info');
  });

  it('GET /v1/agents honors limit and page cursors', async () => {
    const first = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { ...baseAgent, name: `page-first-${Date.now()}` },
    });
    expect(first.statusCode).toBe(200);
    const second = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { ...baseAgent, name: `page-second-${Date.now()}` },
    });
    expect(second.statusCode).toBe(200);

    const firstPage = await app.inject({
      method: 'GET',
      url: '/v1/agents?limit=1',
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
      url: `/v1/agents?limit=1&page=${encodeURIComponent(firstPageBody.next_page!)}`,
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

  it('GET /v1/agents paginates rows in the same millisecond without skips', async () => {
    const ids: string[] = [];
    for (const name of ['micro-first', 'micro-second', 'micro-third']) {
      const res = await app.inject({
        method: 'POST',
        url: '/v1/agents',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        payload: { ...baseAgent, name: `${name}-${Date.now()}` },
      });
      expect(res.statusCode).toBe(200);
      ids.push(res.json<{ id: string }>().id);
    }

    const baseSecond = new Date(Date.now() + 60_000).toISOString().replace(/\.\d{3}Z$/, '');
    await db.execute(sql`
      update ${agents}
      set created_at = case
        when id = ${ids[0]} then ${`${baseSecond}.123456+00`}::timestamptz
        when id = ${ids[1]} then ${`${baseSecond}.123200+00`}::timestamptz
        when id = ${ids[2]} then ${`${baseSecond}.122999+00`}::timestamptz
      end
      where id in (${sql.join(
        ids.map((id) => sql`${id}`),
        sql`, `,
      )})
    `);

    const seen: string[] = [];
    let page: string | null = null;
    for (let i = 0; i < ids.length; i += 1) {
      const res = await app.inject({
        method: 'GET',
        url: `/v1/agents?limit=1${page ? `&page=${encodeURIComponent(page)}` : ''}`,
        headers: { 'x-api-key': apiKey },
      });
      expect(res.statusCode, res.body).toBe(200);
      const body = res.json() as { data: Array<{ id: string }>; next_page: string | null };
      expect(body.data).toHaveLength(1);
      seen.push(body.data[0]!.id);
      page = body.next_page;
    }

    expect(seen).toEqual(ids);
  });

  it('GET /v1/agents rejects out-of-range page cursor timestamps', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/v1/agents?page=9007199254740991_agt_x',
      headers: { 'x-api-key': apiKey },
    });
    expect(res.statusCode).toBe(400);
    expect(errorMessage(res)).toBe('invalid page');
  });

  it('POST /v1/agents accepts top-level description and persists it in versions', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { ...baseAgent, name: `desc-${Date.now()}`, description: 'top-level desc' },
    });
    expect(create.statusCode).toBe(200);
    const created = create.json();
    expect(created.description).toBe('top-level desc');
    expect(created.metadata).toEqual({});

    const update = await app.inject({
      method: 'POST',
      url: `/v1/agents/${created.id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { version: created.version, description: 'updated desc' },
    });
    expect(update.statusCode).toBe(200);
    expect(update.json().description).toBe('updated desc');
    expect(update.json().metadata).toEqual({});
    const updated = update.json();

    const metadataUpdate = await app.inject({
      method: 'POST',
      url: `/v1/agents/${created.id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { version: updated.version, metadata: { owner: 'wire-test' } },
    });
    expect(metadataUpdate.statusCode).toBe(200);
    expect(metadataUpdate.json().description).toBe('updated desc');
    expect(metadataUpdate.json().metadata).toEqual({ owner: 'wire-test' });

    const get = await app.inject({
      method: 'GET',
      url: `/v1/agents/${created.id}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(get.statusCode).toBe(200);
    expect(get.json()).toMatchObject({
      description: 'updated desc',
      metadata: { owner: 'wire-test' },
    });

    const list = await app.inject({
      method: 'GET',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey },
    });
    expect(list.statusCode).toBe(200);
    expect(list.json().data.find((a: { id: string }) => a.id === created.id)).toMatchObject({
      description: 'updated desc',
      metadata: { owner: 'wire-test' },
    });

    const versions = await app.inject({
      method: 'GET',
      url: `/v1/agents/${created.id}/versions`,
      headers: { 'x-api-key': apiKey },
    });
    expect(versions.statusCode).toBe(200);
    expect(versions.json().data.map((v: { description: string }) => v.description)).toEqual([
      'updated desc',
      'updated desc',
      'top-level desc',
    ]);
    expect(versions.json().data[0].metadata).toEqual({ owner: 'wire-test' });
  });

  it('rejects an update whose merged metadata would exceed 16 pairs', async () => {
    const originalMetadata = Object.fromEntries(
      Array.from({ length: 16 }, (_, index) => [`key_${index}`, `value_${index}`]),
    );
    const create = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        ...baseAgent,
        name: `metadata-limit-${Date.now()}`,
        metadata: originalMetadata,
      },
    });
    expect(create.statusCode).toBe(200);

    const rejected = await app.inject({
      method: 'POST',
      url: `/v1/agents/${create.json().id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { metadata: { overflow: 'value' } },
    });
    expect(rejected.statusCode).toBe(400);
    expect(errorMessage(rejected)).toBe('metadata must contain at most 16 pairs');

    const get = await app.inject({
      method: 'GET',
      url: `/v1/agents/${create.json().id}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(get.statusCode).toBe(200);
    expect(get.json().metadata).toEqual(originalMetadata);
    expect(get.json().version).toBe(1);
  });

  it('POST /v1/agents allows duplicate display names in the same workspace', async () => {
    const name = `display-${Date.now()}`;
    const first = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { ...baseAgent, name },
    });
    expect(first.statusCode).toBe(200);

    const second = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { ...baseAgent, name },
    });
    expect(second.statusCode).toBe(200);
    expect(second.json().name).toBe(name);
    expect(second.json().id).not.toBe(first.json().id);
  });

  it('POST /v1/agents rejects non-array skills with 400', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { ...baseAgent, name: `skills-shape-${Date.now()}`, skills: { bad: true } },
    });
    expect(res.statusCode).toBe(400);
    expect(errorMessage(res)).toBe(
      'skills entries must be {type:"anthropic", skill_id, version?} or {type:"custom", skill_id, version?}',
    );
  });

  it('POST /v1/agents rejects more than 500 skill refs with 400', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        ...baseAgent,
        name: `skills-limit-${Date.now()}`,
        skills: Array.from({ length: 501 }, (_, index) => ({
          type: 'anthropic',
          skill_id: `catalog-${index}`,
        })),
      },
    });
    expect(res.statusCode).toBe(400);
    expect(errorMessage(res)).toBe('skills must contain at most 500 entries');
  });

  it('POST /v1/agents accepts typed custom + anthropic skill refs and round-trips them', async () => {
    const skill = await seedCustomSkill('refd');

    const refs = [
      { type: 'custom', skill_id: skill.id, version: skill.versionIdentifier },
      { type: 'anthropic', skill_id: 'pdf' },
    ];
    const res = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { ...baseAgent, name: `skills-typed-${Date.now()}`, skills: refs },
    });
    expect(res.statusCode).toBe(200);
    expect(res.json().skills).toEqual([
      { type: 'custom', skill_id: skill.id, version: skill.versionIdentifier },
      { type: 'anthropic', skill_id: 'pdf', version: 'latest' },
    ]);

    const get = await app.inject({
      method: 'GET',
      url: `/v1/agents/${res.json().id}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(get.json().skills).toEqual(res.json().skills);
  });

  it('POST /v1/agents rejects custom skills with the same name and different packages', async () => {
    const name = `conflicting-skill-${Date.now()}`;
    const first = await seedCustomSkill('conflict-one', {
      name,
      packageSha256: 'a'.repeat(64),
    });
    const second = await seedCustomSkill('conflict-two', {
      name,
      packageSha256: 'b'.repeat(64),
    });

    const res = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        ...baseAgent,
        name: `skill-conflict-${Date.now()}`,
        skills: [
          { type: 'custom', skill_id: first.id },
          { type: 'custom', skill_id: second.id },
        ],
      },
    });

    expect(res.statusCode).toBe(400);
    expect(errorMessage(res)).toBe(
      `skill name ${name} resolves to different packages for skill IDs ${first.id}, ${second.id}`,
    );
  });

  it('POST /v1/agents rejects raw skillver_ skill ids with 400', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { ...baseAgent, name: `skills-raw-${Date.now()}`, skills: ['skillver_legacy'] },
    });
    expect(res.statusCode).toBe(400);
    expect(errorMessage(res)).toBe(
      'skills entries must be {type:"anthropic", skill_id, version?} or {type:"custom", skill_id, version?}',
    );
  });

  it('POST /v1/agents normalizes a null custom Skill version selector to latest', async () => {
    const skill = await seedCustomSkill('null-selector');
    const res = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        ...baseAgent,
        name: `skills-null-selector-${Date.now()}`,
        skills: [{ type: 'custom', skill_id: skill.id, version: null }],
      },
    });
    expect(res.statusCode).toBe(200);
    expect(res.json().skills).toEqual([{ type: 'custom', skill_id: skill.id, version: 'latest' }]);
  });

  it.each([1, 'skillver_legacy'])(
    'POST /v1/agents rejects non-public custom Skill version selector %j with 400',
    async (version) => {
      const skill = await seedCustomSkill('invalid-selector');
      const res = await app.inject({
        method: 'POST',
        url: '/v1/agents',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        payload: {
          ...baseAgent,
          name: `skills-invalid-selector-${Date.now()}`,
          skills: [{ type: 'custom', skill_id: skill.id, version }],
        },
      });
      expect(res.statusCode).toBe(400);
    },
  );

  it('POST /v1/agents rejects a custom skill ref pointing at a missing version with 400', async () => {
    const skill = await seedCustomSkill('missing');
    const missingVersion = '9999999999999999';
    const res = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        ...baseAgent,
        name: `skills-missingver-${Date.now()}`,
        skills: [{ type: 'custom', skill_id: skill.id, version: missingVersion }],
      },
    });
    expect(res.statusCode).toBe(400);
    expect(errorMessage(res)).toBe(
      `skill ${skill.id} version ${missingVersion} not found in workspace`,
    );
  });

  it('POST /v1/agents/:id requires the current version and bumps on match', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { ...baseAgent, name: `iter-${Date.now()}` },
    });
    const created = create.json();
    const id = created.id;

    const update = await app.inject({
      method: 'POST',
      url: `/v1/agents/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { version: created.version, system: 'updated' },
    });
    expect(update.statusCode).toBe(200);
    const updated = update.json();
    expect(updated.version).toBe(2);
    expect(updated.system).toBe('updated');
  });

  it('POST /v1/agents/:id rejects repeated resolved SkillVersions without creating a version', async () => {
    const skill = await seedCustomSkill('update-duplicate');
    const create = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        ...baseAgent,
        name: `update-skill-conflict-${Date.now()}`,
        skills: [{ type: 'custom', skill_id: skill.id }],
      },
    });
    expect(create.statusCode).toBe(200);
    const created = create.json();

    const update = await app.inject({
      method: 'POST',
      url: `/v1/agents/${created.id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        version: created.version,
        skills: [
          { type: 'custom', skill_id: skill.id, version: 'latest' },
          { type: 'custom', skill_id: skill.id, version: skill.versionIdentifier },
        ],
      },
    });

    expect(update.statusCode).toBe(400);
    expect(errorMessage(update)).toBe(
      `custom skill ${skill.id} resolves to the same version more than once`,
    );

    const get = await app.inject({
      method: 'GET',
      url: `/v1/agents/${created.id}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(get.statusCode).toBe(200);
    expect(get.json()).toMatchObject({ version: 1, skills: created.skills });
  });

  it('POST /v1/agents/:id revalidates retained latest Skill refs before creating a version', async () => {
    const name = `updated-latest-conflict-${Date.now()}`;
    const first = await seedCustomSkill('latest-conflict-one', { name });
    const second = await seedCustomSkill('latest-conflict-two', { name });
    const create = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        ...baseAgent,
        name: `update-latest-conflict-${Date.now()}`,
        skills: [
          { type: 'custom', skill_id: first.id },
          { type: 'custom', skill_id: second.id },
        ],
      },
    });
    expect(create.statusCode).toBe(200);
    const created = create.json();

    await seedNextCustomSkillVersion(second, 'b'.repeat(64));
    const update = await app.inject({
      method: 'POST',
      url: `/v1/agents/${created.id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { version: created.version, system: 'must not be persisted' },
    });

    expect(update.statusCode).toBe(400);
    expect(errorMessage(update)).toBe(
      `skill name ${name} resolves to different packages for skill IDs ${first.id}, ${second.id}`,
    );

    const get = await app.inject({
      method: 'GET',
      url: `/v1/agents/${created.id}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(get.statusCode).toBe(200);
    expect(get.json()).toMatchObject({
      version: 1,
      system: created.system,
      skills: created.skills,
    });
  });

  it('POST /v1/agents/:id allows unrelated updates when a retained latest Skill no longer resolves', async () => {
    const skill = await seedCustomSkill('deleted-latest');
    const create = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        ...baseAgent,
        name: `deleted-latest-${Date.now()}`,
        skills: [{ type: 'custom', skill_id: skill.id }],
      },
    });
    expect(create.statusCode).toBe(200);
    const created = create.json();

    const deleted = await app.inject({
      method: 'DELETE',
      url: `/v1/skills/${skill.id}/versions/${skill.versionIdentifier}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(deleted.statusCode).toBe(200);

    const unrelatedUpdate = await app.inject({
      method: 'POST',
      url: `/v1/agents/${created.id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { version: created.version, system: 'updated despite retained missing Skill' },
    });
    expect(unrelatedUpdate.statusCode).toBe(200);
    expect(unrelatedUpdate.json()).toMatchObject({
      version: 2,
      system: 'updated despite retained missing Skill',
      skills: created.skills,
    });

    const explicitSkillsUpdate = await app.inject({
      method: 'POST',
      url: `/v1/agents/${created.id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { version: 2, skills: created.skills },
    });
    expect(explicitSkillsUpdate.statusCode).toBe(400);
    expect(errorMessage(explicitSkillsUpdate)).toBe(
      `skill ${skill.id} version latest not found in workspace`,
    );
  });

  it('POST /v1/agents/:id returns the current version for no-op updates', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { ...baseAgent, name: `noop-${Date.now()}` },
    });
    expect(create.statusCode).toBe(200);
    const created = create.json();
    const id = created.id as string;

    const update = await app.inject({
      method: 'POST',
      url: `/v1/agents/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        version: created.version,
        name: created.name,
        model: created.model,
        system: created.system,
        tools: created.tools,
        mcp_servers: created.mcp_servers,
        skills: created.skills,
        metadata: created.metadata,
      },
    });
    expect(update.statusCode).toBe(200);
    const updated = update.json();
    expect(updated.version).toBe(created.version);
    expect(updated.system).toBe(created.system);

    const versions = await app.inject({
      method: 'GET',
      url: `/v1/agents/${id}/versions`,
      headers: { 'x-api-key': apiKey },
    });
    expect(versions.statusCode).toBe(200);
    expect(versions.json().data.map((v: { version: number }) => v.version)).toEqual([1]);
  });

  it('POST /v1/agents/:id rejects stale version preconditions without bumping the version', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { ...baseAgent, name: `cas-${Date.now()}` },
    });
    expect(create.statusCode).toBe(200);
    const id = create.json().id as string;

    const update = await app.inject({
      method: 'POST',
      url: `/v1/agents/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { version: 1, system: 'v2 system' },
    });
    expect(update.statusCode).toBe(200);
    expect(update.json()).toMatchObject({ version: 2, system: 'v2 system' });

    const stale = await app.inject({
      method: 'POST',
      url: `/v1/agents/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { version: 1, system: 'stale system' },
    });
    expect(stale.statusCode).toBe(409);
    expect(errorMessage(stale)).toMatch(/version/i);

    const get = await app.inject({
      method: 'GET',
      url: `/v1/agents/${id}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(get.statusCode).toBe(200);
    expect(get.json()).toMatchObject({ version: 2, system: 'v2 system' });

    const versions = await app.inject({
      method: 'GET',
      url: `/v1/agents/${id}/versions`,
      headers: { 'x-api-key': apiKey },
    });
    expect(versions.statusCode).toBe(200);
    expect(versions.json().data.map((v: { version: number }) => v.version)).toEqual([2, 1]);
  });

  it('POST /v1/agents/:id accepts versionless updates and rejects invalid fields', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { ...baseAgent, name: `invalid-update-${Date.now()}` },
    });
    expect(create.statusCode).toBe(200);
    const id = create.json().id as string;
    const version = create.json().version as number;

    const versionless = await app.inject({
      method: 'POST',
      url: `/v1/agents/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { system: 'versionless update' },
    });
    expect(versionless.statusCode).toBe(200);
    expect(versionless.json()).toMatchObject({
      version: version + 1,
      system: 'versionless update',
    });
    const currentVersion = versionless.json().version as number;

    const emptyName = await app.inject({
      method: 'POST',
      url: `/v1/agents/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { version: currentVersion, name: '' },
    });
    expect(emptyName.statusCode).toBe(400);
    expect(errorMessage(emptyName)).toMatch(/name/i);

    const badMcpServer = await app.inject({
      method: 'POST',
      url: `/v1/agents/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        version: currentVersion,
        mcp_servers: [{ type: 'url', name: 'github', url: 'not-a-url' }],
      },
    });
    expect(badMcpServer.statusCode).toBe(400);
    expect(errorMessage(badMcpServer)).toMatch(/mcp_servers/i);
  });

  it('serializes concurrent versionless updates without returning a CAS conflict', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { ...baseAgent, name: `unconditional-${Date.now()}` },
    });
    expect(create.statusCode).toBe(200);
    const id = create.json().id as string;

    const [systemUpdate, descriptionUpdate] = await Promise.all([
      app.inject({
        method: 'POST',
        url: `/v1/agents/${id}`,
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        payload: { system: 'concurrent system' },
      }),
      app.inject({
        method: 'POST',
        url: `/v1/agents/${id}`,
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        payload: { description: 'concurrent description' },
      }),
    ]);

    expect([systemUpdate.statusCode, descriptionUpdate.statusCode]).toEqual([200, 200]);
    expect(
      [systemUpdate.json().version, descriptionUpdate.json().version].sort((a, b) => a - b),
    ).toEqual([2, 3]);

    const get = await app.inject({
      method: 'GET',
      url: `/v1/agents/${id}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(get.json()).toMatchObject({
      version: 3,
      system: 'concurrent system',
      description: 'concurrent description',
    });
  });

  it('GET /v1/agents/:id/versions returns persisted versions newest-first', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { ...baseAgent, name: `versions-${Date.now()}` },
    });
    expect(create.statusCode).toBe(200);
    const created = create.json();
    const id = created.id as string;

    const update = await app.inject({
      method: 'POST',
      url: `/v1/agents/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { version: created.version, system: 'v2 system' },
    });
    expect(update.statusCode).toBe(200);

    const versions = await app.inject({
      method: 'GET',
      url: `/v1/agents/${id}/versions`,
      headers: { 'x-api-key': apiKey },
    });
    expect(versions.statusCode).toBe(200);
    const body = versions.json() as {
      data: Array<{ id: string; version: number; system: string }>;
      next_page: string | null;
    };
    expect(body.data.map((v) => v.version)).toEqual([2, 1]);
    expect(body.data[0]).toMatchObject({ id, version: 2, system: 'v2 system' });
    expect(body.data[1]).toMatchObject({ id, version: 1, system: baseAgent.system });
    expect(body.next_page).toBeNull();
    expect(body).not.toHaveProperty('versions');
    expect(body).not.toHaveProperty('page_info');
  });

  it('GET /v1/agents/:id/versions honors limit and page cursors', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { ...baseAgent, name: `versions-page-${Date.now()}` },
    });
    expect(create.statusCode).toBe(200);
    const id = create.json().id as string;
    let currentVersion = create.json().version as number;
    for (const system of ['v2', 'v3']) {
      const update = await app.inject({
        method: 'POST',
        url: `/v1/agents/${id}`,
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        payload: { version: currentVersion, system },
      });
      expect(update.statusCode).toBe(200);
      currentVersion = update.json().version as number;
    }

    const firstPage = await app.inject({
      method: 'GET',
      url: `/v1/agents/${id}/versions?limit=1`,
      headers: { 'x-api-key': apiKey },
    });
    expect(firstPage.statusCode).toBe(200);
    const firstPageBody = firstPage.json() as {
      data: Array<{ version: number }>;
      next_page: string | null;
    };
    expect(firstPageBody.data.map((version) => version.version)).toEqual([3]);
    expect(firstPageBody.next_page).toBe('3');

    const secondPage = await app.inject({
      method: 'GET',
      url: `/v1/agents/${id}/versions?limit=1&page=${encodeURIComponent(firstPageBody.next_page!)}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(secondPage.statusCode).toBe(200);
    const secondPageBody = secondPage.json() as {
      data: Array<{ version: number }>;
      next_page: string | null;
    };
    expect(secondPageBody.data.map((version) => version.version)).toEqual([2]);
  });

  it('GET /v1/agents/:id/versions returns a consumable next_page cursor when capped', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { ...baseAgent, name: `many-versions-${Date.now()}` },
    });
    expect(create.statusCode).toBe(200);
    const id = create.json().id as string;

    await db.insert(agentVersions).values(
      Array.from({ length: 100 }, (_, i) => {
        const version = i + 2;
        return {
          id: newId('agtv'),
          workspaceId,
          agentId: id,
          version,
          snapshot: {
            id,
            name: 'many-versions',
            version,
            model: baseAgent.model,
            system: `v${version} system`,
            tools: [{ type: 'agent_toolset' }],
            mcp_servers: [],
            skills: [],
            metadata: {},
          },
          createdAt: new Date(),
        };
      }),
    );

    const versions = await app.inject({
      method: 'GET',
      url: `/v1/agents/${id}/versions`,
      headers: { 'x-api-key': apiKey },
    });
    expect(versions.statusCode).toBe(200);
    const body = versions.json() as {
      data: Array<{ version: number }>;
      next_page: string | null;
    };
    expect(body.data).toHaveLength(100);
    expect(body.data[0]!.version).toBe(101);
    expect(body.data[99]!.version).toBe(2);
    expect(body.next_page).toBe('2');
    expect(body).not.toHaveProperty('versions');
    expect(body).not.toHaveProperty('page_info');

    const next = await app.inject({
      method: 'GET',
      url: `/v1/agents/${id}/versions?page=${body.next_page}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(next.statusCode).toBe(200);
    const nextBody = next.json() as {
      data: Array<{ version: number }>;
      next_page: string | null;
    };
    expect(nextBody.data.map((v) => v.version)).toEqual([1]);
    expect(nextBody.next_page).toBeNull();
  });

  it('GET /v1/agents/:id without orca-beta returns dated tool names', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { ...baseAgent, name: `wire-${Date.now()}` },
    });
    const id = create.json().id;
    const get = await app.inject({
      method: 'GET',
      url: `/v1/agents/${id}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(get.statusCode).toBe(200);
    expect(get.json().tools[0].type).toBe('agent_toolset_20260401');
  });

  it('GET /v1/agents/:id with orca-beta returns canonical tool names', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { ...baseAgent, name: `beta-${Date.now()}` },
    });
    const id = create.json().id;
    const get = await app.inject({
      method: 'GET',
      url: `/v1/agents/${id}`,
      headers: { 'x-api-key': apiKey, 'orca-beta': 'managed-agents-1' },
    });
    expect(get.statusCode).toBe(200);
    expect(get.json().tools[0].type).toBe('agent_toolset'); // canonical form
  });

  it('POST /v1/agents omits MCP server permission policies from the wire shape', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        ...baseAgent,
        name: `mcp-policy-${Date.now()}`,
        tools: [{ type: 'mcp_toolset', mcp_server_name: 'github' }],
        mcp_servers: [{ type: 'url', name: 'github', url: 'https://api.githubcopilot.com/mcp/' }],
      },
    });

    expect(res.statusCode).toBe(200);
    expect(res.json().mcp_servers[0]).toEqual({
      name: 'github',
      type: 'url',
      url: 'https://api.githubcopilot.com/mcp/',
    });
  });

  it('POST /v1/agents resolves Claude tool configuration defaults', async () => {
    const payload = {
      ...baseAgent,
      name: `opaque-config-${Date.now()}`,
      tools: [
        {
          type: 'agent_toolset_20260401',
          default_config: { permission_policy: { type: 'always_ask' } },
          configs: [
            { name: 'bash', enabled: false, permission_policy: { type: 'always_allow' } },
            { name: 'read', permission_policy: { type: 'always_allow' } },
          ],
        },
        {
          type: 'mcp_toolset',
          mcp_server_name: 'github',
          default_config: { permission_policy: { type: 'always_ask' } },
          configs: [
            { name: 'mcp__github__create_issue', permission_policy: { type: 'always_ask' } },
          ],
        },
      ],
      mcp_servers: [
        {
          type: 'url',
          name: 'github',
          url: 'https://api.githubcopilot.com/mcp/',
        },
      ],
    };

    const create = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload,
    });

    expect(create.statusCode).toBe(200);
    const created = create.json();
    expect(created.tools).toEqual([
      {
        type: 'agent_toolset_20260401',
        default_config: { enabled: true, permission_policy: { type: 'always_ask' } },
        configs: [
          { name: 'bash', enabled: false, permission_policy: { type: 'always_allow' } },
          { name: 'read', enabled: true, permission_policy: { type: 'always_allow' } },
        ],
      },
      {
        type: 'mcp_toolset',
        mcp_server_name: 'github',
        default_config: { enabled: true, permission_policy: { type: 'always_ask' } },
        configs: [
          {
            name: 'mcp__github__create_issue',
            enabled: true,
            permission_policy: { type: 'always_ask' },
          },
        ],
      },
    ]);
    expect(created.mcp_servers).toEqual(payload.mcp_servers);

    const versions = await app.inject({
      method: 'GET',
      url: `/v1/agents/${created.id}/versions`,
      headers: { 'x-api-key': apiKey },
    });
    expect(versions.statusCode).toBe(200);
    expect(versions.json().data[0].tools).toEqual(created.tools);
    expect(versions.json().data[0].mcp_servers).toEqual(payload.mcp_servers);
  });

  it('POST /v1/agents ignores legacy MCP server permission policies', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: {
        'x-api-key': apiKey,
        'content-type': 'application/json',
        'orca-beta': '1',
      },
      payload: {
        ...baseAgent,
        name: `mcp-policy-invalid-${Date.now()}`,
        tools: [{ type: 'mcp_toolset', mcp_server_name: 'github' }],
        mcp_servers: [
          {
            name: 'github',
            url: 'https://api.githubcopilot.com/mcp/',
            permission_policy: 'always_allow',
          },
        ],
      },
    });

    expect(res.statusCode).toBe(200);
    expect(res.json().mcp_servers[0]).toEqual({
      name: 'github',
      url: 'https://api.githubcopilot.com/mcp/',
    });
  });

  it('POST /v1/agents/:id/archive sets archived_at', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { ...baseAgent, name: `arc-${Date.now()}` },
    });
    const id = create.json().id;
    // Omit `payload`: zero-byte body with Content-Type: application/json, as
    // sent by some Anthropic-compatible clients and proxies.
    const archive = await app.inject({
      method: 'POST',
      url: `/v1/agents/${id}/archive`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
    });
    expect(archive.statusCode).toBe(200);
    expect(archive.json().archived_at).toBeTruthy();
  });

  it('GET /v1/agents supports include_archived and created_at filters', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { ...baseAgent, name: `filtered-${Date.now()}` },
    });
    expect(create.statusCode).toBe(200);
    const id = create.json().id as string;
    const createdAt = create.json().created_at as string;

    const atOrAfter = await app.inject({
      method: 'GET',
      url: `/v1/agents?created_at[gte]=${encodeURIComponent(createdAt)}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(atOrAfter.statusCode).toBe(200);
    expect(atOrAfter.json().data.map((item: { id: string }) => item.id)).toContain(id);

    const before = new Date(new Date(createdAt).getTime() - 1).toISOString();
    const tooEarly = await app.inject({
      method: 'GET',
      url: `/v1/agents?created_at[lte]=${encodeURIComponent(before)}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(tooEarly.statusCode).toBe(200);
    expect(tooEarly.json().data.map((item: { id: string }) => item.id)).not.toContain(id);

    const archive = await app.inject({
      method: 'POST',
      url: `/v1/agents/${id}/archive`,
      headers: { 'x-api-key': apiKey },
    });
    expect(archive.statusCode).toBe(200);

    const active = await app.inject({
      method: 'GET',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey },
    });
    expect(active.json().data.map((item: { id: string }) => item.id)).not.toContain(id);

    const all = await app.inject({
      method: 'GET',
      url: '/v1/agents?include_archived=true',
      headers: { 'x-api-key': apiKey },
    });
    expect(all.statusCode).toBe(200);
    expect(all.json().data.map((item: { id: string }) => item.id)).toContain(id);
  });

  it('DELETE /v1/agents/:id returns a tombstone', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { ...baseAgent, name: `del-${Date.now()}` },
    });
    const id = create.json().id;
    // Omit `payload` so the request sends a zero-byte body with
    // Content-Type: application/json — exactly what such a proxy forwards.
    // This is the real-world repro; `payload: {}` would serialize to
    // the non-empty string '{}' and mask the empty-body bug.
    const del = await app.inject({
      method: 'DELETE',
      url: `/v1/agents/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
    });
    expect(del.statusCode).toBe(200);
    expect(del.json()).toEqual({ id, type: 'agent_deleted' });
    const get = await app.inject({
      method: 'GET',
      url: `/v1/agents/${id}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(get.statusCode).toBe(404);
  });

  it('creates a Codex SDK agent, versions model edits, and rejects harness removal', async () => {
    const headers = {
      'x-api-key': apiKey,
      'content-type': 'application/json',
      'orca-beta': 'true',
    };
    const create = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers,
      payload: {
        ...baseAgent,
        name: `codex-sdk-${Date.now()}`,
        model: 'gpt-5.4',
        metadata: { harness: 'codex_sdk' },
      },
    });
    expect(create.statusCode, create.body).toBe(200);
    const created = create.json();
    expect(created.model).toEqual({ provider: 'openai', id: 'gpt-5.4' });
    for (const patch of [
      { metadata: null },
      { metadata: { harness: null } },
      { metadata: { harness: 'claude_agent_sdk' } },
      { model: 'claude-opus-5' },
      { model: 'gpt-made-up' },
    ]) {
      const response = await app.inject({
        method: 'POST',
        url: `/v1/agents/${created.id}`,
        headers,
        payload: patch,
      });
      expect(response.statusCode, response.body).toBe(400);
    }
    const environment = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers,
      payload: { name: `codex-env-${Date.now()}`, config: { type: 'cloud' } },
    });
    expect(environment.statusCode, environment.body).toBe(200);
    for (const model of ['claude-opus-5', 'gpt-made-up']) {
      const session = await app.inject({
        method: 'POST',
        url: '/v1/sessions',
        headers,
        payload: {
          environment_id: environment.json().id,
          agent: { type: 'agent_with_overrides', id: created.id, version: 1, model },
        },
      });
      expect(session.statusCode, session.body).toBe(400);
    }
    const session = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers,
      payload: {
        environment_id: environment.json().id,
        agent: { type: 'agent', id: created.id, version: 1 },
      },
    });
    expect(session.statusCode, session.body).toBe(200);
    const badUpdate = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${session.json().id}`,
      headers,
      payload: { agent: { model: 'claude-opus-5' } },
    });
    expect(badUpdate.statusCode, badUpdate.body).toBe(400);
    const rejected = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers,
      payload: {
        ...baseAgent,
        model: 'gpt-5.4',
        metadata: { harness: 'codex_sdk' },
        multiagent: { agents: [{ type: 'self' }] },
      },
    });
    expect(rejected.statusCode, rejected.body).toBe(400);
    const updated = await app.inject({
      method: 'POST',
      url: `/v1/agents/${created.id}`,
      headers,
      payload: { model: 'gpt-5.5', metadata: { tag: 'keep-harness' } },
    });
    expect(updated.statusCode, updated.body).toBe(200);
    expect(updated.json()).toMatchObject({
      version: 2,
      metadata: { harness: 'codex_sdk', tag: 'keep-harness' },
      model: { provider: 'openai', id: 'gpt-5.5' },
    });
    const pinned = await app.inject({
      method: 'GET',
      url: `/v1/agents/${created.id}?version=1`,
      headers,
    });
    expect(pinned.json()).toMatchObject({ version: 1, model: { id: 'gpt-5.4' } });
    const internalAgentId = created.id.replace(/^agent_/, 'agt_');
    const internalSessionId = session.json().id.replace(/^session_/, 'ses_');
    const prepared = await app.inject({
      method: 'POST',
      url: `/internal/v1/workspaces/${workspaceId}/sessions/${internalSessionId}/executions:prepare`,
      payload: {},
    });
    expect(prepared.statusCode, prepared.body).toBe(200);
    expect(prepared.json().primary_agent.model.id).toBe('gpt-5.4');

    const ownerUrl = `/internal/v1/workspaces/${workspaceId}/sessions/${internalSessionId}/execution-owner`;
    const owner = await app.inject({ method: 'GET', url: ownerUrl });
    expect(owner.statusCode, owner.body).toBe(200);
    expect(owner.json()).toEqual({ owner: 'harness-server' });
    const foreignOwner = await app.inject({
      method: 'GET',
      url: ownerUrl.replace(workspaceId, 'ws_foreign'),
    });
    expect(foreignOwner.statusCode).toBe(404);
    // A migrated historical version with a different harness must not silently
    // run on the current Agent's harness.
    await db
      .update(agentVersions)
      .set({
        snapshot: sql`jsonb_set(${agentVersions.snapshot}, '{harness_type}', '"claude_agent_sdk"'::jsonb)`,
      })
      .where(and(eq(agentVersions.agentId, internalAgentId), eq(agentVersions.version, 1)));
    const conflicting = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers,
      payload: {
        environment_id: environment.json().id,
        agent: { type: 'agent', id: created.id, version: 1 },
      },
    });
    expect(conflicting.statusCode, conflicting.body).toBe(503);
    // Archive and corrupt-history routing are independent cases. Restore the
    // fixed identity before asserting archive alone keeps ownership readable.
    await db
      .update(agentVersions)
      .set({
        snapshot: sql`jsonb_set(${agentVersions.snapshot}, '{harness_type}', '"codex_sdk"'::jsonb)`,
      })
      .where(and(eq(agentVersions.agentId, internalAgentId), eq(agentVersions.version, 1)));
    await db.update(agents).set({ archivedAt: new Date() }).where(eq(agents.id, internalAgentId));
    const archivedOwner = await app.inject({ method: 'GET', url: ownerUrl });
    expect(archivedOwner.statusCode).toBe(200);
    expect(archivedOwner.json()).toEqual({ owner: 'harness-server' });

    await expect(
      db
        .update(agents)
        .set({ harnessType: 'claude_agent_sdk' })
        .where(eq(agents.id, created.id.replace(/^agent_/, 'agt_'))),
    ).rejects.toThrow();
  });

  it('pins managed Skills for both separate and colocated Codex agents', async () => {
    const headers = { 'x-api-key': apiKey, 'content-type': 'application/json' };
    const skill = await seedCustomSkill('codex-skill');
    await seedNextCustomSkillVersion(skill, 'b'.repeat(64));
    const skillRefs = [{ type: 'custom', skill_id: skill.id, version: skill.versionIdentifier }];
    for (const mode of [undefined, 'separate', 'colocated']) {
      const create = await app.inject({
        method: 'POST',
        url: '/v1/agents',
        headers,
        payload: {
          ...baseAgent,
          name: `codex-skills-${mode ?? 'default'}-${Date.now()}`,
          model: 'gpt-5.4',
          skills: skillRefs,
          metadata: { harness: 'codex_sdk', ...(mode ? { mode } : {}) },
        },
      });
      expect(create.statusCode, create.body).toBe(200);
      const created = create.json();
      expect(created.skills).toEqual(skillRefs);
      const update = await app.inject({
        method: 'POST',
        url: `/v1/agents/${created.id}`,
        headers,
        payload: { skills: skillRefs },
      });
      expect(update.statusCode, update.body).toBe(200);
      expect(update.json().skills).toEqual(skillRefs);
      const environment = await app.inject({
        method: 'POST',
        url: '/v1/environments',
        headers,
        payload: { name: `codex-skill-env-${Date.now()}`, config: { type: 'cloud' } },
      });
      expect(environment.statusCode, environment.body).toBe(200);
      const session = await app.inject({
        method: 'POST',
        url: '/v1/sessions',
        headers,
        payload: {
          environment_id: environment.json().id,
          agent: { type: 'agent', id: created.id, version: 1 },
        },
      });
      expect(session.statusCode, session.body).toBe(200);
      const [binding] = await db
        .select({
          version: skillVersions.versionIdentifier,
          digest: sessionSkillBindings.bundleSha256,
        })
        .from(sessionSkillBindings)
        .innerJoin(
          skillVersions,
          and(
            eq(skillVersions.workspaceId, sessionSkillBindings.workspaceId),
            eq(skillVersions.id, sessionSkillBindings.skillVersionId),
          ),
        )
        .where(
          and(
            eq(sessionSkillBindings.workspaceId, workspaceId),
            eq(sessionSkillBindings.sessionId, session.json().id.replace(/^session_/, 'ses_')),
          ),
        );
      expect(binding).toEqual({ version: skill.versionIdentifier, digest: 'a'.repeat(64) });
    }
  });

  it('POST /v1/agents accepts a valid colocated harness annotation', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        ...baseAgent,
        name: `harness-ok-${Date.now()}`,
        metadata: { harness: 'claude_code', mode: 'colocated' },
      },
    });
    expect(res.statusCode).toBe(200);
    expect(res.json().metadata).toEqual({ harness: 'claude_code', mode: 'colocated' });
  });

  it('POST /v1/agents rejects an unsupported (harness, mode) combo with 400', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        ...baseAgent,
        name: `harness-bad-combo-${Date.now()}`,
        metadata: { harness: 'claude_agent_sdk', mode: 'colocated' },
      },
    });
    expect(res.statusCode).toBe(400);
    expect(errorMessage(res)).toBe(
      "harness 'claude_agent_sdk' does not support mode 'colocated' (allowed: separate)",
    );
  });

  it('POST /v1/agents rejects an unknown harness with 400', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        ...baseAgent,
        name: `harness-unknown-${Date.now()}`,
        metadata: { harness: 'gpt5_cli', mode: 'colocated' },
      },
    });
    expect(res.statusCode).toBe(400);
    expect(errorMessage(res)).toBe(
      'metadata.harness must be one of: claude_agent_sdk, claude_agent_sdk_persistent, claude_code, pi_sdk, codex_sdk, codex, cursor, pi, custom, mock',
    );
  });

  it('POST /v1/agents/:id rejects an update to a bad harness annotation with 400', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { ...baseAgent, name: `harness-update-${Date.now()}` },
    });
    expect(create.statusCode).toBe(200);
    const created = create.json();
    const id = created.id as string;

    const res = await app.inject({
      method: 'POST',
      url: `/v1/agents/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { version: created.version, metadata: { harness: 'codex', mode: 'separate' } },
    });
    expect(res.statusCode).toBe(400);
    expect(errorMessage(res)).toBe(
      "harness 'codex' does not support mode 'separate' (allowed: colocated)",
    );
  });

  it('POST /v1/agents/:id rejects changing the immutable harness', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { ...baseAgent, name: `harness-update-ok-${Date.now()}` },
    });
    expect(create.statusCode).toBe(200);
    const created = create.json();
    const id = created.id as string;

    const res = await app.inject({
      method: 'POST',
      url: `/v1/agents/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { version: created.version, metadata: { harness: 'codex', mode: 'colocated' } },
    });
    expect(res.statusCode).toBe(400);
    expect(errorMessage(res)).toMatch(/immutable/);
  });

  it('POST /v1/agents resolves multiagent string roster entries to versioned agent refs', async () => {
    const workerCreate = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { ...baseAgent, name: `worker-${Date.now()}` },
    });
    expect(workerCreate.statusCode).toBe(200);
    const worker = workerCreate.json();
    const workerId = worker.id as string;

    const workerUpdate = await app.inject({
      method: 'POST',
      url: `/v1/agents/${workerId}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { version: worker.version, system: 'worker v2' },
    });
    expect(workerUpdate.statusCode).toBe(200);
    expect(workerUpdate.json().version).toBe(2);

    const coordinatorCreate = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        ...baseAgent,
        name: `coordinator-${Date.now()}`,
        multiagent: { type: 'coordinator', agents: [workerId] },
      },
    });
    expect(coordinatorCreate.statusCode).toBe(200);
    const coordinator = coordinatorCreate.json();
    expect(coordinator.multiagent).toEqual({
      type: 'coordinator',
      agents: [{ type: 'agent', id: workerId, version: 2 }],
    });

    const workerUpdateAgain = await app.inject({
      method: 'POST',
      url: `/v1/agents/${workerId}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { version: workerUpdate.json().version, system: 'worker v3' },
    });
    expect(workerUpdateAgain.statusCode).toBe(200);
    expect(workerUpdateAgain.json().version).toBe(3);

    const coordinatorGet = await app.inject({
      method: 'GET',
      url: `/v1/agents/${coordinator.id}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(coordinatorGet.statusCode).toBe(200);
    expect(coordinatorGet.json().multiagent.agents).toEqual([
      { type: 'agent', id: workerId, version: 2 },
    ]);
  });

  it('POST /v1/agents rejects nested multiagent rosters', async () => {
    const workerCreate = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { ...baseAgent, name: `nested-worker-${Date.now()}` },
    });
    expect(workerCreate.statusCode).toBe(200);

    const nestedCoordinatorCreate = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        ...baseAgent,
        name: `nested-coordinator-${Date.now()}`,
        multiagent: { type: 'coordinator', agents: [workerCreate.json().id] },
      },
    });
    expect(nestedCoordinatorCreate.statusCode).toBe(200);

    const coordinatorCreate = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        ...baseAgent,
        name: `top-coordinator-${Date.now()}`,
        multiagent: { type: 'coordinator', agents: [nestedCoordinatorCreate.json().id] },
      },
    });

    expect(coordinatorCreate.statusCode).toBe(400);
    expect(errorMessage(coordinatorCreate)).toMatch(/must not themselves have multiagent/i);
  });
});
