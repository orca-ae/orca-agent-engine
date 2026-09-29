// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { FastifyInstance } from 'fastify';
import type { AddressInfo } from 'node:net';
import { and, asc, eq, sql } from 'drizzle-orm';
import { unzipSync } from 'fflate';
import { InMemorySkillStore } from '@orca/skill-store';
import { buildCombinedTestApp } from '../../src/server.js';
import type { PreparedExecutionV2 } from '../../src/contracts/internal.contract.js';
import {
  acquireSkillBundleLifecycleLock,
  reconcileSkillBundleDeletionOutbox,
} from '../../src/domain/skill-bundle-deletion-outbox.js';
import { newId } from '../../src/domain/versioning.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import {
  sessionSkillBindings,
  skillBundleDeletionOutbox,
  skills,
  skillVersions,
} from '../../src/persistence/postgres/schema.js';
import {
  buildStubFileStore,
  buildStubStore,
  buildTestJwtMinter,
  closeTestDb,
  getTestDb,
  STUB_SSE_CONFIG,
} from './setup.js';
import { createTestApiKey, uniqueWorkspace } from './fixtures.js';

interface SkillResponse {
  id: string;
  created_at: string;
  display_title: string | null;
  latest_version: string | null;
  source: 'custom';
  type: 'skill';
  updated_at: string;
}

interface SkillVersionResponse {
  id: string;
  created_at: string;
  description: string;
  directory: string;
  name: string;
  skill_id: string;
  type: 'skill_version';
  version: string;
}

describe('Skills progressive disclosure (integration)', () => {
  let app: FastifyInstance;
  let db: DbClient;
  let baseURL: string;
  let workspaceId: string;
  let apiKey: string;
  let skillStore: FlakySkillStore;
  let sequence = 0;

  beforeAll(async () => {
    ({ db } = await getTestDb());
    workspaceId = uniqueWorkspace('skills');
    apiKey = await createTestApiKey(db, workspaceId);
    skillStore = new FlakySkillStore();
    app = buildCombinedTestApp({
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store: buildStubStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
      skillStore,
    });
    await app.listen({ host: '127.0.0.1', port: 0 });
    baseURL = `http://127.0.0.1:${(app.server.address() as AddressInfo).port}`;
  });

  afterAll(async () => {
    if (app) await app.close();
    await closeTestDb();
  });

  it('stores multipart Skill bodies in SkillStore and only package metadata in Postgres', async () => {
    const root = uniqueName('catalog');
    const instructions = 'Read references/guide.txt before answering.';
    const guide = 'The private reference body.';
    const created = await createSkill({
      root,
      name: root,
      description: 'Catalog-only prompt metadata',
      body: instructions,
      displayTitle: 'Progressive disclosure',
      extraFiles: [{ path: 'references/guide.txt', content: guide, type: 'text/plain' }],
    });

    expect(created.status).toBe(200);
    expect(created.body.id).toMatch(/^skill_[A-Za-z0-9_-]+$/);
    expect(created.body).toMatchObject({
      display_title: 'Progressive disclosure',
      source: 'custom',
      type: 'skill',
    });
    expect(Object.keys(created.body).sort()).toEqual([
      'created_at',
      'display_title',
      'id',
      'latest_version',
      'source',
      'type',
      'updated_at',
    ]);

    const rows = await db
      .select()
      .from(skillVersions)
      .where(
        and(eq(skillVersions.workspaceId, workspaceId), eq(skillVersions.skillId, created.body.id)),
      );
    expect(rows).toHaveLength(1);
    const version = rows[0]!;
    expect(version.id).toMatch(/^skillver_[A-Za-z0-9_-]+$/);
    const bundle = await skillStore.open(workspaceId, version.id, version.packageSha256);

    expect(version).toMatchObject({
      entrypoint: 'SKILL.md',
      packageSha256: bundle.record.sha256,
      packageSizeBytes: bundle.record.sizeBytes,
      packageManifest: bundle.record.files,
    });
    expect(version).not.toHaveProperty('contentFiles');
    expect(version).not.toHaveProperty('systemPrompt');
    expect(version).not.toHaveProperty('toolAllowlist');
    expect(JSON.stringify(version.packageManifest)).not.toContain(instructions);
    expect(JSON.stringify(version.packageManifest)).not.toContain(guide);
    expect(
      Object.fromEntries(bundle.files.map((file) => [file.path, file.content.toString('utf8')])),
    ).toEqual({
      'SKILL.md': skillMarkdown(root, 'Catalog-only prompt metadata', instructions),
      'references/guide.txt': guide,
    });
    expect(bundle.files.every((file) => file.mode === 0o644)).toBe(true);

    const content = await fetch(
      `${baseURL}/v1/skills/${created.body.id}/versions/${created.body.latest_version}/content`,
      { headers: authHeaders() },
    );
    expect(content.status).toBe(200);
    expect(content.headers.get('content-type')).toContain('application/zip');
    const archive = unzipSync(new Uint8Array(await content.arrayBuffer()));
    expect(Buffer.from(archive[`${root}/SKILL.md`]!).toString('utf8')).toBe(
      skillMarkdown(root, 'Catalog-only prompt metadata', instructions),
    );
    expect(Buffer.from(archive[`${root}/references/guide.txt`]!).toString('utf8')).toBe(guide);

    const get = await app.inject({
      method: 'GET',
      url: `/v1/skills/${created.body.id}`,
      headers: authHeaders(),
    });
    expect(get.statusCode).toBe(200);
    expect(get.json()).toEqual(created.body);

    const list = await app.inject({
      method: 'GET',
      url: '/v1/skills',
      headers: authHeaders(),
    });
    expect(list.statusCode).toBe(200);
    expect(list.json().data).toContainEqual(created.body);
  });

  it('rejects legacy JSON creation even when the Orca beta header is present', async () => {
    const response = await app.inject({
      method: 'POST',
      url: '/v1/skills',
      headers: {
        ...authHeaders(),
        'content-type': 'application/json',
        'orca-beta': '1',
      },
      payload: {
        name: 'legacy',
        slug: uniqueName('legacy'),
        system_prompt: 'This body must never enter the system prompt.',
      },
    });

    expect(response.statusCode).toBe(400);
    expect(response.body).toContain('multipart/form-data upload is required');
  });

  it('requires safe frontmatter name and a non-empty description', async () => {
    const unsafe = await upload([
      {
        path: `${uniqueName('unsafe')}/SKILL.md`,
        content: '---\nname: Unsafe Name\ndescription: present\n---\n\nInstructions.',
      },
    ]);
    expect(unsafe.status).toBe(400);
    expect(await unsafe.text()).toContain('name');

    const missingDescription = await upload([
      {
        path: 'valid-name/SKILL.md',
        content: '---\nname: valid-name\n---\n\nInstructions.',
      },
    ]);
    expect(missingDescription.status).toBe(400);
    expect(await missingDescription.text()).toContain('description');
  });

  it('keeps immutable version bundles while advancing latest', async () => {
    const name = uniqueName('versions');
    const first = await createSkill({
      root: name,
      name,
      description: 'First version',
      body: 'Version one body.',
    });
    expect(first.status).toBe(200);
    expect(first.body.display_title).toBeNull();

    const second = await createVersion(first.body.id, {
      root: name,
      name,
      description: 'Second version',
      body: 'Version two body.',
    });
    expect(second.status).toBe(200);
    expect(second.body.version).not.toBe(first.body.latest_version);

    const rows = await db
      .select()
      .from(skillVersions)
      .where(
        and(eq(skillVersions.workspaceId, workspaceId), eq(skillVersions.skillId, first.body.id)),
      )
      .orderBy(asc(skillVersions.version));
    expect(rows.map((row) => row.version)).toEqual([1, 2]);
    expect(rows[0]!.packageSha256).not.toBe(rows[1]!.packageSha256);

    const internalIdDelete = await app.inject({
      method: 'DELETE',
      url: `/v1/skills/${first.body.id}/versions/${rows[0]!.id}`,
      headers: authHeaders(),
    });
    expect(internalIdDelete.statusCode).toBe(404);

    const firstBundle = await skillStore.open(workspaceId, rows[0]!.id, rows[0]!.packageSha256);
    const secondBundle = await skillStore.open(workspaceId, rows[1]!.id, rows[1]!.packageSha256);
    expect(firstBundle.files[0]!.content.toString('utf8')).toContain('Version one body.');
    expect(secondBundle.files[0]!.content.toString('utf8')).toContain('Version two body.');

    const current = await app.inject({
      method: 'GET',
      url: `/v1/skills/${first.body.id}`,
      headers: authHeaders(),
    });
    expect(current.statusCode).toBe(200);
    expect(current.json().latest_version).toBe(second.body.version);
  });

  it('keeps timestamp version identifiers monotonic when the wall clock is behind', async () => {
    const name = uniqueName('monotonic-version');
    const first = await createSkill({
      root: name,
      name,
      description: 'First monotonic version',
      body: 'Version one.',
    });
    expect(first.status).toBe(200);

    const futureIdentifier = (BigInt(Date.now()) * 1000n + 10_000_000n).toString();
    await db
      .update(skillVersions)
      .set({ versionIdentifier: futureIdentifier })
      .where(
        and(eq(skillVersions.workspaceId, workspaceId), eq(skillVersions.skillId, first.body.id)),
      );

    const second = await createVersion(first.body.id, {
      root: name,
      name,
      description: 'Second monotonic version',
      body: 'Version two.',
    });
    expect(second.status).toBe(200);
    expect(second.body.version).toBe((BigInt(futureIdentifier) + 1n).toString());
  });

  it('keeps list cursors valid when the cursor row is archived between pages', async () => {
    for (let index = 0; index < 3; index += 1) {
      const name = uniqueName(`cursor-skill-${index}`);
      const created = await createSkill({
        root: name,
        name,
        description: `Cursor Skill ${index}`,
        body: `Cursor body ${index}.`,
      });
      expect(created.status).toBe(200);
    }

    const firstPage = await app.inject({
      method: 'GET',
      url: '/v1/skills?limit=2&source=custom',
      headers: authHeaders(),
    });
    expect(firstPage.statusCode).toBe(200);
    const firstPageBody = firstPage.json() as {
      data: SkillResponse[];
      has_more: boolean;
      next_page: string | null;
    };
    expect(firstPageBody.data).toHaveLength(2);
    expect(firstPageBody.has_more).toBe(true);
    expect(firstPageBody.next_page).not.toBeNull();

    const otherWorkspaceId = uniqueWorkspace('skills-cursor-other');
    const otherApiKey = await createTestApiKey(db, otherWorkspaceId);
    const crossWorkspace = await app.inject({
      method: 'GET',
      url: `/v1/skills?limit=2&source=custom&page=${encodeURIComponent(firstPageBody.next_page!)}`,
      headers: { 'x-api-key': otherApiKey },
    });
    expect(crossWorkspace.statusCode).toBe(400);

    const changedFilter = await app.inject({
      method: 'GET',
      url: `/v1/skills?limit=2&page=${encodeURIComponent(firstPageBody.next_page!)}`,
      headers: authHeaders(),
    });
    expect(changedFilter.statusCode).toBe(400);

    await db
      .update(skills)
      .set({ archivedAt: new Date() })
      .where(
        and(
          eq(skills.workspaceId, workspaceId),
          eq(skills.id, firstPageBody.data[firstPageBody.data.length - 1]!.id),
        ),
      );

    const secondPage = await app.inject({
      method: 'GET',
      url: `/v1/skills?limit=2&source=custom&page=${encodeURIComponent(firstPageBody.next_page!)}`,
      headers: authHeaders(),
    });
    expect(secondPage.statusCode, secondPage.body).toBe(200);
  });

  it('keeps microsecond-distinct rows reachable across list pages', async () => {
    const highName = uniqueName('cursor-micros-high');
    const lowName = uniqueName('cursor-micros-low');
    const high = await createSkill({
      root: highName,
      name: highName,
      description: 'High microsecond cursor row',
      body: 'High microsecond body.',
    });
    const low = await createSkill({
      root: lowName,
      name: lowName,
      description: 'Low microsecond cursor row',
      body: 'Low microsecond body.',
    });
    expect(high.status).toBe(200);
    expect(low.status).toBe(200);

    await db.execute(sql`
      update skills
      set created_at = case
        when id = ${high.body.id} then '2099-01-01T00:00:00.123900Z'::timestamptz
        else '2099-01-01T00:00:00.123800Z'::timestamptz
      end
      where workspace_id = ${workspaceId}
        and id in (${high.body.id}, ${low.body.id})
    `);

    const firstPage = await app.inject({
      method: 'GET',
      url: '/v1/skills?limit=1&source=custom',
      headers: authHeaders(),
    });
    const firstBody = firstPage.json() as {
      data: SkillResponse[];
      has_more: boolean;
      next_page: string;
    };
    expect(firstPage.statusCode, firstPage.body).toBe(200);
    expect(firstBody.data.map((skill) => skill.id)).toEqual([high.body.id]);
    expect(firstBody.has_more).toBe(true);

    const secondPage = await app.inject({
      method: 'GET',
      url: `/v1/skills?limit=1&source=custom&page=${encodeURIComponent(firstBody.next_page)}`,
      headers: authHeaders(),
    });
    expect(secondPage.statusCode, secondPage.body).toBe(200);
    expect((secondPage.json() as { data: SkillResponse[] }).data.map((skill) => skill.id)).toEqual([
      low.body.id,
    ]);

    await db
      .update(skills)
      .set({ archivedAt: new Date() })
      .where(
        and(
          eq(skills.workspaceId, workspaceId),
          sql`${skills.id} in (${high.body.id}, ${low.body.id})`,
        ),
      );
  });

  it('rejects empty and forged list cursors before querying Postgres', async () => {
    const empty = await app.inject({
      method: 'GET',
      url: '/v1/skills?page=',
      headers: authHeaders(),
    });
    expect(empty.statusCode).toBe(400);

    const forged = Buffer.from(
      JSON.stringify({
        v: 1,
        created_at_micros: '253402300800000000',
        id: 'skill_\u0000invalid',
        source: null,
        workspace_id: workspaceId,
      }),
      'utf8',
    ).toString('base64url');
    const invalid = await app.inject({
      method: 'GET',
      url: `/v1/skills?page=${encodeURIComponent(forged)}`,
      headers: authHeaders(),
    });
    expect(invalid.statusCode).toBe(400);
  });

  it('keeps version cursors valid when the cursor version is deleted between pages', async () => {
    const name = uniqueName('version-cursor');
    const created = await createSkill({
      root: name,
      name,
      description: 'Version cursor one',
      body: 'Version one.',
    });
    expect(created.status).toBe(200);
    const second = await createVersion(created.body.id, {
      root: name,
      name,
      description: 'Version cursor two',
      body: 'Version two.',
    });
    const third = await createVersion(created.body.id, {
      root: name,
      name,
      description: 'Version cursor three',
      body: 'Version three.',
    });
    expect(second.status).toBe(200);
    expect(third.status).toBe(200);

    const firstPage = await app.inject({
      method: 'GET',
      url: `/v1/skills/${created.body.id}/versions?limit=2`,
      headers: authHeaders(),
    });
    expect(firstPage.statusCode).toBe(200);
    const firstPageBody = firstPage.json() as {
      data: SkillVersionResponse[];
      has_more: boolean;
      next_page: string | null;
    };
    expect(firstPageBody.data.map((version) => version.version)).toEqual([
      third.body.version,
      second.body.version,
    ]);
    expect(firstPageBody.has_more).toBe(true);
    expect(firstPageBody.next_page).toBe(second.body.version);

    const deleted = await app.inject({
      method: 'DELETE',
      url: `/v1/skills/${created.body.id}/versions/${second.body.version}`,
      headers: authHeaders(),
    });
    expect(deleted.statusCode, deleted.body).toBe(200);

    const secondPage = await app.inject({
      method: 'GET',
      url: `/v1/skills/${created.body.id}/versions?limit=2&page=${firstPageBody.next_page}`,
      headers: authHeaders(),
    });
    expect(secondPage.statusCode, secondPage.body).toBe(200);
    expect(
      (secondPage.json() as { data: SkillVersionResponse[] }).data.map(
        (version) => version.version,
      ),
    ).toEqual([created.body.latest_version]);
  });

  it('retains deleted versions as tombstones while Session bindings still pin them', async () => {
    const name = uniqueName('session-pin');
    const first = await createSkill({
      root: name,
      name,
      description: 'Pinned first version',
      body: 'Version one instructions.',
    });
    expect(first.status).toBe(200);

    const agent = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { ...authHeaders(), 'content-type': 'application/json' },
      payload: {
        name: `agent-${name}`,
        model: { provider: 'anthropic', id: 'claude-sonnet-4-5' },
        system: 'Use relevant skills.',
        tools: [{ type: 'agent_toolset_20260401' }],
        mcp_servers: [],
        skills: [{ type: 'custom', skill_id: first.body.id }],
        metadata: {},
      },
    });
    expect(agent.statusCode).toBe(200);
    expect(agent.json().skills).toEqual([
      { type: 'custom', skill_id: first.body.id, version: 'latest' },
    ]);

    const environment = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { ...authHeaders(), 'content-type': 'application/json' },
      payload: { name: `env-${name}`, config: { type: 'cloud' } },
    });
    expect(environment.statusCode).toBe(200);

    const session1 = await createSession(agent.json().id, environment.json().id);
    const versionsAfterSession1 = await loadSkillVersionRows(first.body.id);
    expect(versionsAfterSession1).toHaveLength(1);

    const second = await createVersion(first.body.id, {
      root: name,
      name,
      description: 'Pinned second version',
      body: 'Version two instructions.',
    });
    expect(second.status).toBe(200);
    const versionsAfterSession2 = await loadSkillVersionRows(first.body.id);
    expect(versionsAfterSession2).toHaveLength(2);

    const persistedAgent = await app.inject({
      method: 'GET',
      url: `/v1/agents/${agent.json().id}`,
      headers: authHeaders(),
    });
    expect(persistedAgent.json().skills).toEqual([
      { type: 'custom', skill_id: first.body.id, version: 'latest' },
    ]);

    const session2 = await createSession(agent.json().id, environment.json().id);
    const bindings = await db
      .select()
      .from(sessionSkillBindings)
      .where(
        and(
          eq(sessionSkillBindings.workspaceId, workspaceId),
          eq(sessionSkillBindings.agentId, agent.json().id),
        ),
      );
    const bindingBySession = new Map(bindings.map((binding) => [binding.sessionId, binding]));
    expect(bindingBySession.get(session1)!.skillVersionId).toBe(versionsAfterSession1[0]!.id);
    expect(bindingBySession.get(session2)!.skillVersionId).toBe(versionsAfterSession2[1]!.id);

    const prepared1 = await prepareExecution(session1);
    const prepared2 = await prepareExecution(session2);
    expect(prepared1.schema_version).toBe(2);
    expect(prepared1.primary_agent).not.toHaveProperty('resolved_skills');
    expect(prepared1.primary_agent.skills).toEqual([
      descriptorFor(versionsAfterSession1[0]!, first.body.id),
    ]);
    expect(prepared2.primary_agent.skills).toEqual([
      descriptorFor(versionsAfterSession2[1]!, first.body.id),
    ]);

    const archiveV1 = await app.inject({
      method: 'DELETE',
      url: `/v1/skills/${first.body.id}/versions/${first.body.latest_version}`,
      headers: authHeaders(),
    });
    expect(archiveV1.statusCode).toBe(200);
    const preparedArchived = await prepareExecution(session1);
    expect(preparedArchived.primary_agent.skills[0]!.id).toBe(versionsAfterSession1[0]!.id);

    const activeVersionDelete = await app.inject({
      method: 'DELETE',
      url: `/v1/skills/${first.body.id}`,
      headers: authHeaders(),
    });
    expect(activeVersionDelete.statusCode).toBe(400);
    expect(activeVersionDelete.json().error).toMatchObject({
      type: 'invalid_request_error',
    });

    const deleteV2 = await app.inject({
      method: 'DELETE',
      url: `/v1/skills/${first.body.id}/versions/${second.body.version}`,
      headers: authHeaders(),
    });
    expect(deleteV2.statusCode).toBe(200);

    const noLatest = await app.inject({
      method: 'GET',
      url: `/v1/skills/${first.body.id}`,
      headers: authHeaders(),
    });
    expect(noLatest.statusCode).toBe(200);
    expect(noLatest.json().latest_version).toBeNull();

    const preparedAfterAllVersionsDeleted1 = await prepareExecution(session1);
    const preparedAfterAllVersionsDeleted2 = await prepareExecution(session2);
    expect(preparedAfterAllVersionsDeleted1.primary_agent.skills[0]!.id).toBe(
      versionsAfterSession1[0]!.id,
    );
    expect(preparedAfterAllVersionsDeleted2.primary_agent.skills[0]!.id).toBe(
      versionsAfterSession2[1]!.id,
    );

    const pinnedDelete = await app.inject({
      method: 'DELETE',
      url: `/v1/skills/${first.body.id}`,
      headers: authHeaders(),
    });
    expect(pinnedDelete.statusCode).toBe(200);
    // A retained parent and bundle keep the existing Session's pinned snapshot valid.
    expect((await prepareExecution(session1)).primary_agent.skills[0]!.id).toBe(
      versionsAfterSession1[0]!.id,
    );
  });

  it('retains tombstones and bindings after the final concurrent Session is deleted', async () => {
    const before = skillStore.size();
    const name = uniqueName('session-pin-gc');
    const created = await createSkill({
      root: name,
      name,
      description: 'Concurrent tombstone collection',
      body: 'Keep this bundle until every pinned Session is deleted.',
    });
    expect(created.status).toBe(200);
    const [version] = await loadSkillVersionRows(created.body.id);
    expect(version).toBeDefined();

    const agent = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { ...authHeaders(), 'content-type': 'application/json' },
      payload: {
        name: `agent-${name}`,
        model: { provider: 'anthropic', id: 'claude-sonnet-4-5' },
        system: 'Use relevant skills.',
        tools: [{ type: 'agent_toolset_20260401' }],
        mcp_servers: [],
        skills: [{ type: 'custom', skill_id: created.body.id }],
        metadata: {},
      },
    });
    expect(agent.statusCode).toBe(200);
    const environment = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { ...authHeaders(), 'content-type': 'application/json' },
      payload: { name: `env-${name}`, config: { type: 'cloud' } },
    });
    expect(environment.statusCode).toBe(200);
    const firstSession = await createSession(agent.json().id, environment.json().id);
    const finalSession = await createSession(agent.json().id, environment.json().id);

    const [versionDelete, firstSessionDelete] = await Promise.all([
      app.inject({
        method: 'DELETE',
        url: `/v1/skills/${created.body.id}/versions/${created.body.latest_version}`,
        headers: authHeaders(),
      }),
      app.inject({
        method: 'DELETE',
        url: `/v1/sessions/${firstSession}`,
        headers: authHeaders(),
      }),
    ]);
    expect(versionDelete.statusCode, versionDelete.body).toBe(200);
    expect(firstSessionDelete.statusCode, firstSessionDelete.body).toBe(200);

    const retainedRows = await loadSkillVersionRows(created.body.id);
    expect(retainedRows).toHaveLength(1);
    expect(retainedRows[0]).toMatchObject({ id: version!.id });
    expect(retainedRows[0]!.deletedAt).toBeInstanceOf(Date);
    const remainingBindings = await db
      .select({ sessionId: sessionSkillBindings.sessionId })
      .from(sessionSkillBindings)
      .where(
        and(
          eq(sessionSkillBindings.workspaceId, workspaceId),
          eq(sessionSkillBindings.skillVersionId, version!.id),
        ),
      );
    expect(remainingBindings.map((row) => row.sessionId).sort()).toEqual(
      [firstSession, finalSession].sort(),
    );
    expect(skillStore.size()).toBe(before + 1);

    const finalSessionDelete = await app.inject({
      method: 'DELETE',
      url: `/v1/sessions/${finalSession}`,
      headers: authHeaders(),
    });
    expect(finalSessionDelete.statusCode, finalSessionDelete.body).toBe(200);
    expect(await loadSkillVersionRows(created.body.id)).toHaveLength(1);
    expect(skillStore.size()).toBe(before + 1);
    const pendingDeletes = await db
      .select()
      .from(skillBundleDeletionOutbox)
      .where(eq(skillBundleDeletionOutbox.skillVersionId, version!.id));
    expect(pendingDeletes).toEqual([]);

    const skillDelete = await app.inject({
      method: 'DELETE',
      url: `/v1/skills/${created.body.id}`,
      headers: authHeaders(),
    });
    expect(skillDelete.statusCode, skillDelete.body).toBe(200);
  });

  it('requires deleting every public version before soft-deleting an unbound Skill', async () => {
    const before = skillStore.size();
    const name = uniqueName('delete');
    const created = await createSkill({
      root: name,
      name,
      description: 'Unbound bundle',
      body: 'Delete this bundle.',
    });
    expect(created.status).toBe(200);
    expect(skillStore.size()).toBe(before + 1);

    const blocked = await app.inject({
      method: 'DELETE',
      url: `/v1/skills/${created.body.id}`,
      headers: authHeaders(),
    });
    expect(blocked.statusCode).toBe(400);
    expect(blocked.json().error).toMatchObject({ type: 'invalid_request_error' });

    const versionDeleted = await app.inject({
      method: 'DELETE',
      url: `/v1/skills/${created.body.id}/versions/${created.body.latest_version}`,
      headers: authHeaders(),
    });
    expect(versionDeleted.statusCode).toBe(200);
    expect((await loadSkillVersionRows(created.body.id))[0]!.deletedAt).toBeInstanceOf(Date);

    const noLatest = await app.inject({
      method: 'GET',
      url: `/v1/skills/${created.body.id}`,
      headers: authHeaders(),
    });
    expect(noLatest.statusCode).toBe(200);
    expect(noLatest.json().latest_version).toBeNull();

    const deleted = await app.inject({
      method: 'DELETE',
      url: `/v1/skills/${created.body.id}`,
      headers: authHeaders(),
    });
    expect(deleted.statusCode).toBe(200);
    expect(deleted.json()).toEqual({ id: created.body.id, type: 'skill_deleted' });
    expect(skillStore.size()).toBe(before + 1);
  });

  it('retains a deleted version even when a stale deletion-outbox entry is reconciled', async () => {
    const before = skillStore.size();
    const name = uniqueName('soft-delete');
    const created = await createSkill({
      root: name,
      name,
      description: 'Retained bundle',
      body: 'Keep the bytes.',
    });
    const [version] = await loadSkillVersionRows(created.body.id);
    const deleted = await app.inject({
      method: 'DELETE',
      url: `/v1/skills/${created.body.id}/versions/${created.body.latest_version}`,
      headers: authHeaders(),
    });
    expect(deleted.statusCode).toBe(200);
    await db
      .insert(skillBundleDeletionOutbox)
      .values({ workspaceId, skillVersionId: version!.id, packageSha256: version!.packageSha256 });
    const result = await reconcileSkillBundleDeletionOutbox(db, skillStore, {
      skillVersionIds: [version!.id],
    });
    expect(result.deleted).toBe(0);
    expect(skillStore.size()).toBe(before + 1);
    expect((await loadSkillVersionRows(created.body.id))[0]!.deletedAt).toBeInstanceOf(Date);
  });

  it('hides a deleted version and rejects repeated deletion without purging the bundle', async () => {
    const before = skillStore.size();
    const name = uniqueName('deleted-version');
    const created = await createSkill({
      root: name,
      name,
      description: 'Retained version',
      body: 'Keep the bytes.',
    });
    const url = `/v1/skills/${created.body.id}/versions/${created.body.latest_version}`;
    expect((await app.inject({ method: 'DELETE', url, headers: authHeaders() })).statusCode).toBe(
      200,
    );
    const [deleted] = await loadSkillVersionRows(created.body.id);
    expect(deleted!.deletedAt).toBeInstanceOf(Date);
    expect((await app.inject({ method: 'GET', url, headers: authHeaders() })).statusCode).toBe(404);
    expect((await app.inject({ method: 'DELETE', url, headers: authHeaders() })).statusCode).toBe(
      404,
    );
    expect((await loadSkillVersionRows(created.body.id))[0]!.deletedAt).toEqual(deleted!.deletedAt);
    expect(skillStore.size()).toBe(before + 1);
  });

  it('waits for an unknown COMMIT before deciding whether a bundle is still referenced', async () => {
    const name = uniqueName('commit-unknown');
    const skillId = newId('skill');
    const skillVersionId = newId('skillver');
    const versionIdentifier = String(Date.now() * 1000 + 1);
    const now = new Date();
    const bundle = await skillStore.put(workspaceId, skillVersionId, [
      {
        path: 'SKILL.md',
        content: Buffer.from(
          skillMarkdown(
            name,
            'Committed metadata must protect this bundle',
            'Keep this bundle when the COMMIT acknowledgement is lost.',
          ),
        ),
      },
    ]);
    await db.insert(skills).values({
      id: skillId,
      workspaceId,
      type: 'custom',
      name,
      slug: name,
      version: 1,
      latestVersionId: null,
      description: 'Committed metadata must protect this bundle',
      displayTitle: null,
      archivedAt: null,
      createdAt: now,
      updatedAt: now,
    });

    let markMetadataInserted!: () => void;
    const metadataInserted = new Promise<void>((resolve) => {
      markMetadataInserted = resolve;
    });
    let releaseCommit!: () => void;
    const commitGate = new Promise<void>((resolve) => {
      releaseCommit = resolve;
    });
    const metadataCommit = db.transaction(async (tx) => {
      await acquireSkillBundleLifecycleLock(tx, workspaceId, skillVersionId);
      await tx.insert(skillVersions).values({
        id: skillVersionId,
        workspaceId,
        skillId,
        version: 1,
        versionIdentifier,
        name,
        description: 'Committed metadata must protect this bundle',
        directory: name,
        entrypoint: 'SKILL.md',
        packageSha256: bundle.sha256,
        packageSizeBytes: bundle.sizeBytes,
        packageManifest: bundle.files,
        createdAt: now,
      });
      markMetadataInserted();
      await commitGate;
    });
    await metadataInserted;

    // The row exists only inside the still-uncommitted metadata transaction,
    // exactly matching the interval after a client has lost the COMMIT ACK.
    await db.insert(skillBundleDeletionOutbox).values({
      workspaceId,
      skillVersionId,
      packageSha256: bundle.sha256,
    });

    const reconciliation = reconcileSkillBundleDeletionOutbox(db, skillStore, {
      skillVersionIds: [skillVersionId],
    });
    try {
      const state = await Promise.race([
        reconciliation.then(() => 'settled' as const),
        new Promise<'waiting'>((resolve) => {
          setTimeout(() => resolve('waiting'), 75);
        }),
      ]);
      expect(state).toBe('waiting');
    } finally {
      releaseCommit();
    }
    await metadataCommit;
    const reconciled = await reconciliation;

    expect(reconciled).toEqual({ processed: 0, deleted: 0, failed: 0 });
    await expect(
      skillStore.open(workspaceId, skillVersionId, bundle.sha256),
    ).resolves.toBeDefined();
    expect(await loadSkillVersionRows(skillId)).toHaveLength(1);
    const pending = await db
      .select()
      .from(skillBundleDeletionOutbox)
      .where(eq(skillBundleDeletionOutbox.skillVersionId, skillVersionId));
    expect(pending).toHaveLength(0);

    const versionDeleted = await app.inject({
      method: 'DELETE',
      url: `/v1/skills/${skillId}/versions/${versionIdentifier}`,
      headers: authHeaders(),
    });
    expect(versionDeleted.statusCode).toBe(200);
    const skillDeleted = await app.inject({
      method: 'DELETE',
      url: `/v1/skills/${skillId}`,
      headers: authHeaders(),
    });
    expect(skillDeleted.statusCode).toBe(200);
  });

  function authHeaders(): { 'x-api-key': string } {
    return { 'x-api-key': apiKey };
  }

  function uniqueName(prefix: string): string {
    sequence += 1;
    return `${prefix}-${Date.now()}-${sequence}`;
  }

  async function createSkill(input: {
    root: string;
    name: string;
    description: string;
    body: string;
    displayTitle?: string;
    extraFiles?: Array<{ path: string; content: string; type?: string }>;
  }): Promise<{ status: number; body: SkillResponse }> {
    const response = await upload(
      [
        {
          path: `${input.root}/SKILL.md`,
          content: skillMarkdown(input.name, input.description, input.body),
          type: 'text/markdown',
        },
        ...(input.extraFiles ?? []).map((file) => ({
          ...file,
          path: `${input.root}/${file.path}`,
        })),
      ],
      input.displayTitle,
    );
    return { status: response.status, body: (await response.json()) as SkillResponse };
  }

  async function createVersion(
    skillId: string,
    input: { root: string; name: string; description: string; body: string },
  ): Promise<{ status: number; body: SkillVersionResponse }> {
    const form = new FormData();
    form.append(
      'files[]',
      new Blob([skillMarkdown(input.name, input.description, input.body)], {
        type: 'text/markdown',
      }),
      `${input.root}/SKILL.md`,
    );
    const response = await fetch(`${baseURL}/v1/skills/${skillId}/versions`, {
      method: 'POST',
      headers: authHeaders(),
      body: form,
    });
    return { status: response.status, body: (await response.json()) as SkillVersionResponse };
  }

  async function upload(
    files: Array<{ path: string; content: string; type?: string }>,
    displayTitle?: string,
  ): Promise<Response> {
    const form = new FormData();
    for (const file of files) {
      form.append(
        'files[]',
        new Blob([file.content], { type: file.type ?? 'text/markdown' }),
        file.path,
      );
    }
    if (displayTitle !== undefined) form.append('display_title', displayTitle);
    return await fetch(`${baseURL}/v1/skills`, {
      method: 'POST',
      headers: authHeaders(),
      body: form,
    });
  }

  async function createSession(agentId: string, environmentId: string): Promise<string> {
    const response = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { ...authHeaders(), 'content-type': 'application/json' },
      payload: { agent_id: agentId, environment_id: environmentId },
    });
    expect(response.statusCode, response.body).toBe(200);
    return response.json().id as string;
  }

  async function loadSkillVersionRows(skillId: string) {
    return await db
      .select()
      .from(skillVersions)
      .where(and(eq(skillVersions.workspaceId, workspaceId), eq(skillVersions.skillId, skillId)))
      .orderBy(asc(skillVersions.version));
  }

  async function prepareExecution(sessionId: string): Promise<PreparedExecutionV2> {
    const response = await fetch(
      `${baseURL}/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/executions:prepare`,
      {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: '{}',
      },
    );
    expect(response.status).toBe(200);
    return (await response.json()) as PreparedExecutionV2;
  }

  function descriptorFor(
    version: typeof skillVersions.$inferSelect,
    skillId: string,
  ): PreparedExecutionV2['primary_agent']['skills'][number] {
    return {
      id: version.id,
      skill_id: skillId,
      source: 'custom',
      version_identifier: version.versionIdentifier,
      name: version.name,
      description: version.description,
      entrypoint: 'SKILL.md',
      package_sha256: version.packageSha256,
      package_size_bytes: version.packageSizeBytes,
    };
  }
});

function skillMarkdown(name: string, description: string, body: string): string {
  return ['---', `name: ${name}`, `description: ${description}`, '---', '', body].join('\n');
}

class FlakySkillStore extends InMemorySkillStore {
  private remainingDeleteFailures = 0;
  private nextDeleteGate:
    | {
        markStarted: () => void;
        release: Promise<void>;
      }
    | undefined;

  failNextDeletes(count: number): void {
    this.remainingDeleteFailures = count;
  }

  blockNextDelete(): { started: Promise<void>; release: () => void } {
    let markStarted!: () => void;
    let release!: () => void;
    const started = new Promise<void>((resolve) => {
      markStarted = resolve;
    });
    const releasePromise = new Promise<void>((resolve) => {
      release = resolve;
    });
    this.nextDeleteGate = { markStarted, release: releasePromise };
    return { started, release };
  }

  override async delete(workspaceId: string, versionId: string, sha256: string): Promise<void> {
    const gate = this.nextDeleteGate;
    if (gate) {
      this.nextDeleteGate = undefined;
      gate.markStarted();
      await gate.release;
    }
    if (this.remainingDeleteFailures > 0) {
      this.remainingDeleteFailures -= 1;
      throw new Error('simulated SkillStore delete failure');
    }
    await super.delete(workspaceId, versionId, sha256);
  }
}
