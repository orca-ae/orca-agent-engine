// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { eq } from 'drizzle-orm';
import { buildCombinedTestApp } from '../../src/server.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import { sessions } from '../../src/persistence/postgres/schema.js';
import { createTestApiKey, uniqueWorkspace } from './fixtures.js';
import {
  buildStubFileStore,
  buildStubStore,
  buildTestJwtMinter,
  closeTestDb,
  getTestDb,
  STUB_SSE_CONFIG,
} from './setup.js';

type Workspace = { apiKey: string; agentId: string; environmentId: string };
type SessionPage = {
  data: Array<{ id: string; metadata: Record<string, string> }>;
  next_page: string | null;
  prev_page: string | null;
};

describe('Session metadata filtering (integration)', () => {
  let app: FastifyInstance;
  let db: DbClient;
  let workspace: Workspace;
  let otherWorkspace: Workspace;
  let otherAgentId: string;
  const triggerName = 'local-trigger-resource';
  const otherTriggerName = 'another-local-trigger';
  const specialTriggerName = "external ' OR true -- & + % / 中文";
  const targetSessionIds: string[] = [];
  let archivedSessionId: string;
  let otherAgentSessionId: string;
  let foreignSessionId: string;
  let emptyMetadataSessionId: string;
  let specialSessionId: string;
  let sequence = 0;

  async function createAgent(apiKey: string, name: string): Promise<string> {
    const response = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey },
      payload: { name, model: { id: 'claude-opus-4-6' } },
    });
    expect(response.statusCode, response.body).toBe(200);
    return response.json().id;
  }

  async function createWorkspace(name: string): Promise<Workspace> {
    const apiKey = await createTestApiKey(db, uniqueWorkspace(name));
    const agentId = await createAgent(apiKey, name);
    const response = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { 'x-api-key': apiKey },
      payload: { name, config: { type: 'cloud' } },
    });
    expect(response.statusCode, response.body).toBe(200);
    return { apiKey, agentId, environmentId: response.json().id };
  }

  async function createSession(
    owner: Workspace,
    trigger?: string,
    metadata: Record<string, string> = {},
  ): Promise<string> {
    // This is the ordinary Session create path used by an external Trigger runner.
    // No Orca Trigger or fire record is created.
    const response = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': owner.apiKey },
      payload: {
        agent: owner.agentId,
        environment_id: owner.environmentId,
        metadata: { ...metadata, ...(trigger === undefined ? {} : { AGENT_TRIGGER: trigger }) },
      },
    });
    expect(response.statusCode, response.body).toBe(200);
    const id = response.json().id as string;
    await db
      .update(sessions)
      .set({ createdAt: new Date(Date.UTC(2026, 0, 1, 0, 0, sequence++)) })
      .where(eq(sessions.id, id));
    return id;
  }

  async function list(
    query: Record<string, string>,
    owner = workspace,
    prefix = '/v1',
    orcaBeta = false,
  ): Promise<SessionPage> {
    const response = await app.inject({
      method: 'GET',
      url: `${prefix}/sessions?${new URLSearchParams(query)}`,
      headers: {
        'x-api-key': owner.apiKey,
        ...(orcaBeta ? { 'orca-beta': 'managed-agents-2026-04-01' } : {}),
      },
    });
    expect(response.statusCode, response.body).toBe(200);
    return response.json();
  }

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
    workspace = await createWorkspace('external-trigger-sessions');
    otherWorkspace = await createWorkspace('external-trigger-sessions-other');
    otherAgentId = await createAgent(workspace.apiKey, 'another-agent');
    // Interleaving two Triggers on one Agent catches filtering after LIMIT.
    for (let index = 0; index < 3; index++) {
      targetSessionIds.push(await createSession(workspace, triggerName));
      await createSession(workspace, otherTriggerName);
    }
    archivedSessionId = await createSession(workspace, triggerName);
    const archived = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${archivedSessionId}/archive`,
      headers: { 'x-api-key': workspace.apiKey },
      payload: {},
    });
    expect(archived.statusCode, archived.body).toBe(200);
    otherAgentSessionId = await createSession({ ...workspace, agentId: otherAgentId }, triggerName);
    foreignSessionId = await createSession(otherWorkspace, triggerName);
    emptyMetadataSessionId = await createSession(workspace, '');
    await createSession(workspace);
    specialSessionId = await createSession(workspace, specialTriggerName);
  }, 60000);

  afterAll(async () => {
    await app?.close();
    await closeTestDb();
  });

  it('finds ordinary Sessions without any provider Trigger or fire records in either dialect', async () => {
    const missingTrigger = await app.inject({
      method: 'GET',
      url: `/v1/triggers/${triggerName}/sessions`,
      headers: { 'x-api-key': workspace.apiKey },
    });
    expect(missingTrigger.statusCode).toBe(404);
    for (const orcaBeta of [false, true]) {
      const page = await list(
        { metadata_AGENT_TRIGGER: triggerName, agent_id: workspace.agentId },
        workspace,
        '/v1',
        orcaBeta,
      );
      expect(page.data.map((session) => session.id)).toEqual([...targetSessionIds].reverse());
      expect(page.data.every((session) => session.metadata.AGENT_TRIGGER === triggerName)).toBe(
        true,
      );
      expect(page.next_page).toBeNull();
    }
    // The parameter is opt-in; ordinary Agent Session listing still includes both Triggers.
    const unfiltered = await list({ agent_id: workspace.agentId });
    expect(
      unfiltered.data.some((session) => session.metadata.AGENT_TRIGGER === otherTriggerName),
    ).toBe(true);
  });

  it('filters before paging in both directions and through the core alias', async () => {
    for (const prefix of ['/v1', '/api/v1']) {
      for (const order of ['asc', 'desc']) {
        const expected = order === 'asc' ? targetSessionIds : [...targetSessionIds].reverse();
        const query = {
          agent_id: workspace.agentId,
          metadata_AGENT_TRIGGER: triggerName,
          limit: '1',
          order,
        };
        const first = await list(query, workspace, prefix);
        const second = await list({ ...query, page: first.next_page! }, workspace, prefix);
        const last = await list({ ...query, page: second.next_page! }, workspace, prefix);
        expect([first, second, last].map((page) => page.data.map((session) => session.id))).toEqual(
          expected.map((id) => [id]),
        );
        expect(last.next_page).toBeNull();
        const previous = await list({ ...query, page: last.prev_page! }, workspace, prefix);
        expect(previous.data.map((session) => session.id)).toEqual([expected[1]]);
      }
    }
  });

  it('composes with archive and Agent filters while keeping Workspaces isolated', async () => {
    const withArchived = await list({
      metadata_AGENT_TRIGGER: triggerName,
      agent_id: workspace.agentId,
      include_archived: 'true',
    });
    expect(withArchived.data.map((session) => session.id)).toEqual([
      archivedSessionId,
      ...[...targetSessionIds].reverse(),
    ]);
    const allAgents = await list({ metadata_AGENT_TRIGGER: triggerName });
    expect(allAgents.data.map((session) => session.id)).toEqual([
      otherAgentSessionId,
      ...[...targetSessionIds].reverse(),
    ]);
    const foreign = await list({ metadata_AGENT_TRIGGER: triggerName }, otherWorkspace);
    expect(foreign.data.map((session) => session.id)).toEqual([foreignSessionId]);
  });

  it('rejects cursors outside the filtered Trigger, Agent or Workspace', async () => {
    const first = await list({
      metadata_AGENT_TRIGGER: triggerName,
      agent_id: workspace.agentId,
      limit: '1',
    });
    expect(first.next_page).toEqual(expect.any(String));
    for (const [owner, trigger, agent] of [
      [workspace, otherTriggerName, workspace.agentId],
      [workspace, triggerName, otherAgentId],
      [otherWorkspace, triggerName, otherWorkspace.agentId],
    ] as const) {
      const response = await app.inject({
        method: 'GET',
        url: `/v1/sessions?${new URLSearchParams({
          metadata_AGENT_TRIGGER: trigger,
          agent_id: agent,
          page: first.next_page!,
        })}`,
        headers: { 'x-api-key': owner.apiKey },
      });
      expect(response.statusCode, response.body).toBe(400);
      expect(response.json().error.message).toBe('invalid page');
    }
  });

  it('matches empty and escaped values exactly without treating absent keys as empty', async () => {
    const empty = await list({ metadata_AGENT_TRIGGER: '' });
    expect(empty.data.map((session) => session.id)).toEqual([emptyMetadataSessionId]);
    const special = await list({ metadata_AGENT_TRIGGER: specialTriggerName });
    expect(special.data.map((session) => session.id)).toEqual([specialSessionId]);
    const partial = await list({ metadata_AGENT_TRIGGER: 'local-trigger' });
    expect(partial.data).toEqual([]);
    expect(partial.next_page).toBeNull();
  });

  it('combines arbitrary metadata keys with AND before pagination and cursor validation', async () => {
    const expected = [];
    for (let index = 0; index < 3; index++) {
      expected.push(await createSession(workspace, undefined, { team: 'ops', area: 'backend' }));
      await createSession(workspace, undefined, { team: 'ops', area: 'frontend' });
      await createSession(workspace, undefined, { team: 'dev', area: 'backend' });
    }
    await createSession(workspace, undefined, { team: 'ops' });
    for (const order of ['asc', 'desc']) {
      const query = { metadata_team: 'ops', metadata_area: 'backend', limit: '1', order };
      const first = await list(query);
      const second = await list({ ...query, page: first.next_page! });
      const last = await list({ ...query, page: second.next_page! });
      expect([first, second, last].map((page) => page.data[0]!.id)).toEqual(
        order === 'asc' ? expected : [...expected].reverse(),
      );
      expect(last.next_page).toBeNull();
      const previous = await list({ ...query, page: last.prev_page! });
      expect(previous.data[0]!.id).toBe(expected[1]);
      const invalidCursor = await app.inject({
        method: 'GET',
        url: `/v1/sessions?${new URLSearchParams({
          ...query,
          metadata_area: 'frontend',
          page: first.next_page!,
        })}`,
        headers: { 'x-api-key': workspace.apiKey },
      });
      expect(invalidCursor.statusCode, invalidCursor.body).toBe(400);
    }
    const mixed = await list({ metadata_AGENT_TRIGGER: triggerName, metadata_team: 'ops' });
    expect(mixed.data).toEqual([]);
  });

  it('treats arbitrary keys and values as literal strings, including SQL and URL metacharacters', async () => {
    const key = "team.' OR true -- & + % / 中文";
    const metadata = { [key]: specialTriggerName, 'team.name': '', Team: 'Ops' };
    const id = await createSession(workspace, undefined, metadata);
    const query = Object.fromEntries(
      Object.entries(metadata).map(([k, v]) => [`metadata_${k}`, v]),
    );
    expect((await list(query)).data.map((session) => session.id)).toEqual([id]);
    expect((await list({ metadata_Team: 'ops' })).data).toEqual([]);
    expect((await list({ metadata_missing: '' })).data).toEqual([]);
    // These object-property names must remain predicates, never disappear during parsing.
    for (const name of ['__proto__', 'constructor', 'toString']) {
      expect((await list({ [`metadata_${name}`]: 'missing' })).data).toEqual([]);
    }
    const maxMetadata = Object.fromEntries(
      Array.from({ length: 16 }, (_, index) => [`${index}`.padEnd(64, 'k'), 'v'.repeat(512)]),
    );
    const maxId = await createSession(workspace, undefined, maxMetadata);
    const maxQuery = Object.fromEntries(
      Object.entries(maxMetadata).map(([k, v]) => [`metadata_${k}`, v]),
    );
    expect((await list(maxQuery)).data.map((session) => session.id)).toEqual([maxId]);
  });

  it('rejects repeated, empty, oversized or excessive metadata filters', async () => {
    for (const query of [
      'metadata_AGENT_TRIGGER=a&metadata_AGENT_TRIGGER=b',
      'metadata_team=a&metadata_team=a',
      'metadata_=value',
      `metadata_${'k'.repeat(65)}=value`,
      `metadata_team=${'v'.repeat(513)}`,
      new URLSearchParams(
        Array.from({ length: 17 }, (_, index) => [`metadata_k${index}`, 'value']),
      ).toString(),
    ]) {
      const response = await app.inject({
        method: 'GET',
        url: `/v1/sessions?${query}`,
        headers: { 'x-api-key': workspace.apiKey },
      });
      expect(response.statusCode, response.body).toBe(400);
      expect(response.json().error.type).toBe('invalid_request_error');
      expect(response.json().error.message).toContain('metadata filters');
    }
  });
});
