// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import Fastify from 'fastify';
import { expect, it, vi } from 'vitest';
import { PgDialect } from 'drizzle-orm/pg-core';
import { InMemorySkillStore } from '@orca/skill-store';
import type { Event, TranscriptStore } from '@orca/transcript-store';
import type { FileStore } from '@orca/file-store';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import { sessionThreads, sessions } from '../../src/persistence/postgres/schema.js';
import { registerSessionsRoutes } from '../../src/api/sessions.routes.js';

vi.mock('../../src/events/session-events-index.js', async (original) => ({
  ...(await original<Record<string, unknown>>()),
  indexTranscriptEvents: vi.fn(async () => {}),
}));

it('actual interrupt/archive HTTP routes persist scoped control intent IDs and deny archived follow-up', async () => {
  const now = new Date();
  const thread = {
    id: 'sth_child',
    workspaceId: 'ws_test',
    sessionId: 'ses_test',
    subpath: 'subagents/11111111-1111-4111-8111-111111111111',
    parentThreadId: 'sth_root',
    agentId: 'agt_test',
    agentVersion: 1,
    agentName: 'child',
    status: 'terminated',
    archivedAt: null as Date | null,
    createdAt: now,
    updatedAt: now,
  };
  const session = {
    id: 'ses_test',
    workspaceId: 'ws_test',
    agentId: 'agt_test',
    agentVersion: 1,
    status: 'idle',
    archivedAt: null,
    createdAt: now,
    updatedAt: now,
  };
  const queries: Array<{ sql: string; params: unknown[] }> = [];
  const db = {
    select: () => ({
      from: (table: unknown) => ({
        where: (where: never) => {
          queries.push(new PgDialect().sqlToQuery(where));
          return {
            limit: async () =>
              table === sessions ? [session] : table === sessionThreads ? [thread] : [],
          };
        },
      }),
    }),
    update: () => ({
      set: (fields: Partial<typeof thread>) => ({
        where: async () => {
          Object.assign(thread, fields);
        },
      }),
    }),
  } as unknown as DbClient;
  const appended: Event[] = [];
  const store = {
    append: vi.fn(async (_ws, _session, events: Event[]) => {
      appended.push(...events);
      return events.map((e) => e.id);
    }),
  } as unknown as TranscriptStore;
  const app = Fastify();
  app.addHook('onRequest', async (request) => {
    request.auth = { workspaceId: 'ws_test', principal: 'test', scopes: [], authMethod: 'api-key' };
  });
  registerSessionsRoutes(
    app,
    db,
    store,
    { bufferSize: 1, dropAgeMs: 1, heartbeatMs: 1 },
    {} as FileStore,
    new InMemorySkillStore(),
  );
  try {
    for (const operation of ['interrupt', 'archive']) {
      const reply = await app.inject({
        method: 'POST',
        url: `/v1/sessions/ses_test/threads/sth_child/${operation}`,
      });
      expect(reply.statusCode).toBe(200);
      expect(reply.json()).toMatchObject({ id: 'sth_child', status: 'terminated' });
      const intent = appended.at(-1)!;
      expect(intent).toMatchObject({
        workspaceId: 'ws_test',
        sessionId: 'ses_test',
        subpath: thread.subpath,
        producedBy: 'registry',
        kind: 'session.thread_status_terminated',
      });
      expect(intent.id).toMatch(/^evt_/);
      expect(JSON.parse(Buffer.from(intent.payload).toString())).toMatchObject({
        session_thread_id: thread.id,
        stop_reason: operation === 'archive' ? 'archived' : 'interrupted',
      });
    }
    expect(new Set(appended.map((e) => e.id)).size).toBe(2);
    const denied = await app.inject({
      method: 'POST',
      url: '/v1/sessions/ses_test/events',
      headers: { 'orca-beta': 'managed-agents-2026-04-01' },
      payload: {
        events: [
          {
            type: 'user.message',
            session_thread_id: thread.id,
            content: [{ type: 'text', text: 'continue' }],
          },
        ],
      },
    });
    expect(denied.statusCode).toBe(400);
    expect(denied.json().error).toContain('archived');
    expect(appended).toHaveLength(2);
    for (const query of queries.filter((q) => q.sql.includes('session_threads'))) {
      expect(query.params).toContain('ws_test');
      expect(query.params).toContain('ses_test');
    }
  } finally {
    await app.close();
  }
});
