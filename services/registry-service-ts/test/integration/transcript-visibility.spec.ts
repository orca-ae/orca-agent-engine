// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/** Shared engine contract: real HTTP/store, deterministic runner frame peer, no model. */
import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { Event } from '@orca/transcript-store';
import { applyPostgresTranscriptMigrations, PostgresTranscriptStore } from '@orca/transcript-store';
import {
  getTestDb,
  closeTestDb,
  buildTestJwtMinter,
  buildStubFileStore,
  STUB_SSE_CONFIG,
} from './setup.js';
import {
  createTestApiKey,
  createTestAgent,
  createTestSession,
  uniqueWorkspace,
} from './fixtures.js';
import { buildCombinedTestApp } from '../../src/server.js';
import { httpEventToProto } from '../../src/domain/events.js';
import { indexTranscriptEventsFromStore } from '../../src/events/session-events-index.js';
import { entryToEvent as separateEntry } from '../../../harness-server/src/harness/claude/event-mapper.js';
import { entryToEvent as runnerEntry } from '../../../session-runner/src/harness/claude/event-mapper.js';
import { streamRunnerEvents, visibilityTranscriptStore } from './transcript-visibility-helpers.js';

interface PublicEvent {
  id: string;
  type: string;
  content?: unknown;
}
interface Page {
  data: PublicEvent[];
  next_page: string | null;
}

describe.each(
  [
    { engine: 'separate', entryToEvent: separateEntry },
    { engine: 'runner/colocated', entryToEvent: runnerEntry },
  ].flatMap((engine) => ['claude', 'orca'].map((wire) => ({ ...engine, wire }))),
)('transcript visibility: $engine / $wire', ({ engine, entryToEvent, wire }) => {
  let database: Awaited<ReturnType<typeof getTestDb>>;
  let transcript: Awaited<ReturnType<typeof visibilityTranscriptStore>>;
  let app: ReturnType<typeof buildCombinedTestApp>;
  let baseURL: string;
  let apiKey: string;
  let agentId: string;
  const workspaceId = uniqueWorkspace('visibility');

  beforeAll(async () => {
    database = await getTestDb();
    transcript = await visibilityTranscriptStore();
    apiKey = await createTestApiKey(database.db, workspaceId);
    app = buildCombinedTestApp({
      db: database.db,
      store: transcript.store,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      sse: { ...STUB_SSE_CONFIG, heartbeatMs: 100 },
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
    });
    baseURL = await app.listen({ host: '127.0.0.1', port: 0 });
    agentId = await createTestAgent(baseURL, apiKey);
  }, 30_000);

  afterAll(async () => {
    await app?.close();
    if (database)
      await database.pool.query('DELETE FROM session_events_index WHERE workspace_id = $1', [
        workspaceId,
      ]);
    if (transcript) {
      await transcript.pool?.query('DELETE FROM transcript_events WHERE workspace_id = $1', [
        workspaceId,
      ]);
      await transcript.store.close();
      await transcript.cleanup();
    }
    await closeTestDb();
  });

  async function raw(sessionId: string): Promise<Event[]> {
    const events: Event[] = [];
    for await (const event of transcript.store.read(workspaceId, sessionId, {
      fromCursor: '',
      maxEvents: 0,
      subpath: '*',
    }))
      events.push(event);
    return events;
  }

  function event(sessionId: string, type: string): Event {
    return httpEventToProto({
      workspaceId,
      sessionId,
      producedBy: 'harness',
      idempotencyKey: '',
      input: {
        id: randomUUID(),
        type,
        ...(type === 'agent.usage'
          ? { usage: { input_tokens: 17, output_tokens: 5 } }
          : { content: [{ type: 'text', text: 'visible reply' }] }),
      },
    });
  }

  function internal(sessionId: string): Event {
    return entryToEvent({
      workspaceId,
      sessionId,
      subpath: '',
      producedBy: 'harness',
      entry: { type: 'assistant', uuid: randomUUID(), content: 'private SDK replay' },
    });
  }

  async function ingest(sessionId: string, events: Event[]) {
    const source = httpEventToProto({
      workspaceId,
      sessionId,
      producedBy: 'client',
      idempotencyKey: '',
      input: {
        id: randomUUID(),
        type: 'user.message',
        content: [{ type: 'text', text: 'visible reply' }],
      },
    });
    await transcript.store.append(workspaceId, sessionId, [source]);
    if (engine === 'separate') await transcript.store.append(workspaceId, sessionId, events);
    else await streamRunnerEvents(transcript.store, workspaceId, sessionId, events);
    return source;
  }

  async function list(sessionId: string, params: URLSearchParams): Promise<Page> {
    const response = await fetch(baseURL + '/v1/sessions/' + sessionId + '/events?' + params, {
      headers: { 'x-api-key': apiKey, ...(wire === 'orca' ? { 'orca-beta': '1' } : {}) },
    });
    expect(response.status).toBe(200);
    const page = (await response.json()) as Page;
    expect(page).toHaveProperty('next_page');
    return page;
  }

  it('filters before limits/cursors and preserves public payloads, raw history and usage counters', async () => {
    const sessionId = await createTestSession(baseURL, apiKey, agentId);
    const visible = Array.from({ length: 3 }, () => event(sessionId, 'agent.message'));
    // Historical usage is seeded separately: current runner ingestion discards it.
    await transcript.store.append(workspaceId, sessionId, [event(sessionId, 'agent.usage')]);
    const source = await ingest(sessionId, [
      internal(sessionId),
      visible[0]!,
      internal(sessionId),
      event(sessionId, 'agent.usage'),
      visible[1]!,
      internal(sessionId),
      visible[2]!,
    ]);
    await indexTranscriptEventsFromStore(database.db, transcript.store, workspaceId, sessionId, {
      fromCursor: '',
    });
    await database.pool.query(
      'UPDATE sessions SET usage_input_tokens = 17, usage_output_tokens = 5 WHERE workspace_id = $1 AND id = $2',
      [workspaceId, sessionId],
    );
    // Old writers could label usage public; filtering must precede the SQL limit.
    await database.pool.query(
      "UPDATE session_events_index SET visibility = 'public' WHERE workspace_id = $1 AND session_id = $2 AND kind = 'agent.usage'",
      [workspaceId, sessionId],
    );
    const before = await raw(sessionId);
    if (transcript.pool) {
      await applyPostgresTranscriptMigrations(database.pool);
      const outside = new PostgresTranscriptStore({ pool: database.pool, closePool: false });
      const outsideEvents: Event[] = [];
      for await (const item of outside.read(workspaceId, sessionId, {
        fromCursor: '',
        maxEvents: 0,
        subpath: '*',
      }))
        outsideEvents.push(item);
      expect(outsideEvents).toEqual([]);
    }
    expect(before.filter((e) => e.kind === 'harness.claude.session_entry')).toHaveLength(3);
    expect(before.filter((e) => e.kind === 'agent.usage')).toHaveLength(
      engine === 'separate' ? 2 : 1,
    );
    const expectedIds = [source, ...visible].map((e) => e.id);
    for (const order of ['asc', 'desc']) {
      const ids: string[] = [];
      let cursor: string | null = null;
      for (let pageNumber = 0; pageNumber < 2; pageNumber++) {
        const params = new URLSearchParams({ limit: '3', order });
        if (cursor) params.set('page', cursor);
        const page = await list(sessionId, params);
        expect(page.data).toHaveLength(pageNumber === 0 ? 3 : 1);
        for (const item of page.data) {
          expect(item.type).toBe(item.id === source.id ? 'user.message' : 'agent.message');
          expect(item.content).toEqual([{ type: 'text', text: 'visible reply' }]);
        }
        ids.push(...page.data.map((e) => e.id));
        cursor = page.next_page;
        if (pageNumber === 0) expect(cursor).toBeTruthy();
        else expect(cursor).toBeNull();
      }
      expect(ids).toEqual(order === 'asc' ? expectedIds : [...expectedIds].reverse());
      const hidden = before.filter((e) => e.kind === 'harness.claude.session_entry')[1]!;
      const resumed = await list(
        sessionId,
        new URLSearchParams({
          limit: '3',
          order,
          page: hidden.seq + ':0:' + hidden.id,
        }),
      );
      expect(resumed.data.map((e) => e.id)).toEqual(
        order === 'asc' ? visible.slice(1).map((e) => e.id) : [visible[0]!.id, source.id],
      );
      expect(resumed.next_page).toBeNull();
    }
    for (const type of ['agent.usage', 'harness.claude.session_entry']) {
      const filtered = await list(sessionId, new URLSearchParams({ limit: '1', types: type }));
      expect(filtered).toMatchObject({ data: [], next_page: null });
    }
    expect(await raw(sessionId)).toEqual(before);
    const counters = await database.pool.query(
      'SELECT usage_input_tokens, usage_output_tokens FROM sessions WHERE workspace_id = $1 AND id = $2',
      [workspaceId, sessionId],
    );
    expect(counters.rows).toEqual([{ usage_input_tokens: 17, usage_output_tokens: 5 }]);
  }, 30_000);

  it.each(['live', 'history'] as const)(
    'hides replay and usage in %s SSE without losing public events',
    async (mode) => {
      const sessionId = await createTestSession(baseURL, apiKey, agentId);
      const historicalUsage = event(sessionId, 'agent.usage');
      await transcript.store.append(workspaceId, sessionId, [historicalUsage]);
      const visible = [event(sessionId, 'agent.message'), event(sessionId, 'agent.message')];
      const batch = [
        internal(sessionId),
        visible[0]!,
        event(sessionId, 'agent.usage'),
        internal(sessionId),
        visible[1]!,
      ];
      let source: Event | undefined;
      if (mode === 'history') source = await ingest(sessionId, batch);
      const abort = new AbortController();
      const response = await fetch(baseURL + '/v1/sessions/' + sessionId + '/events/stream', {
        headers: {
          'x-api-key': apiKey,
          ...(wire === 'orca' ? { 'orca-beta': '1' } : {}),
          // A bounded cursor also prevents a broker's delayed subscription from
          // missing live injection after the HTTP handshake.
          'last-event-id': String((await raw(sessionId))[0]!.seq),
        },
        signal: AbortSignal.any([abort.signal, AbortSignal.timeout(10_000)]),
      });
      expect(response.status).toBe(200);
      expect(response.headers.get('content-type')).toContain('text/event-stream');
      const reader = response.body!.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      const received: PublicEvent[] = [];
      let started = mode === 'history';
      try {
        while (received.length < visible.length + 1) {
          const chunk = await reader.read();
          if (chunk.done) throw new Error('SSE ended before all public events arrived');
          buffer += decoder.decode(chunk.value, { stream: true });
          // Open the live HTTP stream before producing its visible events.
          if (!started && buffer.includes('\n\n')) {
            started = true;
            source = await ingest(sessionId, batch);
          }
          let boundary: number;
          while ((boundary = buffer.indexOf('\n\n')) >= 0) {
            const frame = buffer.slice(0, boundary);
            buffer = buffer.slice(boundary + 2);
            for (const line of frame.split('\n')) {
              if (!line.startsWith('data: ')) continue;
              const item = JSON.parse(line.slice(6)) as PublicEvent;
              expect(item.type).toBe(item.id === source!.id ? 'user.message' : 'agent.message');
              expect(item.content).toEqual([{ type: 'text', text: 'visible reply' }]);
              received.push(item);
            }
          }
        }
        expect(received.map((e) => e.id)).toEqual([source!, ...visible].map((e) => e.id));
      } finally {
        try {
          await reader.cancel();
        } finally {
          abort.abort();
        }
      }
      expect((await raw(sessionId)).some((e) => e.id === historicalUsage.id)).toBe(true);
    },
    30_000,
  );
});
