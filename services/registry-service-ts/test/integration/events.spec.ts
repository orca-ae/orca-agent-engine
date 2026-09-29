// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { and, eq } from 'drizzle-orm';
import type { FastifyInstance } from 'fastify';
import type { AddressInfo } from 'node:net';
import {
  getTestDb,
  closeTestDb,
  buildTestStore,
  STUB_SSE_CONFIG,
  buildTestJwtMinter,
  buildStubFileStore,
} from './setup.js';
import {
  uniqueWorkspace,
  createTestApiKey,
  createTestAgent,
  createTestSession,
} from './fixtures.js';
import { buildCombinedTestApp } from '../../src/server.js';
import type { Event, KafkaTranscriptStore } from '@orca/transcript-store';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import {
  backfillSessionEventProjections,
  indexTranscriptEvents,
} from '../../src/events/session-events-index.js';
import { sessionEventsIndex } from '../../src/persistence/postgres/schema.js';

describe('events sub-paths (integration)', () => {
  let app: FastifyInstance;
  let baseURL: string;
  let apiKey: string;
  let workspaceId: string;
  let store: KafkaTranscriptStore;
  let db: DbClient;

  beforeAll(async () => {
    store = buildTestStore();
    ({ db } = await getTestDb());
    app = buildCombinedTestApp({
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store,
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
    });
    await app.listen({ host: '127.0.0.1', port: 0 });
    baseURL = `http://127.0.0.1:${(app.server.address() as AddressInfo).port}`;
    workspaceId = uniqueWorkspace('events');
    apiKey = await createTestApiKey(db, workspaceId);
  }, 30000);

  afterAll(async () => {
    await app.close();
    await store.close();
    await closeTestDb();
  });

  it('POST events then GET events round-trips', async () => {
    const agentId = await createTestAgent(baseURL, apiKey);
    const sessionId = await createTestSession(baseURL, apiKey, agentId);

    const post = await fetch(`${baseURL}/v1/sessions/${sessionId}/events`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        events: [
          { type: 'user.message', content: [{ type: 'text', text: 'hi' }] },
          { type: 'user.message', content: [{ type: 'text', text: 'still hi' }] },
        ],
      }),
    });
    expect(post.status).toBe(200);
    const posted = (await post.json()) as { data: Array<{ id: string }> };
    expect(posted.data).toHaveLength(2);

    const get = await fetch(`${baseURL}/v1/sessions/${sessionId}/events`, {
      headers: { 'x-api-key': apiKey },
    });
    expect(get.status).toBe(200);
    const got = (await get.json()) as {
      data: Array<{ id: string; type: string }>;
      next_page: string | null;
    };
    expect(got.data.map((e) => e.id)).toEqual(posted.data.map((e) => e.id));
    expect(got.data.every((e) => e.type === 'user.message')).toBe(true);
  }, 30000);

  it('keeps the POST snapshot queued while GET reflects a later acceptance marker', async () => {
    const agentId = await createTestAgent(baseURL, apiKey);
    const sessionId = await createTestSession(baseURL, apiKey, agentId);
    const post = await fetch(`${baseURL}/v1/sessions/${sessionId}/events`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        events: [{ type: 'user.message', content: [{ type: 'text', text: 'queued' }] }],
      }),
    });
    expect(post.status).toBe(200);
    const posted = (await post.json()) as {
      data: Array<{ id: string; processed_at: string | null }>;
    };
    const userEventId = posted.data[0]!.id;
    expect(posted.data[0]!.processed_at).toBeNull();

    const markerAt = '2026-07-15T01:00:03.000Z';
    await indexTranscriptEvents(db, [
      {
        id: `evt_api_marker_${Date.now()}`,
        workspaceId,
        sessionId,
        subpath: '',
        seq: Date.now(),
        producedAt: markerAt,
        producedBy: 'harness',
        kind: 'session.user_event_processed',
        payload: Buffer.from(JSON.stringify({ user_event_id: userEventId })),
        idempotencyKey: '',
      },
    ]);

    const get = await fetch(`${baseURL}/v1/sessions/${sessionId}/events`, {
      headers: { 'x-api-key': apiKey },
    });
    expect(get.status).toBe(200);
    const listed = (await get.json()) as {
      data: Array<{ id: string; processed_at: string | null }>;
    };
    expect(listed.data.find((event) => event.id === userEventId)?.processed_at).toBe(markerAt);
    expect(posted.data[0]!.processed_at).toBeNull();
  }, 30000);

  it('reconciles acceptance markers projected before their event and keeps the earliest marker', async () => {
    const agentId = await createTestAgent(baseURL, apiKey);
    const sessionId = await createTestSession(baseURL, apiKey, agentId);
    const userEventId = `evt_processed_${Date.now()}`;
    const baseSeq = Date.now();
    const clientEvent: Event = {
      id: userEventId,
      workspaceId,
      sessionId,
      subpath: '',
      seq: baseSeq,
      producedAt: '2026-07-15T01:00:00.000Z',
      producedBy: 'client',
      kind: 'user.message',
      payload: Buffer.from(
        JSON.stringify({
          type: 'user.message',
          content: [{ type: 'text', text: 'accept me' }],
          processed_at: null,
        }),
      ),
      idempotencyKey: '',
    };
    const spoofedSourceEvent: Event = {
      ...clientEvent,
      id: `evt_spoofed_source_${Date.now()}`,
      seq: baseSeq + 3,
      payload: Buffer.from(
        JSON.stringify({
          type: 'user.message',
          content: [{ type: 'text', text: 'not accepted' }],
          processed_at: null,
          source_event_id: userEventId,
        }),
      ),
    };
    const marker = (id: string, seq: number, producedAt: string): Event => ({
      id,
      workspaceId,
      sessionId,
      subpath: '',
      seq,
      producedAt,
      producedBy: 'harness',
      kind: 'session.user_event_processed',
      payload: Buffer.from(JSON.stringify({ user_event_id: userEventId })),
      idempotencyKey: '',
    });

    await indexTranscriptEvents(db, [
      marker(`evt_marker_late_${Date.now()}`, baseSeq + 2, '2026-07-15T01:00:02.000Z'),
    ]);
    await indexTranscriptEvents(db, [clientEvent, spoofedSourceEvent]);

    let [indexed] = await db
      .select()
      .from(sessionEventsIndex)
      .where(
        and(
          eq(sessionEventsIndex.workspaceId, workspaceId),
          eq(sessionEventsIndex.sessionId, sessionId),
          eq(sessionEventsIndex.eventId, userEventId),
        ),
      );
    expect(indexed?.processedAt).toBe('2026-07-15T01:00:02.000Z');
    expect(indexed?.processedMarkerSeq).toBe(baseSeq + 2);

    await indexTranscriptEvents(db, [
      marker(`evt_marker_early_${Date.now()}`, baseSeq + 1, '2026-07-15T01:00:01.000Z'),
    ]);
    [indexed] = await db
      .select()
      .from(sessionEventsIndex)
      .where(
        and(
          eq(sessionEventsIndex.workspaceId, workspaceId),
          eq(sessionEventsIndex.sessionId, sessionId),
          eq(sessionEventsIndex.eventId, userEventId),
        ),
      );
    expect(indexed?.processedAt).toBe('2026-07-15T01:00:01.000Z');
    expect(indexed?.processedMarkerSeq).toBe(baseSeq + 1);

    const [spoofed] = await db
      .select()
      .from(sessionEventsIndex)
      .where(
        and(
          eq(sessionEventsIndex.workspaceId, workspaceId),
          eq(sessionEventsIndex.sessionId, sessionId),
          eq(sessionEventsIndex.eventId, spoofedSourceEvent.id),
        ),
      );
    expect(spoofed?.processedAt).toBeNull();
    expect(spoofed?.processedMarkerSeq).toBeNull();
  }, 30000);

  it('rejects client-authored harness replay events', async () => {
    const agentId = await createTestAgent(baseURL, apiKey);
    const sessionId = await createTestSession(baseURL, apiKey, agentId);

    const post = await fetch(`${baseURL}/v1/sessions/${sessionId}/events`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        events: [
          {
            type: 'harness.claude.session_entry',
            sdk_entry: { type: 'assistant', content: [] },
          },
        ],
      }),
    });

    expect(post.status).toBe(400);
    expect(await post.json()).toMatchObject({
      type: 'error',
      error: { type: 'invalid_request_error', message: 'Invalid input' },
      request_id: expect.any(String),
    });

    const threadLifecycle = await fetch(`${baseURL}/v1/sessions/${sessionId}/events`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        events: [{ type: 'session.thread_status_terminated' }],
      }),
    });

    expect(threadLifecycle.status).toBe(400);
    expect(await threadLifecycle.json()).toMatchObject({
      type: 'error',
      error: { type: 'invalid_request_error', message: 'Invalid input' },
      request_id: expect.any(String),
    });
  }, 30000);

  it('keeps legacy client events and public extensions behind orca-beta', async () => {
    const agentId = await createTestAgent(baseURL, apiKey);
    const sessionId = await createTestSession(baseURL, apiKey, agentId);

    const events = [
      { type: 'user.message', content: [{ type: 'text', text: 'hi' }] },
      { type: 'user.interrupt' },
      { type: 'user.tool_confirmation', tool_use_id: 'toolu_1', approved: true },
      { type: 'user.tool_confirmation', tool_use_id: 'toolu_1b', result: 'allow' },
      {
        type: 'user.tool_confirmation',
        tool_use_id: 'toolu_1c',
        result: 'deny',
        deny_message: 'no',
      },
      {
        type: 'user.custom_tool_result',
        tool_use_id: 'toolu_2',
        content: [{ type: 'text', text: 'ok' }],
      },
      {
        type: 'user.define_outcome',
        description: 'all set',
        rubric: 'the agent must complete the task',
      },
      { type: 'orca.extension_event', payload: { ok: true } },
    ];
    const post = await fetch(`${baseURL}/v1/sessions/${sessionId}/events`, {
      method: 'POST',
      headers: {
        'x-api-key': apiKey,
        'content-type': 'application/json',
        'orca-beta': 'legacy-events',
      },
      body: JSON.stringify({ events }),
    });
    expect(post.status).toBe(200);
    const body = (await post.json()) as {
      events: Array<{
        type: string;
        processed_at: string | null;
        outcome_id?: string;
        max_iterations?: number;
      }>;
    };
    expect(body.events.map((e) => e.type)).toEqual(events.map((e) => e.type));
    const definition = body.events.find((event) => event.type === 'user.define_outcome');
    expect(definition).toMatchObject({
      processed_at: expect.any(String),
      rubric: 'the agent must complete the task',
    });
    expect(body.events.at(-1)?.processed_at).toEqual(expect.any(String));

    const listed = await fetch(`${baseURL}/v1/sessions/${sessionId}/events`, {
      headers: { 'x-api-key': apiKey },
    });
    expect(listed.status).toBe(200);
    const listedEvents = (await listed.json()) as { data: Array<{ type: string }> };
    expect(listedEvents.data.map((event) => event.type)).not.toContain('orca.extension_event');
  }, 30000);

  it('correlates a companion system.message without exposing internal metadata', async () => {
    const agentId = await createTestAgent(baseURL, apiKey);
    const sessionId = await createTestSession(baseURL, apiKey, agentId);
    const post = await fetch(`${baseURL}/v1/sessions/${sessionId}/events`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        events: [
          { type: 'user.message', content: [{ type: 'text', text: 'diagnose this' }] },
          {
            type: 'system.message',
            content: [{ type: 'text', text: 'Use production safeguards.' }],
          },
        ],
      }),
    });
    expect(post.status).toBe(200);
    const posted = (await post.json()) as {
      data: Array<Record<string, unknown> & { id: string; type: string }>;
    };
    expect(posted.data.map((event) => event.type)).toEqual(['user.message', 'system.message']);
    expect(posted.data[0]).not.toHaveProperty('_orca_companion_system_event_id');

    const rawEvents: Event[] = [];
    for await (const event of store.read(workspaceId, sessionId, {
      fromCursor: '',
      maxEvents: 0,
      subpath: '*',
    })) {
      rawEvents.push(event);
    }
    const rawUser = rawEvents.find((event) => event.id === posted.data[0]!.id);
    expect(rawUser).toBeDefined();
    expect(JSON.parse(Buffer.from(rawUser!.payload).toString('utf8'))).toMatchObject({
      _orca_companion_system_event_id: posted.data[1]!.id,
    });

    const listed = await fetch(`${baseURL}/v1/sessions/${sessionId}/events`, {
      headers: { 'x-api-key': apiKey },
    });
    expect(listed.status).toBe(200);
    const listedUser = (
      (await listed.json()) as { data: Array<Record<string, unknown> & { id: string }> }
    ).data.find((event) => event.id === posted.data[0]!.id);
    expect(listedUser).not.toHaveProperty('_orca_companion_system_event_id');
  }, 30000);

  it('rejects malformed or unsupported known client session events', async () => {
    const agentId = await createTestAgent(baseURL, apiKey);
    const sessionId = await createTestSession(baseURL, apiKey, agentId);

    for (const event of [
      { type: 'user.message', content: [] },
      { type: 'user.tool_confirmation', tool_use_id: 'toolu_1' },
      { type: 'user.tool_confirmation', tool_use_id: 'toolu_1', result: 'maybe' },
      { type: 'user.custom_tool_result', content: [{ type: 'text', text: 'missing id' }] },
      { type: 'user.message', content: [{ type: 'text' }] },
      { type: 'user.define_outcome', description: '', rubric: 'missing description' },
      { type: 'user.define_outcome', description: 'missing rubric' },
      { type: 'user.define_outcome', description: 'bad max', rubric: 'ok', max_iterations: 21 },
      { type: 'user.define_outcome', description: 'legacy rubric', rubric: 'legacy string' },
      { type: 'orca.internal_event', payload: {} },
      { type: 'system.message', content: [{ type: 'text', text: 'unsupported for now' }] },
    ]) {
      const post = await fetch(`${baseURL}/v1/sessions/${sessionId}/events`, {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({ events: [event] }),
      });
      expect(post.status).toBe(400);
    }
  }, 30000);

  it('keeps legacy event shapes behind orca-beta', async () => {
    const agentId = await createTestAgent(baseURL, apiKey);
    const sessionId = await createTestSession(baseURL, apiKey, agentId);
    const post = await fetch(`${baseURL}/v1/sessions/${sessionId}/events`, {
      method: 'POST',
      headers: {
        'x-api-key': apiKey,
        'content-type': 'application/json',
        'orca-beta': 'legacy-events',
      },
      body: JSON.stringify({ events: [{ type: 'orca.internal_event', payload: {} }] }),
    });

    expect(post.status).toBe(200);
  }, 30000);

  it('rejects known internal session events from clients', async () => {
    const agentId = await createTestAgent(baseURL, apiKey);
    const sessionId = await createTestSession(baseURL, apiKey, agentId);

    const post = await fetch(`${baseURL}/v1/sessions/${sessionId}/events`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ events: [{ type: 'session.deferred_user_message' }] }),
    });

    expect(post.status).toBe(400);
    expect(await post.json()).toMatchObject({
      type: 'error',
      error: { type: 'invalid_request_error', message: 'Invalid input' },
      request_id: expect.any(String),
    });
  }, 30000);

  it('rejects malformed POST events with 400', async () => {
    const agentId = await createTestAgent(baseURL, apiKey);
    const sessionId = await createTestSession(baseURL, apiKey, agentId);

    const post = await fetch(`${baseURL}/v1/sessions/${sessionId}/events`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ events: [{ content: [{ type: 'text', text: 'missing type' }] }] }),
    });

    expect(post.status).toBe(400);
    expect(await post.json()).toMatchObject({
      type: 'error',
      error: { type: 'invalid_request_error', message: 'Invalid input' },
      request_id: expect.any(String),
    });
  }, 30000);

  it('normalizes bad limit values and rejects invalid page cursors', async () => {
    const agentId = await createTestAgent(baseURL, apiKey);
    const sessionId = await createTestSession(baseURL, apiKey, agentId);

    const post = await fetch(`${baseURL}/v1/sessions/${sessionId}/events`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        events: [
          { type: 'user.message', content: [{ type: 'text', text: 'one' }] },
          { type: 'user.message', content: [{ type: 'text', text: 'two' }] },
        ],
      }),
    });
    expect(post.status).toBe(200);

    const badLimit = await fetch(`${baseURL}/v1/sessions/${sessionId}/events?limit=nan`, {
      headers: { 'x-api-key': apiKey },
    });
    expect(badLimit.status).toBe(200);
    const got = (await badLimit.json()) as { data: unknown[] };
    expect(got.data).toHaveLength(2);

    const badCursor = await fetch(`${baseURL}/v1/sessions/${sessionId}/events?page=abc`, {
      headers: { 'x-api-key': apiKey },
    });
    expect(badCursor.status).toBe(400);
  }, 30000);

  it('supports session thread list, events, archive, and thread-targeted control events', async () => {
    const agentId = await createTestAgent(baseURL, apiKey);
    const sessionId = await createTestSession(baseURL, apiKey, agentId);
    const subpath = `threads/integration-${Date.now()}`;

    const post = await fetch(`${baseURL}/v1/sessions/${sessionId}/events`, {
      method: 'POST',
      headers: {
        'x-api-key': apiKey,
        'content-type': 'application/json',
        'orca-beta': 'legacy-event-subpath',
      },
      body: JSON.stringify({
        events: [{ type: 'user.message', subpath, content: [{ type: 'text', text: 'thread hi' }] }],
      }),
    });
    expect(post.status).toBe(200);

    const listThreads = await fetch(`${baseURL}/v1/sessions/${sessionId}/threads`, {
      headers: { 'x-api-key': apiKey },
    });
    expect(listThreads.status).toBe(200);
    const threadsBody = (await listThreads.json()) as {
      data: Array<{
        id: string;
        type: string;
        status: string;
        agent: { id: string; version: number };
      }>;
      next_page: string | null;
    };
    const thread = threadsBody.data.find((candidate) => candidate.agent.id === agentId);
    expect(thread).toMatchObject({
      type: 'session_thread',
      status: 'idle',
      agent: { id: agentId, version: 1 },
    });
    expect(thread?.id).toMatch(/^sth_/);
    expect(threadsBody.next_page).toBeNull();

    const retrieveThread = await fetch(
      `${baseURL}/v1/sessions/${sessionId}/threads/${thread!.id}`,
      {
        headers: { 'x-api-key': apiKey },
      },
    );
    expect(retrieveThread.status).toBe(200);
    expect((await retrieveThread.json()) as unknown).toMatchObject({
      id: thread!.id,
      type: 'session_thread',
      agent: { id: agentId, version: 1 },
    });

    const getEvents = await fetch(
      `${baseURL}/v1/sessions/${sessionId}/threads/${thread!.id}/events`,
      {
        headers: { 'x-api-key': apiKey },
      },
    );
    expect(getEvents.status).toBe(200);
    const eventsBody = (await getEvents.json()) as {
      data: Array<{ type: string }>;
      next_page: string | null;
    };
    expect(eventsBody.data).toEqual([expect.objectContaining({ type: 'user.message' })]);
    expect(eventsBody.data[0]).not.toHaveProperty('subpath');
    expect(eventsBody.next_page).toBeNull();

    const controlEvents = await fetch(`${baseURL}/v1/sessions/${sessionId}/events`, {
      method: 'POST',
      headers: {
        'x-api-key': apiKey,
        'content-type': 'application/json',
        'orca-beta': 'legacy-thread-controls',
      },
      body: JSON.stringify({
        events: [
          { type: 'user.interrupt', session_thread_id: thread!.id },
          {
            type: 'user.tool_confirmation',
            session_thread_id: thread!.id,
            tool_use_id: 'evt_tool_pending',
            approved: true,
          },
          {
            type: 'user.custom_tool_result',
            session_thread_id: thread!.id,
            tool_use_id: 'evt_custom_pending',
            content: [{ type: 'text', text: 'done' }],
          },
        ],
      }),
    });
    expect(controlEvents.status).toBe(200);
    const controlBody = (await controlEvents.json()) as {
      events: Array<{ type: string; session_thread_id: string }>;
    };
    expect(controlBody.events).toEqual([
      expect.objectContaining({ type: 'user.interrupt', session_thread_id: thread!.id }),
      expect.objectContaining({ type: 'user.tool_confirmation', session_thread_id: thread!.id }),
      expect.objectContaining({ type: 'user.custom_tool_result', session_thread_id: thread!.id }),
    ]);

    const firstThreadEventsPage = await fetch(
      `${baseURL}/v1/sessions/${sessionId}/threads/${thread!.id}/events?limit=2`,
      {
        headers: { 'x-api-key': apiKey },
      },
    );
    expect(firstThreadEventsPage.status).toBe(200);
    const firstThreadEventsPageBody = (await firstThreadEventsPage.json()) as {
      data: Array<{ type: string }>;
      next_page: string | null;
    };
    expect(firstThreadEventsPageBody.data.map((event) => event.type)).toEqual([
      'user.message',
      'user.interrupt',
    ]);
    expect(firstThreadEventsPageBody.next_page).toMatch(/^\d+:.+/);

    const secondThreadEventsPage = await fetch(
      `${baseURL}/v1/sessions/${sessionId}/threads/${thread!.id}/events?limit=2&page=${encodeURIComponent(
        firstThreadEventsPageBody.next_page!,
      )}`,
      {
        headers: { 'x-api-key': apiKey },
      },
    );
    expect(secondThreadEventsPage.status).toBe(200);
    const secondThreadEventsPageBody = (await secondThreadEventsPage.json()) as {
      data: Array<{ type: string }>;
      next_page: string | null;
    };
    expect(secondThreadEventsPageBody.data.map((event) => event.type)).toEqual([
      'user.tool_confirmation',
      'user.custom_tool_result',
    ]);
    expect(secondThreadEventsPageBody.next_page).toBeNull();

    const archive = await fetch(
      `${baseURL}/v1/sessions/${sessionId}/threads/${thread!.id}/archive`,
      {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({}),
      },
    );
    expect(archive.status).toBe(200);
    const archiveBody = (await archive.json()) as {
      id: string;
      status: string;
      archived_at: string | null;
    };
    expect(archiveBody).toMatchObject({ id: thread!.id, status: 'terminated' });
    expect(archiveBody.archived_at).toBeTruthy();

    const archivedEvents = await fetch(
      `${baseURL}/v1/sessions/${sessionId}/threads/${thread!.id}/events`,
      {
        headers: { 'x-api-key': apiKey },
      },
    );
    expect(archivedEvents.status).toBe(200);
    const archivedEventsBody = (await archivedEvents.json()) as {
      data: Array<{ type: string; session_thread_id: string }>;
    };
    expect(archivedEventsBody.data).toContainEqual(
      expect.objectContaining({
        type: 'session.thread_status_terminated',
        session_thread_id: thread!.id,
      }),
    );
  }, 30000);

  it('interrupts a session thread — terminates it without archiving — and 404s for a missing session or thread', async () => {
    const agentId = await createTestAgent(baseURL, apiKey);
    const sessionId = await createTestSession(baseURL, apiKey, agentId);
    const subpath = `threads/interrupt-${Date.now()}`;

    const post = await fetch(`${baseURL}/v1/sessions/${sessionId}/events`, {
      method: 'POST',
      headers: {
        'x-api-key': apiKey,
        'content-type': 'application/json',
        'orca-beta': 'legacy-event-subpath',
      },
      body: JSON.stringify({
        events: [
          { type: 'user.message', subpath, content: [{ type: 'text', text: 'interrupt me' }] },
        ],
      }),
    });
    expect(post.status).toBe(200);

    const listThreads = await fetch(`${baseURL}/v1/sessions/${sessionId}/threads`, {
      headers: { 'x-api-key': apiKey },
    });
    const threadsBody = (await listThreads.json()) as {
      data: Array<{ id: string; agent: { id: string } }>;
    };
    const thread = threadsBody.data.find((candidate) => candidate.agent.id === agentId);
    expect(thread).toBeDefined();

    const interrupt = await fetch(
      `${baseURL}/v1/sessions/${sessionId}/threads/${thread!.id}/interrupt`,
      {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({}),
      },
    );
    expect(interrupt.status).toBe(200);
    const interruptBody = (await interrupt.json()) as {
      id: string;
      status: string;
      archived_at: string | null;
    };
    // Terminated — but, unlike archive, `archived_at` stays null: the thread
    // is not hidden.
    expect(interruptBody).toMatchObject({ id: thread!.id, status: 'terminated' });
    expect(interruptBody.archived_at).toBeNull();

    // The GET-by-id read reflects the same terminated-not-archived state —
    // confirms the projection update (via indexTranscriptEvents) landed, not
    // just the response echoed back from the POST.
    const retrieveAfterInterrupt = await fetch(
      `${baseURL}/v1/sessions/${sessionId}/threads/${thread!.id}`,
      { headers: { 'x-api-key': apiKey } },
    );
    expect(retrieveAfterInterrupt.status).toBe(200);
    const retrieveAfterInterruptBody = (await retrieveAfterInterrupt.json()) as {
      status: string;
      archived_at: string | null;
    };
    expect(retrieveAfterInterruptBody).toMatchObject({ status: 'terminated' });
    expect(retrieveAfterInterruptBody.archived_at).toBeNull();

    // The interrupt recorded a `session.thread_status_terminated` event on the
    // thread's own stream — the same durable signal `archive` emits, tagged
    // with a distinct `stop_reason`.
    const threadEvents = await fetch(
      `${baseURL}/v1/sessions/${sessionId}/threads/${thread!.id}/events`,
      { headers: { 'x-api-key': apiKey } },
    );
    const threadEventsBody = (await threadEvents.json()) as {
      data: Array<{ type: string; session_thread_id: string }>;
    };
    expect(threadEventsBody.data).toContainEqual(
      expect.objectContaining({
        type: 'session.thread_status_terminated',
        session_thread_id: thread!.id,
      }),
    );

    const missingSession = await fetch(
      `${baseURL}/v1/sessions/ses_does_not_exist/threads/${thread!.id}/interrupt`,
      {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({}),
      },
    );
    expect(missingSession.status).toBe(404);

    const missingThread = await fetch(
      `${baseURL}/v1/sessions/${sessionId}/threads/sth_does_not_exist/interrupt`,
      {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({}),
      },
    );
    expect(missingThread.status).toBe(404);
  }, 30000);

  it('paginates projected session events without repeating same-seq rows', async () => {
    const agentId = await createTestAgent(baseURL, apiKey);
    const sessionId = await createTestSession(baseURL, apiKey, agentId);
    const subpath = `threads/sth_page_${Date.now()}`;

    const post = await fetch(`${baseURL}/v1/sessions/${sessionId}/events`, {
      method: 'POST',
      headers: {
        'x-api-key': apiKey,
        'content-type': 'application/json',
        'orca-beta': 'legacy-event-subpath',
      },
      body: JSON.stringify({
        events: [{ type: 'user.message', subpath, content: [{ type: 'text', text: 'page me' }] }],
      }),
    });
    expect(post.status).toBe(200);

    const first = await fetch(`${baseURL}/v1/sessions/${sessionId}/events?limit=1&subpath=*`, {
      headers: { 'x-api-key': apiKey },
    });
    expect(first.status).toBe(200);
    const firstBody = (await first.json()) as {
      data: Array<{ id: string; seq: string }>;
      next_page: string | null;
    };
    expect(firstBody.data).toHaveLength(1);
    expect(firstBody.next_page).toMatch(/^\d+:.+/);

    const second = await fetch(
      `${baseURL}/v1/sessions/${sessionId}/events?limit=1&subpath=*&page=${encodeURIComponent(
        firstBody.next_page!,
      )}`,
      { headers: { 'x-api-key': apiKey } },
    );
    expect(second.status).toBe(200);
    const secondBody = (await second.json()) as { data: Array<{ id: string; seq: string }> };
    expect(secondBody.data).toHaveLength(1);
    expect(secondBody.data[0]!.id).not.toBe(firstBody.data[0]!.id);
    expect(secondBody.data[0]!.seq).toBe(firstBody.data[0]!.seq);
  }, 30000);

  it('backfills legacy tool history and paginates projections in content order', async () => {
    const agentId = await createTestAgent(baseURL, apiKey);
    const sessionId = await createTestSession(baseURL, apiKey, agentId);
    const legacyEventId = `evt_legacy_backfill_${Date.now()}`;
    const producedAt = new Date().toISOString();
    await db.insert(sessionEventsIndex).values({
      workspaceId,
      sessionId,
      seq: 700,
      projectionOrdinal: 0,
      projectionVersion: 0,
      eventId: legacyEventId,
      subpath: '',
      processedAt: producedAt,
      producedAt,
      producedBy: 'harness',
      kind: 'agent.message',
      visibility: 'public',
      payload: {
        type: 'agent.message',
        message: {
          content: [
            { type: 'text', text: 'before tools' },
            { type: 'tool_use', id: 'toolu_first', name: 'Bash', input: { cmd: 'pwd' } },
            { type: 'tool_use', id: 'toolu_second', name: 'Read', input: { path: '/tmp/x' } },
          ],
        },
      },
    });

    expect(await backfillSessionEventProjections(db)).toBeGreaterThanOrEqual(1);

    const events: Array<{ type: string; name?: string }> = [];
    let page: string | null = '';
    for (let index = 0; index < 3; index += 1) {
      const response = await fetch(
        `${baseURL}/v1/sessions/${sessionId}/events?limit=1${
          page ? `&page=${encodeURIComponent(page)}` : ''
        }`,
        { headers: { 'x-api-key': apiKey } },
      );
      expect(response.status).toBe(200);
      const body = (await response.json()) as {
        data: Array<{ type: string; name?: string }>;
        next_page: string | null;
      };
      expect(body.data).toHaveLength(1);
      events.push(body.data[0]!);
      if (index < 2) expect(body.next_page).toMatch(/^\d+:\d+:evt_/);
      page = body.next_page;
    }

    expect(events).toEqual([
      expect.objectContaining({ type: 'agent.message' }),
      expect.objectContaining({ type: 'agent.tool_use', name: 'Bash' }),
      expect.objectContaining({ type: 'agent.tool_use', name: 'Read' }),
    ]);
    expect(page).toBeNull();
  }, 30000);

  it('rejects client-created session threads above the 25 concurrent thread limit', async () => {
    const agentId = await createTestAgent(baseURL, apiKey);
    const sessionId = await createTestSession(baseURL, apiKey, agentId);

    const seed = await fetch(`${baseURL}/v1/sessions/${sessionId}/events`, {
      method: 'POST',
      headers: {
        'x-api-key': apiKey,
        'content-type': 'application/json',
        'orca-beta': 'legacy-event-subpath',
      },
      body: JSON.stringify({
        events: Array.from({ length: 25 }, (_, i) => ({
          type: 'user.message',
          subpath: `threads/limit-${i}`,
          content: [{ type: 'text', text: `thread ${i}` }],
        })),
      }),
    });
    expect(seed.status).toBe(200);

    const post = await fetch(`${baseURL}/v1/sessions/${sessionId}/events`, {
      method: 'POST',
      headers: {
        'x-api-key': apiKey,
        'content-type': 'application/json',
        'orca-beta': 'legacy-event-subpath',
      },
      body: JSON.stringify({
        events: [
          {
            type: 'user.message',
            subpath: 'threads/limit-25',
            content: [{ type: 'text', text: 'too many' }],
          },
        ],
      }),
    });

    expect(post.status).toBe(409);
    expect(((await post.json()) as { error: { message: string } }).error.message).toMatch(
      /maximum concurrent session threads/i,
    );
  }, 30000);
});
