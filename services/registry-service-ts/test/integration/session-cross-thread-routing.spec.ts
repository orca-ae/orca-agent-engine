// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { FastifyInstance } from 'fastify';
import type { TranscriptStore, Event } from '@orca/transcript-store';
import {
  getTestDb,
  closeTestDb,
  STUB_SSE_CONFIG,
  buildTestJwtMinter,
  buildStubFileStore,
} from './setup.js';
import { and, eq } from 'drizzle-orm';
import { uniqueWorkspace, createTestApiKey } from './fixtures.js';
import { buildApp } from '../../src/server.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import { sessionThreads } from '../../src/persistence/postgres/schema.js';
import { stableSessionThreadId } from '../../src/domain/thread-projection.js';
import {
  threadSubpath,
  USER_TOOL_CONFIRMATION_KIND,
  USER_CUSTOM_TOOL_RESULT_KIND,
} from '../../src/domain/session-threads.js';

/**
 * Route-level coverage for the cross-thread routing wiring in
 * `POST /v1/sessions/:id/events` (sessions.routes.ts). This spec proves the
 * ROUTE actually:
 *
 *   (a) detects an event carrying a `session_thread_id`,
 *   (b) loads that thread's row from the `session_threads` read model, and
 *   (c) rewrites the event's transcript `subpath` to the row's own subpath
 *       (`subagents/<id>` for a child, `''` for the primary), 404ing when the
 *       named thread does not exist,
 *
 * while leaving replies that name no thread on the primary stream. The
 * capturing store records the exact `subpath` each appended proto event
 * carries, which is the thing the runner-side single writer keys on.
 */

interface CapturingStore extends TranscriptStore {
  readonly appended: Event[];
}

function buildCapturingStore(): CapturingStore {
  const appended: Event[] = [];
  const store: CapturingStore = {
    appended,
    async append(_workspaceId, _sessionId, events) {
      appended.push(...events);
      return [];
    },
    async *read() {
      /* unused */
    },
    async *tail() {
      /* unused */
    },
    async archive() {
      /* no-op */
    },
    async close() {
      /* no-op */
    },
  };
  return store;
}

describe('cross-thread reply routing on POST /events (integration)', () => {
  let app: FastifyInstance;
  let apiKey: string;
  let agentId: string;
  let environmentId: string;
  let store: CapturingStore;
  let db: DbClient;
  let workspaceId: string;

  beforeAll(async () => {
    ({ db } = await getTestDb());
    store = buildCapturingStore();
    app = buildApp({
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store,
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
    });
    await app.ready();
    workspaceId = uniqueWorkspace('xthread');
    apiKey = await createTestApiKey(db, workspaceId);
    const agentResp = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        name: `seed-${Date.now()}`,
        model: { provider: 'anthropic', id: 'claude-3-5-sonnet-20240620' },
        system: '',
        tools: [],
        mcp_servers: [],
        skills: [],
        metadata: {},
      },
    });
    agentId = agentResp.json().id;
    // `POST /v1/sessions` requires `environment_id`, so every session create in
    // this file needs a real environment in the same workspace.
    const environmentResp = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { name: `xthread-env-${Date.now()}`, config: { type: 'cloud' } },
    });
    if (environmentResp.statusCode !== 200) {
      throw new Error(`environment create failed: ${environmentResp.statusCode}`);
    }
    environmentId = environmentResp.json().id as string;
  });

  afterAll(async () => {
    await app.close();
    await closeTestDb();
  });

  async function createSession(): Promise<string> {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { agent_id: agentId, environment_id: environmentId },
    });
    expect(res.statusCode).toBe(200);
    return res.json().id;
  }

  /**
   * `orca-beta` is required, not decoration. Claude's own
   * `user.tool_confirmation` / `user.custom_tool_result` shapes are `.strict()`
   * and carry no `session_thread_id` (only `user.interrupt` does), so a default
   * client's cross-thread reply is rejected 400 by
   * `sessionAppendEventsBodySchema`. The route skips that schema entirely for an
   * `orca-beta` request (`sessions.routes.ts`: `if (!orcaBeta) { … }`), which is
   * the pre-Claude vocabulary this routing was built for and the only public
   * path on which a tool reply can name a thread.
   */
  async function postEvents(
    sessionId: string,
    events: Array<Record<string, unknown> & { type: string }>,
  ): Promise<void> {
    const res = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${sessionId}/events`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json', 'orca-beta': 'true' },
      payload: { events },
    });
    expect(res.statusCode).toBe(200);
  }

  /**
   * Seed a coordinator session's primary + child threads in the read model.
   *
   * This used to POST two `session.thread_created` events through the public
   * events route. That kind is produced by the harness/runner and has never
   * been a member of `sessionEventInputSchema`'s union, so the route rejects
   * the batch with 400 `Invalid input` — the seeding, not the behaviour under
   * test, was wrong. Session create already writes the PRIMARY thread row
   * (`domain/session-creation.ts`), so it is read back rather than assumed, and
   * only the child row is inserted — straight into `session_threads`, the read
   * model `resolveThreadTargetedEvents` actually queries. The child's subpath
   * follows `threadSubpath()`, the module's documented convention.
   */
  async function seedThreads(sessionId: string): Promise<{ primaryId: string; childId: string }> {
    const [primary] = await db
      .select()
      .from(sessionThreads)
      .where(
        and(
          eq(sessionThreads.workspaceId, workspaceId),
          eq(sessionThreads.sessionId, sessionId),
          eq(sessionThreads.subpath, ''),
        ),
      )
      .limit(1);
    expect(primary, 'session create must have written the primary thread row').toBeDefined();
    const primaryId = primary!.id;
    const childId = stableSessionThreadId(workspaceId, sessionId, 'child');
    const now = new Date();
    await db.insert(sessionThreads).values({
      id: childId,
      workspaceId,
      sessionId,
      subpath: threadSubpath(childId, primaryId),
      agentId: primary!.agentId,
      agentVersion: primary!.agentVersion,
      agentName: 'researcher',
      parentThreadId: primaryId,
      status: 'idle',
      stopReason: null,
      archivedAt: null,
      createdAt: now,
      updatedAt: now,
    });
    return { primaryId, childId };
  }

  function appendedByKind(kind: string): Event[] {
    return store.appended.filter((e) => e.kind === kind);
  }

  it('rewrites a child-targeted tool_confirmation to the child subagent subpath', async () => {
    const sessionId = await createSession();
    const { childId } = await seedThreads(sessionId);
    store.appended.length = 0;

    await postEvents(sessionId, [
      {
        type: USER_TOOL_CONFIRMATION_KIND,
        session_thread_id: childId,
        tool_use_id: 'toolu_xthread_1',
        result: 'allow',
      },
    ]);

    const [confirmation] = appendedByKind(USER_TOOL_CONFIRMATION_KIND);
    expect(confirmation).toBeDefined();
    // (a)+(b)+(c): the reply named the child thread, the route resolved the
    // primary, and rewrote the subpath to the child's subagent stream so the
    // runner driving THAT thread receives it.
    expect(confirmation!.subpath).toBe(`subagents/${childId}`);
  });

  it('rewrites a custom_tool_result the same way', async () => {
    const sessionId = await createSession();
    const { childId } = await seedThreads(sessionId);
    store.appended.length = 0;

    await postEvents(sessionId, [
      {
        type: USER_CUSTOM_TOOL_RESULT_KIND,
        session_thread_id: childId,
        custom_tool_use_id: 'ctu_xthread_1',
        content: [{ type: 'text', text: 'result' }],
      },
    ]);

    const [result] = appendedByKind(USER_CUSTOM_TOOL_RESULT_KIND);
    expect(result!.subpath).toBe(`subagents/${childId}`);
  });

  it('keeps a reply targeting the primary thread id on the primary stream', async () => {
    const sessionId = await createSession();
    const { primaryId } = await seedThreads(sessionId);
    store.appended.length = 0;

    await postEvents(sessionId, [
      {
        type: USER_TOOL_CONFIRMATION_KIND,
        session_thread_id: primaryId,
        tool_use_id: 'toolu_xthread_2',
        result: 'deny',
      },
    ]);

    const [confirmation] = appendedByKind(USER_TOOL_CONFIRMATION_KIND);
    // A reply naming the primary thread maps back to the empty subpath.
    expect(confirmation!.subpath).toBe('');
  });

  it('leaves a reply with no session_thread_id on the primary stream', async () => {
    const sessionId = await createSession();
    await seedThreads(sessionId);
    store.appended.length = 0;

    await postEvents(sessionId, [
      { type: USER_TOOL_CONFIRMATION_KIND, tool_use_id: 'toolu_xthread_3', result: 'allow' },
    ]);

    const [confirmation] = appendedByKind(USER_TOOL_CONFIRMATION_KIND);
    expect(confirmation!.subpath).toBe('');
  });

  it('routes a mixed batch: child-targeted reply rewritten, plain message untouched', async () => {
    const sessionId = await createSession();
    const { childId } = await seedThreads(sessionId);
    store.appended.length = 0;

    await postEvents(sessionId, [
      { type: 'user.message', content: [{ type: 'text', text: 'to the coordinator' }] },
      {
        type: USER_TOOL_CONFIRMATION_KIND,
        session_thread_id: childId,
        tool_use_id: 'toolu_xthread_4',
        result: 'allow',
      },
    ]);

    const [message] = appendedByKind('user.message');
    const [confirmation] = appendedByKind(USER_TOOL_CONFIRMATION_KIND);
    // The plain user.message is NOT a cross-thread reply kind — it stays on the
    // primary stream even in a batch that also carries a child-targeted reply.
    expect(message!.subpath).toBe('');
    expect(confirmation!.subpath).toBe(`subagents/${childId}`);
  });

  it('404s a reply naming a thread that does not exist, appending nothing', async () => {
    // This asserted a silent fallback to the primary stream, from when routing
    // was computed by the pure `replyThreadSubpath()` helper. The route now
    // RESOLVES the thread row (`resolveThreadTargetedEvents`) and returns
    // `404 session thread … not found` instead — a typo'd thread id can no
    // longer land a reply on the wrong stream. `replyThreadSubpath` survives in
    // `domain/session-threads.ts` but no longer has a caller in `src/`.
    const sessionId = await createSession();
    store.appended.length = 0;

    const res = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${sessionId}/events`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json', 'orca-beta': 'true' },
      payload: {
        events: [
          {
            type: USER_TOOL_CONFIRMATION_KIND,
            session_thread_id: 'sth_never_projected',
            tool_use_id: 'toolu_xthread_5',
            result: 'allow',
          },
        ],
      },
    });
    expect(res.statusCode).toBe(404);
    expect(res.json().error.message).toBe('session thread sth_never_projected not found');
    // The whole batch is refused — nothing reached the transcript store.
    expect(store.appended).toHaveLength(0);
  });

  it('does not resolve the primary thread when no reply carries a thread id', async () => {
    // A batch of only plain user.messages must skip the primary-thread lookup
    // entirely (the `carriesThreadReply` short-circuit) and leave every event on
    // the primary stream.
    const sessionId = await createSession();
    await seedThreads(sessionId);
    store.appended.length = 0;

    await postEvents(sessionId, [
      { type: 'user.message', content: [{ type: 'text', text: 'one' }] },
      { type: 'user.message', content: [{ type: 'text', text: 'two' }] },
    ]);

    for (const e of appendedByKind('user.message')) {
      expect(e.subpath).toBe('');
    }
  });
});
