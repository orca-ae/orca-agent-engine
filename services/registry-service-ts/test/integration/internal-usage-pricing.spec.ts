// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { FastifyInstance } from 'fastify';
import type { AddressInfo } from 'node:net';
import type { Event } from '@orca/transcript-store';
import { and, eq, inArray } from 'drizzle-orm';
import { SEED_PRICE_PROVIDER } from '@orca/harness-catalog';
import {
  getTestDb,
  closeTestDb,
  buildStubStore,
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
import type { DbClient } from '../../src/persistence/postgres/client.js';
import {
  agentTriggerFires,
  agentTriggers,
  modelPrices,
  sessionEventsIndex,
  sessionThreads,
  sessions,
} from '../../src/persistence/postgres/schema.js';
import { stableSessionThreadId } from '../../src/domain/thread-projection.js';
import { indexTranscriptEvents } from '../../src/events/session-events-index.js';

/** The default provider: a usage report that names only a model id resolves here. */
const PROVIDER = SEED_PRICE_PROVIDER;
const PRICED_MODEL = 'orcatest-priced-4';
const UNPRICED_MODEL = 'orcatest-unpriced-4';
const OPENAI_PRICED_MODEL = 'orcatest-openai-priced';

describe('/internal/v1/.../usage pricing (integration)', () => {
  let app: FastifyInstance;
  let baseURL: string;
  let apiKey: string;
  let workspaceId: string;
  let agentId: string;
  let db: DbClient;

  async function report(
    sessionId: string,
    body: Record<string, unknown>,
  ): Promise<{ status: number; body: Record<string, unknown> }> {
    const res = await fetch(
      `${baseURL}/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/usage`,
      {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body),
      },
    );
    return {
      status: res.status,
      body: (await res.json()) as Record<string, unknown>,
    };
  }

  async function sessionRow(sessionId: string) {
    const rows = await db
      .select()
      .from(sessions)
      .where(and(eq(sessions.workspaceId, workspaceId), eq(sessions.id, sessionId)))
      .limit(1);
    return rows[0]!;
  }

  async function threadRow(sessionId: string, threadId: string) {
    const rows = await db
      .select()
      .from(sessionThreads)
      .where(
        and(
          eq(sessionThreads.workspaceId, workspaceId),
          eq(sessionThreads.sessionId, sessionId),
          eq(sessionThreads.id, threadId),
        ),
      )
      .limit(1);
    return rows[0]!;
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
    await app.listen({ host: '127.0.0.1', port: 0 });
    baseURL = `http://127.0.0.1:${(app.server.address() as AddressInfo).port}`;
    workspaceId = uniqueWorkspace('usage_pricing');
    apiKey = await createTestApiKey(db, workspaceId);
    agentId = await createTestAgent(baseURL, apiKey);

    await db
      .insert(modelPrices)
      .values({
        provider: PROVIDER,
        organizationId: '',
        modelId: PRICED_MODEL,
        source: 'seed',
        inputPerMillionTokens: 3,
        outputPerMillionTokens: 15,
        cacheReadPerMillionTokens: 0.3,
        cacheWritePerMillionTokens: 3.75,
      })
      .onConflictDoUpdate({
        target: [
          modelPrices.provider,
          modelPrices.modelId,
          modelPrices.source,
          modelPrices.organizationId,
        ],
        set: { inputPerMillionTokens: 3, outputPerMillionTokens: 15 },
      });
  }, 60000);

  afterAll(async () => {
    await db
      .delete(modelPrices)
      .where(inArray(modelPrices.modelId, [PRICED_MODEL, OPENAI_PRICED_MODEL]));
    await app?.close();
    await closeTestDb();
  });

  it('prices explicitly identified OpenAI usage against its provider catalog', async () => {
    await db.insert(modelPrices).values({
      provider: 'openai',
      organizationId: '',
      modelId: OPENAI_PRICED_MODEL,
      source: 'seed',
      inputPerMillionTokens: 2,
      outputPerMillionTokens: 8,
      cacheReadPerMillionTokens: 0.2,
    });
    const sessionId = await createTestSession(baseURL, apiKey, agentId);
    const response = await report(sessionId, {
      provider: 'openai',
      model: OPENAI_PRICED_MODEL,
      usage: { input_tokens: 1_000_000, output_tokens: 100_000, cache_read_input_tokens: 500_000 },
    });
    expect(response.status).toBe(200);
    const row = await sessionRow(sessionId);
    expect(row.usageCostNanoUsd).toBe(2_900_000_000n);
    expect(row.usageHasUnpriced).toBe(false);
  });

  it('accumulates exact nano-USD for a priced delta', async () => {
    const sessionId = await createTestSession(baseURL, apiKey, agentId);
    const first = await report(sessionId, {
      usage: { input_tokens: 1_000_000, output_tokens: 100_000 },
      model: PRICED_MODEL,
    });
    expect(first.status).toBe(200);
    expect(first.body.guardrail_usage_state).toEqual({
      total_tokens: 1_100_000,
      session_cost_usd: 4.5,
    });
    // $3.00 + $1.50
    expect((await sessionRow(sessionId)).usageCostNanoUsd).toBe(4_500_000_000n);

    const second = await report(sessionId, {
      usage: { input_tokens: 1_000_000 },
      model: PRICED_MODEL,
    });
    expect(second.body.guardrail_usage_state).toEqual({
      total_tokens: 2_100_000,
      session_cost_usd: 7.5,
    });
    expect((await sessionRow(sessionId)).usageCostNanoUsd).toBe(7_500_000_000n);
    expect((await sessionRow(sessionId)).usageInputTokens).toBe(2_000_000);
  });

  it('updates and refreshes a principal daily window exactly once across sessions', async () => {
    const firstSessionId = await createTestSession(baseURL, apiKey, agentId);
    const secondSessionId = await createTestSession(baseURL, apiKey, agentId);
    const guardrailSubject = `api-key:daily-${Date.now()}`;
    const firstTurnEventId = 'evt_daily_turn_first';
    const secondTurnEventId = 'evt_daily_turn_second';
    const producedAt = new Date().toISOString();
    const turnEvents: Event[] = [
      {
        workspaceId,
        sessionId: firstSessionId,
        seq: 1,
        id: firstTurnEventId,
        subpath: '',
        producedAt,
        producedBy: 'client',
        kind: 'user.message',
        idempotencyKey: firstTurnEventId,
        payload: Buffer.from(JSON.stringify({ type: 'user.message' })),
      },
      {
        workspaceId,
        sessionId: secondSessionId,
        seq: 1,
        id: secondTurnEventId,
        subpath: '',
        producedAt,
        producedBy: 'client',
        kind: 'user.message',
        idempotencyKey: secondTurnEventId,
        payload: Buffer.from(JSON.stringify({ type: 'user.message' })),
      },
    ];
    // The transcript consumer may index first; the authenticated public path
    // must still be able to attach its Registry-owned subject afterwards.
    await indexTranscriptEvents(db, turnEvents);
    await indexTranscriptEvents(db, turnEvents, { guardrailSubject });
    await indexTranscriptEvents(db, turnEvents, { guardrailSubject: 'api-key:later-replay' });
    const indexedSubjects = await db
      .select({ subject: sessionEventsIndex.guardrailSubject })
      .from(sessionEventsIndex)
      .where(
        and(
          eq(sessionEventsIndex.workspaceId, workspaceId),
          inArray(sessionEventsIndex.eventId, [firstTurnEventId, secondTurnEventId]),
        ),
      );
    expect(indexedSubjects.map((row) => row.subject)).toEqual([guardrailSubject, guardrailSubject]);

    const firstBody = {
      usage: { input_tokens: 1_000_000 },
      model: PRICED_MODEL,
      turn_event_id: firstTurnEventId,
      usage_event_id: 'evt_daily_usage_first',
    };
    const first = await report(firstSessionId, firstBody);
    expect(first.status).toBe(200);
    expect(first.body.guardrail_subject_window_state).toEqual({ daily_cost_usd: 3 });

    const replay = await report(firstSessionId, firstBody);
    expect(replay.status).toBe(200);
    expect(replay.body.guardrail_subject_window_state).toEqual({ daily_cost_usd: 3 });
    expect((await sessionRow(firstSessionId)).usageInputTokens).toBe(1_000_000);

    const second = await report(secondSessionId, {
      usage: { input_tokens: 1_000_000 },
      model: PRICED_MODEL,
      turn_event_id: secondTurnEventId,
      usage_event_id: 'evt_daily_usage_second',
    });
    expect(second.body.guardrail_subject_window_state).toEqual({ daily_cost_usd: 6 });

    const refreshed = await fetch(
      `${baseURL}/internal/v1/workspaces/${workspaceId}/sessions/${firstSessionId}/guardrail-subject-window`,
      {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ turn_event_id: firstTurnEventId }),
      },
    );
    expect(refreshed.status).toBe(200);
    expect(await refreshed.json()).toEqual({
      guardrail_subject_window_state: { daily_cost_usd: 6 },
    });
  });

  it('resolves an autonomous cron turn through its authenticated trigger creator', async () => {
    const sessionId = await createTestSession(baseURL, apiKey, agentId);
    const session = await sessionRow(sessionId);
    const suffix = `${Date.now()}_${Math.random().toString(36).slice(2)}`;
    const triggerId = `trg_usage_${suffix}`;
    const fireId = `trgfire_usage_${suffix}`;
    const turnEventId = `evt_trigger_turn_${suffix}`;
    const guardrailSubject = `api-key:trigger-creator-${suffix}`;
    const now = new Date();

    await db.insert(agentTriggers).values({
      id: triggerId,
      workspaceId,
      guardrailSubject,
      name: 'usage-pricing-trigger',
      agentId: session.agentId,
      agentVersion: session.agentVersion,
      environmentId: session.environmentId!,
      metadata: {},
      vaultIds: [],
      payload: 'run',
      cronExpression: '* * * * *',
      timezone: 'Etc/UTC',
      status: 'active',
      generation: 1,
    });
    await db.insert(agentTriggerFires).values({
      id: fireId,
      workspaceId,
      triggerId,
      generation: 1,
      scheduledFor: now,
      status: 'enqueued',
      plannedSessionId: sessionId,
      sessionId,
      eventId: turnEventId,
      nextAttemptAt: now,
      enqueuedAt: now,
    });
    await db.insert(sessionEventsIndex).values({
      workspaceId,
      sessionId,
      seq: 1,
      eventId: turnEventId,
      subpath: '',
      producedAt: now.toISOString(),
      producedBy: 'client',
      kind: 'user.message',
      visibility: 'public',
      payload: {},
    });

    const usage = await report(sessionId, {
      usage: { input_tokens: 1_000_000 },
      model: PRICED_MODEL,
      turn_event_id: turnEventId,
      usage_event_id: `evt_trigger_usage_${suffix}`,
    });
    expect(usage.status).toBe(200);
    expect(usage.body.guardrail_subject_window_state).toEqual({ daily_cost_usd: 3 });

    const refreshed = await fetch(
      `${baseURL}/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/guardrail-subject-window`,
      {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ turn_event_id: turnEventId }),
      },
    );
    expect(refreshed.status).toBe(200);
    expect(await refreshed.json()).toEqual({
      guardrail_subject_window_state: { daily_cost_usd: 3 },
    });
  });

  it('records tokens but no cost for a model with no price data', async () => {
    const sessionId = await createTestSession(baseURL, apiKey, agentId);
    const reportResult = await report(sessionId, {
      usage: { input_tokens: 500, output_tokens: 200 },
      model: UNPRICED_MODEL,
    });
    expect(reportResult.status).toBe(200);
    expect(reportResult.body.guardrail_usage_state).toEqual({
      total_tokens: 700,
      session_usage_has_unpriced: true,
    });
    const row = await sessionRow(sessionId);
    expect(row.usageInputTokens).toBe(500);
    expect(row.usageOutputTokens).toBe(200);
    // Unpriced is not zero: nothing may default the cost to 0.
    expect(row.usageCostNanoUsd).toBeNull();
    expect(row.usageHasUnpriced).toBe(true);
  });

  it('marks the principal daily window when a turn runs on an unpriced model', async () => {
    // The daily window used to be written *only* when a cost could be
    // computed, so an unpriced turn left it untouched entirely. That made a
    // day whose spend went unmeasured indistinguishable from a day that cost
    // nothing, and `user_daily_cost_budget` — which reads this flag — failed
    // open across sessions.
    //
    // The flag is 1, not `true`: `guardrail_counters.value_num` is
    // `double precision NOT NULL` and the route refuses a non-numeric `set`
    // at this scope, which is why the boolean its session-scoped twin uses
    // could never have been stored here.
    const sessionId = await createTestSession(baseURL, apiKey, agentId);
    // Unique per run. The sibling daily-window case hardcodes its ids and
    // queries them without a session filter, so its rows accumulate across
    // runs against a reused database.
    const turnEventId = `evt_unpriced_turn_${Date.now()}`;
    await indexTranscriptEvents(
      db,
      [
        {
          workspaceId,
          sessionId,
          seq: 1,
          id: turnEventId,
          subpath: '',
          producedAt: new Date().toISOString(),
          producedBy: 'client',
          kind: 'user.message',
          idempotencyKey: turnEventId,
          payload: Buffer.from(JSON.stringify({ type: 'user.message' })),
        },
      ],
      { guardrailSubject: `api-key:unpriced-${Date.now()}` },
    );

    const result = await report(sessionId, {
      usage: { input_tokens: 500 },
      model: UNPRICED_MODEL,
      turn_event_id: turnEventId,
      usage_event_id: `evt_unpriced_usage_${Date.now()}`,
    });
    expect(result.status).toBe(200);
    expect(result.body.guardrail_subject_window_state).toEqual({ daily_cost_unpriced: 1 });
  });

  it('records tokens but no cost when the delta names no model', async () => {
    const sessionId = await createTestSession(baseURL, apiKey, agentId);
    await report(sessionId, { usage: { input_tokens: 7 } });
    const row = await sessionRow(sessionId);
    expect(row.usageInputTokens).toBe(7);
    expect(row.usageCostNanoUsd).toBeNull();
    expect(row.usageHasUnpriced).toBe(true);
  });

  it('preserves priced spend while marking a later unpriced delta', async () => {
    const sessionId = await createTestSession(baseURL, apiKey, agentId);
    await report(sessionId, { usage: { input_tokens: 1_000_000 }, model: PRICED_MODEL });
    await report(sessionId, { usage: { input_tokens: 5 }, model: UNPRICED_MODEL });

    const row = await sessionRow(sessionId);
    expect(row.usageCostNanoUsd).toBe(3_000_000_000n);
    expect(row.usageHasUnpriced).toBe(true);
  });

  it('leaves a delta that consumed nothing as a no-op', async () => {
    const sessionId = await createTestSession(baseURL, apiKey, agentId);
    await report(sessionId, { usage: { input_tokens: 0, output_tokens: 0 }, model: PRICED_MODEL });
    const row = await sessionRow(sessionId);
    expect(row.usageInputTokens).toBe(0);
    // An empty flush must not turn an unpriced session into a $0.00 one.
    expect(row.usageCostNanoUsd).toBeNull();
    expect(row.usageHasUnpriced).toBe(false);
  });

  it("attributes a dispatched subagent's spend to its own thread", async () => {
    const sessionId = await createTestSession(baseURL, apiKey, agentId);
    const primaryThreadId = stableSessionThreadId(workspaceId, sessionId, '');
    const subagentThreadId = stableSessionThreadId(workspaceId, sessionId, 'research');
    await db.insert(sessionThreads).values({
      id: subagentThreadId,
      workspaceId,
      sessionId,
      subpath: 'research',
      agentId,
      agentVersion: 1,
      agentName: 'research',
      parentThreadId: primaryThreadId,
      status: 'idle',
    });

    await report(sessionId, {
      usage: { input_tokens: 1_000_000, output_tokens: 0 },
      model: PRICED_MODEL,
      thread_id: subagentThreadId,
      subagent_id: agentId,
    });
    await report(sessionId, {
      usage: { input_tokens: 2_000_000, output_tokens: 0 },
      model: PRICED_MODEL,
      thread_id: primaryThreadId,
    });

    const subagent = await threadRow(sessionId, subagentThreadId);
    expect(subagent.usageInputTokens).toBe(1_000_000);
    expect(subagent.usageCostNanoUsd).toBe(3_000_000_000n);
    expect(subagent.usageHasUnpriced).toBe(false);

    const primary = await threadRow(sessionId, primaryThreadId);
    expect(primary.usageInputTokens).toBe(2_000_000);
    expect(primary.usageCostNanoUsd).toBe(6_000_000_000n);

    // The session total still carries every thread's spend.
    const session = await sessionRow(sessionId);
    expect(session.usageInputTokens).toBe(3_000_000);
    expect(session.usageCostNanoUsd).toBe(9_000_000_000n);

    const finalAck = await report(sessionId, {
      usage: { input_tokens: 1 },
      model: UNPRICED_MODEL,
      subagent_id: agentId,
    });
    expect(finalAck.body.guardrail_usage_state).toEqual({
      total_tokens: 3_000_001,
      session_cost_usd: 9,
      session_usage_has_unpriced: true,
      [`subagent_cost_${agentId}`]: 3,
      [`subagent_usage_has_unpriced_${agentId}`]: true,
    });
  });

  it('rejects a thread that does not belong to the session without recording anything', async () => {
    const sessionId = await createTestSession(baseURL, apiKey, agentId);
    const foreignThreadId = stableSessionThreadId(workspaceId, 'ses_elsewhere', '');
    expect(
      (
        await report(sessionId, {
          usage: { input_tokens: 11 },
          model: PRICED_MODEL,
          thread_id: foreignThreadId,
        })
      ).status,
    ).toBe(404);
    const row = await sessionRow(sessionId);
    expect(row.usageInputTokens).toBe(0);
    expect(row.usageCostNanoUsd).toBeNull();
  });

  it('rejects a malformed model or thread id without recording anything', async () => {
    const sessionId = await createTestSession(baseURL, apiKey, agentId);
    for (const body of [
      { usage: { input_tokens: 5 }, model: '' },
      { usage: { input_tokens: 5 }, model: 12 },
      { usage: { input_tokens: 5 }, thread_id: 'not-a-thread' },
    ]) {
      expect((await report(sessionId, body)).status).toBe(400);
    }
    expect((await sessionRow(sessionId)).usageInputTokens).toBe(0);
  });
});
