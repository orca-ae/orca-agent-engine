// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { and, asc, eq } from 'drizzle-orm';
import { buildCombinedTestApp } from '../../src/server.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import {
  agentTriggerFires,
  agentTriggers,
  sessionLifecycleOutbox,
  sessions,
  sessionThreads,
} from '../../src/persistence/postgres/schema.js';
import {
  dispatchPendingAgentTriggerFires,
  reconcileDueAgentTriggers,
  TRIGGER_DISPATCH_MAX_ATTEMPTS,
  TRIGGER_DISPATCH_RETRY_BASE_MS,
} from '../../src/domain/trigger-reconciler.js';
import { newId } from '../../src/domain/versioning.js';
import {
  buildStubFileStore,
  buildStubStore,
  buildTestJwtMinter,
  closeTestDb,
  getTestDb,
  STUB_SSE_CONFIG,
} from './setup.js';
import { createTestApiKey, uniqueWorkspace } from './fixtures.js';

describe('cron Triggers (integration)', () => {
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
    workspaceId = uniqueWorkspace('triggers');
    apiKey = await createTestApiKey(db, workspaceId);
  });

  afterAll(async () => {
    await app?.close();
    await closeTestDb();
  });

  it('rejects an invalid cron timezone with a stable Claude error', async () => {
    const agent = await createAgent();
    const environment = await createEnvironment();
    const response = await app.inject({
      method: 'POST',
      url: '/v1/triggers',
      headers: requestHeaders(apiKey),
      payload: {
        name: 'invalid-timezone',
        agent: { type: 'agent', id: agent.id, version: agent.version },
        session_mode: 'SESSION_PER_EVENT',
        source: {
          type: 'cron',
          schedule: '* * * * *',
          timezone: 'Not/A_Zone',
          payload: 'This must not run.',
        },
        session: { environment_id: environment.id },
        replicas: 1,
      },
    });

    expect(response.statusCode).toBe(400);
    expect(response.json()).toMatchObject({
      type: 'error',
      error: {
        type: 'invalid_request_error',
        message: 'source.timezone must be a valid IANA time zone',
      },
    });
  });

  it('rejects invalid Session LLM egress metadata on Trigger create and update', async () => {
    const agent = await createAgent();
    const environment = await createEnvironment();
    const body = {
      name: 'llm-egress-check',
      agent: { type: 'agent', id: agent.id, version: agent.version },
      session_mode: 'SESSION_PER_EVENT',
      source: {
        type: 'cron',
        schedule: '* * * * *',
        timezone: 'UTC',
        payload: 'Check the route.',
      },
      session: { environment_id: environment.id, metadata: { orca_llm_egress: 'other' } },
      replicas: 1,
    };
    const invalidCreate = await app.inject({
      method: 'POST',
      url: '/v1/triggers',
      headers: requestHeaders(apiKey),
      payload: body,
    });
    expect(invalidCreate.statusCode).toBe(400);
    expect(invalidCreate.json().error.message).toContain('orca_llm_egress');

    const create = await app.inject({
      method: 'POST',
      url: '/v1/triggers',
      headers: requestHeaders(apiKey),
      payload: {
        ...body,
        session: { environment_id: environment.id, metadata: { orca_llm_egress: 'gateway' } },
      },
    });
    expect(create.statusCode).toBe(200);
    const invalidUpdate = await app.inject({
      method: 'POST',
      url: `/v1/triggers/${create.json().id}`,
      headers: requestHeaders(apiKey),
      payload: { session: { metadata: { orca_llm_egress: 'other' } } },
    });
    expect(invalidUpdate.statusCode).toBe(400);
    expect(invalidUpdate.json().error.message).toContain('orca_llm_egress');

    const archived = await app.inject({
      method: 'DELETE',
      url: `/v1/triggers/${create.json().id}`,
      headers: requestHeaders(apiKey),
      payload: {},
    });
    expect(archived.statusCode).toBe(200);
  });

  it('materializes each slot into one ordinary Session across concurrent workers', async () => {
    const agent = await createAgent();
    const environment = await createEnvironment();
    const create = await app.inject({
      method: 'POST',
      url: '/v1/triggers',
      headers: requestHeaders(apiKey),
      payload: {
        name: 'weekday-report',
        agent: { type: 'agent', id: agent.id, version: agent.version },
        session_mode: 'SESSION_PER_EVENT',
        source: {
          type: 'cron',
          schedule: '* * * * *',
          timezone: 'Asia/Shanghai',
          payload: 'Create the report.',
        },
        session: {
          environment_id: environment.id,
          title_template: '${trigger.name}: ${payload}',
          metadata: { source: 'cron-test' },
        },
        replicas: 1,
      },
    });
    expect(create.statusCode).toBe(200);
    const trigger = create.json<{
      id: string;
      session_mode: string;
      source: { timezone: string };
      replicas: number;
    }>();
    expect(trigger).toMatchObject({ session_mode: 'SESSION_PER_EVENT', replicas: 1 });
    const [persistedTrigger] = await db
      .select({ guardrailSubject: agentTriggers.guardrailSubject })
      .from(agentTriggers)
      .where(and(eq(agentTriggers.workspaceId, workspaceId), eq(agentTriggers.id, trigger.id)))
      .limit(1);
    expect(persistedTrigger?.guardrailSubject).toMatch(/^api-key:/);

    const update = await app.inject({
      method: 'POST',
      url: `/v1/triggers/${trigger.id}`,
      headers: requestHeaders(apiKey),
      payload: {
        source: {
          type: 'cron',
          payload: 'Create the updated report.',
          schedule: '*/2 * * * *',
        },
      },
    });
    expect(update.statusCode).toBe(200);
    expect(update.json().source).toMatchObject({
      schedule: '*/2 * * * *',
      timezone: 'Asia/Shanghai',
    });

    const firstNow = new Date('2035-08-18T03:00:00.000Z');
    await db
      .update(agentTriggers)
      .set({ nextFireAt: new Date(firstNow.getTime() - 30_000) })
      .where(and(eq(agentTriggers.workspaceId, workspaceId), eq(agentTriggers.id, trigger.id)));
    await Promise.all([
      reconcileDueAgentTriggers(db, { now: firstNow, batchSize: 1_000 }),
      reconcileDueAgentTriggers(db, { now: firstNow, batchSize: 1_000 }),
    ]);

    let fires = await loadFires(trigger.id);
    expect(fires).toHaveLength(1);
    expect(fires[0]).toMatchObject({ status: 'pending', sessionId: null });

    await Promise.all([
      dispatchPendingAgentTriggerFires(db, { now: firstNow, batchSize: 1_000 }),
      dispatchPendingAgentTriggerFires(db, { now: firstNow, batchSize: 1_000 }),
    ]);
    fires = await loadFires(trigger.id);
    expect(fires).toHaveLength(1);
    expect(fires[0]).toMatchObject({ status: 'enqueued' });
    expect(fires[0]!.sessionId).toBe(fires[0]!.plannedSessionId);

    const [session] = await db
      .select()
      .from(sessions)
      .where(and(eq(sessions.workspaceId, workspaceId), eq(sessions.id, fires[0]!.sessionId!)))
      .limit(1);
    expect(session).toMatchObject({
      agentId: agent.id,
      agentVersion: agent.version,
      environmentId: environment.id,
      title: 'weekday-report: Create the updated report.',
      status: 'running',
    });
    const [thread] = await db
      .select({ id: sessionThreads.id })
      .from(sessionThreads)
      .where(
        and(
          eq(sessionThreads.workspaceId, workspaceId),
          eq(sessionThreads.sessionId, fires[0]!.sessionId!),
        ),
      );
    expect(thread).toBeDefined();
    const [outbox] = await db
      .select({ kind: sessionLifecycleOutbox.kind, events: sessionLifecycleOutbox.events })
      .from(sessionLifecycleOutbox)
      .where(
        and(
          eq(sessionLifecycleOutbox.workspaceId, workspaceId),
          eq(sessionLifecycleOutbox.sessionId, fires[0]!.sessionId!),
        ),
      );
    expect(outbox?.kind).toBe('session.initial_events');
    expect(outbox?.events).toEqual(
      expect.arrayContaining([expect.objectContaining({ id: fires[0]!.eventId })]),
    );
    const triggerInitialEvent = (outbox?.events as Array<Record<string, unknown>>).find(
      (event) => event.id === fires[0]!.eventId,
    );
    expect(triggerInitialEvent).toBeDefined();
    expect(triggerInitialEvent).not.toHaveProperty('userId');

    // A pending old-generation fire is fenced by pause and never creates a Session.
    const secondNow = new Date('2035-08-18T03:02:00.000Z');
    await db
      .update(agentTriggers)
      .set({ nextFireAt: new Date(secondNow.getTime() - 30_000) })
      .where(and(eq(agentTriggers.workspaceId, workspaceId), eq(agentTriggers.id, trigger.id)));
    await reconcileDueAgentTriggers(db, { now: secondNow, batchSize: 1_000 });
    const pause = await app.inject({
      method: 'POST',
      url: `/v1/triggers/${trigger.id}/pause`,
      headers: requestHeaders(apiKey),
      payload: {},
    });
    expect(pause.statusCode).toBe(200);
    await dispatchPendingAgentTriggerFires(db, { now: secondNow, batchSize: 1_000 });
    fires = await loadFires(trigger.id);
    expect(fires.map((fire) => fire.status)).toEqual(['enqueued', 'canceled']);

    const history = await app.inject({
      method: 'GET',
      url: `/v1/triggers/${trigger.id}/sessions`,
      headers: { 'x-api-key': apiKey },
    });
    expect(history.statusCode).toBe(200);
    expect(history.json().data).toHaveLength(1);
    expect(history.json().data[0].id).toBe(fires[0]!.sessionId);

    const otherApiKey = await createTestApiKey(db, uniqueWorkspace('triggers-other'));
    const isolated = await app.inject({
      method: 'GET',
      url: `/v1/triggers/${trigger.id}`,
      headers: { 'x-api-key': otherApiKey },
    });
    expect(isolated.statusCode).toBe(404);

    const unpause = await app.inject({
      method: 'POST',
      url: `/v1/triggers/${trigger.id}/unpause`,
      headers: requestHeaders(apiKey),
      payload: {},
    });
    expect(unpause.statusCode).toBe(200);

    // Several overdue slots inside the grace window coalesce to the one oldest
    // stored slot; next_fire_at advances directly beyond planner wall-clock time.
    const coalesceNow = new Date('2035-08-18T03:30:00.000Z');
    const oldestCoalescedSlot = new Date(coalesceNow.getTime() - 4 * 60_000);
    await db
      .update(agentTriggers)
      .set({ nextFireAt: oldestCoalescedSlot })
      .where(and(eq(agentTriggers.workspaceId, workspaceId), eq(agentTriggers.id, trigger.id)));
    const coalesced = await reconcileDueAgentTriggers(db, {
      now: coalesceNow,
      batchSize: 1_000,
    });
    expect(coalesced).toMatchObject({ created: 1, misfired: 0 });
    const afterCoalesceFires = await loadFires(trigger.id);
    expect(afterCoalesceFires).toHaveLength(3);
    expect(afterCoalesceFires[2]!.scheduledFor).toEqual(oldestCoalescedSlot);
    const [afterCoalesce] = await db
      .select({ nextFireAt: agentTriggers.nextFireAt })
      .from(agentTriggers)
      .where(and(eq(agentTriggers.workspaceId, workspaceId), eq(agentTriggers.id, trigger.id)));
    expect(afterCoalesce!.nextFireAt!.getTime()).toBeGreaterThan(coalesceNow.getTime());

    const misfireNow = new Date('2035-08-18T04:00:00.000Z');
    await db
      .update(agentTriggers)
      .set({ nextFireAt: new Date(misfireNow.getTime() - 10 * 60_000) })
      .where(and(eq(agentTriggers.workspaceId, workspaceId), eq(agentTriggers.id, trigger.id)));
    await reconcileDueAgentTriggers(db, { now: misfireNow, batchSize: 1_000 });
    expect(await loadFires(trigger.id)).toHaveLength(3);
    const [afterMisfire] = await db
      .select({ nextFireAt: agentTriggers.nextFireAt })
      .from(agentTriggers)
      .where(and(eq(agentTriggers.workspaceId, workspaceId), eq(agentTriggers.id, trigger.id)));
    expect(afterMisfire!.nextFireAt!.getTime()).toBeGreaterThan(misfireNow.getTime());

    const archived = await app.inject({
      method: 'DELETE',
      url: `/v1/triggers/${trigger.id}`,
      headers: requestHeaders(apiKey),
      payload: {},
    });
    expect(archived.statusCode).toBe(200);
    const getArchived = await app.inject({
      method: 'GET',
      url: `/v1/triggers/${trigger.id}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(getArchived.statusCode).toBe(404);
    const [tombstone] = await db
      .select()
      .from(agentTriggers)
      .where(eq(agentTriggers.id, trigger.id));
    expect(tombstone!.deletedAt).toBeInstanceOf(Date);

    const deleteSession = await app.inject({
      method: 'DELETE',
      url: `/v1/sessions/${fires[0]!.sessionId}`,
      headers: requestHeaders(apiKey),
      payload: {},
    });
    expect(deleteSession.statusCode).toBe(200);
    expect(await loadFires(trigger.id)).toHaveLength(3);
    const historyAfterDelete = await app.inject({
      method: 'GET',
      url: `/v1/triggers/${trigger.id}/sessions`,
      headers: { 'x-api-key': apiKey },
    });
    expect(historyAfterDelete.statusCode).toBe(404);
  });

  it('counts an existing fire slot as deduplicated instead of created', async () => {
    const agent = await createAgent();
    const environment = await createEnvironment();
    const trigger = await createTrigger(agent, environment, 'deduplicated-slot');
    const now = new Date('2035-08-18T05:00:30.000Z');
    const scheduledFor = new Date(now.getTime() - 30_000);
    const [storedTrigger] = await db
      .select()
      .from(agentTriggers)
      .where(and(eq(agentTriggers.workspaceId, workspaceId), eq(agentTriggers.id, trigger.id)));
    await db
      .update(agentTriggers)
      .set({ nextFireAt: scheduledFor })
      .where(and(eq(agentTriggers.workspaceId, workspaceId), eq(agentTriggers.id, trigger.id)));
    await db.insert(agentTriggerFires).values({
      id: newId('trgfire'),
      workspaceId,
      triggerId: trigger.id,
      generation: storedTrigger!.generation,
      scheduledFor,
      status: 'pending',
      plannedSessionId: newId('ses'),
      sessionId: null,
      eventId: newId('evt'),
      attemptCount: 0,
      lastError: null,
      nextAttemptAt: now,
      enqueuedAt: null,
      createdAt: now,
      updatedAt: now,
    });

    const result = await reconcileDueAgentTriggers(db, { now, batchSize: 1_000 });
    expect(result).toMatchObject({ processed: 1, created: 0, deduplicated: 1 });
    expect(await loadFires(trigger.id)).toHaveLength(1);

    const archived = await app.inject({
      method: 'DELETE',
      url: `/v1/triggers/${trigger.id}`,
      headers: requestHeaders(apiKey),
      payload: {},
    });
    expect(archived.statusCode).toBe(200);
  });

  it('backs off a poison fire without blocking newer fires and eventually exhausts it', async () => {
    const agent = await createAgent();
    const environment = await createEnvironment();
    const trigger = await createTrigger(agent, environment, 'poison-fire');
    const existingSessionResult = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: requestHeaders(apiKey),
      payload: { agent_id: agent.id, environment_id: environment.id },
    });
    expect(existingSessionResult.statusCode).toBe(200);
    const existingSession = existingSessionResult.json<{ id: string }>();
    const [storedTrigger] = await db
      .select()
      .from(agentTriggers)
      .where(and(eq(agentTriggers.workspaceId, workspaceId), eq(agentTriggers.id, trigger.id)));
    const now = new Date('2035-08-18T06:00:00.000Z');
    const poisonScheduledFor = new Date('2000-01-01T00:00:00.000Z');
    const poisonFireId = newId('trgfire');
    const healthyFireId = newId('trgfire');
    await db.insert(agentTriggerFires).values([
      {
        id: poisonFireId,
        workspaceId,
        triggerId: trigger.id,
        generation: storedTrigger!.generation,
        scheduledFor: poisonScheduledFor,
        status: 'pending',
        plannedSessionId: existingSession.id,
        sessionId: null,
        eventId: newId('evt'),
        attemptCount: 0,
        lastError: null,
        nextAttemptAt: now,
        enqueuedAt: null,
        createdAt: now,
        updatedAt: now,
      },
      {
        id: healthyFireId,
        workspaceId,
        triggerId: trigger.id,
        generation: storedTrigger!.generation,
        scheduledFor: new Date(poisonScheduledFor.getTime() + 1_000),
        status: 'pending',
        plannedSessionId: newId('ses'),
        sessionId: null,
        eventId: newId('evt'),
        attemptCount: 0,
        lastError: null,
        nextAttemptAt: now,
        enqueuedAt: null,
        createdAt: now,
        updatedAt: now,
      },
    ]);

    const first = await dispatchPendingAgentTriggerFires(db, { now, batchSize: 2 });
    expect(first).toMatchObject({ processed: 2, enqueued: 1, retried: 1, failed: 0 });
    let [poison, healthy] = await loadFires(trigger.id);
    expect(poison).toMatchObject({ id: poisonFireId, status: 'pending', attemptCount: 1 });
    expect(poison!.lastError).toBeTruthy();
    expect(poison!.nextAttemptAt).toEqual(new Date(now.getTime() + TRIGGER_DISPATCH_RETRY_BASE_MS));
    expect(healthy).toMatchObject({ id: healthyFireId, status: 'enqueued', attemptCount: 1 });

    await dispatchPendingAgentTriggerFires(db, {
      now: new Date(poison!.nextAttemptAt.getTime() - 1),
      batchSize: 1,
    });
    [poison] = await loadFires(trigger.id);
    expect(poison).toMatchObject({ status: 'pending', attemptCount: 1 });

    for (let attempt = 2; attempt <= TRIGGER_DISPATCH_MAX_ATTEMPTS; attempt += 1) {
      const retry = await dispatchPendingAgentTriggerFires(db, {
        now: poison!.nextAttemptAt,
        batchSize: 1,
      });
      if (attempt < TRIGGER_DISPATCH_MAX_ATTEMPTS) {
        expect(retry).toMatchObject({ processed: 1, retried: 1, failed: 0 });
      } else {
        expect(retry).toMatchObject({ processed: 1, retried: 0, failed: 1 });
      }
      [poison, healthy] = await loadFires(trigger.id);
    }

    expect(poison).toMatchObject({
      id: poisonFireId,
      status: 'failed',
      attemptCount: TRIGGER_DISPATCH_MAX_ATTEMPTS,
    });
    const [triggerAfterFailure] = await db
      .select({ status: agentTriggers.status })
      .from(agentTriggers)
      .where(and(eq(agentTriggers.workspaceId, workspaceId), eq(agentTriggers.id, trigger.id)));
    expect(triggerAfterFailure?.status).toBe('active');
  });

  async function createAgent(): Promise<{ id: string; version: number }> {
    const response = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: requestHeaders(apiKey),
      payload: {
        name: `trigger-agent-${Date.now()}`,
        model: { provider: 'anthropic', id: 'claude-3-5-sonnet-20240620' },
        tools: [],
        mcp_servers: [],
        skills: [],
        metadata: {},
      },
    });
    expect(response.statusCode).toBe(200);
    return response.json();
  }

  async function createEnvironment(): Promise<{ id: string }> {
    const response = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: requestHeaders(apiKey),
      payload: {
        name: `trigger-env-${Date.now()}`,
        config: { type: 'cloud' },
      },
    });
    expect(response.statusCode).toBe(200);
    return response.json();
  }

  async function createTrigger(
    agent: { id: string; version: number },
    environment: { id: string },
    name: string,
  ): Promise<{ id: string }> {
    const response = await app.inject({
      method: 'POST',
      url: '/v1/triggers',
      headers: requestHeaders(apiKey),
      payload: {
        name: `${name}-${Date.now()}`,
        agent: { type: 'agent', id: agent.id, version: agent.version },
        session_mode: 'SESSION_PER_EVENT',
        source: {
          type: 'cron',
          schedule: '0 0 1 1 *',
          timezone: 'Etc/UTC',
          payload: `Run ${name}.`,
        },
        session: { environment_id: environment.id },
        replicas: 1,
      },
    });
    expect(response.statusCode).toBe(200);
    return response.json();
  }

  async function loadFires(triggerId: string) {
    return db
      .select()
      .from(agentTriggerFires)
      .where(
        and(
          eq(agentTriggerFires.workspaceId, workspaceId),
          eq(agentTriggerFires.triggerId, triggerId),
        ),
      )
      .orderBy(asc(agentTriggerFires.scheduledFor));
  }
});

function requestHeaders(apiKey: string): Record<string, string> {
  return { 'x-api-key': apiKey, 'content-type': 'application/json' };
}
