// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/** Native DSH + loopback Anthropic protocol; real Registry, Postgres and Kafka.
 * Synthetic prices and provider replies exercise accounting, not provider billing. */
import { randomUUID } from 'node:crypto';
import { createServer } from 'node:http';
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';
import { and, eq } from 'drizzle-orm';
import { Kafka, logLevel } from 'kafkajs';
import { DshSdkWorker } from '../../../../packages/dsh-harness/src/index.js';
import { InMemorySkillStore } from '@orca/skill-store';
import { KafkaTranscriptStore, type Event } from '@orca/transcript-store';
import type { HarnessTurnRequest, HarnessTurnSnapshot } from '@orca/harness-catalog';
import { replyMessages } from '../../../../packages/dsh-harness/test/support/native-tree.js';
import { CodexSdkHarness } from '../../../harness-server/src/harness/codex-sdk/index.js';
import { Dispatcher } from '../../../harness-server/src/runner/dispatcher.js';
import type { RegistryClient } from '../../../harness-server/src/clients/registry.js';
import type {
  SessionStartInput,
  UserEvent,
  SubmitHooks,
} from '../../../harness-server/src/harness/agent-harness.js';
import { buildInternalApp, buildPublicApp } from '../../src/server.js';
import { indexTranscriptEvents } from '../../src/events/session-events-index.js';
import { loadGuardrailUsageState } from '../../src/domain/usage-accounting.js';
import {
  modelPrices,
  guardrailState,
  sessionUsageEvents,
  sessions,
} from '../../src/persistence/postgres/schema.js';
import { createTestApiKey, uniqueWorkspace } from './fixtures.js';
import {
  getTestDb,
  closeTestDb,
  buildStubFileStore,
  buildTestJwtMinter,
  STUB_SSE_CONFIG,
} from './setup.js';

const model = 'claude-sonnet-4-6';
const pricedModel = 'dsh-budget-synthetic-' + randomUUID();
const workspaceId = uniqueWorkspace('dsh_budget');
const topicPrefix = (process.env['KAFKA_TOPIC_PREFIX'] ?? 'dsh-budget.') + randomUUID() + '.';
const headers = { authorization: 'Bearer harness' };
const request = (): UserEvent => ({
  id: 'evt_' + randomUUID(),
  kind: 'user.message',
  payload: { content: 'Reply OK' },
});
const guard = (cap: number, builtin = 'cost_budget', extra = {}) => ({
  name: 'budget',
  scope: 'explicit',
  phases: ['request'],
  rule: { kind: 'builtin', builtin, params: { max_cost_usd: cap, on_unpriced: 'deny', ...extra } },
});

describe('DSH guarded cost budget (native protocol fixture, real storage)', () => {
  let db: Awaited<ReturnType<typeof getTestDb>>['db'];
  let app: ReturnType<typeof buildInternalApp>;
  let publicApp: ReturnType<typeof buildPublicApp>;
  let transcript: KafkaTranscriptStore;
  let apiKey: string;
  const resources: string[] = [];
  const running: Array<() => Promise<void>> = [];

  beforeAll(async () => {
    ({ db } = await getTestDb());
    apiKey = await createTestApiKey(db, workspaceId);
    // Unique accounting alias: never overwrite a provider/model's shared prices.
    await db.insert(modelPrices).values({
      provider: 'anthropic',
      organizationId: '',
      modelId: pricedModel,
      source: 'seed',
      inputPerMillionTokens: 1_000_000,
      outputPerMillionTokens: 1_000_000,
      cacheReadPerMillionTokens: 1_000_000,
      cacheWritePerMillionTokens: 1_000_000,
    });
    transcript = new KafkaTranscriptStore({
      topicPrefix,
      kafka: new Kafka({
        clientId: 'dsh-budget',
        brokers: (process.env['KAFKA_BROKERS'] ?? 'localhost:9092').split(','),
        logLevel: logLevel.ERROR,
      }),
    });
    const options = {
      db,
      store: transcript,
      oidc: { allowedIssuers: [], audience: 'test' },
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
      skillStore: new InMemorySkillStore(),
    };
    app = buildInternalApp(options, {
      verify: async (token) =>
        token === 'harness' ? { caller: 'harness', subject: 'harness' } : null,
    });
    publicApp = buildPublicApp(options);
    await Promise.all([app.ready(), publicApp.ready()]);
  }, 60000);

  afterEach(async () => {
    await Promise.all(running.splice(0).map((close) => close()));
  });
  afterAll(async () => {
    try {
      const errors: unknown[] = [];
      // Each exact path was returned by this workspace's authenticated API.
      for (const url of resources.reverse()) {
        try {
          const response = await publicApp.inject({
            method: 'DELETE',
            url,
            headers: { 'x-api-key': apiKey },
          });
          expect(response.statusCode, response.body).toBe(200);
        } catch (error) {
          errors.push(error);
        }
      }
      if (errors.length) throw new AggregateError(errors, 'Native budget cleanup failed');
    } finally {
      if (db)
        await db
          .delete(modelPrices)
          .where(and(eq(modelPrices.modelId, pricedModel), eq(modelPrices.provider, 'anthropic')));
      await Promise.all([app?.close(), publicApp?.close()]);
      await transcript?.close();
      await closeTestDb();
    }
  }, 60000);

  async function publicPost(url: string, payload: Record<string, unknown>) {
    const response = await publicApp.inject({
      method: 'POST',
      url,
      headers: { 'x-api-key': apiKey },
      payload,
    });
    expect([200, 201], response.body).toContain(response.statusCode);
    return response.json();
  }

  async function fixture(
    options: {
      sessionId?: string;
      cap?: number;
      unpriced?: boolean;
      missingUsage?: boolean;
      unknownUsage?: boolean;
      usageAckLost?: boolean;
      missingRecord?: boolean;
      duplicateUsage?: boolean;
      concurrentFirstUsage?: boolean;
      eventAckLost?: boolean;
      unsupported?: 'soft' | 'subagent';
    } = {},
  ) {
    let sessionId = options.sessionId;
    let policyId: string | undefined;
    let guards: NonNullable<SessionStartInput['guardrails']>;
    if (!sessionId) {
      const policy = await publicPost(
        '/apis/policy.runorca.ai/v1/guardrails',
        guard(options.cap ?? 3),
      );
      policyId = policy.id;
      resources.push('/apis/policy.runorca.ai/v1/guardrails/' + policy.id);
      const environment = await publicPost('/v1/environments', { name: 'budget-' + randomUUID() });
      resources.push('/v1/environments/' + environment.id);
      const agent = await publicPost('/v1/agents', {
        name: 'native-budget',
        model: { provider: 'anthropic', id: model },
        tools: [],
        mcp_servers: [],
        skills: [],
        metadata: { harness: 'dsh_sdk', mode: 'colocated' },
        guardrail_ids: [policy.id],
      });
      resources.push('/v1/agents/' + agent.id);
      const session = await publicPost('/v1/sessions', {
        environment_id: environment.id,
        agent_id: agent.id,
      });
      sessionId = session.id as string;
      resources.push('/v1/sessions/' + sessionId);
    }
    const base = '/internal/v1/workspaces/' + workspaceId + '/sessions/' + sessionId;
    const statuses: Array<{ action: string; status: number }> = [];
    const raw = async (body: HarnessTurnRequest) => {
      const response = await app.inject({
        method: 'POST',
        url: base + '/harness-turn',
        headers,
        payload: body,
      });
      statuses.push({ action: body.action.type, status: response.statusCode });
      expect(response.statusCode, response.body).toBe(200);
      return response.json() as HarnessTurnSnapshot;
    };
    const post = async (path: string, payload: Record<string, unknown>) => {
      const response = await app.inject({ method: 'POST', url: base + path, headers, payload });
      expect(response.statusCode, response.body).toBe(200);
      return response.json();
    };
    const prepared = await post('/executions:prepare', {});
    guards = prepared.guardrails;
    expect(guards).toHaveLength(1);
    if (policyId) expect(guards[0]!.id).toBe(policyId);
    expect(guards![0]!.rule).toMatchObject({
      builtin: 'cost_budget',
      params: { max_cost_usd: options.cap ?? 3 },
    });
    if (options.unsupported) {
      guards = [
        {
          ...guards![0]!,
          rule:
            options.unsupported === 'soft'
              ? guard(3, 'cost_budget', { ask_thresholds_usd: [1] }).rule
              : guard(3, 'subagent_cost_budget').rule,
        },
      ];
    }
    const totals = () => loadGuardrailUsageState(db, workspaceId, sessionId!);
    const usageRows = () =>
      db
        .select()
        .from(sessionUsageEvents)
        .where(
          and(
            eq(sessionUsageEvents.workspaceId, workspaceId),
            eq(sessionUsageEvents.sessionId, sessionId!),
          ),
        );
    const markers = () =>
      db
        .select()
        .from(guardrailState)
        .where(
          and(
            eq(guardrailState.workspaceId, workspaceId),
            eq(guardrailState.sessionId, sessionId!),
          ),
        );
    let usagePayload: Record<string, unknown> | undefined;
    const onUsage = vi.fn(async (usage, _model, _child, turnEventId, usageEventId) => {
      usagePayload = {
        usage,
        provider: 'anthropic',
        model: options.unpriced ? pricedModel + '-unpriced' : pricedModel,
        turn_event_id: turnEventId,
        usage_event_id: usageEventId,
      };
      if (options.missingRecord) return await totals();
      const copies = await Promise.all(
        Array.from({ length: options.concurrentFirstUsage ? 3 : 1 }, () =>
          post('/usage', usagePayload!),
        ),
      );
      for (const copy of copies)
        expect(copy.guardrail_usage_state).toEqual(copies[0].guardrail_usage_state);
      if (options.duplicateUsage) {
        const replays = await Promise.all([
          post('/usage', usagePayload),
          post('/usage', usagePayload),
        ]);
        for (const replay of replays)
          expect(replay.guardrail_usage_state).toEqual(copies[0].guardrail_usage_state);
      }
      if (options.usageAckLost) throw new Error('usage committed; ACK lost');
      if (options.unknownUsage) return undefined;
      return copies[0].guardrail_usage_state;
    });
    const registry = {
      harnessTurn: async ({ request }: { request: HarnessTurnRequest }) => raw(request),
    } as unknown as RegistryClient;
    const dispatcher = new Dispatcher({
      store: transcript,
      registry,
      groupId: 'dsh-budget',
      anthropicApiKey: '',
      modelDefault: model,
    });
    const initial = await raw({ action: { type: 'inspect' } });
    const underlying = dispatcher['codexTurnStore'](
      workspaceId,
      sessionId!,
      prepared.session.runtime_revision,
      initial?.ownershipRevision ?? 0,
    );
    const commits: Array<{ before: HarnessTurnSnapshot; after: HarnessTurnSnapshot }> = [];
    const acknowledged = new Set<string>();
    const turns = {
      ...underlying,
      update: async (action: Parameters<typeof underlying.update>[0]) => {
        const before =
          action.type === 'commit' ? await raw({ action: { type: 'inspect' } }) : undefined;
        if (before && action.type === 'commit' && !action.error)
          expect(acknowledged.has(before.receipt!.usageEventId)).toBe(true);
        const after = await underlying.update(action);
        if (before) commits.push({ before, after });
        return after;
      },
    };
    let providerCalls = 0;
    const server = createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on('data', (chunk) => chunks.push(chunk));
      req.on('end', () => {
        providerCalls++;
        const body = JSON.parse(Buffer.concat(chunks).toString());
        res.writeHead(200, { 'content-type': 'text/event-stream' });
        replyMessages(res, body.model, [{ type: 'text', text: 'OK' }], 'end_turn', {
          input_tokens: 1,
          output_tokens: 0,
          cache_read_input_tokens: 0,
        });
      });
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    let sdkSubmits = 0;
    const starts: unknown[] = [];
    const createWorker = vi.fn((emit) => {
      const worker = new DshSdkWorker((event) => {
        // Fault injection drops only terminal usage; execution/checkpoint remain native.
        if (options.missingUsage && event.type === 'event' && event.event.type === 'turn.completed')
          return;
        emit(event);
      });
      const handle = worker.handle.bind(worker);
      worker.handle = async (command) => {
        if (command.type === 'submit') sdkSubmits++;
        if (command.type === 'start') starts.push(command.checkpoint);
        await handle(command);
      };
      return worker;
    });
    const h = new CodexSdkHarness({
      harness: 'dsh_sdk',
      apiKey: 'fixture',
      turns,
      createWorker,
      dshGatewayUrl: 'http://127.0.0.1:' + (server.address() as { port: number }).port,
      llmGatewayJwtProvider: {
        getValidToken: async () => ({ token: 'fixture', expiresAt: Date.now() / 1000 + 660 }),
        close() {},
      },
      onUsage,
      onGuardrailState: async (updates) => {
        await post('/guardrail-state', { updates });
      },
    });
    const events: Event[] = [];
    let failedAppend = false;
    const pump = (async () => {
      for await (const event of h.events()) {
        const row: Event = {
          ...event,
          workspaceId,
          sessionId: sessionId!,
          seq: events.length,
          producedAt: event.producedAt ?? new Date().toISOString(),
          producedBy: 'harness',
          subpath: event.subpath ?? '',
          payload: Buffer.from(JSON.stringify(event.payload)),
          idempotencyKey: '',
        };
        try {
          await transcript.append(workspaceId, sessionId!, [row]);
          events.push(row);
          if (options.eventAckLost && event.kind === 'agent.usage')
            throw new Error('Kafka append committed; ACK lost');
          acknowledged.add(event.id);
          event.persistence?.resolve();
        } catch (error) {
          failedAppend = true;
          event.persistence?.reject(error as Error);
        }
      }
    })();
    let closed = false;
    const close = async () => {
      if (closed) return;
      closed = true;
      await h.stop('replica.shutting_down');
      await pump;
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
      expect(failedAppend).toBe(options.eventAckLost ?? false);
    };
    running.push(close);
    const submit = async (event = request(), hooks?: SubmitHooks) => {
      const source: Event = {
        ...event,
        id: event.id!,
        workspaceId,
        sessionId: sessionId!,
        subpath: '',
        seq: 0,
        producedAt: new Date().toISOString(),
        producedBy: 'client',
        payload: Buffer.from(JSON.stringify(event.payload)),
        idempotencyKey: event.id!,
      };
      await transcript.append(workspaceId, sessionId!, [source]);
      await indexTranscriptEvents(db, [source], { guardrailSubject: 'native-budget-test' });
      return h.submit(event, hooks);
    };
    const assertCommitted = async () => {
      const snapshot = await raw({ action: { type: 'inspect' } });
      expect(snapshot.receipt).toMatchObject({ guarded: true, phase: 'settled', error: null });
      expect(snapshot.state).toMatchObject({ format: 'dsh_sdk' });
      expect(commits.length).toBeGreaterThan(0);
      for (const commit of commits) {
        const key = 'codex_sdk_usage_pending:' + commit.before.receipt!.turnId;
        expect(commit.before.receipt!.phase).toBe('pending');
        expect(commit.before.guardrailState).toHaveProperty(key);
        expect(commit.after.receipt!.phase).toBe('ready');
        expect(commit.after.guardrailState).not.toHaveProperty(key);
        expect(
          (await usageRows()).some((row) => row.eventId === commit.after.receipt!.usageEventId),
        ).toBe(true);
      }
      const received: Event[] = [];
      for await (const event of transcript.read(workspaceId, sessionId!, {
        fromCursor: '',
        maxEvents: 1000,
        subpath: '*',
      }))
        received.push(event);
      expect(events.every((event) => received.some((row) => row.id === event.id))).toBe(true);
      expect(received.some((event) => event.id === snapshot.receipt!.usageEventId)).toBe(true);
      expect(received.some((event) => event.id === snapshot.receipt!.terminalEventId)).toBe(true);
      return snapshot;
    };
    return {
      h,
      sessionId,
      submit,
      close,
      totals,
      usageRows,
      markers,
      statuses,
      commits,
      onUsage,
      createWorker,
      starts,
      count: () => sdkSubmits,
      providerCalls: () => providerCalls,
      inspect: () => raw({ action: { type: 'inspect' } }),
      start: () =>
        h.start({
          workspaceId,
          sessionId: sessionId!,
          archivedThreadSubpaths: [],
          agentSnapshot: { model_provider: 'anthropic', model_id: model },
          guardrails: guards,
        }),
      assertCommitted,
      replayUsage: () => post('/usage', usagePayload!),
      turns,
      raw,
    };
  }

  it('under cap admits two native turns with durable matching usage, Kafka events and settled receipts', async () => {
    const f = await fixture({ cap: 10 });
    await f.start();
    await f.submit();
    await f.assertCommitted();
    expect(await f.totals()).toMatchObject({ session_cost_usd: 3, total_tokens: 3 });
    await f.submit();
    await f.assertCommitted();
    expect(await f.totals()).toMatchObject({ session_cost_usd: 6, total_tokens: 6 });
    expect(await f.usageRows()).toHaveLength(2);
    expect(f.count()).toBe(2);
    expect(f.providerCalls()).toBe(2);
  });

  it.each([3, 2])('at/above cap %s denies next root without SDK/model/tool work', async (cap) => {
    const f = await fixture({ cap });
    await f.start();
    await f.submit();
    await f.assertCommitted();
    const before = await f.totals();
    await expect(f.submit()).rejects.toThrow(/budget/);
    expect(await f.totals()).toEqual(before);
    expect(f.count()).toBe(1);
    expect(f.providerCalls()).toBe(1);
    expect(await f.usageRows()).toHaveLength(1);
  });

  it('replacement native adapter claims persisted checkpoint/spend and denies before execution', async () => {
    const old = await fixture();
    await old.start();
    await old.submit();
    const snapshot = await old.assertCommitted();
    await old.close();
    const fresh = await fixture({ sessionId: old.sessionId });
    await fresh.start();
    const restored = await fresh.inspect();
    expect(restored.state).toEqual(snapshot.state);
    expect(fresh.starts).toEqual([snapshot.state]);
    expect(restored.ownershipRevision).toBe(snapshot.ownershipRevision + 1);
    await expect(fresh.submit()).rejects.toThrow(/budget/);
    expect(fresh.providerCalls()).toBe(0);
    expect(fresh.count()).toBe(0);
    expect(await fresh.totals()).toMatchObject({ session_cost_usd: 3 });
    await expect(
      old.turns.update({ type: 'settle', turnId: snapshot.receipt!.turnId }),
    ).rejects.toThrow();
    expect(old.statuses.at(-1)).toEqual({ action: 'settle', status: 409 });
  });

  it('unpriced usage persists NULL cost and denies the next inference', async () => {
    const f = await fixture({ cap: 10, unpriced: true });
    await f.start();
    await f.submit();
    await f.assertCommitted();
    expect(await f.totals()).toMatchObject({ session_usage_has_unpriced: true, total_tokens: 3 });
    const [stored] = await db
      .select({ cost: sessions.usageCostNanoUsd })
      .from(sessions)
      .where(and(eq(sessions.workspaceId, workspaceId), eq(sessions.id, f.sessionId!)));
    expect(stored!.cost).toBeNull();
    await expect(f.submit()).rejects.toThrow(/price data/);
    expect(f.providerCalls()).toBe(1);
  });

  it.each([
    'missingUsage',
    'unknownUsage',
    'usageAckLost',
    'missingRecord',
    'eventAckLost',
  ] as const)(
    '%s preserves pending accounting and fails closed after replacement',
    async (mode) => {
      const f = await fixture({ cap: 10, [mode]: true });
      await f.start();
      await expect(f.submit()).rejects.toThrow();
      const failed = await f.inspect();
      // These fresh Sessions have no earlier committed checkpoint to preserve.
      expect(failed.state).toBeNull();
      expect(failed.guardrailState).toHaveProperty(
        'codex_sdk_usage_pending:' + failed.receipt!.turnId,
      );
      if (mode === 'missingRecord')
        expect(f.statuses).toContainEqual({ action: 'commit', status: 409 });
      if (mode === 'eventAckLost')
        expect(f.statuses).not.toContainEqual({ action: 'commit', status: 200 });
      expect(await f.usageRows()).toHaveLength(
        ['missingUsage', 'missingRecord'].includes(mode) ? 0 : 1,
      );
      if (mode === 'usageAckLost') {
        await Promise.all([f.replayUsage(), f.replayUsage()]);
        expect(await f.usageRows()).toHaveLength(1);
        expect(await f.totals()).toMatchObject({ session_cost_usd: 3, total_tokens: 3 });
      }
      await expect(f.submit()).rejects.toThrow();
      expect(f.providerCalls()).toBe(1);
      await f.close();
      const fresh = await fixture({ sessionId: f.sessionId, cap: 10 });
      await fresh.start();
      await expect(fresh.submit()).rejects.toThrow(/accounting/);
      expect(fresh.count()).toBe(0);
      expect(fresh.providerCalls()).toBe(0);
      expect((await fresh.inspect()).guardrailState).toHaveProperty(
        'codex_sdk_usage_pending:' + failed.receipt!.turnId,
      );
    },
  );

  it.each(['duplicateUsage', 'concurrentFirstUsage'] as const)(
    '%s charges one durable identity once',
    async (mode) => {
      const f = await fixture({ [mode]: true });
      await f.start();
      await f.submit();
      await f.assertCommitted();
      expect(await f.usageRows()).toHaveLength(1);
      expect(await f.totals()).toMatchObject({ session_cost_usd: 3, total_tokens: 3 });
    },
  );

  it('queued concurrent root admission sees authoritative first-turn totals', async () => {
    const f = await fixture();
    await f.start();
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const accepted = vi.fn(() => gate);
    const nextAccepted = vi.fn();
    const first = f.submit(request(), { onAccepted: accepted });
    try {
      await vi.waitFor(() => expect(accepted).toHaveBeenCalledTimes(1));
      const second = expect(f.submit(request(), { onAccepted: nextAccepted })).rejects.toThrow(
        /budget/,
      );
      expect(f.providerCalls()).toBe(0);
      expect(nextAccepted).not.toHaveBeenCalled();
      release();
      await first;
      await second;
      // Acceptance records the source; only SDK/model admission is budget-gated.
      expect(nextAccepted).toHaveBeenCalledTimes(1);
      expect(f.providerCalls()).toBe(1);
      expect(f.count()).toBe(1);
      await f.assertCommitted();
    } finally {
      release();
    }
  });

  it.each(['soft', 'subagent'] as const)(
    'rejects unsupported %s policy before native worker creation',
    async (unsupported) => {
      const f = await fixture({ unsupported });
      await expect(f.start()).rejects.toThrow(
        unsupported === 'soft' ? /soft approval/ : /subagent budget/,
      );
      expect(f.createWorker).not.toHaveBeenCalled();
      expect(f.onUsage).not.toHaveBeenCalled();
      expect(f.providerCalls()).toBe(0);
      expect(await f.usageRows()).toHaveLength(0);
    },
  );
});
