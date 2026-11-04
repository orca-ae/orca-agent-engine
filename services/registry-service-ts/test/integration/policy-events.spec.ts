// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterAll, beforeAll, beforeEach, describe, it, expect } from 'vitest';
import { randomUUID } from 'node:crypto';
import { eq, and } from 'drizzle-orm';
import { evaluate, finalizeOutcome, type Rule, type PolicyCall } from '@runorca/dogwood';
import { buildCombinedTestApp } from '../../src/server.js';
import {
  bindPolicyTurn,
  readPolicyEvents,
  appendPolicyEvents,
  PolicyLogError,
} from '../../src/domain/policy-events.js';
import {
  guardrailPolicyCalls,
  guardrailState,
  sessionEventsIndex,
  vaults,
  sessions,
  apiKeys,
  guardrails,
} from '../../src/persistence/postgres/schema.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import type { PolicyAppendBody } from '../../src/contracts/policy-events.contract.js';
import { applyCounterStateUpdate } from '../../src/domain/usage-accounting.js';
import { PolicyRead, PolicyAppendResult } from '../../src/contracts/policy-events.contract.js';
import { GUARDRAIL_READER_CAPABILITIES } from '@orca/guardrails';
import {
  getTestDb,
  closeTestDb,
  buildStubFileStore,
  buildStubStore,
  buildTestJwtMinter,
  STUB_SSE_CONFIG,
} from './setup.js';
import {
  createTestApiKey,
  createTestAgent,
  createTestSession,
  uniqueWorkspace,
} from './fixtures.js';
const allow = '@id("baseline") permit(principal,action,resource);';
const temporal =
  allow +
  '\n@id("once") forbid(principal,action == Orca::Action::"bash",resource) when temporal { count_within(1h, Orca::Action::"bash"::request{}) > 1 };';
const ask =
  allow + '\n@id("confirm") @ask forbid(principal,action == Orca::Action::"bash",resource);';
describe('Dogwood PostgreSQL authorization transaction', () => {
  let db: DbClient,
    app: ReturnType<typeof buildCombinedTestApp>,
    ws: string,
    key: string,
    id: string,
    agent: string;
  let time = Date.parse('2026-11-02T00:00:00Z');
  const clock = () => new Date(time);
  const prepared = (policy: string, mode: 'enforce' | 'observe' = 'enforce') => [
    {
      id: 'grd_policytest',
      name: 'policy',
      tier: 'workspace' as const,
      phases: ['tool_call', 'tool_result'],
      stateful: true,
      mode,
      rule: { kind: 'dogwood', policy },
    },
  ];
  async function bind(
    policy = allow,
    owner = randomUUID(),
    turnEventId?: string,
    extra: import('../../src/domain/dogwood-types.js').DogwoodPrepared[] = [],
    principalHistory = false,
  ) {
    if (!turnEventId) {
      turnEventId = `evt_${randomUUID()}`;
      await db.insert(sessionEventsIndex).values({
        workspaceId: ws,
        sessionId: id,
        seq: time,
        eventId: turnEventId,
        producedAt: clock().toISOString(),
        producedBy: 'client',
        kind: 'user.message',
        visibility: 'public',
        guardrailSubject: 'user:caller',
        payload: {},
      });
    }
    return db.transaction((tx) =>
      bindPolicyTurn(tx, {
        workspaceId: ws,
        sessionId: id,
        owner,
        guardrails: [
          ...prepared(policy).map((g) => ({
            ...g,
            rule: { ...g.rule, ...(principalHistory ? { history: 'principal' } : {}) },
          })),
          ...extra,
        ],
        enforcer: 'harness',
        resources: [{ id: agent, name: 'test-agent' }],
        mcpServers: [],
        now: clock,
        ...(turnEventId ? { turnEventId } : {}),
      }),
    );
  }
  async function decision(
    tool: string,
    verdict?: 'allow' | 'ask' | 'deny',
  ): Promise<PolicyAppendBody> {
    const view = await readPolicyEvents(db, ws, id, { history: 'principal' }, clock, 'harness');
    const call: PolicyCall = {
      action: 'bash',
      principal: view.principal,
      resource: view.resource,
      request_id: tool,
      session_id: id,
      input: { command: 'ls' },
    };
    const results = view.guardrails.map((g) => ({
      g,
      r: evaluate({
        guardrail_id: g.id,
        rule: g.rule as Rule,
        call,
        now_ms: time,
        history: view.events.map((e) => e.event),
      }),
    }));
    const folded =
      verdict ?? (results[0]!.r.verdict === 'abstain' ? 'allow' : results[0]!.r.verdict);
    return {
      batch_id: randomUUID(),
      tool_use_id: tool,
      epoch: view.epoch,
      bundle_id: view.bundle_id,
      expected_head: view.head,
      expected_state_revision: view.state_revision,
      day_window: view.day_window,
      month_window: view.month_window,
      decision: {
        call,
        verdict: folded,
        dogwood: results.map(({ g, r }) => ({
          guardrail_id: g.id,
          verdict: r.verdict,
          deciding_rules: r.deciding_rules.map((v) => v.rule_id),
          errors: r.errors,
        })),
        public_tool_use_id: `public-${tool}`,
      },
      events: folded === 'ask' ? [] : results[0]!.r.events[folded === 'allow' ? 'allow' : 'deny'],
    };
  }
  beforeAll(async () => {
    ({ db } = await getTestDb());
    ws = uniqueWorkspace('policy');
    key = await createTestApiKey(db, ws);
    await db
      .update(apiKeys)
      .set({ scopes: ['workspace.full_access'] })
      .where(eq(apiKeys.workspaceId, ws));
    app = buildCombinedTestApp({
      db,
      oidc: { allowedIssuers: [], audience: 'test' },
      store: buildStubStore(),
      fileStore: buildStubFileStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      guardrails: { dogwoodEnabled: true, observeEnabled: true, now: clock },
    });
    await app.listen({ host: '127.0.0.1', port: 0 });
    const address = app.server.address() as { port: number };
    const url = `http://127.0.0.1:${address.port}`;
    agent = await createTestAgent(url, key);
    id = await createTestSession(url, key, agent);
    await bind();
  });
  beforeEach(async () => {
    const address = app.server.address() as { port: number };
    id = await createTestSession(`http://127.0.0.1:${address.port}`, key, agent);
    await bind();
  });
  afterAll(async () => {
    await app?.close();
    await closeTestDb();
  });
  it('returns immutable historical receipt before new epoch/time checks, with no extra reservation', async () => {
    const body = await decision('one');
    const first = await appendPolicyEvents(db, ws, id, body, clock, 'harness');
    expect(first.disposition).toBe('created');
    expect(first.events.map((e) => e.event.kind)).toEqual(['request']);
    await bind();
    time += 1000;
    const replay = await appendPolicyEvents(db, ws, id, body, clock, 'harness');
    expect(replay.disposition).toBe('replayed');
    expect(replay.receipt).toEqual(first.receipt);
    await expect(
      appendPolicyEvents(db, ws, id, { ...body, events: [] }, clock, 'harness'),
    ).rejects.toMatchObject({ code: 'decision_changed' });
  });
  it('fences heads, tenant scope, epoch and changed policy independently', async () => {
    const body = await decision('fenced');
    await expect(
      appendPolicyEvents(db, ws, id, { ...body, epoch: '0' }, clock, 'harness'),
    ).rejects.toMatchObject({ code: 'stale_epoch' });
    await expect(
      appendPolicyEvents(db, ws, id, { ...body, bundle_id: 'wrong' }, clock, 'harness'),
    ).rejects.toMatchObject({ code: 'bundle_conflict' });
    await expect(
      appendPolicyEvents(
        db,
        ws,
        id,
        { ...body, expected_head: { session: '999', principal: null } },
        clock,
        'harness',
      ),
    ).rejects.toMatchObject({ code: 'head_conflict' });
    await expect(readPolicyEvents(db, 'ws_other', id, {}, clock, 'harness')).rejects.toThrow();
  });
  it('keeps a committed request reservation when preparation closes it interrupted', async () => {
    await bind(temporal);
    const body = await decision('reservation');
    expect(body.decision!.dogwood[0]!.errors).toEqual([]);
    expect(body.decision!.verdict).toBe('allow');
    await appendPolicyEvents(db, ws, id, body, clock, 'harness');
    await bind(temporal);
    const next = await decision('second');
    expect(next.decision!.verdict).toBe('deny');
    const view = await readPolicyEvents(db, ws, id, {}, clock, 'harness');
    expect(
      view.events.some(
        (e) => e.event.request_id === 'reservation' && e.event.error_type === 'interrupted',
      ),
    ).toBe(true);
  });
  it('re-evaluates after idle without a head change and rejects crossing the final exclusive boundary', async () => {
    await appendPolicyEvents(db, ws, id, await decision('seed'), clock, 'harness');
    const seedView = await readPolicyEvents(db, ws, id, {}, clock, 'harness');
    await appendPolicyEvents(
      db,
      ws,
      id,
      {
        batch_id: 'seed-result',
        tool_use_id: 'seed',
        epoch: seedView.epoch,
        bundle_id: seedView.bundle_id,
        expected_head: seedView.head,
        events: [],
        outcome: 'response',
      },
      clock,
      'harness',
    );
    await bind(
      allow +
        '\n@id("wait") forbid(principal,action,resource) unless temporal { formerly within 1h Orca::Action::"bash"::response{} };',
    );
    const body = await decision('idle');
    time += 3600001;
    await expect(appendPolicyEvents(db, ws, id, body, clock, 'harness')).rejects.toMatchObject({
      code: 'decision_changed',
    });
    await bind(temporal);
    const finite = await decision('finite');
    const before = time;
    let calls = 0;
    await expect(
      appendPolicyEvents(
        db,
        ws,
        id,
        finite,
        () => new Date(++calls === 1 ? before : before + 7200000),
        'harness',
      ),
    ).rejects.toBeInstanceOf(PolicyLogError);
    expect(
      (await readPolicyEvents(db, ws, id, {}, clock, 'harness')).events.some(
        (e) => e.event.request_id === 'finite',
      ),
    ).toBe(false);
  });
  it('accepts a bound constant ask confirmation; rejects foreign or obsolete confirmation', async () => {
    await bind(ask);
    let body = await decision('asked');
    expect(body.decision!.verdict).toBe('ask');
    await appendPolicyEvents(db, ws, id, body, clock, 'harness');
    const eventId = `evt_${randomUUID()}`;
    await db.insert(sessionEventsIndex).values({
      workspaceId: ws,
      sessionId: id,
      seq: 91,
      eventId,
      producedAt: clock().toISOString(),
      producedBy: 'client',
      kind: 'user.tool_confirmation',
      visibility: 'public',
      guardrailSubject: 'user:approver',
      payload: { tool_use_id: 'public-asked', result: 'allow' },
    });
    body = await decision('asked', 'allow');
    body.decision!.confirmation = { event_id: eventId, tool_use_id: 'public-asked' };
    body.events = finalizeOutcome({
      guardrail_id: 'grd_policytest',
      rule: { policy: ask },
      call: body.decision!.call as PolicyCall,
      now_ms: time,
      outcome: 'allowed',
    }).events;
    const valid = body.decision!.confirmation!;
    body.decision!.confirmation = { event_id: 'evt_missing', tool_use_id: 'public-asked' };
    await expect(appendPolicyEvents(db, ws, id, body, clock, 'harness')).rejects.toMatchObject({
      code: 'decision_changed',
    });
    body.decision!.confirmation = valid;
    const result = await appendPolicyEvents(db, ws, id, body, clock, 'harness');
    expect(result.events.map((e) => e.event.kind)).toEqual(['approval', 'request']);
    expect(result.events[0]!.event.approver).toBe('user:approver');
  });
  it('records completion after expiry, once, without authorizing again', async () => {
    await appendPolicyEvents(db, ws, id, await decision('asked'), clock, 'harness');
    const view = await readPolicyEvents(db, ws, id, {}, clock, 'harness');
    time += 7200000;
    const completion: PolicyAppendBody = {
      batch_id: 'completion-asked',
      tool_use_id: 'asked',
      epoch: view.epoch,
      bundle_id: view.bundle_id,
      expected_head: view.head,
      events: [],
      outcome: 'response',
      output: { raw: 'not retained' },
    };
    const first = await appendPolicyEvents(db, ws, id, completion, clock, 'harness');
    expect(first.events[0]!.event.kind).toBe('response');
    expect(first.events[0]!.event.output).toBeUndefined();
    expect((await appendPolicyEvents(db, ws, id, completion, clock, 'harness')).disposition).toBe(
      'replayed',
    );
  });
  it('rolls private state back on failure and commits its authoritative snapshot with the log', async () => {
    await bind();
    const row = (
      await db
        .select()
        .from(guardrailPolicyCalls)
        .where(
          and(eq(guardrailPolicyCalls.workspaceId, ws), eq(guardrailPolicyCalls.sessionId, id)),
        )
    )[0];
    expect(row).toBeUndefined();
    const body = await decision('atomic');
    body.state_updates = [{ scope: 'session', key: 'g:unknown:counter', action: 'increment' }];
    await expect(appendPolicyEvents(db, ws, id, body, clock, 'harness')).rejects.toThrow('private');
    expect(
      (
        await db
          .select()
          .from(guardrailState)
          .where(
            and(eq(guardrailState.workspaceId, ws), eq(guardrailState.key, 'g:unknown:counter')),
          )
      ).length,
    ).toBe(0);
    expect(
      (await readPolicyEvents(db, ws, id, {}, clock, 'harness')).events.some(
        (e) => e.event.request_id === 'atomic',
      ),
    ).toBe(false);
  });
  it('commits private state and log together and fences usage changes across unrelated sessions', async () => {
    const counter = {
      id: 'grd_counter',
      name: 'counter',
      tier: 'workspace' as const,
      phases: ['tool_call'],
      stateful: true,
      state_scope: 'session' as const,
      rule: { kind: 'builtin', builtin: 'max_tool_calls', params: { max_calls: 10 } },
    };
    await bind(allow, randomUUID(), undefined, [counter]);
    const body = await decision('atomic-valid');
    body.state_updates = [
      { scope: 'session', key: 'g:grd_counter:count', action: 'increment', value: 1 },
    ];
    const first = await appendPolicyEvents(db, ws, id, body, clock, 'harness');
    expect(first.guardrail_state!.session['g:grd_counter:count']).toBe(1);
    expect(first.events[0]!.event.kind).toBe('request');
    const resultView = await readPolicyEvents(db, ws, id, {}, clock, 'harness');
    const resultBody: PolicyAppendBody = {
      batch_id: 'atomic-result',
      tool_use_id: 'atomic-valid',
      expected_head: resultView.head,
      expected_state_revision: resultView.state_revision,
      epoch: resultView.epoch,
      bundle_id: resultView.bundle_id,
      day_window: resultView.day_window,
      month_window: resultView.month_window,
      outcome: 'response',
      events: [],
      state_updates: [
        { scope: 'session', key: 'g:grd_counter:count', action: 'increment', value: 1 },
      ],
    };
    const result = await appendPolicyEvents(db, ws, id, resultBody, clock, 'harness');
    expect(result.events[0]!.event.kind).toBe('response');
    expect(result.guardrail_state!.session['g:grd_counter:count']).toBe(2);
    expect((await appendPolicyEvents(db, ws, id, resultBody, clock, 'harness')).disposition).toBe(
      'replayed',
    );
    const beforeTime = time,
      wrongPeriod = await decision('wrong-period');
    time += 86400000;
    await expect(
      appendPolicyEvents(db, ws, id, wrongPeriod, clock, 'harness'),
    ).rejects.toMatchObject({ code: 'head_conflict' });
    time = beforeTime;
    const stale = await decision('stale-state');
    await db.transaction((tx) =>
      applyCounterStateUpdate(
        tx,
        ws,
        'user:caller',
        '2026-11-02',
        { scope: 'subject_window', key: 'daily_cost_usd', action: 'increment', value: 5 },
        clock(),
      ),
    );
    await expect(appendPolicyEvents(db, ws, id, stale, clock, 'harness')).rejects.toMatchObject({
      code: 'head_conflict',
    });
    const fresh = await readPolicyEvents(db, ws, id, {}, clock, 'harness');
    expect(fresh.guardrail_state!.subject_window.daily_cost_usd).toBe(5);
    expect(BigInt(fresh.state_revision!)).toBeGreaterThan(BigInt(stale.expected_state_revision!));
    expect(
      (
        await db
          .select()
          .from(guardrailState)
          .where(
            and(
              eq(guardrailState.workspaceId, ws),
              eq(guardrailState.sessionId, id),
              eq(guardrailState.key, 'g:grd_counter:count'),
            ),
          )
      )[0]!.valueNum,
    ).toBe(2);
  });
  it('rejects a Dogwood private key behind a shorter builtin namespace atomically', async () => {
    const short = {
      id: 'grd_prefix',
      name: 'short',
      tier: 'workspace' as const,
      phases: ['tool_call'],
      stateful: true,
      state_scope: 'session' as const,
      rule: { kind: 'builtin', builtin: 'max_tool_calls', params: { max_calls: 10 } },
    };
    const longer = { ...prepared(allow)[0]!, id: 'grd_prefix:dogwood' };
    await bind(allow, randomUUID(), undefined, [short, longer]);
    const before = await readPolicyEvents(db, ws, id, {}, clock, 'harness');
    const body = await decision('prefix-owner');
    body.state_updates = [
      { scope: 'session', key: 'g:grd_prefix:dogwood:count', action: 'increment', value: 1 },
    ];
    await expect(appendPolicyEvents(db, ws, id, body, clock, 'harness')).rejects.toThrow('private');
    const after = await readPolicyEvents(db, ws, id, {}, clock, 'harness');
    expect(after.head).toEqual(before.head);
    expect(after.state_revision).toEqual(before.state_revision);
    expect(after.guardrail_state!.session['g:grd_prefix:dogwood:count']).toBeUndefined();
    expect(after.events.some((e) => e.event.request_id === 'prefix-owner')).toBe(false);
  });
  it('reads and writes declared counter periods with overlapping colon-containing IDs', async () => {
    const daily = {
      id: 'grd_period',
      name: 'daily',
      tier: 'workspace' as const,
      phases: ['tool_call'],
      stateful: true,
      state_scope: 'subject_window' as const,
      rule: { kind: 'builtin', builtin: 'user_daily_cost_budget', params: { max_cost_usd: 10 } },
    };
    const monthly = {
      ...daily,
      id: 'grd_period:monthly',
      name: 'monthly',
      rule: { kind: 'builtin', builtin: 'user_monthly_cost_budget', params: { max_cost_usd: 100 } },
    };
    await bind(allow, randomUUID(), undefined, [daily, monthly]);
    const view = await readPolicyEvents(db, ws, id, {}, clock, 'harness');
    const dailyKey = 'g:grd_period:asked',
      monthlyKey = 'g:grd_period:monthly:asked';
    await db.transaction(async (tx) => {
      for (const key of [dailyKey, monthlyKey, 'monthly_note']) {
        for (const [window, value] of [
          [view.day_window, 2],
          [view.month_window, 7],
        ] as const) {
          await applyCounterStateUpdate(
            tx,
            ws,
            'user:caller',
            window,
            { scope: 'subject_window', key, action: 'set', value },
            clock(),
          );
        }
      }
    });
    const snapshot = await readPolicyEvents(db, ws, id, {}, clock, 'harness');
    expect(snapshot.guardrail_state!.subject_window).toMatchObject({
      [dailyKey]: 2,
      [monthlyKey]: 7,
      monthly_note: 2,
    });
    const body = await decision('monthly-private');
    body.turn_event_id = snapshot.turn_event_id!;
    body.window = snapshot.month_window;
    body.state_updates = [
      { scope: 'subject_window', key: monthlyKey, action: 'increment', value: 1 },
    ];
    const committed = await appendPolicyEvents(db, ws, id, body, clock, 'harness');
    expect(committed.guardrail_state!.subject_window[monthlyKey]).toBe(8);
    expect(committed.guardrail_state!.subject_window[dailyKey]).toBe(2);
    expect(committed.events[0]!.event.kind).toBe('request');
  });
  it('maps backwards preparation clocks to 503 and unavailable sessions to 404', async () => {
    const guardrail = await app.inject({
      method: 'POST',
      url: '/apis/policy.runorca.ai/v1/guardrails',
      headers: { 'x-api-key': key },
      payload: { name: 'clock-policy', rule: { kind: 'dogwood', policy: allow } },
    });
    expect(guardrail.statusCode, guardrail.body).toBe(201);
    await db
      .update(sessions)
      .set({ agentOverrides: { guardrailIds: [guardrail.json().id] } })
      .where(and(eq(sessions.workspaceId, ws), eq(sessions.id, id)));
    await appendPolicyEvents(db, ws, id, await decision('clock-seed'), clock, 'harness');
    const before = await readPolicyEvents(db, ws, id, {}, clock, 'harness');
    const path = `/internal/v1/workspaces/${ws}/sessions/${id}`;
    const initialTime = time;
    try {
      time -= 1;
      const prepared = await app.inject({
        method: 'POST',
        url: `${path}/executions:prepare`,
        payload: {
          policy_owner: 'backwards-clock',
          guardrail_capabilities: GUARDRAIL_READER_CAPABILITIES,
        },
      });
      expect(prepared.statusCode, prepared.body).toBe(503);
      expect(prepared.json()).toEqual({ error: 'policy clock moved backwards' });
    } finally {
      time = initialTime;
    }
    expect((await readPolicyEvents(db, ws, id, {}, clock, 'harness')).epoch).toBe(before.epoch);
    await db
      .update(sessions)
      .set({ status: 'terminated' })
      .where(and(eq(sessions.workspaceId, ws), eq(sessions.id, id)));
    const unavailable = await app.inject({ url: `${path}/policy-events` });
    expect(unavailable.statusCode, unavailable.body).toBe(404);
  });
  it('serializes one Vault principal across sessions without duplicate reservations', async () => {
    const vaultId = `vlt_${randomUUID()}`;
    await db.insert(vaults).values({ id: vaultId, workspaceId: ws, displayName: 'principal' });
    await db
      .update(sessions)
      .set({ vaultIds: [vaultId] })
      .where(and(eq(sessions.workspaceId, ws), eq(sessions.id, id)));
    await bind(temporal, randomUUID(), undefined, [], true);
    const firstId = id,
      first = await decision('principal-first');
    const address = app.server.address() as { port: number };
    id = await createTestSession(`http://127.0.0.1:${address.port}`, key, agent);
    await db
      .update(sessions)
      .set({ vaultIds: [vaultId] })
      .where(and(eq(sessions.workspaceId, ws), eq(sessions.id, id)));
    await bind(temporal, randomUUID(), undefined, [], true);
    const secondId = id,
      second = await decision('principal-second');
    expect(second.bundle_id).not.toBe(first.bundle_id);
    const results = await Promise.allSettled([
      appendPolicyEvents(db, ws, firstId, first, clock, 'harness'),
      appendPolicyEvents(db, ws, secondId, second, clock, 'harness'),
    ]);
    expect(results.filter((r) => r.status === 'fulfilled')).toHaveLength(1);
    const rejected = results.find((r) => r.status === 'rejected') as PromiseRejectedResult;
    expect(rejected.reason).toMatchObject({ code: 'head_conflict' });
    const view = await readPolicyEvents(db, ws, id, { history: 'principal' }, clock, 'harness');
    expect(view.events.filter((e) => e.event.kind === 'request')).toHaveLength(1);
  });
  it('serves actual schemas, tolerant unknown-kind reads, and strict prepared reader gates over HTTP', async () => {
    const headers = { 'x-api-key': key };
    const schema = await app.inject({
      url: '/apis/policy.runorca.ai/v1/policyschemas/dogwood',
      headers,
    });
    expect(schema.statusCode).toBe(200);
    expect(schema.json()).toMatchObject({ core_version: '0.1.0', language_version: '1.1.0' });
    const created = await app.inject({
      method: 'POST',
      url: '/apis/policy.runorca.ai/v1/guardrails',
      headers,
      payload: { name: 'dogwood-http', rule: { kind: 'dogwood', policy: allow } },
    });
    expect(created.statusCode, created.body).toBe(201);
    const guardrailId = created.json().id;
    await db
      .update(sessions)
      .set({ agentOverrides: { guardrailIds: [guardrailId] } })
      .where(and(eq(sessions.workspaceId, ws), eq(sessions.id, id)));
    const path = `/internal/v1/workspaces/${ws}/sessions/${id}`;
    const before = await readPolicyEvents(db, ws, id, {}, clock, 'harness');
    const old = await app.inject({
      method: 'POST',
      url: `${path}/executions:prepare`,
      payload: { policy_owner: 'http-reader' },
    });
    expect(old.statusCode).toBe(409);
    expect((await readPolicyEvents(db, ws, id, {}, clock, 'harness')).epoch).toBe(before.epoch);
    const prepared = await app.inject({
      method: 'POST',
      url: `${path}/executions:prepare`,
      payload: {
        policy_owner: 'http-reader',
        guardrail_capabilities: GUARDRAIL_READER_CAPABILITIES,
      },
    });
    expect(prepared.statusCode, prepared.body).toBe(200);
    expect(prepared.json().dogwood.enforcer).toBe('harness');
    const read = await app.inject({ url: `${path}/policy-events` });
    expect(read.statusCode, read.body).toBe(200);
    expect(read.headers['cache-control']).toBe('no-store');
    expect(PolicyRead.safeParse(read.json()).success).toBe(true);
    for (const since of ['invalid', '-1', '01', '1.5', '99999999999999999999']) {
      const invalid = await app.inject({
        url: `${path}/policy-events?since=${encodeURIComponent(since)}`,
      });
      expect(invalid.statusCode, invalid.body).toBe(400);
    }

    const body = await decision('http-authorized');
    const append = await app.inject({
      method: 'POST',
      url: `${path}/policy-events`,
      payload: body,
    });
    expect(append.statusCode, append.body).toBe(200);
    expect(append.headers['cache-control']).toBe('no-store');
    expect(PolicyAppendResult.safeParse(append.json()).success).toBe(true);
    const metrics = await app.inject({ url: '/metrics' });
    expect(metrics.statusCode).toBe(200);
    expect(metrics.body).toContain('orca_registry_policy_appends_total{result="created"}');
    expect(metrics.body).toContain('orca_registry_dogwood_evaluation_seconds_count');
    await db
      .update(guardrails)
      .set({ rule: { kind: 'future_rule', opaque: { nested: true } } })
      .where(and(eq(guardrails.workspaceId, ws), eq(guardrails.id, guardrailId)));
    const listed = await app.inject({
      url: `/apis/policy.runorca.ai/v1/guardrails/${guardrailId}`,
      headers,
    });
    expect(listed.statusCode).toBe(200);
    expect(listed.json().rule.kind).toBe('future_rule');
    const unsupported = await app.inject({
      method: 'POST',
      url: `${path}/executions:prepare`,
      payload: {
        policy_owner: 'http-reader',
        guardrail_capabilities: GUARDRAIL_READER_CAPABILITIES,
      },
    });
    expect(unsupported.statusCode).toBe(409);
  });
});
