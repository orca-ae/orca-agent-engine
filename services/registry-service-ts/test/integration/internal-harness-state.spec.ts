// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomUUID } from 'node:crypto';
import type { HarnessTurnReceipt, HarnessTurnRequest } from '@orca/harness-catalog';
import { loadHarnessState, saveRunnerHarnessState } from '../../src/domain/harness-state.js';
import { buildSnapshotRecordLoader } from '../../src/api/snapshot-loader.js';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { eq } from 'drizzle-orm';
import { InMemorySkillStore } from '@orca/skill-store';
import { buildInternalApp, buildPublicApp } from '../../src/server.js';
import type { InternalAuthVerifier } from '../../src/auth/internal-auth.js';
import { newId } from '../../src/domain/versioning.js';
import {
  agents,
  agentVersions,
  environments,
  guardrails,
  sessions,
  sessionHarnessStates,
  sessionUsageEvents,
  guardrailState,
  workspaces,
} from '../../src/persistence/postgres/schema.js';
import { createTestApiKey, uniqueWorkspace, TEST_ORGANIZATION_ID } from './fixtures.js';
import {
  getTestDb,
  closeTestDb,
  buildStubStore,
  buildStubFileStore,
  buildTestJwtMinter,
  STUB_SSE_CONFIG,
} from './setup.js';

const describeSdk = describe.each(['codex_sdk', 'pi_sdk'] as const);
describeSdk('private %s checkpoint persistence', (sdkHarness) => {
  const checkpoint = (text: string) =>
    sdkHarness === 'pi_sdk'
      ? {
          version: 1,
          format: 'pi_sdk',
          sdkVersion: '0.87.0',
          threadId: 'thread-123',
          instructionsSha256: 'a'.repeat(64),
          files: { 'session.json': Buffer.from(text).toString('base64') },
        }
      : {
          version: 1,
          threadId: 'thread-123',
          files: {
            'sessions/2026/09/20/rollout-thread-123.jsonl': Buffer.from(text).toString('base64'),
          },
        };
  let db: Awaited<ReturnType<typeof getTestDb>>['db'];
  let app: ReturnType<typeof buildInternalApp>;
  let publicApp: ReturnType<typeof buildPublicApp>;
  let apiKey: string;
  const workspaceId = uniqueWorkspace('codex_checkpoint');
  const store = buildStubStore();
  const append = vi.spyOn(store, 'append');
  const verifier: InternalAuthVerifier = {
    verify: async (token) =>
      token === 'harness' || token === 'ai-gateway' ? { caller: token, subject: token } : null,
  };

  beforeAll(async () => {
    ({ db } = await getTestDb());
    apiKey = await createTestApiKey(db, workspaceId);
    const options = {
      db,
      store,
      oidc: { allowedIssuers: [], audience: 'test' },
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
      skillStore: new InMemorySkillStore(),
    };
    app = buildInternalApp(options, verifier);
    publicApp = buildPublicApp(options);
    await Promise.all([app.ready(), publicApp.ready()]);
  });

  afterAll(async () => {
    await Promise.all([app?.close(), publicApp?.close()]);
    await closeTestDb();
  });

  async function seed(harness = sdkHarness, mode = 'separate', target = 'cloud') {
    const agentId = newId('agt');
    const sessionId = newId('ses');
    const environmentId = newId('env');
    const versionId = newId('agtv');
    const metadata = { harness, mode };
    const model =
      harness === 'codex_sdk' || harness === 'pi_sdk'
        ? { provider: 'openai', id: 'gpt-5.4-mini' }
        : { provider: 'anthropic', id: 'claude-sonnet-4' };
    await db.insert(agents).values({
      id: agentId,
      workspaceId,
      name: 'checkpoint',
      harnessType: harness,
      modelProvider: model.provider,
      modelId: model.id,
      metadata,
    });
    await db.insert(agentVersions).values({
      id: versionId,
      workspaceId,
      agentId,
      version: 1,
      snapshot: {
        id: agentId,
        name: 'checkpoint',
        version: 1,
        harness_type: harness,
        model,
        system: '',
        tools: [],
        mcp_servers: [],
        skills: [],
        metadata,
        multiagent: null,
      },
    });
    await db
      .insert(environments)
      .values({ id: environmentId, workspaceId, name: environmentId, target });
    await db.insert(sessions).values({
      id: sessionId,
      workspaceId,
      agentId,
      agentVersion: 1,
      environmentId,
      runtimeRevision: 7,
    });
    return { sessionId, agentId, versionId, environmentId };
  }

  const path = (sessionId: string, ws = workspaceId) =>
    `/internal/v1/workspaces/${ws}/sessions/${sessionId}/harness-state`;
  const save = (
    sessionId: string,
    state: unknown,
    previous: string | null = null,
    revision = 7,
    token = 'harness',
  ) =>
    app.inject({
      method: 'POST',
      url: path(sessionId),
      headers: { authorization: `Bearer ${token}` },
      payload: { runtime_revision: revision, expected_checkpoint_revision: previous, state },
    });
  const prepare = (sessionId: string) =>
    app.inject({
      method: 'POST',
      url: `/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/executions:prepare`,
      headers: { authorization: 'Bearer harness' },
      payload: {},
    });

  it('round trips native state through private preparation, with idempotent retries and no public disclosure', async () => {
    const { sessionId } = await seed();
    const initial = await prepare(sessionId);
    expect(initial.statusCode, initial.body).toBe(200);
    expect(initial.json().session).toMatchObject({
      harness_state: null,
      harness_state_revision: null,
    });
    const state = { ...checkpoint('private native history'), instructionsSha256: 'a'.repeat(64) };
    const first = await save(sessionId, state);
    expect(first.statusCode, first.body).toBe(200);
    const revision = first.json().checkpoint_revision;
    expect(revision).toMatch(/^[a-f0-9]{64}$/);
    const retry = await save(sessionId, state);
    expect(retry.statusCode, retry.body).toBe(200);
    expect(retry.json()).toEqual(first.json());
    const restored = await prepare(sessionId);
    expect(restored.json().session).toMatchObject({
      harness_state: state,
      harness_state_revision: revision,
    });
    for (const beta of [undefined, 'managed-agents-2026-04-01']) {
      const publicResponse = await publicApp.inject({
        method: 'GET',
        url: `/v1/sessions/${sessionId}`,
        headers: { 'x-api-key': apiKey, ...(beta ? { 'orca-beta': beta } : {}) },
      });
      expect(publicResponse.statusCode, publicResponse.body).toBe(200);
      expect(publicResponse.body).not.toMatch(
        /harness_state|checkpoint_revision|private native history|cHJpdmF0Z/,
      );
    }
    expect(append).not.toHaveBeenCalled();
    expect(
      (
        await publicApp.inject({
          method: 'POST',
          url: path(sessionId),
          headers: { 'x-api-key': apiKey },
          payload: {},
        })
      ).statusCode,
    ).toBe(404);
  });

  it('keeps large checkpoints out of ordinary Session rows and list responses', async () => {
    const { sessionId } = await seed();
    const state = { ...checkpoint('x'.repeat(1024 * 1024)), instructionsSha256: 'b'.repeat(64) };
    expect((await save(sessionId, state)).statusCode).toBe(200);
    const [row] = await db.select().from(sessions).where(eq(sessions.id, sessionId));
    expect(row).not.toHaveProperty('harnessState');
    expect(JSON.stringify(row).length).toBeLessThan(4096);
    expect(await loadHarnessState(db, workspaceId, sessionId)).toEqual(state);
    expect((await buildSnapshotRecordLoader(db).loadSession(sessionId))?.harnessState).toEqual(
      state,
    );
    const listed = await publicApp.inject({
      method: 'GET',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey },
    });
    expect(listed.statusCode, listed.body).toBe(200);
    expect(listed.body).not.toContain(Object.values(state.files)[0]);
  });

  it('upserts colocated state only for the current workspace and runner binding', async () => {
    const { sessionId } = await seed(sdkHarness, 'colocated', 'self_hosted');
    await db
      .update(sessions)
      .set({ runnerId: 'runner_current', distributionState: 'assigned' })
      .where(eq(sessions.id, sessionId));
    await saveRunnerHarnessState(db, workspaceId, sessionId, 'runner_current', checkpoint('first'));
    await expect(
      saveRunnerHarnessState(db, workspaceId, sessionId, 'runner_old', checkpoint('stale')),
    ).rejects.toThrow('ownership');
    await expect(
      saveRunnerHarnessState(
        db,
        'another-workspace',
        sessionId,
        'runner_current',
        checkpoint('foreign'),
      ),
    ).rejects.toThrow('ownership');
    await saveRunnerHarnessState(db, workspaceId, sessionId, 'runner_current', checkpoint('next'));
    expect(await loadHarnessState(db, workspaceId, sessionId)).toEqual(checkpoint('next'));
    await db.update(sessions).set({ deletedAt: new Date() }).where(eq(sessions.id, sessionId));
    await expect(
      saveRunnerHarnessState(db, workspaceId, sessionId, 'runner_current', checkpoint('deleted')),
    ).rejects.toThrow('ownership');
  });

  it('permits only one concurrent successor and fences stale runtime revisions even on retries', async () => {
    const { sessionId } = await seed();
    const first = await save(sessionId, checkpoint('first'));
    const previous = first.json().checkpoint_revision;
    const results = await Promise.all([
      save(sessionId, checkpoint('second'), previous),
      save(sessionId, checkpoint('third'), previous),
    ]);
    expect(results.map((r) => r.statusCode).sort()).toEqual([200, 409]);
    const prepared = (await prepare(sessionId)).json().session;
    await db.update(sessions).set({ runtimeRevision: 8 }).where(eq(sessions.id, sessionId));
    expect(
      (await save(sessionId, prepared.harness_state, prepared.harness_state_revision)).statusCode,
    ).toBe(409);
    expect(
      (await save(sessionId, prepared.harness_state, prepared.harness_state_revision, 8))
        .statusCode,
    ).toBe(200);
  });

  it('preserves an active checkpoint across cosmetic edits and fences an execution update', async () => {
    const { sessionId } = await seed();
    await db.update(sessions).set({ status: 'running' }).where(eq(sessions.id, sessionId));
    const cosmetic = await publicApp.inject({
      method: 'POST',
      url: `/v1/sessions/${sessionId}`,
      headers: { 'x-api-key': apiKey },
      payload: { title: 'Updated while running', metadata: { label: 'cosmetic' } },
    });
    expect(cosmetic.statusCode).toBe(200);
    const first = await save(sessionId, checkpoint('after cosmetic'));
    expect(first.statusCode).toBe(200);
    const [unchanged] = await db.select().from(sessions).where(eq(sessions.id, sessionId));
    expect(unchanged!.runtimeRevision).toBe(7);
    await db.update(sessions).set({ status: 'idle' }).where(eq(sessions.id, sessionId));
    const changed = await publicApp.inject({
      method: 'POST',
      url: `/v1/sessions/${sessionId}`,
      headers: { 'x-api-key': apiKey },
      payload: { metadata: { orca_llm_egress: 'gateway' } },
    });
    expect(changed.statusCode).toBe(200);
    expect(
      (await save(sessionId, checkpoint('stale'), first.json().checkpoint_revision)).statusCode,
    ).toBe(409);
  });

  it('rejects a stale cosmetic merge after a concurrent execution update', async () => {
    const { sessionId } = await seed();
    const { pool } = await getTestDb();
    const connection = await pool.connect();
    const update = vi.spyOn(db, 'update');
    let pending: Promise<unknown> | undefined;
    try {
      await connection.query('BEGIN');
      await connection.query(
        'UPDATE sessions SET metadata = \'{"orca_llm_egress":"gateway"}\', runtime_revision = 8 WHERE id = $1',
        [sessionId],
      );
      const request = publicApp
        .inject({
          method: 'POST',
          url: `/v1/sessions/${sessionId}`,
          headers: { 'x-api-key': apiKey },
          payload: { metadata: { label: 'stale cosmetic' } },
        })
        .then((response) => response);
      pending = request;
      // The route read revision7 and is ready to update while the execution
      // transaction still owns the row. Its CAS is evaluated after that commits.
      await vi.waitFor(() => expect(update).toHaveBeenCalledWith(sessions));
      await connection.query('COMMIT');
      expect((await request).statusCode).toBe(409);
      const [current] = await db.select().from(sessions).where(eq(sessions.id, sessionId));
      expect(current!.runtimeRevision).toBe(8);
      expect(current!.metadata).toEqual({ orca_llm_egress: 'gateway' });
    } finally {
      await connection.query('ROLLBACK');
      connection.release();
      await pending;
      update.mockRestore();
    }
  });

  it.each(['idle', 'running', 'rescheduling'])(
    'persists the final checkpoint while lifecycle is %s',
    async (status) => {
      const { sessionId } = await seed();
      await db.update(sessions).set({ status }).where(eq(sessions.id, sessionId));
      expect((await save(sessionId, checkpoint(status))).statusCode).toBe(200);
    },
  );

  it.each([
    ['terminated', { status: 'terminated' }, 409],
    ['archived', { archivedAt: new Date() }, 409],
    ['deleted', { deletedAt: new Date() }, 404],
  ] as const)('rejects %s sessions', async (_name, patch, expected) => {
    const { sessionId } = await seed();
    await db.update(sessions).set(patch).where(eq(sessions.id, sessionId));
    expect((await save(sessionId, checkpoint('history'))).statusCode).toBe(expected);
  });

  it('persists cloud colocated checkpoints through the same host ownership fence', async () => {
    const { sessionId } = await seed(sdkHarness, 'colocated', 'cloud');
    expect((await save(sessionId, checkpoint('cloud-colocated'))).statusCode).toBe(200);
  });

  it.each([
    ['codex_sdk', 'colocated', 'self_hosted'],
    ['codex_sdk', 'separate', 'self_hosted'],
    ['claude_agent_sdk', 'separate', 'cloud'],
  ])('rejects an incompatible owner: %s %s %s', async (harness, mode, target) => {
    const { sessionId } = await seed(harness, mode, target);
    expect((await save(sessionId, checkpoint('history'))).statusCode).toBe(409);
  });

  it('uses the pinned binding, fails closed on inconsistencies, and requires authenticated workspace ownership', async () => {
    const { sessionId, agentId, versionId } = await seed();
    await db
      .update(agents)
      .set({ metadata: { harness: sdkHarness, mode: 'colocated' } })
      .where(eq(agents.id, agentId));
    expect((await save(sessionId, checkpoint('history'))).statusCode).toBe(200);
    for (const token of ['ai-gateway', 'invalid']) {
      expect((await save(sessionId, checkpoint('next'), null, 7, token)).statusCode).toBe(
        token === 'invalid' ? 401 : 403,
      );
    }
    const foreign = await app.inject({
      method: 'POST',
      url: path(sessionId, 'ws_foreign'),
      headers: { authorization: 'Bearer harness' },
      payload: {
        runtime_revision: 7,
        expected_checkpoint_revision: null,
        state: checkpoint('next'),
      },
    });
    expect(foreign.statusCode).toBe(404);
    await db
      .update(agentVersions)
      .set({ snapshot: { metadata: { harness: 'claude_agent_sdk', mode: 'separate' } } })
      .where(eq(agentVersions.id, versionId));
    expect((await save(sessionId, checkpoint('history'))).statusCode).toBe(409);
  });

  it('prepares stateless tool policies and rejects stateful or response policies before execution', async () => {
    const { sessionId } = await seed();
    const guardrailId = `grd_checkpoint_${Date.now()}`;
    await db.insert(guardrails).values({
      id: guardrailId,
      organizationId: TEST_ORGANIZATION_ID,
      workspaceId,
      name: 'checkpoint policy',
      scope: 'explicit',
      phases: ['tool_call'],
      rule: { kind: 'builtin', builtin: 'block_tools', params: { tools: ['Bash'] } },
    });
    await db
      .update(sessions)
      .set({ agentOverrides: { guardrailIds: [guardrailId] } })
      .where(eq(sessions.id, sessionId));
    const supported = await prepare(sessionId);
    expect(supported.statusCode, supported.body).toBe(200);
    expect(supported.json().guardrails).toHaveLength(1);
    await db
      .update(guardrails)
      .set({ phases: ['response'] })
      .where(eq(guardrails.id, guardrailId));
    const unsupportedPhase = await prepare(sessionId);
    expect(unsupportedPhase.statusCode, unsupportedPhase.body).toBe(409);
    expect(unsupportedPhase.json()).toMatchObject({
      error: 'invalid_runtime_binding',
      resource_type: 'guardrail',
    });
    await db
      .update(guardrails)
      .set({
        phases: ['tool_call'],
        rule: { kind: 'builtin', builtin: 'max_tool_calls_per_session', params: { limit: 5 } },
      })
      .where(eq(guardrails.id, guardrailId));
    const stateful = await prepare(sessionId);
    expect(stateful.statusCode, stateful.body).toBe(409);
    expect(stateful.json()).toMatchObject({
      error: 'invalid_runtime_binding',
      resource_type: 'guardrail',
    });
  });

  it('rejects self-hosted separate Codex preparation rather than launching an incompatible provider', async () => {
    const { sessionId } = await seed(sdkHarness, 'separate', 'self_hosted');
    const response = await prepare(sessionId);
    expect(response.statusCode, response.body).toBe(409);
    expect(response.json()).toMatchObject({
      error: 'invalid_runtime_binding',
      resource_type: 'harness',
    });
  });

  it('rejects malformed, traversal, oversized, and missing checkpoint payloads without modifying state', async () => {
    const { sessionId } = await seed();
    for (const invalid of [
      null,
      {},
      { ...checkpoint('history'), version: 2 },
      { version: 1, threadId: 'thread-123', files: { '../escape': 'YQ==' } },
      {
        ...checkpoint('history'),
        files: {
          'sessions/2026/09/20/rollout-thread-123.jsonl': 'A'.repeat(24 * 1024 * 1024 + 1),
        },
      },
    ]) {
      const response = await save(sessionId, invalid);
      expect(response.statusCode, response.body).toBe(400);
    }
    expect((await save(sessionId, checkpoint('history'), null, 0)).statusCode).toBe(400);
    expect((await prepare(sessionId)).json().session.harness_state).toBeNull();
  });

  it('fails preparation permanently and refuses writes when stored native state is corrupt', async () => {
    const { sessionId } = await seed();
    await db
      .insert(sessionHarnessStates)
      .values({ workspaceId, sessionId, state: { threadId: 'corrupt' } });
    const response = await prepare(sessionId);
    expect(response.statusCode, response.body).toBe(409);
    expect(response.json()).toMatchObject({
      error: 'invalid_runtime_binding',
      resource_type: 'harness_state',
    });
    expect((await save(sessionId, checkpoint('replacement'))).statusCode).toBe(409);
  });
  const turn = (sessionId: string, request: HarnessTurnRequest, token = 'harness') =>
    app.inject({
      method: 'POST',
      url: path(sessionId).replace('/harness-state', '/harness-turn'),
      headers: { authorization: `Bearer ${token}` },
      payload: request,
    });
  const receipt = (guarded = false): HarnessTurnReceipt => ({
    turnId: `evt_${randomUUID()}`,
    sourceIds: [],
    usageEventId: `evt_${randomUUID()}`,
    guarded,
    phase: 'pending',
    terminalEventId: `evt_${randomUUID()}`,
    errorEventId: `evt_${randomUUID()}`,
    producedAt: new Date().toISOString(),
    error: null,
  });
  async function own(sessionId: string, expectedOwnershipRevision = 0) {
    const identity = {
      ownerToken: randomUUID(),
      runtimeRevision: 7,
      ownershipRevision: expectedOwnershipRevision + 1,
    };
    const claimed = await turn(sessionId, {
      ...identity,
      action: { type: 'claim', expectedOwnershipRevision },
    });
    expect(claimed.statusCode, claimed.body).toBe(200);
    return identity;
  }
  async function begin(
    sessionId: string,
    identity: Awaited<ReturnType<typeof own>>,
    guarded = false,
  ) {
    const pending = receipt(guarded);
    pending.sourceIds = [pending.turnId];
    const result = await turn(sessionId, {
      ...identity,
      action: { type: 'begin', receipt: pending },
    });
    expect(result.statusCode, result.body).toBe(200);
    return pending;
  }

  it('fences generations and legacy writers even when native history is identical', async () => {
    const { sessionId } = await seed();
    const native = checkpoint('same history');
    expect((await save(sessionId, native)).statusCode).toBe(200);
    const owner = await own(sessionId);
    const claimRetry = await turn(sessionId, {
      ...owner,
      action: { type: 'claim', expectedOwnershipRevision: 0 },
    });
    expect(claimRetry.json().ownershipRevision).toBe(1);
    const pending = await begin(sessionId, owner);
    expect((await save(sessionId, native)).statusCode).toBe(409);
    const commit = {
      type: 'commit' as const,
      turnId: pending.turnId,
      state: native,
      responsePersisted: true as const,
      error: null,
    };
    const first = await turn(sessionId, { ...owner, action: commit });
    expect(first.statusCode, first.body).toBe(200);
    expect(first.json().receipt.phase).toBe('ready');
    expect((await turn(sessionId, { ...owner, action: commit })).json()).toEqual(first.json());
    const successor = await own(sessionId, 1);
    expect((await turn(sessionId, { ...owner, action: commit })).statusCode).toBe(409);
    expect(
      (await turn(sessionId, { ...owner, action: { type: 'settle', turnId: pending.turnId } }))
        .statusCode,
    ).toBe(409);
    expect(
      (
        await turn(sessionId, {
          ...successor,
          action: { type: 'settle', turnId: pending.turnId },
        })
      ).statusCode,
    ).toBe(200);
    const prepared = await prepare(sessionId);
    expect(prepared.json().session).toMatchObject({
      harness_state: native,
      harness_ownership_revision: 2,
    });
    expect((await buildSnapshotRecordLoader(db).loadSession(sessionId))?.harnessState).toEqual(
      native,
    );
    expect(await loadHarnessState(db, workspaceId, sessionId)).toEqual(native);
  });

  it('atomically clears only a matching durable usage marker with the ready checkpoint receipt', async () => {
    const { sessionId } = await seed();
    const owner = await own(sessionId);
    const pending = await begin(sessionId, owner, true);
    const key = `codex_sdk_usage_pending:${pending.turnId}`;
    await db.insert(guardrailState).values({
      workspaceId,
      sessionId,
      key,
      valueJson: { version: 1, turnEventId: pending.turnId, usageEventId: pending.usageEventId },
    });
    const action = {
      type: 'commit' as const,
      turnId: pending.turnId,
      state: checkpoint('finished'),
      responsePersisted: true as const,
      error: null,
    };
    expect((await turn(sessionId, { ...owner, action })).statusCode).toBe(409);
    expect(await loadHarnessState(db, workspaceId, sessionId)).toBeNull();
    await db
      .insert(sessionUsageEvents)
      .values({ workspaceId, sessionId, eventId: pending.usageEventId });
    const result = await turn(sessionId, { ...owner, action });
    expect(result.statusCode, result.body).toBe(200);
    expect(
      await db.select().from(guardrailState).where(eq(guardrailState.sessionId, sessionId)),
    ).toHaveLength(0);
    expect((await turn(sessionId, { ...owner, action })).json()).toEqual(result.json());
    expect(result.json().receipt.phase).toBe('ready');
  });

  it('preserves unknown spend and previous native state when recovering pending required actions', async () => {
    const { sessionId, agentId } = await seed();
    const native = checkpoint('previous');
    await save(sessionId, native);
    const owner = await own(sessionId);
    const pending = await begin(sessionId, owner, true);
    const key = `codex_sdk_usage_pending:${pending.turnId}`;
    await db.insert(guardrailState).values({
      workspaceId,
      sessionId,
      key,
      valueJson: { version: 1, turnEventId: pending.turnId, usageEventId: pending.usageEventId },
    });
    const callback = `evt_${randomUUID()}`;
    expect(
      (
        await turn(sessionId, {
          ...owner,
          action: { type: 'accept_source', turnId: pending.turnId, sourceId: callback },
        })
      ).statusCode,
    ).toBe(200);
    await db.update(agents).set({ archivedAt: new Date() }).where(eq(agents.id, agentId));
    const inspected = await turn(sessionId, { action: { type: 'inspect' } });
    expect(inspected.statusCode, inspected.body).toBe(200);
    const successor = await own(sessionId, 1);
    const abandoned = await turn(sessionId, {
      ...successor,
      action: { type: 'abandon', turnId: pending.turnId },
    });
    expect(abandoned.json()).toMatchObject({
      state: native,
      receipt: { phase: 'ready', sourceIds: [pending.turnId, callback] },
    });
    expect(abandoned.json().receipt.error).toContain('without replay');
    const clear = await app.inject({
      method: 'POST',
      url: path(sessionId).replace('/harness-state', '/guardrail-state'),
      headers: { authorization: 'Bearer harness' },
      payload: { updates: [{ scope: 'session', key, action: 'delete' }] },
    });
    expect(clear.statusCode, clear.body).toBe(409);
    expect(
      await db.select().from(guardrailState).where(eq(guardrailState.sessionId, sessionId)),
    ).toHaveLength(1);
  });

  it('validates immutable retry identity, bounded receipts, caller authorization and lifecycle', async () => {
    const { sessionId } = await seed();
    const owner = await own(sessionId);
    const pending = await begin(sessionId, owner);
    expect(
      (
        await turn(sessionId, {
          ...owner,
          action: { type: 'begin', receipt: { ...pending, guarded: true } },
        })
      ).statusCode,
    ).toBe(409);
    expect(
      (
        await turn(sessionId, {
          ...owner,
          action: {
            type: 'begin',
            receipt: { ...pending, sourceIds: Array(257).fill('evt_source') },
          },
        })
      ).statusCode,
    ).toBe(400);
    expect((await turn(sessionId, { action: { type: 'inspect' } }, 'ai-gateway')).statusCode).toBe(
      403,
    );
    await db.update(sessions).set({ runtimeRevision: 8 }).where(eq(sessions.id, sessionId));
    expect(
      (await turn(sessionId, { ...owner, action: { type: 'abandon', turnId: pending.turnId } }))
        .statusCode,
    ).toBe(409);
  });
  it.each(['native', 'receipt'] as const)(
    'reports malformed stored %s as a permanent binding failure during inspection and claim',
    async (kind) => {
      const { sessionId } = await seed();
      const owner = await own(sessionId);
      await begin(sessionId, owner);
      const [row] = await db
        .select()
        .from(sessionHarnessStates)
        .where(eq(sessionHarnessStates.sessionId, sessionId));
      const malformed = {
        ...(row!.state as Record<string, unknown>),
        ...(kind === 'native'
          ? { state: { threadId: 'corrupt' } }
          : { receipt: { phase: 'ready', sourceIds: 'corrupt' } }),
      };
      await db
        .update(sessionHarnessStates)
        .set({ state: malformed })
        .where(eq(sessionHarnessStates.sessionId, sessionId));
      for (const request of [
        { action: { type: 'inspect' as const } },
        { ...owner, action: { type: 'claim' as const, expectedOwnershipRevision: 0 } },
      ]) {
        const response = await turn(sessionId, request);
        expect(response.statusCode, response.body).toBe(409);
        expect(response.json()).toEqual({
          error: 'invalid_runtime_binding',
          resource_type: 'harness_state',
          resource_id: sessionId,
        });
      }
      const [after] = await db
        .select()
        .from(sessionHarnessStates)
        .where(eq(sessionHarnessStates.sessionId, sessionId));
      expect(after!.state).toEqual(malformed);
    },
  );

  it.each(['archived', 'terminated'] as const)(
    'returns no recoverable receipt for a %s Session while refusing all owner mutations',
    async (lifecycle) => {
      const { sessionId } = await seed();
      const owner = await own(sessionId);
      const pending = await begin(sessionId, owner);
      await db
        .update(sessions)
        .set(lifecycle === 'archived' ? { archivedAt: new Date() } : { status: 'terminated' })
        .where(eq(sessions.id, sessionId));
      const inspected = await turn(sessionId, { action: { type: 'inspect' } });
      expect(inspected.statusCode, inspected.body).toBe(200);
      expect(inspected.json()).toBeNull();
      expect(
        (await turn(sessionId, { ...owner, action: { type: 'abandon', turnId: pending.turnId } }))
          .statusCode,
      ).toBe(409);
      expect(
        (
          await turn(sessionId, {
            ...owner,
            action: { type: 'claim', expectedOwnershipRevision: 0 },
          })
        ).statusCode,
      ).toBe(409);
      const [stored] = await db
        .select()
        .from(sessionHarnessStates)
        .where(eq(sessionHarnessStates.sessionId, sessionId));
      expect(stored?.state).toMatchObject({
        receipt: { phase: 'pending' },
        ownerToken: owner.ownerToken,
      });
    },
  );

  it('returns no recoverable receipt for an inactive workspace while preserving mutation isolation', async () => {
    const { sessionId } = await seed();
    const owner = await own(sessionId);
    const pending = await begin(sessionId, owner);
    await db
      .update(workspaces)
      .set({ status: 'archived', archivedAt: new Date() })
      .where(eq(workspaces.id, workspaceId));
    try {
      const inspected = await turn(sessionId, { action: { type: 'inspect' } });
      expect(inspected.statusCode, inspected.body).toBe(200);
      expect(inspected.json()).toBeNull();
      expect(
        (await turn(sessionId, { ...owner, action: { type: 'abandon', turnId: pending.turnId } }))
          .statusCode,
      ).toBe(404);
      expect(
        (
          await turn(sessionId, {
            ...owner,
            action: { type: 'claim', expectedOwnershipRevision: 0 },
          })
        ).statusCode,
      ).toBe(404);
    } finally {
      await db
        .update(workspaces)
        .set({ status: 'active', archivedAt: null })
        .where(eq(workspaces.id, workspaceId));
    }
  });
});
