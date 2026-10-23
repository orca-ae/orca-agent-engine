// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { FastifyInstance } from 'fastify';
import type { AddressInfo } from 'node:net';
import { and, eq, sql } from 'drizzle-orm';
import type { Event } from '@orca/transcript-store';
import { stableSessionThreadId, type ModelUsageCounts } from '@orca/agent-event-contract';
import { buildCombinedTestApp } from '../../src/server.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import { agentVersions, sessions } from '../../src/persistence/postgres/schema.js';
import { indexTranscriptEvents, loadDshChildUsage } from '../../src/events/session-events-index.js';
import {
  createTestAgent,
  createTestApiKey,
  createTestSession,
  uniqueWorkspace,
} from './fixtures.js';
import {
  buildStubFileStore,
  buildStubStore,
  buildTestJwtMinter,
  closeTestDb,
  getTestDb,
  STUB_SSE_CONFIG,
} from './setup.js';

const spawn = 'subagents/11111111-1111-4111-8111-111111111111';
const fork = 'subagents/22222222-2222-4222-8222-222222222222';
const grandchild = 'subagents/33333333-3333-4333-8333-333333333333';
const inactive = 'subagents/44444444-4444-4444-8444-444444444444';
const counts = {
  input_tokens: 8,
  output_tokens: 266,
  cache_creation_input_tokens: 881,
  cache_read_input_tokens: 8249,
};

describe('DSH public child self usage (Postgres)', () => {
  let app: FastifyInstance;
  let db: DbClient;
  let workspaceId: string;
  let sessionId: string;
  let apiKey: string;
  let agentId: string;
  let seq = 0;

  function event(subpath: string, kind: string, payload: Record<string, unknown>): Event {
    return {
      id: `evt_usage_${seq}`,
      workspaceId,
      sessionId,
      subpath,
      seq: seq++,
      producedAt: new Date().toISOString(),
      producedBy: 'harness',
      kind,
      payload: Buffer.from(JSON.stringify(payload)),
      idempotencyKey: '',
    };
  }

  function summary(subpath: string, usage: ModelUsageCounts = counts): Event {
    return event(subpath, 'span.model_request_end', {
      subpath,
      native_session_id: subpath.slice('subagents/'.length),
      session_thread_id: stableSessionThreadId(workspaceId, sessionId, subpath),
      turn_event_id: `evt_turn_${seq}`,
      model_request_start_id: `evt_start_${seq}`,
      model_observation_kind: 'turn_model_summary',
      is_error: false,
      model_usage: usage,
    });
  }

  async function sessionRow() {
    return (
      await db
        .select()
        .from(sessions)
        .where(and(eq(sessions.workspaceId, workspaceId), eq(sessions.id, sessionId)))
    )[0]!;
  }

  async function getThread(subpath: string, beta = false) {
    const response = await app.inject({
      method: 'GET',
      url: `/v1/sessions/${sessionId}/threads/${stableSessionThreadId(workspaceId, sessionId, subpath)}`,
      headers: {
        'x-api-key': apiKey,
        ...(beta ? { 'orca-beta': 'managed-agents-2026-04-01' } : {}),
      },
    });
    expect(response.statusCode, response.body).toBe(200);
    return response.json();
  }

  beforeAll(async () => {
    ({ db } = await getTestDb());
    app = buildCombinedTestApp({
      db,
      oidc: { allowedIssuers: [], audience: 'orca-agent-engine' },
      store: buildStubStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
    });
    await app.listen({ host: '127.0.0.1', port: 0 });
    const baseURL = `http://127.0.0.1:${(app.server.address() as AddressInfo).port}`;
    workspaceId = uniqueWorkspace('dsh_usage');
    apiKey = await createTestApiKey(db, workspaceId);
    agentId = await createTestAgent(baseURL, apiKey);
    sessionId = await createTestSession(baseURL, apiKey, agentId);
    // Keep fixture creation independent of DSH deployment admission.
    await db
      .update(agentVersions)
      .set({
        snapshot: sql`jsonb_set(${agentVersions.snapshot}, '{metadata}', '{"harness":"dsh_sdk","mode":"colocated"}'::jsonb)`,
      })
      .where(and(eq(agentVersions.workspaceId, workspaceId), eq(agentVersions.agentId, agentId)));
    await db
      .update(sessions)
      .set({
        usageInputTokens: 10000,
        usageOutputTokens: 2000,
        usageCacheReadInputTokens: 30000,
        usageCacheCreationEphemeral1hInputTokens: 40,
        usageCacheCreationEphemeral5mInputTokens: 50,
        usageCostNanoUsd: 123456n,
      })
      .where(eq(sessions.id, sessionId));
  }, 60000);

  afterAll(async () => {
    await app?.close();
    await closeTestDb();
  });

  it('accumulates full self history with stable reads, follow-ups and replay without Session mutations', async () => {
    const before = await sessionRow();
    const declarations = [spawn, fork, grandchild, inactive].map((subpath) =>
      event(subpath, 'session.thread_created', {
        subpath,
        parent_subpath: subpath === grandchild ? spawn : '',
      }),
    );
    await indexTranscriptEvents(db, declarations);
    const first = summary(spawn);
    const second = summary(spawn, { ...counts, input_tokens: 7, output_tokens: 11 });
    const forkFact = summary(fork, { ...counts, input_tokens: 3, output_tokens: 5 });
    const split = summary(grandchild, { ...counts, cache_creation_input_tokens_1h: 400 });
    const root = summary('', { ...counts, input_tokens: 900 });
    await indexTranscriptEvents(db, [first, first, second, forkFact, split, root]);
    await Promise.all([
      indexTranscriptEvents(db, [first, second, forkFact]),
      indexTranscriptEvents(db, [first, second, split]),
    ]);
    const expected = {
      input_tokens: 15,
      output_tokens: 277,
      cache_read_input_tokens: 16498,
      cache_creation: { ephemeral_1h_input_tokens: 0, ephemeral_5m_input_tokens: 1762 },
    };
    for (const beta of [false, true]) {
      expect((await getThread(spawn, beta)).usage).toEqual(expected);
      expect((await getThread(spawn, beta)).usage).toEqual(expected);
      const response = await app.inject({
        method: 'GET',
        url: `/v1/sessions/${sessionId}/threads?limit=100`,
        headers: {
          'x-api-key': apiKey,
          ...(beta ? { 'orca-beta': 'managed-agents-2026-04-01' } : {}),
        },
      });
      expect(response.statusCode, response.body).toBe(200);
      expect(
        response
          .json()
          .data.find(
            (thread: { id: string }) =>
              thread.id === stableSessionThreadId(workspaceId, sessionId, spawn),
          ).usage,
      ).toEqual(expected);
    }
    expect((await getThread(fork)).usage.input_tokens).toBe(3);
    expect((await getThread(grandchild)).usage.cache_creation).toEqual({
      ephemeral_1h_input_tokens: 400,
      ephemeral_5m_input_tokens: 481,
    });
    expect((await getThread(inactive)).usage.input_tokens).toBe(0);
    expect((await getThread('')).usage.input_tokens).toBe(10000);
    await indexTranscriptEvents(db, [
      summary(spawn, { ...counts, input_tokens: 2, cache_creation_input_tokens_1h: 100 }),
    ]);
    expect((await getThread(spawn)).usage).toEqual({
      input_tokens: 17,
      output_tokens: 543,
      cache_read_input_tokens: 24747,
      cache_creation: { ephemeral_1h_input_tokens: 100, ephemeral_5m_input_tokens: 2543 },
    });
    expect((await getThread(fork)).usage.input_tokens).toBe(3);
    await indexTranscriptEvents(
      db,
      Array.from({ length: 1001 }, () =>
        summary(fork, {
          input_tokens: 0,
          output_tokens: 1,
          cache_read_input_tokens: 0,
          cache_creation_input_tokens: 0,
        }),
      ),
    );
    expect((await getThread(fork)).usage.output_tokens).toBe(1006);
    expect(await sessionRow()).toEqual(before);
  });

  it('ignores malformed, non-authoritative, cross-scope and other harness facts', async () => {
    const before = await getThread(spawn);
    const original = summary(spawn);
    const payload = JSON.parse(Buffer.from(original.payload).toString());
    const bad = [
      { ...payload, model_observation_kind: 'provider_call' },
      { ...payload, native_session_id: 'other-child' },
      { ...payload, session_thread_id: 'sth_other' },
      { ...payload, subpath: grandchild },
      { ...payload, model_usage: { ...counts, input_tokens: -1 } },
      { ...payload, model_usage: { ...counts, output_tokens: 1.5 } },
      { ...payload, model_usage: { ...counts, cache_creation_input_tokens_1h: 882 } },
      { ...payload, model_usage: { ...counts, cache_creation_input_tokens_1h: null } },
      { ...payload, model_usage: [] },
    ].map((value) => event(spawn, 'span.model_request_end', value));
    await indexTranscriptEvents(db, [
      ...bad,
      { ...summary(spawn), producedBy: 'client' },
      event(spawn, 'session.usage', { usage: counts }),
    ]);
    expect((await getThread(spawn)).usage).toEqual(before.usage);
    expect(await loadDshChildUsage(db, 'ws_other', sessionId, [spawn])).toEqual(new Map());
    expect(await loadDshChildUsage(db, workspaceId, 'ses_other', [spawn])).toEqual(new Map());
    await db
      .update(agentVersions)
      .set({
        snapshot: sql`jsonb_set(${agentVersions.snapshot}, '{metadata,harness}', '"pi_sdk"'::jsonb)`,
      })
      .where(and(eq(agentVersions.workspaceId, workspaceId), eq(agentVersions.agentId, agentId)));
    expect((await getThread(spawn)).usage).toEqual({
      input_tokens: 0,
      output_tokens: 0,
      cache_read_input_tokens: 0,
      cache_creation: { ephemeral_1h_input_tokens: 0, ephemeral_5m_input_tokens: 0 },
    });
  });
});
