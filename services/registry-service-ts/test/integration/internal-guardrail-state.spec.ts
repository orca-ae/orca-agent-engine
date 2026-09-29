// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { FastifyInstance } from 'fastify';
import type { AddressInfo } from 'node:net';
import { and, eq } from 'drizzle-orm';
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
import { guardrailCounters, guardrailState } from '../../src/persistence/postgres/schema.js';

interface StateUpdateBody {
  scope: string;
  key: string;
  action: string;
  value?: unknown;
}

describe('/internal/v1/.../guardrail-state (integration)', () => {
  let app: FastifyInstance;
  let baseURL: string;
  let apiKey: string;
  let workspaceId: string;
  let agentId: string;
  let sessionId: string;
  let db: DbClient;

  async function apply(
    updates: StateUpdateBody[],
    extra: Record<string, unknown> = {},
    session = sessionId,
  ): Promise<Response> {
    return fetch(
      `${baseURL}/internal/v1/workspaces/${workspaceId}/sessions/${session}/guardrail-state`,
      {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ updates, ...extra }),
      },
    );
  }

  async function sessionValue(key: string, session = sessionId) {
    const rows = await db
      .select()
      .from(guardrailState)
      .where(
        and(
          eq(guardrailState.workspaceId, workspaceId),
          eq(guardrailState.sessionId, session),
          eq(guardrailState.key, key),
        ),
      );
    return rows[0] ?? null;
  }

  async function counterValue(subject: string, window: string, key: string) {
    const rows = await db
      .select()
      .from(guardrailCounters)
      .where(
        and(
          eq(guardrailCounters.workspaceId, workspaceId),
          eq(guardrailCounters.subject, subject),
          eq(guardrailCounters.window, window),
          eq(guardrailCounters.key, key),
        ),
      );
    return rows[0] ?? null;
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
    workspaceId = uniqueWorkspace('guardrail_state');
    apiKey = await createTestApiKey(db, workspaceId);
    agentId = await createTestAgent(baseURL, apiKey);
    sessionId = await createTestSession(baseURL, apiKey, agentId);
  }, 60000);

  afterAll(async () => {
    await app?.close();
    await closeTestDb();
  });

  it('sets a numeric value in the counter column and anything else as json', async () => {
    const res = await apply([
      { scope: 'session', key: 'spend_usd', action: 'set', value: 1.5 },
      { scope: 'session', key: 'asked_thresholds', action: 'set', value: ['80%'] },
    ]);
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ applied: 2 });

    expect(await sessionValue('spend_usd')).toMatchObject({ valueNum: 1.5, valueJson: null });
    expect(await sessionValue('asked_thresholds')).toMatchObject({
      valueNum: null,
      valueJson: ['80%'],
    });
  });

  it('increments without a read-modify-write and defaults the delta to one', async () => {
    await apply([{ scope: 'session', key: 'tool_calls', action: 'increment', value: 4 }]);
    await apply([{ scope: 'session', key: 'tool_calls', action: 'increment' }]);
    expect(await sessionValue('tool_calls')).toMatchObject({ valueNum: 5 });
  });

  it('lands every concurrent increment', async () => {
    const key = 'concurrent_calls';
    const results = await Promise.all(
      Array.from({ length: 8 }, () =>
        apply([{ scope: 'session', key, action: 'increment', value: 1 }]),
      ),
    );
    expect(results.every((r) => r.status === 200)).toBe(true);
    expect(await sessionValue(key)).toMatchObject({ valueNum: 8 });
  });

  it('overwrites a non-numeric value when it is incremented', async () => {
    await apply([{ scope: 'session', key: 'was_text', action: 'set', value: 'text' }]);
    await apply([{ scope: 'session', key: 'was_text', action: 'increment', value: 2 }]);
    expect(await sessionValue('was_text')).toMatchObject({ valueNum: 2, valueJson: null });
  });

  it('deletes a key and treats a valueless set as a delete', async () => {
    await apply([{ scope: 'session', key: 'doomed', action: 'set', value: 3 }]);
    await apply([{ scope: 'session', key: 'doomed', action: 'delete' }]);
    expect(await sessionValue('doomed')).toBeNull();

    await apply([{ scope: 'session', key: 'cleared', action: 'set', value: 3 }]);
    await apply([{ scope: 'session', key: 'cleared', action: 'set' }]);
    expect(await sessionValue('cleared')).toBeNull();
  });

  it('appends onto an array and replaces a value that is not one', async () => {
    await apply([{ scope: 'session', key: 'approvals', action: 'append', value: 'a' }]);
    await apply([{ scope: 'session', key: 'approvals', action: 'append', value: 'b' }]);
    expect(await sessionValue('approvals')).toMatchObject({ valueJson: ['a', 'b'] });

    await apply([{ scope: 'session', key: 'scalar', action: 'set', value: 7 }]);
    await apply([{ scope: 'session', key: 'scalar', action: 'append', value: 'x' }]);
    expect(await sessionValue('scalar')).toMatchObject({ valueNum: null, valueJson: ['x'] });
  });

  it('rejects a turn-scoped update instead of silently dropping it', async () => {
    const res = await apply([
      { scope: 'session', key: 'batched_with_turn', action: 'increment', value: 1 },
      { scope: 'turn', key: 'this_turn', action: 'increment', value: 1 },
    ]);
    expect(res.status).toBe(400);
    // All-or-nothing: the session-scoped sibling in the rejected batch is not
    // applied either, so a caller never has to guess what landed.
    expect(await sessionValue('batched_with_turn')).toBeNull();
  });

  it('accumulates a cross-session counter and starts a fresh one on window rollover', async () => {
    const subject = `usr_${workspaceId}`;
    await apply([{ scope: 'subject_window', key: 'spend_usd', action: 'increment', value: 0.25 }], {
      subject,
      window: '2026-08-01',
    });
    await apply([{ scope: 'subject_window', key: 'spend_usd', action: 'increment', value: 0.5 }], {
      subject,
      window: '2026-08-01',
    });
    await apply([{ scope: 'subject_window', key: 'spend_usd', action: 'increment', value: 0.1 }], {
      subject,
      window: '2026-08-02',
    });

    expect(await counterValue(subject, '2026-08-01', 'spend_usd')).toMatchObject({
      valueNum: 0.75,
    });
    expect(await counterValue(subject, '2026-08-02', 'spend_usd')).toMatchObject({ valueNum: 0.1 });
  });

  it('sets and deletes a cross-session counter', async () => {
    const subject = `usr_set_${workspaceId}`;
    await apply([{ scope: 'subject_window', key: 'k', action: 'set', value: 9 }], {
      subject,
      window: '2026-08-01',
    });
    expect(await counterValue(subject, '2026-08-01', 'k')).toMatchObject({ valueNum: 9 });
    await apply([{ scope: 'subject_window', key: 'k', action: 'delete' }], {
      subject,
      window: '2026-08-01',
    });
    expect(await counterValue(subject, '2026-08-01', 'k')).toBeNull();
  });

  it('refuses a counter update without the subject and window it is keyed by', async () => {
    const res = await apply([
      { scope: 'subject_window', key: 'spend_usd', action: 'increment', value: 1 },
    ]);
    expect(res.status).toBe(400);
  });

  it('restores persisted session state into the prepared runtime', async () => {
    const restoreSession = await createTestSession(baseURL, apiKey, agentId);
    await apply(
      [
        { scope: 'session', key: 'tool_calls', action: 'increment', value: 3 },
        { scope: 'session', key: 'asked', action: 'append', value: '80%' },
      ],
      {},
      restoreSession,
    );
    const res = await fetch(
      `${baseURL}/internal/v1/workspaces/${workspaceId}/sessions/${restoreSession}/executions:prepare`,
      { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{}' },
    );
    expect(res.status).toBe(200);
    const prepared = (await res.json()) as { guardrail_state: Record<string, unknown> };
    expect(prepared.guardrail_state).toEqual({ tool_calls: 3, asked: ['80%'] });
  });

  it('404s an unknown session rather than orphaning state', async () => {
    const res = await apply(
      [{ scope: 'session', key: 'k', action: 'increment', value: 1 }],
      {},
      'ses_does_not_exist',
    );
    expect(res.status).toBe(404);
  });
});
