// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { drizzle } from 'drizzle-orm/node-postgres';
import { and, eq } from 'drizzle-orm';
import type { Pool } from 'pg';
import type { FastifyInstance } from 'fastify';
import { SpanEventKind } from '@orca/agent-event-contract';
import * as schema from '../../src/persistence/postgres/schema.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import { buildCombinedTestApp } from '../../src/server.js';
import { loadSession, loadSessionRows } from '../../src/api/sessions.routes.js';
import { createSessionReadContext } from '../../src/domain/session-read-context.js';
import { hashApiKey } from '../../src/auth/api-key.js';
import { createTestApiKey, uniqueWorkspace } from './fixtures.js';
import {
  getTestDb,
  closeTestDb,
  buildStubStore,
  buildStubFileStore,
  buildTestJwtMinter,
  STUB_SSE_CONFIG,
} from './setup.js';

describe('API batch read parity and query budgets (real Postgres)', () => {
  let db: DbClient;
  let pool: Pool;
  let legacy: FastifyInstance;
  let batched: FastifyInstance;
  let workspaceId: string;
  let foreignWorkspace: string;
  let apiKey: string;
  let foreignKey: string;
  let primaryId: string;
  let sessionId: string;
  let triggerId: string;
  let statements: string[] = [];
  const createdAt = new Date('2026-01-01T00:00:00Z');
  const now = new Date('2026-09-12T00:00:00Z');

  beforeAll(async () => {
    ({ pool } = await getTestDb());
    db = drizzle(pool, {
      schema,
      logger: {
        logQuery(query) {
          statements.push(query);
        },
      },
    });
    workspaceId = uniqueWorkspace('batch');
    foreignWorkspace = uniqueWorkspace('batch_other');
    apiKey = await createTestApiKey(db, workspaceId);
    foreignKey = await createTestApiKey(db, foreignWorkspace);
    primaryId = `agt_${workspaceId}_primary`;
    const childId = `agt_${workspaceId}_child`;
    const environmentId = `env_${workspaceId}`;
    triggerId = `trg_${workspaceId}`;
    const skillId = `skill_${workspaceId}`;
    await db.insert(schema.environments).values({ id: environmentId, workspaceId, name: 'batch' });
    await db.insert(schema.agents).values(
      [
        primaryId,
        childId,
        ...Array.from({ length: 98 }, (_, i) => `agt_${workspaceId}_extra_${i}`),
      ].map((id) => ({
        id,
        workspaceId,
        name: id,
        modelProvider: 'anthropic',
        modelId: 'claude-test',
        createdAt,
        updatedAt: createdAt,
      })),
    );
    await db.insert(schema.agentVersions).values([
      {
        id: `${primaryId}_1`,
        workspaceId,
        agentId: primaryId,
        version: 1,
        snapshot: {
          name: 'pinned primary',
          model: { provider: 'anthropic', id: 'claude-pinned' },
          system: 'original',
          skills: [{ type: 'custom', skill_id: skillId, version: 'latest' }],
          multiagent: {
            type: 'coordinator',
            agents: [{ id: childId, version: 1 }, { id: 'agt_missing', version: 1 }, { bad: true }],
          },
        },
      },
      {
        id: `${primaryId}_2`,
        workspaceId,
        agentId: primaryId,
        version: 2,
        snapshot: 'legacy-invalid-snapshot',
      },
      {
        id: `${childId}_1`,
        workspaceId,
        agentId: childId,
        version: 1,
        snapshot: {
          name: 'pinned child',
          skills: [],
          model: { provider: 'anthropic', id: 'claude-child' },
        },
      },
    ]);
    await db
      .insert(schema.skills)
      .values({ id: skillId, workspaceId, name: 'skill', slug: 'skill' });
    await db.insert(schema.skillVersions).values(
      [1, 2].map((version) => ({
        id: `${skillId}_${version}`,
        workspaceId,
        skillId,
        version,
        versionIdentifier: String(version),
        name: 'skill',
        directory: 'skill',
        packageSha256: String(version).repeat(64),
        packageSizeBytes: 1,
      })),
    );
    const sessionRows = Array.from({ length: 100 }, (_, i) => ({
      id: `ses_${workspaceId}_${String(i).padStart(3, '0')}`,
      workspaceId,
      agentId: primaryId,
      agentVersion: i % 10 === 9 ? 2 : 1,
      environmentId,
      title: `session-${i}`,
      metadata: { group: i % 2 ? 'odd' : 'even' },
      agentOverrides: { system: i % 2 ? null : `override-${i}` },
      tools: [],
      mcpServers: [],
      status: i % 2 ? 'running' : 'idle',
      createdAt,
      updatedAt: createdAt,
      startedAt: createdAt,
      activeSeconds: i,
      usageInputTokens: i * 10,
    }));
    await db.insert(schema.sessions).values(sessionRows);
    sessionId = sessionRows[0]!.id;
    await db.insert(schema.sessionResources).values(
      sessionRows.flatMap((row) =>
        [false, true].map((detached) => ({
          id: `rsc_${row.id}_${detached}`,
          workspaceId,
          sessionId: row.id,
          type: 'memory_store',
          memoryStoreId: `memstore_${row.id}`,
          mountPath: '/memory',
          access: 'read_only',
          attachedAt: createdAt,
          updatedAt: createdAt,
          detachedAt: detached ? createdAt : null,
        })),
      ),
    );
    await db.insert(schema.sessionSkillBindings).values(
      sessionRows
        .filter((row) => row.agentVersion === 1)
        .map((row, i) => {
          const version = (i % 2) + 1;
          return {
            workspaceId,
            sessionId: row.id,
            agentId: primaryId,
            agentVersion: 1,
            ordinal: 0,
            skillVersionId: `${skillId}_${version}`,
            bundleSha256: String(version).repeat(64),
          };
        }),
    );
    await db.insert(schema.sessionEventsIndex).values(
      sessionRows.flatMap((row) =>
        [1, 2, 3].map((seq) => ({
          workspaceId,
          sessionId: row.id,
          seq,
          eventId: `evt_${row.id}_${seq}`,
          producedAt: createdAt.toISOString(),
          producedBy: 'test',
          kind: seq === 1 ? 'user.define_outcome' : SpanEventKind.outcomeEvaluationEnd,
          visibility: 'public',
          payload: {
            outcome_id: `outc_${row.id}`,
            description: row.title,
            result: seq === 3 ? 'satisfied' : 'needs_revision',
            iteration: seq - 1,
          },
        })),
      ),
    );
    await db.insert(schema.sessionThreads).values(
      Array.from({ length: 100 }, (_, i) => ({
        id: `sth_${workspaceId}_${i}`,
        workspaceId,
        sessionId,
        subpath: i === 0 ? '' : `child-${i}`,
        agentId: i === 0 ? primaryId : childId,
        agentVersion: 1,
        agentName: 'thread',
        createdAt,
        updatedAt: createdAt,
      })),
    );
    await db.insert(schema.agentTriggers).values({
      id: triggerId,
      workspaceId,
      guardrailSubject: 'test-principal',
      name: 'trigger',
      agentId: primaryId,
      agentVersion: 1,
      environmentId,
      payload: 'go',
      cronExpression: '* * * * *',
    });
    await db.insert(schema.agentTriggerFires).values(
      sessionRows.map((row, i) => ({
        id: `trgfire_${row.id}`,
        workspaceId,
        triggerId,
        generation: 1,
        scheduledFor: new Date(createdAt.getTime() + i * 1000),
        status: 'enqueued',
        plannedSessionId: row.id,
        sessionId: row.id,
        eventId: `evt_fire_${row.id}`,
      })),
    );
    const opts = {
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store: buildStubStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
    };
    legacy = buildCombinedTestApp({ ...opts, batchReadsEnabled: false });
    batched = buildCombinedTestApp({ ...opts, batchReadsEnabled: true });
    await Promise.all([legacy.ready(), batched.ready()]);
  });

  afterAll(async () => {
    vi.useRealTimers();
    await Promise.all([legacy?.close(), batched?.close()]);
    // This suite uses unique tenant IDs; the disposable test database owns their cleanup.
    await closeTestDb();
  });

  async function compare(url: string, beta = false, key = apiKey) {
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(now);
    try {
      const headers = {
        'x-api-key': key,
        ...(beta ? { 'orca-beta': 'managed-agents-2026-04-01' } : {}),
      };
      statements = [];
      const old = await legacy.inject({ method: 'GET', url, headers });
      const oldQueries = statements.length;
      statements = [];
      const current = await batched.inject({ method: 'GET', url, headers });
      const queries = statements.length;
      expect(current.statusCode, current.body).toBe(old.statusCode);
      // Error request IDs intentionally differ, all successful response fields must match.
      if (current.statusCode === 200) expect(current.json()).toEqual(old.json());
      return { response: current, queries, oldQueries };
    } finally {
      vi.useRealTimers();
    }
  }

  for (const beta of [false, true]) {
    for (const limit of [1, 20, 100]) {
      it(`preserves Session fields and bounds queries at limit=${limit}, beta=${beta}`, async () => {
        const { response, queries, oldQueries } = await compare(
          `/v1/sessions?limit=${limit}&order=asc`,
          beta,
        );
        expect(response.statusCode, response.body).toBe(200);
        expect(response.json().data).toHaveLength(limit);
        expect(queries).toBeLessThanOrEqual(14); // includes authoritative auth and missing-child fallback
        if (limit > 1) expect(queries).toBeLessThan(oldQueries / 2);
        console.info(
          `session batch beta=${beta} limit=${limit}: ${oldQueries} -> ${queries} queries`,
        );
      });
    }
    it(`preserves forward/reverse cursors and filters, beta=${beta}`, async () => {
      const first = await compare('/v1/sessions?limit=20&order=asc&metadata_group=even', beta);
      expect(
        first.response
          .json()
          .data.every((row: { metadata: { group: string } }) => row.metadata.group === 'even'),
      ).toBe(true);
      const next = await compare(
        `/v1/sessions?limit=20&metadata_group=even&page=${first.response.json().next_page}`,
        beta,
      );
      const prev = await compare(
        `/v1/sessions?limit=20&metadata_group=even&page=${next.response.json().prev_page}`,
        beta,
      );
      expect(prev.response.json()).toEqual(first.response.json());
      await compare(`/v1/sessions?agent_id=${primaryId}&agent_version=2&statuses[]=idle`, beta);
      await compare('/v1/sessions?deployment_id=unsupported', beta);
      await compare(`/v1/sessions/${sessionId}`, beta);
    });
    it(`batches Agent and Thread lists, beta=${beta}`, async () => {
      for (const limit of [1, 20, 100]) {
        const agents = await compare(`/v1/agents?limit=${limit}`, beta);
        expect(agents.response.statusCode).toBe(200);
        expect(agents.response.json().data).toHaveLength(limit);
        expect(agents.queries).toBe(4); // auth lookup, last_used, workspace, page
        const threads = await compare(`/v1/sessions/${sessionId}/threads?limit=${limit}`, beta);
        expect(threads.response.statusCode, threads.response.body).toBe(200);
        expect(threads.response.json().data).toHaveLength(limit);
        expect(threads.queries).toBeLessThanOrEqual(8);
        console.info(
          `agent/thread batch beta=${beta} limit=${limit}: ${agents.oldQueries} -> ${agents.queries} / ${threads.oldQueries} -> ${threads.queries} queries`,
        );
      }
    });
  }

  it('shares the Session batch reader with Trigger history and preserves its cursor', async () => {
    const first = await compare(`/v1/triggers/${triggerId}/sessions?limit=20`);
    expect(first.response.statusCode).toBe(200);
    expect(first.queries).toBeLessThanOrEqual(15);
    await compare(
      `/v1/triggers/${triggerId}/sessions?limit=20&page=${first.response.json().next_page}`,
    );
  });

  it('keeps every association workspace scoped and rejects mixed-tenant input', async () => {
    const context = createSessionReadContext(db, foreignWorkspace, false);
    expect(await context.agentVersion({ agentId: primaryId, version: 1 })).toBeNull();
    expect(await context.agent(primaryId)).toBeNull();
    expect(await context.resources(sessionId)).toEqual([]);
    expect(await context.bindings({ sessionId, agentId: primaryId, version: 1 })).toEqual([]);
    expect(await context.outcomes(sessionId)).toEqual([]);
    const rows = await db.select().from(schema.sessions).where(eq(schema.sessions.id, sessionId));
    await expect(loadSessionRows(db, foreignWorkspace, rows)).rejects.toThrow('workspace mismatch');
    for (const url of [
      `/v1/sessions/${sessionId}`,
      `/v1/sessions/${sessionId}/threads`,
      `/v1/triggers/${triggerId}/sessions`,
    ]) {
      expect((await compare(url, false, foreignKey)).response.statusCode).toBe(404);
    }
  });

  it('retains strict skill-binding validation on both paths', async () => {
    const condition = and(
      eq(schema.sessionSkillBindings.workspaceId, workspaceId),
      eq(schema.sessionSkillBindings.sessionId, sessionId),
    );
    await db.update(schema.sessionSkillBindings).set({ ordinal: 2 }).where(condition);
    try {
      const rows = await db.select().from(schema.sessions).where(eq(schema.sessions.id, sessionId));
      await expect(loadSession(db, workspaceId, sessionId)).rejects.toThrow(
        'invalid session skill bindings',
      );
      await expect(loadSessionRows(db, workspaceId, rows)).rejects.toThrow(
        'invalid session skill bindings',
      );
    } finally {
      await db.update(schema.sessionSkillBindings).set({ ordinal: 0 }).where(condition);
    }
  });

  it('filters deleted batch associations while retaining pinned Agent and Skill history', async () => {
    const agentCondition = eq(schema.agents.id, primaryId);
    const resourceCondition = eq(schema.sessionResources.sessionId, sessionId);
    const skillCondition = eq(schema.skills.id, `skill_${workspaceId}`);
    const versionCondition = eq(schema.skillVersions.skillId, `skill_${workspaceId}`);
    try {
      await db.update(schema.agents).set({ deletedAt: now }).where(agentCondition);
      await db.update(schema.sessionResources).set({ deletedAt: now }).where(resourceCondition);
      await db.update(schema.skills).set({ deletedAt: now }).where(skillCondition);
      await db.update(schema.skillVersions).set({ deletedAt: now }).where(versionCondition);
      const context = createSessionReadContext(db, workspaceId, false);
      expect(await context.agent(primaryId)).toBeNull();
      expect(await context.resources(sessionId)).toEqual([]);
      expect(await context.agentVersion({ agentId: primaryId, version: 1 })).not.toBeNull();
      expect(await context.bindings({ sessionId, agentId: primaryId, version: 1 })).toHaveLength(1);
      for (const beta of [false, true]) {
        const detail = await compare(`/v1/sessions/${sessionId}`, beta);
        expect(detail.response.statusCode, detail.response.body).toBe(200);
        expect(detail.response.json().resources).toEqual([]);
        expect(detail.response.json().agent).not.toBeNull();
        const fallback = await compare(`/v1/sessions/ses_${workspaceId}_009`, beta);
        expect(fallback.response.statusCode, fallback.response.body).toBe(200);
        expect(fallback.response.json().agent).toBeNull();
        const list = await compare('/v1/sessions?limit=20&order=asc', beta);
        expect(list.response.statusCode, list.response.body).toBe(200);
      }
    } finally {
      await db.update(schema.agents).set({ deletedAt: null }).where(agentCondition);
      await db.update(schema.sessionResources).set({ deletedAt: null }).where(resourceCondition);
      await db.update(schema.skills).set({ deletedAt: null }).where(skillCondition);
      await db.update(schema.skillVersions).set({ deletedAt: null }).where(versionCondition);
    }
  });

  for (const history of [false, true]) {
    for (const beta of history ? [false] : [false, true]) {
      for (const batch of [false, true]) {
        for (const withSkills of [false, true]) {
          it(`drops a deleted ${withSkills ? 'skill-bound' : 'unbound'} Session after page selection, history=${history}, beta=${beta}, batch=${batch}`, async () => {
            const key = `delete_${history}_${beta}_${batch}_${withSkills}`;
            const ids = [0, 1, 2].map((i) => `ses_${workspaceId}_${key}_${i}`);
            const skillId = `skill_${workspaceId}`;
            await db.insert(schema.sessions).values(
              ids.map((id) => ({
                id,
                workspaceId,
                agentId: primaryId,
                agentVersion: 1,
                metadata: { delete_case: key },
                agentOverrides: withSkills ? {} : { skills: [] },
                createdAt,
                updatedAt: createdAt,
              })),
            );
            if (withSkills) {
              await db.insert(schema.sessionSkillBindings).values(
                ids.map((id) => ({
                  workspaceId,
                  sessionId: id,
                  agentId: primaryId,
                  agentVersion: 1,
                  ordinal: 0,
                  skillVersionId: `${skillId}_1`,
                  bundleSha256: '1'.repeat(64),
                })),
              );
            }
            const caseTriggerId = `trg_${workspaceId}_${key}`;
            if (history) {
              await db.insert(schema.agentTriggers).values({
                id: caseTriggerId,
                workspaceId,
                guardrailSubject: 'test-principal',
                name: key,
                agentId: primaryId,
                agentVersion: 1,
                environmentId: `env_${workspaceId}`,
                payload: 'go',
                cronExpression: '* * * * *',
              });
              await db.insert(schema.agentTriggerFires).values(
                ids.map((id, i) => ({
                  id: `trgfire_${id}`,
                  workspaceId,
                  triggerId: caseTriggerId,
                  generation: 1,
                  scheduledFor: new Date(createdAt.getTime() + i * 1000),
                  status: 'enqueued',
                  plannedSessionId: id,
                  sessionId: id,
                  eventId: `evt_fire_${id}`,
                })),
              );
            }
            const app = batch ? batched : legacy;
            const url = history
              ? `/v1/triggers/${caseTriggerId}/sessions?limit=2`
              : `/v1/sessions?metadata_delete_case=${key}&order=asc&limit=2`;
            const headers = {
              'x-api-key': apiKey,
              ...(beta ? { 'orca-beta': 'managed-agents-2026-04-01' } : {}),
            };
            vi.useFakeTimers({ toFake: ['Date'] });
            vi.setSystemTime(now);
            let querySpy: ReturnType<typeof vi.spyOn> | undefined;
            try {
              const before = await app.inject({ method: 'GET', url, headers });
              expect(before.statusCode, before.body).toBe(200);
              expect(before.json().data).toHaveLength(2);
              expect(before.json().data.map((row: { id: string }) => row.id)).toEqual(
                history ? [ids[2], ids[1]] : [ids[0], ids[1]],
              );
              expect(before.json().next_page).not.toBeNull();
              // Delete the last selected item, including the original cursor boundary.
              const victimId = ids[1]!;
              const query = pool.query;
              let armed = true;
              querySpy = vi.spyOn(pool, 'query').mockImplementation(function (
                this: Pool,
                ...args: unknown[]
              ) {
                const result = Reflect.apply(query, this, args);
                const text =
                  typeof args[0] === 'string'
                    ? args[0]
                    : ((args[0] as { text?: string })?.text ?? '');
                const table = history ? 'agent_trigger_fires' : 'sessions';
                if (
                  !armed ||
                  !text.includes(`from "${table}"`) ||
                  !text.includes('order by') ||
                  !text.includes('limit')
                )
                  return result;
                armed = false;
                return result.then(async (rows: unknown) => {
                  // A separate committed SQL statement after the actual page SELECT.
                  // Soft deletion retains bindings; neither reader may hydrate the stale row.
                  await db
                    .update(schema.sessions)
                    .set({ deletedAt: now })
                    .where(
                      and(
                        eq(schema.sessions.workspaceId, workspaceId),
                        eq(schema.sessions.id, victimId),
                      ),
                    );
                  return rows;
                });
              } as Pool['query']);
              const after = await app.inject({ method: 'GET', url, headers });
              expect(armed).toBe(false);
              expect(after.statusCode, after.body).toBe(200);
              expect(after.json()).toEqual({
                ...before.json(),
                data: before.json().data.filter((row: { id: string }) => row.id !== victimId),
              });
              expect(after.json().data).toHaveLength(1);
              expect(
                await db
                  .select()
                  .from(schema.sessionSkillBindings)
                  .where(eq(schema.sessionSkillBindings.sessionId, victimId)),
              ).toHaveLength(withSkills ? 1 : 0);
            } finally {
              querySpy?.mockRestore();
              vi.useRealTimers();
            }
          });
        }
      }
    }
  }

  it('rechecks scopes, rotation, expiry and workspace/key revocation after warming a proof', async () => {
    const condition = eq(schema.apiKeys.workspaceId, workspaceId);
    const [original] = await db.select().from(schema.apiKeys).where(condition);
    const get = (remoteAddress = '127.0.0.1') =>
      batched.inject({
        method: 'GET',
        url: '/v1/sessions?limit=1',
        headers: { 'x-api-key': apiKey },
        remoteAddress,
      });
    expect((await get()).statusCode).toBe(200);
    try {
      await db.update(schema.apiKeys).set({ scopes: [] }).where(condition);
      expect(
        (
          await batched.inject({
            method: 'GET',
            url: `/v1/triggers/${triggerId}/sessions?limit=1`,
            headers: { 'x-api-key': apiKey },
          })
        ).statusCode,
      ).toBe(403);
      await db
        .update(schema.apiKeys)
        .set({ scopes: original!.scopes, hashedKey: await hashApiKey('orca_rotated_test_key') })
        .where(condition);
      expect((await get()).statusCode).toBe(401);
      await db
        .update(schema.apiKeys)
        .set({ hashedKey: original!.hashedKey, expiresAt: new Date(0) })
        .where(condition);
      expect((await get('127.0.0.2')).statusCode).toBe(401);
      await db
        .update(schema.apiKeys)
        .set({ expiresAt: null, revokedAt: new Date() })
        .where(condition);
      expect((await get('127.0.0.3')).statusCode).toBe(401);
      await db.update(schema.apiKeys).set({ revokedAt: null }).where(condition);
      await db
        .update(schema.workspaces)
        .set({ status: 'archived', archivedAt: new Date() })
        .where(eq(schema.workspaces.id, workspaceId));
      expect((await get('127.0.0.4')).statusCode).toBe(401);
    } finally {
      await db
        .update(schema.apiKeys)
        .set({
          scopes: original!.scopes,
          hashedKey: original!.hashedKey,
          expiresAt: original!.expiresAt,
          revokedAt: original!.revokedAt,
        })
        .where(condition);
      await db
        .update(schema.workspaces)
        .set({ status: 'active', archivedAt: null })
        .where(eq(schema.workspaces.id, workspaceId));
    }
    expect((await get()).statusCode).toBe(200);
  });
});
