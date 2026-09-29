// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { eq } from 'drizzle-orm';
import { HARNESS_CATALOG, resolveExecutionOwner, type HarnessType } from '@orca/harness-catalog';
import { buildCombinedTestApp } from '../../src/server.js';
import { buildDistributionSessionStore } from '../../src/api/sessions.routes.js';
import {
  agents,
  agentVersions,
  environments,
  sessions,
} from '../../src/persistence/postgres/schema.js';
import { newId } from '../../src/domain/versioning.js';
import { createTestWorkspace, uniqueWorkspace } from './fixtures.js';
import {
  getTestDb,
  closeTestDb,
  buildStubStore,
  STUB_SSE_CONFIG,
  buildTestJwtMinter,
  buildStubFileStore,
} from './setup.js';

describe('shared execution ownership (integration)', () => {
  let db: Awaited<ReturnType<typeof getTestDb>>['db'];
  let app: ReturnType<typeof buildCombinedTestApp>;
  const workspaceId = uniqueWorkspace('ownership');

  beforeAll(async () => {
    ({ db } = await getTestDb());
    await createTestWorkspace(db, workspaceId);
    app = buildCombinedTestApp({
      db,
      oidc: { allowedIssuers: [], audience: 'test' },
      store: buildStubStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
    });
    await app.ready();
  });
  afterAll(async () => {
    await app?.close();
    await closeTestDb();
  });

  async function seed(
    harness: HarnessType,
    target: string | null,
    mode = HARNESS_CATALOG[harness].supportedModes[0],
  ) {
    const agentId = newId('agt');
    const versionId = newId('agtv');
    const environmentId = newId('env');
    const sessionId = newId('ses');
    const metadata = { harness, mode };
    await db.insert(agents).values({
      id: agentId,
      workspaceId,
      name: 'routing',
      modelProvider: 'anthropic',
      modelId: 'claude-sonnet-4',
      metadata,
    });
    await db.insert(agentVersions).values({
      id: versionId,
      workspaceId,
      agentId,
      version: 1,
      snapshot: { id: agentId, version: 1, metadata, harness_type: harness },
    });
    await db
      .insert(environments)
      .values({ id: environmentId, workspaceId, name: environmentId, target });
    await db
      .insert(sessions)
      .values({ id: sessionId, workspaceId, agentId, agentVersion: 1, environmentId });
    return { agentId, versionId, environmentId, sessionId, metadata };
  }
  const ownerUrl = (id: string, ws = workspaceId) =>
    `/internal/v1/workspaces/${ws}/sessions/${id}/execution-owner`;

  for (const harness of Object.keys(HARNESS_CATALOG) as HarnessType[]) {
    for (const target of ['cloud', 'self_hosted']) {
      it(`agrees with distribution for ${harness}/${target}, including archived dependencies`, async () => {
        const row = await seed(harness, target);
        const expected = resolveExecutionOwner(target, row.metadata);
        const assertRouting = async () => {
          const response = await app.inject({ method: 'GET', url: ownerUrl(row.sessionId) });
          expect(response.statusCode, response.body).toBe(200);
          expect(response.json()).toEqual({ owner: expected });
          const distribution = await buildDistributionSessionStore(db).load(row.sessionId);
          expect(distribution?.agentHarness).toBe(HARNESS_CATALOG[harness].provider);
          expect(
            resolveExecutionOwner(target, {
              harness: distribution!.agentHarnessType,
              mode: distribution!.agentMode,
            }),
          ).toBe(expected);
        };
        await assertRouting();
        await db.update(agents).set({ archivedAt: new Date() }).where(eq(agents.id, row.agentId));
        await db
          .update(environments)
          .set({ archivedAt: new Date() })
          .where(eq(environments.id, row.environmentId));
        await assertRouting();
        const foreign = await app.inject({
          method: 'GET',
          url: ownerUrl(row.sessionId, 'ws_foreign'),
        });
        expect(foreign.statusCode).toBe(404);
      });
    }
  }

  it('reads pinned metadata rather than changed latest Agent metadata', async () => {
    const row = await seed('codex_sdk', 'cloud', 'colocated');
    await db
      .update(agents)
      .set({ version: 2, metadata: { harness: 'codex_sdk', mode: 'invalid' } })
      .where(eq(agents.id, row.agentId));
    const response = await app.inject({ method: 'GET', url: ownerUrl(row.sessionId) });
    expect(response.statusCode, response.body).toBe(200);
    expect(response.json()).toEqual({ owner: 'harness-server' });
    expect((await buildDistributionSessionStore(db).load(row.sessionId))?.agentMode).toBe(
      'colocated',
    );
  });

  it('routes cloud Codex separate through harness-server and preserves its pinned mode', async () => {
    const row = await seed('codex_sdk', 'cloud', 'separate');
    await db
      .update(agents)
      .set({ metadata: { harness: 'codex_sdk', mode: 'colocated' } })
      .where(eq(agents.id, row.agentId));
    const response = await app.inject({ method: 'GET', url: ownerUrl(row.sessionId) });
    expect(response.statusCode, response.body).toBe(200);
    expect(response.json()).toEqual({ owner: 'harness-server' });
    const distribution = await buildDistributionSessionStore(db).load(row.sessionId);
    expect(distribution?.agentMode).toBe('separate');
    expect(
      resolveExecutionOwner('cloud', {
        harness: distribution!.agentHarnessType,
        mode: distribution!.agentMode,
      }),
    ).toBe('harness-server');
  });

  it('rejects inconsistent persisted bindings instead of routing them to Claude', async () => {
    const row = await seed('codex_sdk', 'cloud');
    await db
      .update(agentVersions)
      .set({ snapshot: { harness_type: 'codex_sdk', metadata: { harness: 'claude_agent_sdk' } } })
      .where(eq(agentVersions.id, row.versionId));
    const response = await app.inject({ method: 'GET', url: ownerUrl(row.sessionId) });
    expect(response.statusCode, response.body).toBe(409);
    await expect(buildDistributionSessionStore(db).load(row.sessionId)).rejects.toThrow(/harness/);
  });

  it('preserves the cloud default for legacy environments without a target', async () => {
    const row = await seed('claude_agent_sdk', null);
    const response = await app.inject({ method: 'GET', url: ownerUrl(row.sessionId) });
    expect(response.statusCode, response.body).toBe(200);
    expect(response.json()).toEqual({ owner: 'harness-server' });
  });
});
