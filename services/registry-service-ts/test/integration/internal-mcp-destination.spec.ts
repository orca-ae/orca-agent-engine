// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { and, eq } from 'drizzle-orm';
import type { InternalAuthVerifier } from '../../src/auth/internal-auth.js';
import { buildInternalApp } from '../../src/server.js';
import { mcpDestinationRevision } from '../../src/domain/mcp-destination.js';
import { newId } from '../../src/domain/versioning.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import {
  agents,
  agentVersions,
  agentObservabilityWorkspaceArchiveRevocations,
  sessions,
  vaultCredentials,
  vaults,
  workspaces,
} from '../../src/persistence/postgres/schema.js';
import {
  buildStubFileStore,
  buildStubStore,
  buildTestJwtMinter,
  closeTestDb,
  getTestDb,
  STUB_SSE_CONFIG,
} from './setup.js';
import { createTestWorkspace, uniqueWorkspace } from './fixtures.js';

const gatewayToken = 'gateway';
const harnessToken = 'harness';
const sharedToken = 'shared';

describe('POST internal mcp-destination/resolve', () => {
  let app: FastifyInstance;
  let db: DbClient;
  let workspaceId: string;
  let otherWorkspaceId: string;
  let sessionId: string;
  let agentId: string;
  let firstVaultId: string;
  let firstCredentialId: string;
  let secondCredentialId: string;
  let normalizedCredentialId: string;
  let unattachedCredentialId: string;

  beforeAll(async () => {
    ({ db } = await getTestDb());
    workspaceId = uniqueWorkspace('mcp_destination');
    otherWorkspaceId = uniqueWorkspace('mcp_destination_other');
    await createTestWorkspace(db, workspaceId);
    await createTestWorkspace(db, otherWorkspaceId);

    app = buildInternalApp(
      {
        db,
        oidc: { allowedIssuers: [], audience: 'test' },
        store: buildStubStore(),
        sse: STUB_SSE_CONFIG,
        jwtMinter: buildTestJwtMinter(),
        fileStore: buildStubFileStore(),
      },
      callerVerifier(),
    );
    await app.ready();

    const now = new Date();
    agentId = newId('agt');
    const pinnedVersionId = newId('agtv');
    const latestVersionId = newId('agtv');
    await db.insert(agents).values({
      id: agentId,
      workspaceId,
      name: 'resolver agent',
      version: 2,
      latestVersionId: null,
      modelProvider: 'anthropic',
      modelId: 'claude',
      system: '',
      tools: [],
      mcpServers: [],
      skills: [],
      metadata: {},
      multiagent: null,
      archivedAt: null,
      createdAt: now,
      updatedAt: now,
    });
    await db.insert(agentVersions).values([
      {
        id: pinnedVersionId,
        workspaceId,
        agentId,
        version: 1,
        snapshot: agentSnapshot(agentId, 1, [
          { name: 'duplicate', url: 'https://old.example.test/mcp' },
          { name: 'public', url: 'https://public.example.test/mcp' },
          { name: 'duplicate', url: 'https://matched.example.test/mcp' },
        ]),
        createdAt: now,
      },
      {
        id: latestVersionId,
        workspaceId,
        agentId,
        version: 2,
        snapshot: agentSnapshot(agentId, 2, [
          { name: 'latest-only', url: 'https://latest.example.test/mcp' },
        ]),
        createdAt: new Date(now.getTime() + 1),
      },
    ]);
    await db
      .update(agents)
      .set({ latestVersionId })
      .where(and(eq(agents.workspaceId, workspaceId), eq(agents.id, agentId)));

    firstVaultId = newId('vlt');
    const secondVaultId = newId('vlt');
    const unattachedVaultId = newId('vlt');
    await db.insert(vaults).values(
      [firstVaultId, secondVaultId, unattachedVaultId].map((id) => ({
        id,
        workspaceId,
        displayName: id,
        metadata: {},
        archivedAt: null,
        createdAt: now,
        updatedAt: now,
      })),
    );
    firstCredentialId = newId('vcrd');
    secondCredentialId = newId('vcrd');
    normalizedCredentialId = newId('vcrd');
    unattachedCredentialId = newId('vcrd');
    await db
      .insert(vaultCredentials)
      .values([
        credential(firstCredentialId, workspaceId, firstVaultId, new Date(now.getTime() + 20)),
        credential(secondCredentialId, workspaceId, secondVaultId, new Date(now.getTime() + 10)),
        credential(unattachedCredentialId, workspaceId, unattachedVaultId, new Date(now.getTime())),
        credential(
          normalizedCredentialId,
          workspaceId,
          firstVaultId,
          new Date(now.getTime() + 30),
          'HTTPS://PUBLIC.EXAMPLE.TEST:443/mcp/',
        ),
      ]);

    sessionId = newId('ses');
    await db.insert(sessions).values({
      id: sessionId,
      workspaceId,
      agentId,
      agentVersion: 1,
      runtimeRevision: 9,
      vaultIds: [firstVaultId, secondVaultId],
      status: 'idle',
      archivedAt: null,
      createdAt: now,
      updatedAt: now,
    });
  });

  afterAll(async () => {
    if (app) await app.close();
    await closeTestDb();
  });

  it('uses last duplicate backend, session vault order, and returns revision without secrets', async () => {
    const response = await resolve(workspaceId, sessionId, 'duplicate');

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({
      url: 'https://matched.example.test/mcp',
      credential_id: firstCredentialId,
      revision: mcpDestinationRevision('https://matched.example.test/mcp', firstCredentialId),
    });
    const serialized = response.body;
    expect(serialized).not.toContain(secondCredentialId);
    expect(serialized).not.toContain(unattachedCredentialId);
    expect(serialized).not.toContain('access_secret_ref');
    expect(serialized).not.toContain('test-secret');
    expect((await resolve(workspaceId, sessionId, 'latest-only')).statusCode).toBe(404);
  });

  it('matches credentials using Anthropic URL normalization', async () => {
    const response = await resolve(workspaceId, sessionId, 'public');
    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({
      url: 'https://public.example.test/mcp',
      credential_id: normalizedCredentialId,
      revision: mcpDestinationRevision('https://public.example.test/mcp', normalizedCredentialId),
    });
  });

  it('rejects pre-existing normalized URL conflicts within one vault', async () => {
    const conflictingCredentialId = newId('vcrd');
    const now = new Date();
    await db
      .insert(vaultCredentials)
      .values(
        credential(
          conflictingCredentialId,
          workspaceId,
          firstVaultId,
          now,
          'https://public.example.test/mcp//',
        ),
      );
    try {
      const response = await resolve(workspaceId, sessionId, 'public');
      expect(response.statusCode).toBe(409);
      expect(response.json()).toEqual({
        error: 'invalid_runtime_binding',
        resource_type: 'vault_credential_binding',
        resource_id: firstVaultId,
      });
    } finally {
      await db.delete(vaultCredentials).where(eq(vaultCredentials.id, conflictingCredentialId));
    }
  });

  it('keeps idempotency revision stable across unrelated runtime revisions', async () => {
    const before = (await resolve(workspaceId, sessionId, 'duplicate')).json<{
      revision: number;
    }>();
    await db
      .update(sessions)
      .set({ runtimeRevision: 10 })
      .where(and(eq(sessions.workspaceId, workspaceId), eq(sessions.id, sessionId)));
    try {
      const after = (await resolve(workspaceId, sessionId, 'duplicate')).json<{
        revision: number;
      }>();
      expect(after.revision).toBe(before.revision);
    } finally {
      await db
        .update(sessions)
        .set({ runtimeRevision: 9 })
        .where(and(eq(sessions.workspaceId, workspaceId), eq(sessions.id, sessionId)));
    }
  });

  it('ignores archived credentials and falls through in deterministic order', async () => {
    await db
      .update(vaultCredentials)
      .set({ archivedAt: new Date() })
      .where(eq(vaultCredentials.id, firstCredentialId));
    try {
      const fallback = await resolve(workspaceId, sessionId, 'duplicate');
      expect(fallback.statusCode).toBe(200);
      expect(fallback.json()).toMatchObject({ credential_id: secondCredentialId });

      await db
        .update(vaultCredentials)
        .set({ archivedAt: new Date() })
        .where(eq(vaultCredentials.id, secondCredentialId));
      const noActiveMatch = await resolve(workspaceId, sessionId, 'duplicate');
      expect(noActiveMatch.statusCode).toBe(200);
      expect(noActiveMatch.json()).toMatchObject({ credential_id: null });
    } finally {
      await db
        .update(vaultCredentials)
        .set({ archivedAt: null })
        .where(eq(vaultCredentials.id, firstCredentialId));
      await db
        .update(vaultCredentials)
        .set({ archivedAt: null })
        .where(eq(vaultCredentials.id, secondCredentialId));
    }
  });

  it('uses session override as a full replacement', async () => {
    await db
      .update(sessions)
      .set({
        mcpServers: [{ name: 'override', url: 'http://override.example.test/mcp' }],
        runtimeRevision: 10,
      })
      .where(and(eq(sessions.workspaceId, workspaceId), eq(sessions.id, sessionId)));
    try {
      const response = await resolve(workspaceId, sessionId, 'override');
      expect(response.statusCode).toBe(200);
      expect(response.json()).toEqual({
        url: 'http://override.example.test/mcp',
        credential_id: null,
        revision: mcpDestinationRevision('http://override.example.test/mcp', null),
      });
      expect((await resolve(workspaceId, sessionId, 'duplicate')).statusCode).toBe(404);
    } finally {
      await db
        .update(sessions)
        .set({ mcpServers: null, runtimeRevision: 9 })
        .where(and(eq(sessions.workspaceId, workspaceId), eq(sessions.id, sessionId)));
    }
  });

  it('hides unknown, cross-workspace, archived workspace/session, and terminated session', async () => {
    expect((await resolve(workspaceId, sessionId, 'unknown')).statusCode).toBe(404);
    expect((await resolve(otherWorkspaceId, sessionId, 'duplicate')).statusCode).toBe(404);

    await db.update(sessions).set({ archivedAt: new Date() }).where(eq(sessions.id, sessionId));
    expect((await resolve(workspaceId, sessionId, 'duplicate')).statusCode).toBe(404);
    await db
      .update(sessions)
      .set({ archivedAt: null, status: 'terminated' })
      .where(eq(sessions.id, sessionId));
    expect((await resolve(workspaceId, sessionId, 'duplicate')).statusCode).toBe(404);
    await db.update(sessions).set({ status: 'idle' }).where(eq(sessions.id, sessionId));
    await db
      .update(workspaces)
      .set({ status: 'archived', archivedAt: new Date(), updatedAt: new Date() })
      .where(eq(workspaces.id, workspaceId));
    expect((await resolve(workspaceId, sessionId, 'duplicate')).statusCode).toBe(404);
    await db.transaction(async (tx) => {
      await tx
        .update(workspaces)
        .set({ status: 'active', archivedAt: null, updatedAt: new Date() })
        .where(eq(workspaces.id, workspaceId));
      await tx
        .delete(agentObservabilityWorkspaceArchiveRevocations)
        .where(eq(agentObservabilityWorkspaceArchiveRevocations.workspaceId, workspaceId));
    });
  });

  it('returns existing 409 shape for unsafe persisted URLs', async () => {
    try {
      for (const url of [
        'file:///etc/passwd',
        'https://user:secret@mcp.example.test/mcp',
        'https://mcp.example.test/mcp#credential',
      ]) {
        await db
          .update(sessions)
          .set({ mcpServers: [{ name: 'broken', url }] })
          .where(eq(sessions.id, sessionId));
        const response = await resolve(workspaceId, sessionId, 'broken');
        expect(response.statusCode).toBe(409);
        expect(response.json()).toEqual({
          error: 'invalid_runtime_binding',
          resource_type: 'mcp_server',
          resource_id: 'broken',
        });
      }
    } finally {
      await db.update(sessions).set({ mcpServers: null }).where(eq(sessions.id, sessionId));
    }
  });

  it('rejects a non-positive persisted runtime revision', async () => {
    await db
      .update(sessions)
      .set({ runtimeRevision: 0 })
      .where(and(eq(sessions.workspaceId, workspaceId), eq(sessions.id, sessionId)));
    try {
      const response = await resolve(workspaceId, sessionId, 'duplicate');
      expect(response.statusCode).toBe(409);
      expect(response.json()).toEqual({
        error: 'invalid_runtime_binding',
        resource_type: 'session_revision',
        resource_id: sessionId,
      });
    } finally {
      await db
        .update(sessions)
        .set({ runtimeRevision: 9 })
        .where(and(eq(sessions.workspaceId, workspaceId), eq(sessions.id, sessionId)));
    }
  });

  it('allows ai-gateway/shared internal callers and rejects harness', async () => {
    expect((await resolve(workspaceId, sessionId, 'duplicate', gatewayToken)).statusCode).toBe(200);
    expect((await resolve(workspaceId, sessionId, 'duplicate', sharedToken)).statusCode).toBe(200);
    expect((await resolve(workspaceId, sessionId, 'duplicate', harnessToken)).statusCode).toBe(403);
  });

  it('rejects body fields beyond backend', async () => {
    const response = await app.inject({
      method: 'POST',
      url: `/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/mcp-destination/resolve`,
      headers: { authorization: `Bearer ${gatewayToken}` },
      payload: { backend: 'duplicate', url: 'https://attacker.example.test' },
    });
    expect(response.statusCode).toBe(400);
  });

  function resolve(workspace: string, session: string, backend: string, token = gatewayToken) {
    return app.inject({
      method: 'POST',
      url: `/internal/v1/workspaces/${workspace}/sessions/${session}/mcp-destination/resolve`,
      headers: { authorization: `Bearer ${token}` },
      payload: { backend },
    });
  }
});

function callerVerifier(): InternalAuthVerifier {
  return {
    async verify(token) {
      if (token === gatewayToken) return { caller: 'ai-gateway', subject: token };
      if (token === harnessToken) return { caller: 'harness', subject: token };
      if (token === sharedToken) return { caller: 'shared', subject: token };
      return null;
    },
  };
}

function agentSnapshot(
  agentId: string,
  version: number,
  mcpServers: unknown[],
): Record<string, unknown> {
  return {
    id: agentId,
    name: 'resolver agent',
    version,
    model: { provider: 'anthropic', id: 'claude' },
    system: '',
    tools: [],
    mcp_servers: mcpServers,
    skills: [],
    metadata: {},
    multiagent: null,
  };
}

function credential(
  id: string,
  workspaceId: string,
  vaultId: string,
  createdAt: Date,
  mcpServerUrl = 'https://matched.example.test/mcp',
) {
  return {
    id,
    workspaceId,
    vaultId,
    displayName: id,
    authType: 'static_bearer',
    mcpServerUrl,
    secretName: null,
    networking: {},
    accessSecretRef: `test-secret-ref-${id}`,
    refreshSecretRef: null,
    tokenEndpoint: null,
    clientId: null,
    tokenEndpointAuthType: null,
    clientSecretRef: null,
    metadata: {},
    archivedAt: null,
    createdAt,
    updatedAt: createdAt,
  };
}
