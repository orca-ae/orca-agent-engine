// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { FastifyInstance } from 'fastify';
import type { AddressInfo } from 'node:net';
import { and, eq } from 'drizzle-orm';
import type { FileRecord, FileStore } from '@orca/file-store';
import { buildCombinedTestApp } from '../../src/server.js';
import type { PreparedExecutionV2 } from '../../src/contracts/internal.contract.js';
import { newId } from '../../src/domain/versioning.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import {
  agents,
  agentVersions,
  environments,
  sessionResources,
  sessionSkillBindings,
  sessions,
  skills,
  skillVersions,
  vaultCredentials,
  vaults,
} from '../../src/persistence/postgres/schema.js';
import {
  buildStubStore,
  buildTestJwtMinter,
  closeTestDb,
  getTestDb,
  STUB_SSE_CONFIG,
} from './setup.js';
import { createTestApiKey, createTestWorkspace, uniqueWorkspace } from './fixtures.js';

describe('POST /internal/v1/workspaces/:workspaceId/sessions/:sessionId/executions:prepare', () => {
  let app: FastifyInstance;
  let baseURL: string;
  let db: DbClient;
  let workspaceId: string;
  let otherWorkspaceId: string;
  let apiKey: string;
  let sessionId: string;
  let agentId: string;
  let skillId: string;
  let skillVersionId: string;
  let environmentId: string;
  let vaultId: string;
  let fileId: string;
  let foreignEnvironmentId: string;
  const files = new Map<string, FileRecord>();
  const skillBundleSha256 = 'c'.repeat(64);

  beforeAll(async () => {
    ({ db } = await getTestDb());
    workspaceId = uniqueWorkspace('prepare_execution');
    otherWorkspaceId = uniqueWorkspace('prepare_execution_other');
    apiKey = await createTestApiKey(db, workspaceId);
    await createTestWorkspace(db, otherWorkspaceId);
    const fileStore = buildFileStore(files);
    app = buildCombinedTestApp({
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store: buildStubStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore,
    });
    await app.listen({ host: '127.0.0.1', port: 0 });
    baseURL = `http://127.0.0.1:${(app.server.address() as AddressInfo).port}`;

    const now = new Date();
    skillId = newId('skill');
    skillVersionId = newId('skillver');
    await db.insert(skills).values({
      id: skillId,
      workspaceId,
      type: 'custom',
      name: 'runtime-skill',
      slug: `runtime-skill-${skillId}`,
      version: 1,
      latestVersionId: null,
      description: 'Runtime skill',
      displayTitle: null,
      archivedAt: null,
      createdAt: now,
      updatedAt: now,
    });
    await db.insert(skillVersions).values({
      id: skillVersionId,
      workspaceId,
      skillId,
      version: 1,
      versionIdentifier: '1759178010641129',
      name: 'runtime-skill',
      description: 'Runtime skill',
      directory: 'runtime-skill',
      entrypoint: 'SKILL.md',
      packageSha256: skillBundleSha256,
      packageSizeBytes: 128,
      packageManifest: [
        {
          path: 'SKILL.md',
          sizeBytes: 32,
          sha256: 'd'.repeat(64),
          mode: 0o644,
          mimeType: 'text/markdown',
        },
      ],
      archivedAt: null,
      createdAt: now,
    });
    await db
      .update(skills)
      .set({ latestVersionId: skillVersionId })
      .where(and(eq(skills.workspaceId, workspaceId), eq(skills.id, skillId)));

    const childAgentId = newId('agt');
    const childVersionId = newId('agtv');
    await db.insert(agents).values({
      id: childAgentId,
      workspaceId,
      name: 'child',
      version: 1,
      latestVersionId: null,
      modelProvider: 'anthropic',
      modelId: 'claude-child',
      system: null,
      tools: [],
      mcpServers: [],
      skills: [],
      metadata: {},
      multiagent: null,
      archivedAt: null,
      createdAt: now,
      updatedAt: now,
    });
    await db.insert(agentVersions).values({
      id: childVersionId,
      workspaceId,
      agentId: childAgentId,
      version: 1,
      snapshot: agentSnapshot({
        id: childAgentId,
        name: 'child',
        version: 1,
        model: {
          provider: 'anthropic',
          id: 'claude-opus-4-8',
          speed: 'fast',
          effort: 'low',
        },
        system: null,
      }),
      createdAt: now,
    });
    await db
      .update(agents)
      .set({ latestVersionId: childVersionId })
      .where(and(eq(agents.workspaceId, workspaceId), eq(agents.id, childAgentId)));

    agentId = newId('agt');
    const agentVersion1Id = newId('agtv');
    const agentVersion2Id = newId('agtv');
    await db.insert(agents).values({
      id: agentId,
      workspaceId,
      name: 'primary latest',
      version: 2,
      latestVersionId: null,
      modelProvider: 'anthropic',
      modelId: 'claude-v2',
      system: 'version two prompt',
      tools: [{ type: 'bash' }],
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
        id: agentVersion1Id,
        workspaceId,
        agentId,
        version: 1,
        snapshot: agentSnapshot({
          id: agentId,
          name: 'primary pinned',
          version: 1,
          model: {
            provider: 'anthropic',
            id: 'claude-opus-5',
            speed: 'fast',
            effort: 'high',
          },
          system: 'version one prompt',
          tools: [{ type: 'bash' }],
          mcpServers: [{ name: 'base', url: 'https://base.example/mcp' }],
          skills: [
            {
              type: 'custom',
              skill_id: skillId,
              version: '1759178010641129',
            },
          ],
          multiagent: {
            type: 'coordinator',
            agents: [{ type: 'agent', id: childAgentId, version: 1 }],
          },
        }),
        createdAt: now,
      },
      {
        id: agentVersion2Id,
        workspaceId,
        agentId,
        version: 2,
        snapshot: agentSnapshot({
          id: agentId,
          name: 'primary latest',
          version: 2,
          system: 'version two prompt',
        }),
        createdAt: now,
      },
    ]);
    await db
      .update(agents)
      .set({ latestVersionId: agentVersion2Id })
      .where(and(eq(agents.workspaceId, workspaceId), eq(agents.id, agentId)));

    environmentId = newId('env');
    foreignEnvironmentId = newId('env');
    await db.insert(environments).values([
      {
        id: environmentId,
        workspaceId,
        name: 'runtime env',
        description: null,
        metadata: {},
        packages: { apt: ['curl'] },
        networking: { type: 'unrestricted' },
        image: 'ghcr.io/orca/runtime:1',
        target: 'cloud',
        archivedAt: null,
        createdAt: now,
        updatedAt: now,
      },
      {
        id: foreignEnvironmentId,
        workspaceId: otherWorkspaceId,
        name: 'foreign env',
        description: null,
        metadata: {},
        packages: {},
        networking: {},
        image: null,
        target: null,
        archivedAt: null,
        createdAt: now,
        updatedAt: now,
      },
    ]);

    vaultId = newId('vlt');
    const credentialId = newId('vcrd');
    await db.insert(vaults).values({
      id: vaultId,
      workspaceId,
      displayName: 'runtime vault',
      metadata: {},
      archivedAt: null,
      createdAt: now,
      updatedAt: now,
    });
    await db.insert(vaultCredentials).values({
      id: credentialId,
      workspaceId,
      vaultId,
      displayName: 'runtime credential',
      authType: 'static_bearer',
      mcpServerUrl: 'https://session.example/mcp',
      secretName: null,
      networking: { egress: 'allowed' },
      accessSecretRef: 'must-not-leak-secret-ref',
      refreshSecretRef: null,
      tokenEndpoint: null,
      clientId: null,
      tokenEndpointAuthType: null,
      clientSecretRef: null,
      metadata: {},
      archivedAt: null,
      createdAt: now,
      updatedAt: now,
    });

    sessionId = newId('ses');
    await db.insert(sessions).values({
      id: sessionId,
      workspaceId,
      agentId,
      agentVersion: 1,
      title: null,
      environmentId,
      metadata: { purpose: 'test' },
      vaultIds: [vaultId],
      tools: [{ type: 'custom', name: 'session-tool' }],
      mcpServers: [{ name: 'session', url: 'https://session.example/mcp' }],
      status: 'idle',
      archivedAt: null,
      createdAt: now,
      updatedAt: now,
    });
    await db.insert(sessionSkillBindings).values({
      workspaceId,
      sessionId,
      agentId,
      agentVersion: 1,
      ordinal: 0,
      skillVersionId,
      bundleSha256: skillBundleSha256,
    });

    fileId = newId('file');
    files.set(`${workspaceId}/${fileId}`, {
      id: fileId,
      workspaceId,
      filename: 'input.txt',
      mimeType: 'text/plain',
      sizeBytes: 12,
      sha256: 'a'.repeat(64),
      metadata: {},
      purpose: 'agent',
      scopeId: null,
      downloadable: false,
      archivedAt: null,
      createdAt: now,
      updatedAt: now,
    });
    await db.insert(sessionResources).values([
      {
        id: newId('sesrsc'),
        workspaceId,
        sessionId,
        type: 'file',
        fileId,
        memoryStoreId: null,
        repoRef: null,
        mountPath: '/workspace/input.txt',
        access: 'read_only',
        mountStrategy: 'tarball_prefetch',
        instructions: 'read this file',
        attachedAt: now,
        updatedAt: now,
        detachedAt: null,
      },
      {
        id: newId('sesrsc'),
        workspaceId,
        sessionId,
        type: 'file',
        fileId: newId('file'),
        memoryStoreId: null,
        repoRef: null,
        mountPath: '/workspace/detached.txt',
        access: 'read_only',
        mountStrategy: null,
        instructions: null,
        attachedAt: now,
        updatedAt: now,
        detachedAt: now,
      },
    ]);
  }, 30_000);

  afterAll(async () => {
    if (app) await app.close();
    await closeTestDb();
  });

  it('returns a secret-free pinned runtime snapshot without an API key', async () => {
    const response = await fetch(prepareUrl(workspaceId, sessionId), {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: '{}',
    });
    expect(response.status).toBe(200);
    const body = (await response.json()) as PreparedExecutionV2;

    expect(body.schema_version).toBe(2);
    expect(body.workspace_id).toBe(workspaceId);
    expect(body.primary_agent.version).toBe(1);
    expect(body.primary_agent.name).toBe('primary pinned');
    expect(body.primary_agent.model).toEqual({
      provider: 'anthropic',
      id: 'claude-opus-5',
      speed: 'fast',
      effort: 'high',
    });
    expect(body.primary_agent.system).toBe('version one prompt');
    expect(body.primary_agent.tools).toEqual([{ type: 'custom', name: 'session-tool' }]);
    expect(body.primary_agent.mcp_servers).toEqual([
      { name: 'session', url: 'https://session.example/mcp' },
    ]);
    expect(body.primary_agent).not.toHaveProperty('resolved_skills');
    expect(body.primary_agent.skills).toEqual([
      {
        id: skillVersionId,
        skill_id: skillId,
        source: 'custom',
        version_identifier: '1759178010641129',
        name: 'runtime-skill',
        description: 'Runtime skill',
        entrypoint: 'SKILL.md',
        package_sha256: skillBundleSha256,
        package_size_bytes: 128,
      },
    ]);
    expect(body.subagents).toHaveLength(1);
    expect(body.subagents[0]?.name).toBe('child');
    expect(body.subagents[0]?.model).toEqual({
      provider: 'anthropic',
      id: 'claude-opus-4-8',
      speed: 'fast',
      effort: 'low',
    });
    expect(body.subagents[0]?.system).toBeNull();
    expect(body.environment).toEqual(
      expect.objectContaining({ id: body.session.environment_id, name: 'runtime env' }),
    );
    expect(body.vault_credentials).toEqual([
      expect.objectContaining({
        auth_type: 'static_bearer',
        mcp_server_url: 'https://session.example/mcp',
      }),
    ]);
    expect(body.resources).toEqual([
      expect.objectContaining({
        type: 'file',
        file: expect.objectContaining({ filename: 'input.txt', sha256: 'a'.repeat(64) }),
      }),
    ]);
    const serialized = JSON.stringify(body);
    expect(serialized).not.toContain('must-not-leak-secret-ref');
    expect(serialized).not.toContain('access_secret_ref');
    expect(serialized).not.toContain('secret_value');
  });

  it('applies the stored session model override to the prepared primary agent', async () => {
    await db
      .update(sessions)
      .set({
        agentOverrides: {
          model: {
            provider: 'anthropic',
            id: 'claude-opus-4-8',
            speed: 'fast',
            effort: 'low',
          },
        },
      })
      .where(and(eq(sessions.workspaceId, workspaceId), eq(sessions.id, sessionId)));

    try {
      const response = await fetch(prepareUrl(workspaceId, sessionId), {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: '{}',
      });
      expect(response.status).toBe(200);
      const body = (await response.json()) as PreparedExecutionV2;
      expect(body.primary_agent.model).toEqual({
        provider: 'anthropic',
        id: 'claude-opus-4-8',
        speed: 'fast',
        effort: 'low',
      });
    } finally {
      await db
        .update(sessions)
        .set({ agentOverrides: null })
        .where(and(eq(sessions.workspaceId, workspaceId), eq(sessions.id, sessionId)));
    }
  });

  it('rejects legacy persisted coordinator snapshots with mixed effective speeds', async () => {
    await db
      .update(sessions)
      .set({
        agentOverrides: {
          model: {
            provider: 'anthropic',
            id: 'claude-opus-5',
          },
        },
      })
      .where(and(eq(sessions.workspaceId, workspaceId), eq(sessions.id, sessionId)));

    try {
      const response = await fetch(prepareUrl(workspaceId, sessionId), {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: '{}',
      });
      expect(response.status).toBe(409);
      expect(await response.json()).toEqual({
        error: 'invalid_runtime_binding',
        resource_type: 'model_config',
        resource_id: expect.stringMatching(/mixed model\.speed/),
      });
    } finally {
      await db
        .update(sessions)
        .set({ agentOverrides: null })
        .where(and(eq(sessions.workspaceId, workspaceId), eq(sessions.id, sessionId)));
    }
  });

  it('rejects legacy persisted fast controls for an unsupported model', async () => {
    await db
      .update(sessions)
      .set({
        agentOverrides: {
          model: {
            provider: 'anthropic',
            id: 'claude-sonnet-4-6',
            speed: 'fast',
          },
        },
      })
      .where(and(eq(sessions.workspaceId, workspaceId), eq(sessions.id, sessionId)));

    try {
      const response = await fetch(prepareUrl(workspaceId, sessionId), {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: '{}',
      });
      expect(response.status).toBe(409);
      expect(await response.json()).toEqual({
        error: 'invalid_runtime_binding',
        resource_type: 'model_config',
        resource_id: expect.stringMatching(/claude-opus-5/),
      });
    } finally {
      await db
        .update(sessions)
        .set({ agentOverrides: null })
        .where(and(eq(sessions.workspaceId, workspaceId), eq(sessions.id, sessionId)));
    }
  });

  it('rejects legacy persisted effort unsupported by the selected model', async () => {
    await db
      .update(sessions)
      .set({
        agentOverrides: {
          model: {
            provider: 'anthropic',
            id: 'claude-sonnet-4-6',
            effort: 'xhigh',
          },
        },
      })
      .where(and(eq(sessions.workspaceId, workspaceId), eq(sessions.id, sessionId)));

    try {
      const response = await fetch(prepareUrl(workspaceId, sessionId), {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: '{}',
      });
      expect(response.status).toBe(409);
      expect(await response.json()).toEqual({
        error: 'invalid_runtime_binding',
        resource_type: 'model_config',
        resource_id: expect.stringMatching(/supported levels are low, medium, high, max/),
      });
    } finally {
      await db
        .update(sessions)
        .set({ agentOverrides: null })
        .where(and(eq(sessions.workspaceId, workspaceId), eq(sessions.id, sessionId)));
    }
  });

  it('returns 404 when the path workspace does not own the session', async () => {
    const response = await fetch(prepareUrl(otherWorkspaceId, sessionId), {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: '{}',
    });
    expect(response.status).toBe(404);
  });

  it('returns 409 when the session is no longer runnable', async () => {
    await db
      .update(sessions)
      .set({ status: 'terminated' })
      .where(and(eq(sessions.workspaceId, workspaceId), eq(sessions.id, sessionId)));
    try {
      await expectInvalidRuntimeBinding('session', sessionId);
    } finally {
      await db
        .update(sessions)
        .set({ status: 'idle' })
        .where(and(eq(sessions.workspaceId, workspaceId), eq(sessions.id, sessionId)));
    }
  });

  it('returns 409 when the pinned agent version is no longer available', async () => {
    await db
      .update(agents)
      .set({ archivedAt: new Date() })
      .where(and(eq(agents.workspaceId, workspaceId), eq(agents.id, agentId)));
    try {
      await expectInvalidRuntimeBinding('agent_version', `${agentId}@1`);
    } finally {
      await db
        .update(agents)
        .set({ archivedAt: null })
        .where(and(eq(agents.workspaceId, workspaceId), eq(agents.id, agentId)));
    }
  });

  it('keeps an archived skill version resolvable for an existing session', async () => {
    await db
      .update(skillVersions)
      .set({ archivedAt: new Date() })
      .where(and(eq(skillVersions.workspaceId, workspaceId), eq(skillVersions.id, skillVersionId)));
    try {
      const response = await fetch(prepareUrl(workspaceId, sessionId), {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: '{}',
      });
      expect(response.status).toBe(200);
      const body = (await response.json()) as PreparedExecutionV2;
      expect(body.primary_agent.skills).toEqual([
        expect.objectContaining({
          id: skillVersionId,
          package_sha256: skillBundleSha256,
        }),
      ]);
    } finally {
      await db
        .update(skillVersions)
        .set({ archivedAt: null })
        .where(
          and(eq(skillVersions.workspaceId, workspaceId), eq(skillVersions.id, skillVersionId)),
        );
    }
  });

  it('keeps an archived environment resolvable for an existing session', async () => {
    await db
      .update(environments)
      .set({ archivedAt: new Date() })
      .where(and(eq(environments.workspaceId, workspaceId), eq(environments.id, environmentId)));
    try {
      const response = await fetch(prepareUrl(workspaceId, sessionId), {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: '{}',
      });
      expect(response.status).toBe(200);
      const body = (await response.json()) as PreparedExecutionV2;
      expect(body.environment).toEqual(expect.objectContaining({ id: environmentId }));
    } finally {
      await db
        .update(environments)
        .set({ archivedAt: null })
        .where(and(eq(environments.workspaceId, workspaceId), eq(environments.id, environmentId)));
    }
  });

  it('returns 409 when a bound vault is archived', async () => {
    await db
      .update(vaults)
      .set({ archivedAt: new Date() })
      .where(and(eq(vaults.workspaceId, workspaceId), eq(vaults.id, vaultId)));
    try {
      await expectInvalidRuntimeBinding('vault', vaultId);
    } finally {
      await db
        .update(vaults)
        .set({ archivedAt: null })
        .where(and(eq(vaults.workspaceId, workspaceId), eq(vaults.id, vaultId)));
    }
  });

  it('returns 409 when an attached file is no longer available', async () => {
    const key = `${workspaceId}/${fileId}`;
    const record = files.get(key);
    expect(record).toBeDefined();
    files.delete(key);
    try {
      await expectInvalidRuntimeBinding('file', fileId);
    } finally {
      files.set(key, record!);
    }
  });

  it('rejects a cross-workspace runtime binding at the persistence boundary', async () => {
    await expect(
      db.insert(sessions).values({
        id: newId('ses'),
        workspaceId,
        agentId,
        agentVersion: 1,
        environmentId: foreignEnvironmentId,
      }),
    ).rejects.toThrow();
  });

  it('rejects a cross-workspace environment when creating a session', async () => {
    const response = await fetch(`${baseURL}/v1/sessions`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-api-key': apiKey },
      body: JSON.stringify({ agent_id: agentId, environment_id: foreignEnvironmentId }),
    });
    expect(response.status).toBe(404);
    expect(await response.json()).toEqual({
      type: 'error',
      error: {
        type: 'not_found_error',
        message: 'environment not found',
      },
      request_id: expect.any(String),
    });
  });

  function prepareUrl(workspace: string, session: string): string {
    return `${baseURL}/internal/v1/workspaces/${workspace}/sessions/${session}/executions:prepare`;
  }

  async function expectInvalidRuntimeBinding(resourceType: string, resourceId: string) {
    const response = await fetch(prepareUrl(workspaceId, sessionId), {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: '{}',
    });
    expect(response.status).toBe(409);
    expect(await response.json()).toEqual({
      error: 'invalid_runtime_binding',
      resource_type: resourceType,
      resource_id: resourceId,
    });
  }
});

function agentSnapshot(input: {
  id: string;
  name: string;
  version: number;
  model?: Record<string, unknown>;
  system: string | null;
  tools?: unknown[];
  mcpServers?: unknown[];
  skills?: unknown[];
  multiagent?: Record<string, unknown> | null;
}): Record<string, unknown> {
  return {
    id: input.id,
    name: input.name,
    version: input.version,
    model: input.model ?? { provider: 'anthropic', id: `claude-v${input.version}` },
    system: input.system,
    tools: input.tools ?? [],
    mcp_servers: input.mcpServers ?? [],
    skills: input.skills ?? [],
    metadata: {},
    multiagent: input.multiagent ?? null,
  };
}

function buildFileStore(records: Map<string, FileRecord>): FileStore {
  return {
    async create() {
      throw new Error('not implemented');
    },
    async get(workspaceId, fileId) {
      return records.get(`${workspaceId}/${fileId}`) ?? null;
    },
    async list() {
      return { items: [], nextCursor: null };
    },
    async open() {
      return null;
    },
    async archive() {},
    async delete() {},
    async close() {},
  };
}
