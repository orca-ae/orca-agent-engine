// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import type { FastifyInstance } from 'fastify';
import type { AddressInfo } from 'node:net';
import { generateKeyPairSync } from 'node:crypto';
import fs from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { and, eq } from 'drizzle-orm';
import {
  getTestDb,
  closeTestDb,
  buildStubStore,
  STUB_SSE_CONFIG,
  buildTestJwtMinter,
  buildStubFileStore,
} from './setup.js';
import { createTestWorkspace, TEST_ORGANIZATION_ID, uniqueWorkspace } from './fixtures.js';
import { buildCombinedTestApp } from '../../src/server.js';
import { SessionJwtMinter } from '../../src/auth/session-jwt.js';
import { DefaultSecretProvider, EnvSecretProvider } from '../../src/secrets/index.js';
import {
  agents,
  agentVersions,
  agentObservabilityWorkspaceArchiveRevocations,
  gitCredentials,
  sessions,
  sessionResources,
  workspaces,
} from '../../src/persistence/postgres/schema.js';
import { newId } from '../../src/domain/versioning.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const PRIVATE_PEM = fs.readFileSync(
  resolve(__dirname, '../fixtures/session-jwt-private.pem'),
  'utf8',
);

/**
 * Integration coverage for `POST /v1/git-creds`.
 *
 * The route is JWT-only (no api-key) — `auth.ts` exempts the path from the
 * global pre-handler so the in-sandbox `orca-git-creds` helper can call it
 * with just the session-scoped JWT (`aud='git-creds'`).
 *
 * Test scenarios cover the three trust layers (JWT verify → repo allowlist
 * → git credential repo_url match → secret resolution) plus path normalization
 * (the helper sends git's actual URL, the JWT carries the bare repo URL).
 */

const PAT_ENV_KEY = 'TEST_GITHUB_PAT';
const PAT_VALUE = 'ghp_test_token_value_xyz';

describe('POST /v1/git-creds (integration)', () => {
  let app: FastifyInstance;
  let baseURL: string;
  let workspaceId: string;

  // Re-use the same minter the app uses so the JWTs verify correctly.
  const minter = buildTestJwtMinter();
  const secretProvider = new DefaultSecretProvider(
    new EnvSecretProvider((k) => process.env[k] ?? null),
    [],
  );

  beforeAll(async () => {
    process.env[PAT_ENV_KEY] = PAT_VALUE;
    const { db } = await getTestDb();
    app = buildCombinedTestApp({
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store: buildStubStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: minter,
      fileStore: buildStubFileStore(),
      secretProvider,
    });
    await app.listen({ host: '127.0.0.1', port: 0 });
    baseURL = `http://127.0.0.1:${(app.server.address() as AddressInfo).port}`;
  }, 30000);

  afterAll(async () => {
    if (app) await app.close();
    delete process.env[PAT_ENV_KEY];
    await closeTestDb();
  });

  beforeEach(() => {
    workspaceId = uniqueWorkspace('gitcreds');
  });

  /**
   * Seed a workspace + git credential + session + github_repository session resource.
   * Direct DB inserts keep this route-focused: production traffic normally
   * creates repo bindings through the validated session resource path.
   */
  async function seedRepoBinding(opts: {
    repoUrl: string;
    credentialRepoUrl?: string;
    secretRef?: string;
    credentialSessionResourceId?: string;
  }): Promise<{ sessionId: string; gitCredentialId: string; resourceId: string }> {
    const { db } = await getTestDb();
    const gitCredentialId = newId('gitcred');
    const agentId = newId('agt');
    const agentVersionId = newId('agtv');
    const sessionId = newId('ses');
    const resourceId = newId('sesrsc');
    const now = new Date();

    await createTestWorkspace(db, workspaceId);
    await db.insert(agents).values({
      id: agentId,
      workspaceId,
      name: 'git-creds fixture',
      version: 1,
      latestVersionId: null,
      modelProvider: 'anthropic',
      modelId: 'claude-test',
      createdAt: now,
      updatedAt: now,
    });
    await db.insert(agentVersions).values({
      id: agentVersionId,
      workspaceId,
      agentId,
      version: 1,
      snapshot: {
        id: agentId,
        name: 'git-creds fixture',
        version: 1,
        model: { provider: 'anthropic', id: 'claude-test' },
      },
      createdAt: now,
    });
    await db
      .update(agents)
      .set({ latestVersionId: agentVersionId })
      .where(and(eq(agents.workspaceId, workspaceId), eq(agents.id, agentId)));

    await db.insert(sessions).values({
      id: sessionId,
      workspaceId,
      agentId,
      agentVersion: 1,
      environmentId: null,
      vaultIds: [],
      status: 'idle',
      lastEventSeq: 0,
      sandboxHandleId: null,
      startedAt: null,
      lastActiveAt: null,
      archivedAt: null,
      createdAt: now,
      updatedAt: now,
    });

    await db.insert(sessionResources).values({
      id: resourceId,
      workspaceId,
      sessionId,
      type: 'github_repository',
      fileId: null,
      memoryStoreId: null,
      repoRef: { git_credential_id: gitCredentialId, url: opts.repoUrl },
      mountPath: '/workspace/repo/',
      access: 'read_write',
      mountStrategy: null,
      instructions: null,
      attachedAt: now,
      detachedAt: null,
    });

    if (opts.credentialSessionResourceId) {
      await db.insert(sessionResources).values({
        id: opts.credentialSessionResourceId,
        workspaceId,
        sessionId,
        type: 'file',
        fileId: null,
        memoryStoreId: null,
        repoRef: null,
        mountPath: '/workspace/other-resource/',
        access: 'read_only',
        mountStrategy: null,
        instructions: null,
        attachedAt: now,
        detachedAt: null,
      });
    }

    await db.insert(gitCredentials).values({
      id: gitCredentialId,
      workspaceId,
      provider: 'github',
      repoUrl: opts.credentialRepoUrl ?? opts.repoUrl,
      secretRef: opts.secretRef ?? `env:${PAT_ENV_KEY}`,
      sessionResourceId: opts.credentialSessionResourceId ?? null,
      metadata: {},
      archivedAt: null,
      createdAt: now,
      updatedAt: now,
    });

    return { sessionId, gitCredentialId, resourceId };
  }

  async function mintGitCredsToken(opts: {
    sessionId: string;
    repoUrls: string[];
    audience?: string;
    minter?: SessionJwtMinter;
    ttlSecs?: number;
  }): Promise<string> {
    const m =
      opts.minter ??
      (opts.ttlSecs
        ? new SessionJwtMinter({
            // Same private key + issuer as the integration minter so a
            // short-TTL token can still verify against the running app.
            privateKeyPem: PRIVATE_PEM,
            issuer: 'orca-registry',
            audience: 'ai-gateway',
            ttlSecs: opts.ttlSecs,
          })
        : minter);
    const { token } = await m.mint(
      {
        org_id: TEST_ORGANIZATION_ID,
        workspace_id: workspaceId,
        session_id: opts.sessionId,
        mcp_server_names: [],
        vault_ids: [],
        credential_ids: [],
      },
      {
        audience: opts.audience ?? 'git-creds',
        repoUrls: opts.repoUrls,
      },
    );
    return token;
  }

  it('happy path: 200 with x-access-token + PAT (helper sends path-with-.git)', async () => {
    const repoUrl = 'https://github.com/org/repo';
    const { sessionId } = await seedRepoBinding({ repoUrl });
    const token = await mintGitCredsToken({ sessionId, repoUrls: [repoUrl] });

    const res = await fetch(`${baseURL}/v1/git-creds`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${token}`,
        'content-type': 'application/json',
      },
      body: JSON.stringify({
        protocol: 'https',
        host: 'github.com',
        path: '/org/repo.git/info/refs',
      }),
    });
    expect(res.status).toBe(200);
    const got = (await res.json()) as { username: string; password: string };
    expect(got.username).toBe('x-access-token');
    expect(got.password).toBe(PAT_VALUE);
  }, 30000);

  it('path normalization: bare /org/repo (no .git) also matches the bare allowlist URL', async () => {
    const repoUrl = 'https://github.com/org/repo';
    const { sessionId } = await seedRepoBinding({ repoUrl });
    const token = await mintGitCredsToken({ sessionId, repoUrls: [repoUrl] });

    const res = await fetch(`${baseURL}/v1/git-creds`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${token}`,
        'content-type': 'application/json',
      },
      body: JSON.stringify({
        protocol: 'https',
        host: 'github.com',
        path: '/org/repo',
      }),
    });
    expect(res.status).toBe(200);
    const got = (await res.json()) as { username: string; password: string };
    expect(got.password).toBe(PAT_VALUE);
  }, 30000);

  it('fails closed when legacy rows contain multiple matching repository resources', async () => {
    const repoUrl = 'https://github.com/org/repo';
    const { sessionId } = await seedRepoBinding({ repoUrl });
    const { db } = await getTestDb();
    const secondResourceId = newId('sesrsc');
    const secondCredentialId = newId('gitcred');
    const now = new Date();

    await db.insert(sessionResources).values({
      id: secondResourceId,
      workspaceId,
      sessionId,
      type: 'github_repository',
      fileId: null,
      memoryStoreId: null,
      repoRef: { git_credential_id: secondCredentialId, url: `${repoUrl}.git` },
      mountPath: '/workspace/repo-2/',
      access: 'read_write',
      mountStrategy: null,
      instructions: null,
      attachedAt: now,
      updatedAt: now,
      detachedAt: null,
    });
    await db.insert(gitCredentials).values({
      id: secondCredentialId,
      workspaceId,
      provider: 'github',
      repoUrl,
      secretRef: `env:${PAT_ENV_KEY}`,
      sessionResourceId: secondResourceId,
      metadata: {},
      archivedAt: null,
      createdAt: now,
      updatedAt: now,
    });

    const token = await mintGitCredsToken({ sessionId, repoUrls: [repoUrl] });
    const res = await fetch(`${baseURL}/v1/git-creds`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${token}`,
        'content-type': 'application/json',
      },
      body: JSON.stringify({ protocol: 'https', host: 'github.com', path: '/org/repo' }),
    });

    expect(res.status).toBe(409);
    const body = (await res.json()) as { error: { message: string } };
    expect(body.error.message).toBe(
      'multiple github_repository session resources match the requested URL',
    );
    expect(JSON.stringify(body)).not.toContain(PAT_VALUE);
  }, 30000);

  it('refuses to release a PAT for a plaintext HTTP remote', async () => {
    const repoUrl = 'http://github.com/org/repo';
    const { sessionId } = await seedRepoBinding({ repoUrl });
    const token = await mintGitCredsToken({ sessionId, repoUrls: [repoUrl] });

    const res = await fetch(`${baseURL}/v1/git-creds`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${token}`,
        'content-type': 'application/json',
      },
      body: JSON.stringify({ protocol: 'http', host: 'github.com', path: '/org/repo' }),
    });

    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: { message: string } };
    expect(body.error.message).toBe('protocol must be https');
    expect(JSON.stringify(body)).not.toContain(PAT_VALUE);
  }, 30000);

  it('JWT signature invalid (signed with a different key): 401', async () => {
    const repoUrl = 'https://github.com/org/repo';
    const { sessionId } = await seedRepoBinding({ repoUrl });
    // Fresh keypair the running app does NOT trust.
    const { privateKey } = generateKeyPairSync('rsa', { modulusLength: 2048 });
    const otherPem = privateKey.export({ format: 'pem', type: 'pkcs8' }) as string;
    const rogueMinter = new SessionJwtMinter({
      privateKeyPem: otherPem,
      issuer: 'orca-registry',
      audience: 'ai-gateway',
      ttlSecs: 300,
    });
    const token = await mintGitCredsToken({
      sessionId,
      repoUrls: [repoUrl],
      minter: rogueMinter,
    });

    const res = await fetch(`${baseURL}/v1/git-creds`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${token}`,
        'content-type': 'application/json',
      },
      body: JSON.stringify({ protocol: 'https', host: 'github.com', path: '/org/repo' }),
    });
    expect(res.status).toBe(401);
  }, 30000);

  it('JWT expired: 401', async () => {
    const repoUrl = 'https://github.com/org/repo';
    const { sessionId } = await seedRepoBinding({ repoUrl });
    // 1s TTL so we can sleep past it deterministically.
    const token = await mintGitCredsToken({
      sessionId,
      repoUrls: [repoUrl],
      ttlSecs: 1,
    });
    await new Promise((r) => setTimeout(r, 1500));

    const res = await fetch(`${baseURL}/v1/git-creds`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${token}`,
        'content-type': 'application/json',
      },
      body: JSON.stringify({ protocol: 'https', host: 'github.com', path: '/org/repo' }),
    });
    expect(res.status).toBe(401);
  }, 30000);

  it('JWT wrong audience (default audience): 401 — no cross-audience replay', async () => {
    const repoUrl = 'https://github.com/org/repo';
    const { sessionId } = await seedRepoBinding({ repoUrl });
    // Default audience for buildTestJwtMinter is 'ai-gateway'.
    const token = await mintGitCredsToken({
      sessionId,
      repoUrls: [repoUrl],
      audience: 'ai-gateway',
    });

    const res = await fetch(`${baseURL}/v1/git-creds`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${token}`,
        'content-type': 'application/json',
      },
      body: JSON.stringify({ protocol: 'https', host: 'github.com', path: '/org/repo' }),
    });
    expect(res.status).toBe(401);
  }, 30000);

  it('repo URL not in JWT claims: 404', async () => {
    const repoUrl = 'https://github.com/org/repo';
    const { sessionId } = await seedRepoBinding({ repoUrl });
    // JWT pins a different repo; the request is for `other/repo`.
    const token = await mintGitCredsToken({
      sessionId,
      repoUrls: ['https://github.com/x/y'],
    });

    const res = await fetch(`${baseURL}/v1/git-creds`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${token}`,
        'content-type': 'application/json',
      },
      body: JSON.stringify({ protocol: 'https', host: 'github.com', path: '/other/repo' }),
    });
    expect(res.status).toBe(404);
  }, 30000);

  it('git credential repo_url mismatch: 403', async () => {
    // The session resource binds to repoUrl; the JWT pins repoUrl; but the
    // git credential's repo_url points at a different repo. The route MUST
    // refuse even though the helper's request URL is in the JWT — credential
    // rebinding post-spawn must not silently broaden access.
    const repoUrl = 'https://github.com/org/repo';
    const { sessionId, gitCredentialId } = await seedRepoBinding({
      repoUrl,
      credentialRepoUrl: 'https://github.com/different/repo',
    });
    const token = await mintGitCredsToken({ sessionId, repoUrls: [repoUrl] });

    const res = await fetch(`${baseURL}/v1/git-creds`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${token}`,
        'content-type': 'application/json',
      },
      body: JSON.stringify({ protocol: 'https', host: 'github.com', path: '/org/repo' }),
    });
    expect(res.status).toBe(403);

    const internalRoute =
      `${baseURL}/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}` +
      `/git-credentials/${gitCredentialId}/resolve`;
    expect((await fetch(internalRoute, { method: 'POST', body: '{}' })).status).toBe(404);
  }, 30000);

  it('resource-owned credential bound to another resource: 404', async () => {
    const repoUrl = 'https://github.com/org/repo';
    const { sessionId, gitCredentialId } = await seedRepoBinding({
      repoUrl,
      credentialSessionResourceId: newId('sesrsc'),
    });
    const token = await mintGitCredsToken({ sessionId, repoUrls: [repoUrl] });

    const res = await fetch(`${baseURL}/v1/git-creds`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${token}`,
        'content-type': 'application/json',
      },
      body: JSON.stringify({ protocol: 'https', host: 'github.com', path: '/org/repo' }),
    });
    expect(res.status).toBe(404);

    const internalRoute =
      `${baseURL}/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}` +
      `/git-credentials/${gitCredentialId}/resolve`;
    expect((await fetch(internalRoute, { method: 'POST', body: '{}' })).status).toBe(404);
  }, 30000);

  it('archived or terminated session cannot resolve a still-valid helper JWT: 404', async () => {
    const repoUrl = 'https://github.com/org/repo';
    const { sessionId } = await seedRepoBinding({ repoUrl });
    const token = await mintGitCredsToken({ sessionId, repoUrls: [repoUrl] });
    const { db } = await getTestDb();
    await db.update(sessions).set({ archivedAt: new Date() }).where(eq(sessions.id, sessionId));

    const res = await fetch(`${baseURL}/v1/git-creds`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${token}`,
        'content-type': 'application/json',
      },
      body: JSON.stringify({ protocol: 'https', host: 'github.com', path: '/org/repo' }),
    });
    expect(res.status).toBe(404);

    await db
      .update(sessions)
      .set({ archivedAt: null, status: 'terminated' })
      .where(eq(sessions.id, sessionId));
    const terminatedRes = await fetch(`${baseURL}/v1/git-creds`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${token}`,
        'content-type': 'application/json',
      },
      body: JSON.stringify({ protocol: 'https', host: 'github.com', path: '/org/repo' }),
    });
    expect(terminatedRes.status).toBe(404);

    await db.update(sessions).set({ status: 'idle' }).where(eq(sessions.id, sessionId));
    await db
      .update(workspaces)
      .set({ status: 'archived', archivedAt: new Date(), updatedAt: new Date() })
      .where(eq(workspaces.id, workspaceId));
    try {
      const archivedWorkspaceRes = await fetch(`${baseURL}/v1/git-creds`, {
        method: 'POST',
        headers: {
          authorization: `Bearer ${token}`,
          'content-type': 'application/json',
        },
        body: JSON.stringify({ protocol: 'https', host: 'github.com', path: '/org/repo' }),
      });
      expect(archivedWorkspaceRes.status).toBe(404);
    } finally {
      await db.transaction(async (tx) => {
        await tx
          .update(workspaces)
          .set({ status: 'active', archivedAt: null, updatedAt: new Date() })
          .where(eq(workspaces.id, workspaceId));
        await tx
          .delete(agentObservabilityWorkspaceArchiveRevocations)
          .where(eq(agentObservabilityWorkspaceArchiveRevocations.workspaceId, workspaceId));
      });
    }
  }, 30000);

  it('internal resolver rejects a terminated session', async () => {
    const repoUrl = 'https://github.com/org/repo';
    const { sessionId, gitCredentialId } = await seedRepoBinding({ repoUrl });
    const route =
      `${baseURL}/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}` +
      `/git-credentials/${gitCredentialId}/resolve`;

    expect((await fetch(route, { method: 'POST', body: '{}' })).status).toBe(200);
    const { db } = await getTestDb();
    await db.update(sessions).set({ status: 'terminated' }).where(eq(sessions.id, sessionId));
    expect((await fetch(route, { method: 'POST', body: '{}' })).status).toBe(404);
  }, 30000);

  it('Authorization header missing: 401', async () => {
    const res = await fetch(`${baseURL}/v1/git-creds`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ protocol: 'https', host: 'github.com', path: '/org/repo' }),
    });
    expect(res.status).toBe(401);
  }, 30000);

  it('body missing required fields: 400', async () => {
    const repoUrl = 'https://github.com/org/repo';
    const { sessionId } = await seedRepoBinding({ repoUrl });
    const token = await mintGitCredsToken({ sessionId, repoUrls: [repoUrl] });

    const res = await fetch(`${baseURL}/v1/git-creds`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${token}`,
        'content-type': 'application/json',
      },
      // Missing host.
      body: JSON.stringify({ protocol: 'https' }),
    });
    expect(res.status).toBe(400);
  }, 30000);
});
