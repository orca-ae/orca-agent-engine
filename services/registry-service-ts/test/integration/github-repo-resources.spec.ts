// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomUUID } from 'node:crypto';
import { describe, it, expect, beforeAll, afterAll, vi } from 'vitest';
import type { FastifyInstance } from 'fastify';
import type { AddressInfo } from 'node:net';
import { eq } from 'drizzle-orm';
import {
  getTestDb,
  closeTestDb,
  buildStubStore,
  STUB_SSE_CONFIG,
  buildTestJwtMinter,
  buildStubFileStore,
} from './setup.js';
import {
  createTestAgent,
  createTestApiKey,
  createTestEnvironment,
  createTestWorkspace,
  uniqueWorkspace,
} from './fixtures.js';
import { buildCombinedTestApp } from '../../src/server.js';
import { newId } from '../../src/domain/versioning.js';
import {
  managedGitCredentialSecretRef,
  reconcileGitCredentialStagingIntents,
} from '../../src/domain/git-credentials.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import {
  gitCredentialStagingIntents,
  gitCredentials,
  idempotencyKeys,
  sessionResources,
} from '../../src/persistence/postgres/schema.js';
import { LocalSecretStore } from '../../src/secrets/index.js';

interface GithubRepositoryResourceOut {
  id: string;
  type: 'github_repository';
  url: string;
  mount_path: string;
  checkout: Record<string, unknown> | null;
  created_at: string;
  updated_at: string;
}

interface SessionOut {
  id: string;
  resources: Array<GithubRepositoryResourceOut | { id: string; type: string }>;
}

interface ClaudeErrorOut {
  type: 'error';
  error: { type: string; message: string };
  request_id: string | null;
}

function expectCreateSessionError(body: unknown, message: string | RegExp): void {
  const error = body as ClaudeErrorOut;
  expect(error.type).toBe('error');
  expect(error.error.type).toBe('invalid_request_error');
  if (typeof message === 'string') {
    expect(error.error.message).toBe(message);
  } else {
    expect(error.error.message).toMatch(message);
  }
  expect(error.request_id).toEqual(expect.any(String));
}

describe('session resources: github_repository (integration)', () => {
  let app: FastifyInstance;
  let baseURL: string;
  let db: DbClient;
  let secretStore: LocalSecretStore;

  beforeAll(async () => {
    ({ db } = await getTestDb());
    secretStore = new LocalSecretStore();
    app = buildCombinedTestApp({
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store: buildStubStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
      secretStore,
    });
    await app.listen({ host: '127.0.0.1', port: 0 });
    baseURL = `http://127.0.0.1:${(app.server.address() as AddressInfo).port}`;
  }, 60_000);

  afterAll(async () => {
    if (app) await app.close();
    await closeTestDb();
  });

  async function createGitCredential(opts: {
    workspaceId: string;
    repoUrl: string;
    archived?: boolean;
  }): Promise<string> {
    const id = newId('gitcred');
    const now = new Date();
    await db.insert(gitCredentials).values({
      id,
      workspaceId: opts.workspaceId,
      provider: 'github',
      repoUrl: opts.repoUrl,
      secretRef: 'env:GITHUB_PAT_PLACEHOLDER',
      sessionResourceId: null,
      metadata: {},
      archivedAt: opts.archived ? now : null,
      createdAt: now,
      updatedAt: now,
    });
    return id;
  }

  async function buildWorkspace(prefix: string): Promise<{
    workspaceId: string;
    apiKey: string;
    agentId: string;
  }> {
    const workspaceId = uniqueWorkspace(prefix);
    const apiKey = await createTestApiKey(db, workspaceId);
    const agentId = await createTestAgent(baseURL, apiKey);
    return { workspaceId, apiKey, agentId };
  }

  async function createRepoSession(input: {
    apiKey: string;
    agentId: string;
    repoUrl: string;
    token: string;
    mountPath?: string;
  }): Promise<{ response: Response; body: SessionOut }> {
    const environmentId = await createTestEnvironment(baseURL, input.apiKey);
    const response = await fetch(`${baseURL}/v1/sessions`, {
      method: 'POST',
      headers: { 'x-api-key': input.apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        environment_id: environmentId,
        agent_id: input.agentId,
        resources: [
          {
            type: 'github_repository',
            url: input.repoUrl,
            authorization_token: input.token,
            ...(input.mountPath ? { mount_path: input.mountPath } : {}),
            checkout: { type: 'branch', name: 'main' },
          },
        ],
      }),
    });
    return { response, body: (await response.json()) as SessionOut };
  }

  function githubResource(session: SessionOut): GithubRepositoryResourceOut {
    return session.resources.find(
      (resource): resource is GithubRepositoryResourceOut => resource.type === 'github_repository',
    )!;
  }

  async function persistedCredential(resourceId: string) {
    const resourceRows = await db
      .select()
      .from(sessionResources)
      .where(eq(sessionResources.id, resourceId))
      .limit(1);
    const repoRef = resourceRows[0]!.repoRef as { git_credential_id: string; url: string };
    const credentialRows = await db
      .select()
      .from(gitCredentials)
      .where(eq(gitCredentials.id, repoRef.git_credential_id))
      .limit(1);
    return { resource: resourceRows[0]!, repoRef, credential: credentialRows[0]! };
  }

  it('accepts a raw PAT, stores only a resource-owned secret reference, and never echoes it', async () => {
    const { workspaceId, apiKey, agentId } = await buildWorkspace('ghraw');
    const repoUrl = `https://github.com/org/raw-${randomUUID()}`;
    const token = `ghp_${randomUUID()}`;

    const { response, body } = await createRepoSession({
      apiKey,
      agentId,
      repoUrl,
      token,
      mountPath: '/custom/path/',
    });
    expect(response.status).toBe(200);
    const repo = githubResource(body);
    expect(repo).toMatchObject({
      type: 'github_repository',
      url: repoUrl,
      mount_path: '/custom/path/',
      checkout: { type: 'branch', name: 'main' },
    });
    expect(repo.created_at).toBeTruthy();
    expect(repo.updated_at).toBe(repo.created_at);
    expect(repo).not.toHaveProperty('authorization_token');
    expect(repo).not.toHaveProperty('repo_ref');
    expect(JSON.stringify(body)).not.toContain(token);
    expect(JSON.stringify(body)).not.toContain('gitcred_');

    const persisted = await persistedCredential(repo.id);
    expect(persisted.repoRef.url).toBe(repoUrl);
    expect(persisted.credential.workspaceId).toBe(workspaceId);
    expect(persisted.credential.sessionResourceId).toBe(repo.id);
    expect(persisted.credential.secretRef).not.toContain(token);
    expect(await secretStore.resolve(persisted.credential.secretRef)).toBe(token);
    const stagingRows = await db
      .select()
      .from(gitCredentialStagingIntents)
      .where(eq(gitCredentialStagingIntents.workspaceId, workspaceId));
    expect(stagingRows).toHaveLength(0);

    const get = await fetch(`${baseURL}/v1/sessions/${body.id}`, {
      headers: { 'x-api-key': apiKey },
    });
    expect(get.status).toBe(200);
    const fetchedText = await get.text();
    expect(fetchedText).not.toContain(token);
    expect(fetchedText).not.toContain(persisted.credential.id);
    expect(githubResource(JSON.parse(fetchedText) as SessionOut).url).toBe(repoUrl);
  }, 30_000);

  it('rejects plaintext repository URLs before persisting a raw token', async () => {
    const { workspaceId, apiKey, agentId } = await buildWorkspace('ghhttp');
    const token = `ghp_${randomUUID()}`;
    const { response, body } = await createRepoSession({
      apiKey,
      agentId,
      repoUrl: `http://github.com/org/plaintext-${randomUUID()}`,
      token,
    });

    expect(response.status).toBe(400);
    expectCreateSessionError(body, 'github_repository url must use https');
    expect(JSON.stringify(body)).not.toContain(token);

    const rows = await db
      .select()
      .from(gitCredentials)
      .where(eq(gitCredentials.workspaceId, workspaceId));
    expect(rows).toHaveLength(0);
  }, 30_000);

  it('accepts GitHub App tokens and defaults mount_path while stripping .git', async () => {
    const { apiKey, agentId } = await buildWorkspace('ghapp');
    const repoUrl = `https://github.com/org/app-${randomUUID()}.git`;
    const { response, body } = await createRepoSession({
      apiKey,
      agentId,
      repoUrl,
      token: `ghs_${randomUUID()}`,
    });
    expect(response.status).toBe(200);
    expect(githubResource(body).mount_path).toMatch(/^\/workspace\/app-.*\/$/);
  }, 30_000);

  it('keeps git_cred:// as a sanitized Orca extension for pre-provisioned credentials', async () => {
    const { workspaceId, apiKey, agentId } = await buildWorkspace('ghref');
    const repoUrl = `https://github.com/org/ref-${randomUUID()}`;
    const gitCredentialId = await createGitCredential({ workspaceId, repoUrl });

    const { response, body } = await createRepoSession({
      apiKey,
      agentId,
      repoUrl,
      token: `git_cred://${gitCredentialId}`,
    });
    expect(response.status).toBe(200);
    const repo = githubResource(body);
    expect(repo.url).toBe(repoUrl);
    expect(repo).not.toHaveProperty('repo_ref');
    const persisted = await persistedCredential(repo.id);
    expect(persisted.repoRef.git_credential_id).toBe(gitCredentialId);
    expect(persisted.credential.sessionResourceId).toBeNull();
  }, 30_000);

  it('rejects cross-workspace, archived, and URL-mismatched git_cred references', async () => {
    const workspaceA = uniqueWorkspace('ghref-a');
    const { workspaceId: workspaceB, apiKey, agentId } = await buildWorkspace('ghref-b');
    await createTestWorkspace(db, workspaceA);
    const repoUrl = `https://github.com/org/ref-errors-${randomUUID()}`;
    const crossWorkspaceId = await createGitCredential({ workspaceId: workspaceA, repoUrl });
    const archivedId = await createGitCredential({
      workspaceId: workspaceB,
      repoUrl: `${repoUrl}-archived`,
      archived: true,
    });
    const mismatchId = await createGitCredential({
      workspaceId: workspaceB,
      repoUrl: `${repoUrl}-other`,
    });

    for (const [credentialId, expected] of [
      [crossWorkspaceId, `git credential ${crossWorkspaceId} not found in workspace`],
      [archivedId, `git credential ${archivedId} is archived`],
      [
        mismatchId,
        `git credential repo_url ${repoUrl}-other does not match resource.url ${repoUrl}`,
      ],
    ] as const) {
      const { response, body } = await createRepoSession({
        apiKey,
        agentId,
        repoUrl,
        token: `git_cred://${credentialId}`,
      });
      expect(response.status).toBe(400);
      expectCreateSessionError(body, expected);
    }

    const hostWide = await createRepoSession({
      apiKey,
      agentId,
      repoUrl: 'https://github.com',
      token: `ghp_host_wide_${randomUUID()}`,
    });
    expect(hostWide.response.status).toBe(400);
    expectCreateSessionError(hostWide.body, /owner and repository/);
  }, 60_000);

  it('allows independent resource-owned tokens for the same repository', async () => {
    const { workspaceId, apiKey, agentId } = await buildWorkspace('ghsame');
    const repoUrl = `https://github.com/org/shared-${randomUUID()}`;
    const first = await createRepoSession({
      apiKey,
      agentId,
      repoUrl,
      token: `ghp_first_${randomUUID()}`,
    });
    const secondToken = `ghp_second_${randomUUID()}`;
    const second = await createRepoSession({ apiKey, agentId, repoUrl, token: secondToken });
    expect(first.response.status).toBe(200);
    expect(second.response.status).toBe(200);

    const rows = await db
      .select()
      .from(gitCredentials)
      .where(eq(gitCredentials.workspaceId, workspaceId));
    const owned = rows.filter((row) => row.repoUrl === repoUrl && row.sessionResourceId !== null);
    expect(owned).toHaveLength(2);
    expect(
      await secretStore.resolve(
        owned.find((row) => row.sessionResourceId === githubResource(second.body).id)!.secretRef,
      ),
    ).toBe(secondToken);
  }, 30_000);

  it('rejects duplicate repository URLs in one session before storing either token', async () => {
    const { workspaceId, apiKey, agentId } = await buildWorkspace('ghduplicate-create');
    const environmentId = await createTestEnvironment(baseURL, apiKey);
    const repoUrl = `https://github.com/org/duplicate-${randomUUID()}`;
    const firstToken = `ghp_first_${randomUUID()}`;
    const secondToken = `ghp_second_${randomUUID()}`;

    const response = await fetch(`${baseURL}/v1/sessions`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        environment_id: environmentId,
        agent_id: agentId,
        resources: [
          {
            type: 'github_repository',
            url: repoUrl,
            mount_path: '/workspace/first/',
            authorization_token: firstToken,
          },
          {
            type: 'github_repository',
            url: `${repoUrl}.git/`,
            mount_path: '/workspace/second/',
            authorization_token: secondToken,
          },
        ],
      }),
    });

    expect(response.status).toBe(400);
    const text = await response.text();
    expect(text).toContain(
      'session already has an active github_repository resource for this repository URL',
    );
    expect(text).not.toContain(firstToken);
    expect(text).not.toContain(secondToken);
    const rows = await db
      .select()
      .from(gitCredentials)
      .where(eq(gitCredentials.workspaceId, workspaceId));
    expect(rows).toHaveLength(0);
  }, 30_000);

  it('serializes concurrent attaches and purges the rejected duplicate token', async () => {
    const { workspaceId, apiKey, agentId } = await buildWorkspace('ghduplicate-attach');
    const environmentId = await createTestEnvironment(baseURL, apiKey);
    const create = await fetch(`${baseURL}/v1/sessions`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ agent_id: agentId, environment_id: environmentId }),
    });
    expect(create.status).toBe(200);
    const session = (await create.json()) as SessionOut;
    const repoUrl = `https://github.com/org/concurrent-${randomUUID()}`;
    const tokens = [`ghp_first_${randomUUID()}`, `ghp_second_${randomUUID()}`];
    // Hold both secret writes before either request can enter the row-locked
    // transaction, so this deterministically exercises rejected-secret cleanup.
    const originalPut = secretStore.put.bind(secretStore);
    let releasePuts!: () => void;
    const bothPutsStarted = new Promise<void>((resolve) => {
      releasePuts = resolve;
    });
    let putCount = 0;
    const putSpy = vi.spyOn(secretStore, 'put').mockImplementation(async (reference, value) => {
      putCount += 1;
      if (putCount === tokens.length) releasePuts();
      await bothPutsStarted;
      return originalPut(reference, value);
    });
    const deleteSpy = vi.spyOn(secretStore, 'delete');

    try {
      const attach = (index: number) =>
        fetch(`${baseURL}/v1/sessions/${session.id}/resources`, {
          method: 'POST',
          headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
          body: JSON.stringify({
            type: 'github_repository',
            url: index === 0 ? repoUrl : `${repoUrl}.git`,
            mount_path: `/workspace/repo-${index}/`,
            authorization_token: tokens[index],
          }),
        });

      const responses = await Promise.all([attach(0), attach(1)]);
      expect(responses.map((response) => response.status).sort()).toEqual([200, 400]);
      const rejected = responses.find((response) => response.status === 400)!;
      expectCreateSessionError(
        await rejected.json(),
        'session already has an active github_repository resource for this repository URL',
      );

      const rows = await db
        .select()
        .from(gitCredentials)
        .where(eq(gitCredentials.workspaceId, workspaceId));
      expect(rows).toHaveLength(1);
      const storedToken = await secretStore.resolve(rows[0]!.secretRef);
      expect(tokens).toContain(storedToken);
      const rejectedToken = tokens.find((token) => token !== storedToken)!;
      const rejectedRef = putSpy.mock.calls.find(([, token]) => token === rejectedToken)?.[0];
      expect(rejectedRef).toBeDefined();
      expect(deleteSpy).toHaveBeenCalledWith(rejectedRef);
      expect(await secretStore.resolve(rejectedRef!)).toBeNull();
      const stagingRows = await db
        .select()
        .from(gitCredentialStagingIntents)
        .where(eq(gitCredentialStagingIntents.workspaceId, workspaceId));
      expect(stagingRows).toHaveLength(0);
    } finally {
      putSpy.mockRestore();
      deleteSpy.mockRestore();
    }
  }, 60_000);

  it('accepts raw tokens on attach and enforces the 8-repository cap', async () => {
    const { workspaceId, apiKey, agentId } = await buildWorkspace('ghattach');
    const environmentId = await createTestEnvironment(baseURL, apiKey);
    const create = await fetch(`${baseURL}/v1/sessions`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ agent_id: agentId, environment_id: environmentId }),
    });
    expect(create.status).toBe(200);
    const session = (await create.json()) as SessionOut;

    const now = new Date();
    await db.insert(sessionResources).values(
      Array.from({ length: 7 }, (_, index) => ({
        id: newId('sesrsc'),
        workspaceId,
        sessionId: session.id,
        type: 'github_repository',
        fileId: null,
        memoryStoreId: null,
        repoRef: {
          git_credential_id: `gitcred_fixture_${index}_${randomUUID()}`,
          url: `https://github.com/org/fixture-${index}-${randomUUID()}`,
        },
        mountPath: `/workspace/fixture-${index}/`,
        access: 'read_write',
        mountStrategy: null,
        instructions: null,
        attachedAt: now,
        updatedAt: now,
        detachedAt: null,
      })),
    );

    const attach = (suffix: string) =>
      fetch(`${baseURL}/v1/sessions/${session.id}/resources`, {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({
          type: 'github_repository',
          url: `https://github.com/org/attach-${suffix}-${randomUUID()}`,
          authorization_token: `ghp_attach_${suffix}_${randomUUID()}`,
        }),
      });

    const concurrent = await Promise.all([attach('a'), attach('b')]);
    expect(concurrent.map((response) => response.status).sort()).toEqual([200, 400]);
    const successful = concurrent.find((response) => response.status === 200)!;
    const rejected = concurrent.find((response) => response.status === 400)!;
    const resource = (await successful.json()) as GithubRepositoryResourceOut;
    expect(resource.type).toBe('github_repository');
    expect(resource).not.toHaveProperty('authorization_token');
    expect(resource).not.toHaveProperty('repo_ref');
    expectCreateSessionError(await rejected.json(), /8.*github_repository|github_repository.*8/);
  }, 60_000);

  it('rotates raw tokens atomically and makes idempotent retries side-effect free', async () => {
    const { apiKey, agentId } = await buildWorkspace('ghrotate');
    const repoUrl = `https://github.com/org/rotate-${randomUUID()}`;
    const firstToken = `ghp_old_${randomUUID()}`;
    const created = await createRepoSession({ apiKey, agentId, repoUrl, token: firstToken });
    expect(created.response.status).toBe(200);
    const repo = githubResource(created.body);
    const before = await persistedCredential(repo.id);
    const nextToken = `ghs_new_${randomUUID()}`;
    const idempotencyKey = `rotate-${randomUUID()}`;

    const rotate = () =>
      fetch(`${baseURL}/v1/sessions/${created.body.id}/resources/${repo.id}`, {
        method: 'POST',
        headers: {
          'x-api-key': apiKey,
          'content-type': 'application/json',
          'idempotency-key': idempotencyKey,
        },
        body: JSON.stringify({ authorization_token: nextToken }),
      });

    const firstResponse = await rotate();
    expect(firstResponse.status).toBe(200);
    const firstBodyText = await firstResponse.text();
    expect(firstBodyText).not.toContain(nextToken);
    expect(firstBodyText).not.toContain('gitcred_');
    const rotated = JSON.parse(firstBodyText) as GithubRepositoryResourceOut;
    expect(rotated.url).toBe(repoUrl);
    expect(new Date(rotated.updated_at).getTime()).toBeGreaterThanOrEqual(
      new Date(rotated.created_at).getTime(),
    );
    const idempotencyRows = await db
      .select()
      .from(idempotencyKeys)
      .where(eq(idempotencyKeys.key, idempotencyKey));
    expect(idempotencyRows).toHaveLength(1);
    expect(idempotencyRows[0]!.responseBody).toBe(firstBodyText);
    expect(idempotencyRows[0]!.responseBody).not.toContain(nextToken);
    expect(idempotencyRows[0]!.responseBody).not.toContain('gitcred_');

    const after = await persistedCredential(repo.id);
    expect(after.credential.id).not.toBe(before.credential.id);
    expect(await secretStore.resolve(after.credential.secretRef)).toBe(nextToken);
    expect(await secretStore.resolve(before.credential.secretRef)).toBeNull();
    const oldRows = await db
      .select()
      .from(gitCredentials)
      .where(eq(gitCredentials.id, before.credential.id));
    expect(oldRows).toHaveLength(1);
    expect(oldRows[0]!.deletedAt).toBeInstanceOf(Date);

    const replay = await rotate();
    expect(replay.status).toBe(200);
    expect(await replay.text()).toBe(firstBodyText);
    const ownedRows = await db
      .select()
      .from(gitCredentials)
      .where(eq(gitCredentials.sessionResourceId, repo.id));
    expect(ownedRows).toHaveLength(2);
    expect(ownedRows.filter((row) => row.deletedAt === null).map((row) => row.id)).toEqual([
      after.credential.id,
    ]);
    expect(ownedRows.find((row) => row.id === before.credential.id)?.deletedAt).toEqual(
      oldRows[0]!.deletedAt,
    );
    // Releasing the tombstone's key must not permit two undeleted owners.
    await expect(
      db.insert(gitCredentials).values({ ...after.credential, id: newId('gitcred') }),
    ).rejects.toThrow();

    const conflictToken = `ghp_conflict_${randomUUID()}`;
    const conflict = await fetch(`${baseURL}/v1/sessions/${created.body.id}/resources/${repo.id}`, {
      method: 'POST',
      headers: {
        'x-api-key': apiKey,
        'content-type': 'application/json',
        'idempotency-key': idempotencyKey,
      },
      body: JSON.stringify({ authorization_token: conflictToken }),
    });
    expect(conflict.status).toBe(409);
    expect(await secretStore.resolve(after.credential.secretRef)).toBe(nextToken);
    expect(JSON.stringify(await conflict.json())).not.toContain(conflictToken);
  }, 90_000);

  it('rebinds a raw-token resource to a pre-provisioned credential and purges the old secret', async () => {
    const { workspaceId, apiKey, agentId } = await buildWorkspace('ghrebind');
    const repoUrl = `https://github.com/org/rebind-${randomUUID()}`;
    const created = await createRepoSession({
      apiKey,
      agentId,
      repoUrl,
      token: `ghp_owned_${randomUUID()}`,
    });
    const repo = githubResource(created.body);
    const before = await persistedCredential(repo.id);
    const sharedCredentialId = await createGitCredential({ workspaceId, repoUrl });

    const response = await fetch(`${baseURL}/v1/sessions/${created.body.id}/resources/${repo.id}`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ authorization_token: `git_cred://${sharedCredentialId}` }),
    });
    expect(response.status).toBe(200);
    const after = await persistedCredential(repo.id);
    expect(after.repoRef.git_credential_id).toBe(sharedCredentialId);
    expect(after.credential.sessionResourceId).toBeNull();
    expect(await secretStore.resolve(before.credential.secretRef)).toBeNull();
  }, 30_000);

  it('soft-deletes resource-owned credentials and purges their secrets when detached', async () => {
    const { apiKey, agentId } = await buildWorkspace('ghdetach');
    const created = await createRepoSession({
      apiKey,
      agentId,
      repoUrl: `https://github.com/org/detach-${randomUUID()}`,
      token: `ghp_detach_${randomUUID()}`,
    });
    const repo = githubResource(created.body);
    const before = await persistedCredential(repo.id);

    const response = await fetch(`${baseURL}/v1/sessions/${created.body.id}/resources/${repo.id}`, {
      method: 'DELETE',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({}),
    });
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      id: repo.id,
      type: 'session_resource_deleted',
    });
    expect(await secretStore.resolve(before.credential.secretRef)).toBeNull();
    const rows = await db
      .select()
      .from(gitCredentials)
      .where(eq(gitCredentials.id, before.credential.id));
    expect(rows).toHaveLength(1);
    expect(rows[0]!.deletedAt).toBeInstanceOf(Date);
  }, 30_000);

  it('archives resource-owned credentials and purges their tokens with the session', async () => {
    const { apiKey, agentId } = await buildWorkspace('gharchive');
    const created = await createRepoSession({
      apiKey,
      agentId,
      repoUrl: `https://github.com/org/archive-${randomUUID()}`,
      token: `ghp_archive_${randomUUID()}`,
    });
    const repo = githubResource(created.body);
    const before = await persistedCredential(repo.id);

    const response = await fetch(`${baseURL}/v1/sessions/${created.body.id}/archive`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({}),
    });
    expect(response.status).toBe(200);
    expect(await secretStore.resolve(before.credential.secretRef)).toBeNull();
    const rows = await db
      .select()
      .from(gitCredentials)
      .where(eq(gitCredentials.id, before.credential.id));
    expect(rows).toHaveLength(1);
    expect(rows[0]!.archivedAt).not.toBeNull();
  }, 60_000);

  it('reconciles expired credential staging intents and preserves committed credentials', async () => {
    const workspaceId = uniqueWorkspace('ghstaging-reconcile');
    await createTestWorkspace(db, workspaceId);
    const now = new Date();
    const expiredAt = new Date(now.getTime() - 20 * 60 * 1_000);
    const freshCleanupAt = new Date(now.getTime() + 20 * 60 * 1_000);
    const orphanId = newId('gitcred');
    const activeId = newId('gitcred');
    const retryId = newId('gitcred');
    const freshId = newId('gitcred');
    const orphanRef = managedGitCredentialSecretRef(workspaceId, orphanId);
    const activeRef = managedGitCredentialSecretRef(workspaceId, activeId);
    const retryRef = managedGitCredentialSecretRef(workspaceId, retryId);
    const freshRef = managedGitCredentialSecretRef(workspaceId, freshId);

    await secretStore.put(orphanRef, `ghp_orphan_${randomUUID()}`);
    await secretStore.put(activeRef, `ghp_active_${randomUUID()}`);
    await secretStore.put(retryRef, `ghp_retry_${randomUUID()}`);
    await secretStore.put(freshRef, `ghp_fresh_${randomUUID()}`);
    await db.insert(gitCredentialStagingIntents).values([
      {
        credentialId: orphanId,
        workspaceId,
        sessionResourceId: newId('sesrsc'),
        secretRef: orphanRef,
        status: 'pending',
        cleanupAfter: expiredAt,
        createdAt: expiredAt,
        updatedAt: expiredAt,
      },
      {
        credentialId: activeId,
        workspaceId,
        sessionResourceId: newId('sesrsc'),
        secretRef: activeRef,
        status: 'pending',
        cleanupAfter: expiredAt,
        createdAt: expiredAt,
        updatedAt: expiredAt,
      },
      {
        credentialId: retryId,
        workspaceId,
        sessionResourceId: newId('sesrsc'),
        secretRef: retryRef,
        status: 'cleaning',
        cleanupAfter: expiredAt,
        createdAt: expiredAt,
        updatedAt: expiredAt,
      },
      {
        credentialId: freshId,
        workspaceId,
        sessionResourceId: newId('sesrsc'),
        secretRef: freshRef,
        status: 'pending',
        cleanupAfter: freshCleanupAt,
        createdAt: now,
        updatedAt: now,
      },
    ]);
    await db.insert(gitCredentials).values({
      id: activeId,
      workspaceId,
      provider: 'github',
      repoUrl: `https://github.com/org/active-${randomUUID()}`,
      secretRef: activeRef,
      sessionResourceId: null,
      metadata: { source: 'session_resource' },
      archivedAt: null,
      createdAt: now,
      updatedAt: now,
    });

    const result = await reconcileGitCredentialStagingIntents(db, secretStore, {
      now,
      batchSize: 10,
      cleaningRetryMs: 5 * 60 * 1_000,
    });

    expect(result).toEqual({ processed: 3, purged: 2, preserved: 1, failed: 0 });
    expect(await secretStore.resolve(orphanRef)).toBeNull();
    expect(await secretStore.resolve(retryRef)).toBeNull();
    expect(await secretStore.resolve(activeRef)).not.toBeNull();
    expect(await secretStore.resolve(freshRef)).not.toBeNull();
    const remaining = await db
      .select()
      .from(gitCredentialStagingIntents)
      .where(eq(gitCredentialStagingIntents.workspaceId, workspaceId));
    expect(remaining.map((row) => row.credentialId)).toEqual([freshId]);
  }, 30_000);

  it('returns 503 without persisting a credential when raw token storage is unavailable', async () => {
    const noStoreApp = buildCombinedTestApp({
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store: buildStubStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
    });
    await noStoreApp.listen({ host: '127.0.0.1', port: 0 });
    try {
      const noStoreBase = `http://127.0.0.1:${(noStoreApp.server.address() as AddressInfo).port}`;
      const workspaceId = uniqueWorkspace('ghnostore');
      const apiKey = await createTestApiKey(db, workspaceId);
      const agentId = await createTestAgent(noStoreBase, apiKey);
      const environmentId = await createTestEnvironment(noStoreBase, apiKey);
      const token = `ghp_unavailable_${randomUUID()}`;
      const response = await fetch(`${noStoreBase}/v1/sessions`, {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({
          environment_id: environmentId,
          agent_id: agentId,
          resources: [
            {
              type: 'github_repository',
              url: `https://github.com/org/no-store-${randomUUID()}`,
              authorization_token: token,
            },
          ],
        }),
      });
      expect(response.status).toBe(503);
      const error = (await response.json()) as ClaudeErrorOut;
      expect(error).toMatchObject({
        type: 'error',
        error: {
          type: 'overloaded_error',
          message: 'secret store unavailable',
        },
        request_id: expect.any(String),
      });
      expect(JSON.stringify(error)).not.toContain(token);
      const rows = await db
        .select()
        .from(gitCredentials)
        .where(eq(gitCredentials.workspaceId, workspaceId));
      expect(rows).toHaveLength(0);
    } finally {
      await noStoreApp.close();
    }
  }, 30_000);
});
