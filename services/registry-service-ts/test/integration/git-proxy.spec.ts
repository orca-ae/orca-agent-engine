// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import Fastify, { type FastifyInstance } from 'fastify';
import { execFile, spawn } from 'node:child_process';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { request as httpRequest } from 'node:http';
import { promisify } from 'node:util';
import { gzipSync } from 'node:zlib';
import { eq } from 'drizzle-orm';
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { buildAuth } from '../../src/auth/auth.js';
import { registerGitCredsRoutes } from '../../src/api/git-creds.routes.js';
import { registerGitProxyRoutes } from '../../src/api/git-proxy.routes.js';
import { gitProxyContract } from '../../src/contracts/git-proxy.contract.js';
import { mintGitProxyCapability } from '../../src/domain/git-proxy-capability.js';
import { prepareRunnerGitSnapshot } from '../../src/domain/runner-git-snapshot.js';
import type { GitProxyRequest } from '../../src/domain/git-proxy-transport.js';
import {
  agents,
  agentVersions,
  environments,
  gitCredentials,
  sessionResources,
  sessions,
  workspaces,
} from '../../src/persistence/postgres/schema.js';
import { newId } from '../../src/domain/versioning.js';
import { buildTestJwtMinter, closeTestDb, getTestDb } from './setup.js';
import { createTestWorkspace, uniqueWorkspace } from './fixtures.js';

const execute = promisify(execFile);
const PAT = 'fixture-PAT-never-returned';
const REPO = 'https://github.com/fixture/repo.git';

describe('scoped Git read proxy (integration)', () => {
  const minter = buildTestJwtMinter();
  let app: FastifyInstance;
  let baseUrl: string;
  let directory: string;
  let head: string;
  let workspaceId: string;
  let sessionId: string;
  let resourceId: string;
  let credentialId: string;
  const upstream = vi.fn<GitProxyRequest>();
  const resolveSecret = vi.fn(async () => PAT);
  const clientEncodings: Array<string | undefined> = [];
  const refs = Array.from(
    { length: 100 },
    (_, index) => `refs/heads/feature-${String(index).padStart(3, '0')}`,
  );

  beforeAll(async () => {
    const { db } = await getTestDb();
    directory = await mkdtemp(join(tmpdir(), 'orca-git-proxy-test-'));
    const repo = join(directory, 'repo.git');
    await execute('git', ['init', '--initial-branch=main', repo]);
    await writeFile(join(repo, 'README.md'), 'private fixture\n');
    await execute('git', ['-C', repo, 'add', 'README.md']);
    await execute('git', [
      '-C',
      repo,
      '-c',
      'user.name=Fixture',
      '-c',
      'user.email=fixture@example.com',
      'commit',
      '-m',
      'fixture',
    ]);
    head = (await execute('git', ['-C', repo, 'rev-parse', 'HEAD'])).stdout.trim();
    for (const ref of refs) await execute('git', ['-C', repo, 'update-ref', ref, head]);
    app = Fastify();
    app.addHook('onRequest', async (req) => {
      clientEncodings.push(req.headers['content-encoding']);
    });
    app.addHook('preHandler', buildAuth({ db, oidc: { allowedIssuers: [], audience: 'test' } }));
    const deps = { db, jwtMinter: minter, secretProvider: { resolve: resolveSecret } };
    registerGitProxyRoutes(app, { ...deps, request: upstream });
    registerGitCredsRoutes(app, deps);
    baseUrl = await app.listen({ host: '127.0.0.1', port: 0 });
  }, 30_000);

  afterAll(async () => {
    await app?.close();
    if (directory) await rm(directory, { recursive: true, force: true });
    await closeTestDb();
  });

  beforeEach(async () => {
    upstream.mockReset();
    resolveSecret.mockClear();
    clientEncodings.length = 0;
    upstream.mockResolvedValue(Buffer.from('0000'));
    const { db } = await getTestDb();
    workspaceId = uniqueWorkspace('gitproxy');
    sessionId = newId('ses');
    resourceId = newId('sesrsc');
    credentialId = newId('gitcred');
    const agentId = newId('agt');
    await createTestWorkspace(db, workspaceId);
    await db.insert(agents).values({
      id: agentId,
      workspaceId,
      name: 'git proxy',
      version: 1,
      modelProvider: 'anthropic',
      modelId: 'fixture',
    });
    await db
      .insert(agentVersions)
      .values({ id: newId('agtv'), workspaceId, agentId, version: 1, snapshot: {} });
    await db
      .insert(sessions)
      .values({ id: sessionId, workspaceId, agentId, agentVersion: 1, status: 'idle' });
    await db.insert(sessionResources).values({
      id: resourceId,
      workspaceId,
      sessionId,
      type: 'github_repository',
      mountPath: '/repo',
      access: 'read_only',
      repoRef: { url: REPO, git_credential_id: credentialId },
    });
    await db.insert(gitCredentials).values({
      id: credentialId,
      workspaceId,
      repoUrl: REPO,
      secretRef: 'env:TEST_GIT_PROXY_PAT',
      sessionResourceId: resourceId,
    });
  });

  it('rechecks the Environment host policy before forwarding even a valid Git request', async () => {
    const { db } = await getTestDb();
    const environmentId = newId('env');
    await db.insert(environments).values({
      id: environmentId,
      workspaceId,
      name: 'limited',
      networking: { type: 'limited', allowed_hosts: ['github.com'] },
    });
    await db.update(sessions).set({ environmentId }).where(eq(sessions.id, sessionId));
    const cap = await capability();
    const token = cap.authorizationHeader.slice('Authorization: Bearer '.length);
    expect((await advertise(token)).statusCode).toBe(200);
    upstream.mockClear();
    resolveSecret.mockClear();
    await db
      .update(environments)
      .set({ networking: { type: 'limited', allowed_hosts: [] } })
      .where(eq(environments.id, environmentId));
    expect((await advertise(token)).statusCode).toBe(404);
    const response = await app.inject({
      method: 'POST',
      url: `${cap.remoteUrl}/git-upload-pack`,
      headers: {
        authorization: `Bearer ${token}`,
        'git-protocol': 'version=2',
        'content-type': 'application/x-git-upload-pack-request',
      },
      payload: Buffer.from('0014command=ls-refs\n00010009peel\n0000'),
    });
    expect(response.statusCode).toBe(404);
    expect(upstream).not.toHaveBeenCalled();
    expect(resolveSecret).not.toHaveBeenCalled();
    await expect(capability()).rejects.toThrow('unavailable');
  });

  it.each([false, true])(
    'rejects arbitrary POST bytes before secret resolution (gzip=%s)',
    async (gzip) => {
      const cap = await capability();
      const bytes = Buffer.from('private session data');
      const response = await app.inject({
        method: 'POST',
        url: `${cap.remoteUrl}/git-upload-pack`,
        headers: {
          authorization: cap.authorizationHeader.slice('Authorization: '.length),
          'content-type': 'application/x-git-upload-pack-request',
          ...(gzip ? { 'content-encoding': 'gzip' } : {}),
        },
        payload: gzip ? gzipSync(bytes) : bytes,
      });
      expect(response.statusCode).toBe(400);
      expect(upstream).not.toHaveBeenCalled();
      expect(resolveSecret).not.toHaveBeenCalled();
    },
  );

  async function capability() {
    return mintGitProxyCapability((await getTestDb()).db, minter, {
      registryBaseUrl: baseUrl,
      workspaceId,
      sessionId,
      resourceId,
    });
  }

  async function advertise(token?: string, target = resourceId) {
    const cap =
      token ?? (await capability()).authorizationHeader.slice('Authorization: Bearer '.length);
    return app.inject({
      url: `/v1/git-proxy/${target}/info/refs?service=git-upload-pack`,
      headers: { authorization: `Bearer ${cap}` },
    });
  }

  function useGitBackend() {
    upstream.mockImplementation(async (input) => {
      expect(input.pat).toBe(PAT);
      expect(input.repoUrl).toBe(REPO);
      return new Promise<Buffer>((resolve, reject) => {
        const child = spawn('git', ['http-backend'], {
          env: {
            ...process.env,
            GIT_PROJECT_ROOT: directory,
            GIT_HTTP_EXPORT_ALL: '1',
            REQUEST_METHOD: input.method,
            PATH_INFO: `/repo.git/${input.method === 'GET' ? 'info/refs' : 'git-upload-pack'}`,
            QUERY_STRING: input.method === 'GET' ? 'service=git-upload-pack' : '',
            CONTENT_TYPE: 'application/x-git-upload-pack-request',
            HTTP_GIT_PROTOCOL: input.gitProtocol ?? '',
            REMOTE_USER: 'fixture',
          },
          stdio: ['pipe', 'pipe', 'pipe'],
        });
        const chunks: Buffer[] = [];
        child.stdout.on('data', (bytes: Buffer) => chunks.push(bytes));
        child.on('error', reject);
        child.on('close', (code) => {
          if (code !== 0) return reject(new Error('fixture backend failed'));
          const bytes = Buffer.concat(chunks);
          resolve(bytes.subarray(bytes.indexOf('\r\n\r\n') + 4));
        });
        child.stdin.end(input.body);
      });
    });
  }

  it('supports real Git protocol v2 ls-remote with an upload-pack POST and no PAT in client output', async () => {
    useGitBackend();
    const cap = await capability();
    const result = await execute(
      'git',
      ['-c', 'protocol.version=2', 'ls-remote', cap.remoteUrl, 'HEAD'],
      {
        env: {
          ...process.env,
          GIT_TERMINAL_PROMPT: '0',
          GIT_CONFIG_COUNT: '1',
          GIT_CONFIG_KEY_0: 'http.extraHeader',
          GIT_CONFIG_VALUE_0: cap.authorizationHeader,
        },
      },
    );
    expect(result.stdout).toBe(`${head}\tHEAD\n`);
    expect(result.stdout + result.stderr).not.toContain(PAT);
    expect(upstream.mock.calls.map(([input]) => input.method)).toEqual(['GET', 'POST']);
    expect(upstream.mock.calls[1]![0].body?.toString()).toContain('command=ls-refs');
  });

  it('supports a real native Git fetch whose 100 explicit refs trigger gzip upload-pack requests', async () => {
    useGitBackend();
    const cap = await capability();
    const checkout = join(directory, 'fetched');
    await execute('git', ['init', '--initial-branch=main', checkout]);
    const result = await execute(
      'git',
      ['-C', checkout, '-c', 'protocol.version=2', 'fetch', '--no-tags', cap.remoteUrl, ...refs],
      {
        env: {
          ...process.env,
          GIT_TERMINAL_PROMPT: '0',
          GIT_CONFIG_COUNT: '1',
          GIT_CONFIG_KEY_0: 'http.extraHeader',
          GIT_CONFIG_VALUE_0: cap.authorizationHeader,
        },
      },
    );
    expect(clientEncodings).toContain('gzip');
    expect((await execute('git', ['-C', checkout, 'rev-parse', 'FETCH_HEAD'])).stdout.trim()).toBe(
      head,
    );
    expect((await execute('git', ['-C', checkout, 'show', 'FETCH_HEAD:README.md'])).stdout).toBe(
      'private fixture\n',
    );
    expect(result.stdout + result.stderr).not.toContain(PAT);
    expect(result.stdout + result.stderr).not.toContain(cap.authorizationHeader);
    expect(
      upstream.mock.calls.some(([input]) => input.body?.toString().includes('command=fetch')),
    ).toBe(true);
  });

  it.each(['0', '1'])('supports a native shallow clone over Git protocol v%s', async (version) => {
    useGitBackend();
    const cap = await capability();
    const checkout = join(directory, `legacy-${version}`);
    await execute(
      'git',
      ['-c', `protocol.version=${version}`, 'clone', '--depth=1', cap.remoteUrl, checkout],
      {
        env: {
          ...process.env,
          GIT_TERMINAL_PROMPT: '0',
          GIT_CONFIG_COUNT: '1',
          GIT_CONFIG_KEY_0: 'http.extraHeader',
          GIT_CONFIG_VALUE_0: cap.authorizationHeader,
        },
      },
    );
    expect((await execute('git', ['-C', checkout, 'rev-parse', 'HEAD'])).stdout.trim()).toBe(head);
  });

  it('inflates a bounded gzip request before forwarding Git bytes upstream', async () => {
    const cap = await capability();
    const bytes = Buffer.from('0014command=ls-refs\n00010009peel\n0000');
    const response = await app.inject({
      method: 'POST',
      url: `${cap.remoteUrl}/git-upload-pack`,
      headers: {
        authorization: cap.authorizationHeader.slice('Authorization: '.length),
        'content-type': 'application/x-git-upload-pack-request',
        'content-encoding': 'gzip',
        'git-protocol': 'version=2',
      },
      payload: gzipSync(bytes),
    });
    expect(response.statusCode).toBe(200);
    expect(upstream.mock.calls[0]![0].body).toEqual(bytes);
  });

  it('prepares a real initial checkout through the scoped proxy without PAT or JWT bytes in the snapshot', async () => {
    useGitBackend();
    const cap = await capability();
    const snapshot = await prepareRunnerGitSnapshot({
      url: REPO,
      source: { url: cap.remoteUrl, authorizationHeader: cap.authorizationHeader },
      remoteUrl: cap.remoteUrl,
      proxyResourceId: resourceId,
    });
    try {
      const contents = new Map<string, string>();
      for (const file of snapshot.files) {
        let value = '';
        for await (const chunk of snapshot.open(file.path)) value += String(chunk);
        contents.set(file.path, value);
        expect(value).not.toContain(PAT);
        expect(value).not.toContain(cap.authorizationHeader.slice('Authorization: Bearer '.length));
      }
      expect(contents.get('README.md')).toBe('private fixture\n');
      expect(contents.get('.git/config')).toContain(cap.remoteUrl);
      expect(contents.get('.git/config')).toContain(`/.orca/git/${resourceId}.config`);
      expect(
        upstream.mock.calls.some(([input]) => input.body?.toString().includes('command=fetch')),
      ).toBe(true);
    } finally {
      await snapshot.close();
    }
  });

  it.each([
    {
      encoding: 'gzip',
      body: gzipSync(Buffer.alloc(1024 * 1024 + 1)),
      status: 413,
      errorType: 'request_too_large',
      message: 'Git request body is too large',
    },
    {
      encoding: 'gzip',
      body: Buffer.from(PAT),
      status: 400,
      errorType: 'invalid_request_error',
      message: 'Invalid Git upload-pack encoding',
    },
    {
      encoding: 'br',
      body: Buffer.from(PAT),
      status: 415,
      errorType: 'invalid_request_error',
      message: 'Unsupported Git request encoding',
    },
  ] as const)(
    'bounds and validates encoded Git requests: $encoding $status',
    async ({ encoding, body, status, errorType, message }) => {
      const cap = await capability();
      const response = await app.inject({
        method: 'POST',
        url: `${cap.remoteUrl}/git-upload-pack`,
        headers: {
          authorization: cap.authorizationHeader.slice('Authorization: '.length),
          'content-type': 'application/x-git-upload-pack-request',
          'content-encoding': encoding,
        },
        payload: body,
      });
      expect(response.statusCode).toBe(status);
      expect(gitProxyContract.uploadPack.responses[status].parse(response.json())).toEqual({
        type: 'error',
        error: { type: errorType, message },
        request_id: expect.any(String),
      });
      expect(response.headers['cache-control']).toBe('no-store');
      expect(response.body).not.toContain(PAT);
      expect(upstream).not.toHaveBeenCalled();
      expect(resolveSecret).not.toHaveBeenCalled();
    },
  );

  it('rejects proxy tokens at the raw credential endpoint and wrong-audience tokens at proxy', async () => {
    const cap = await capability();
    const token = cap.authorizationHeader.slice('Authorization: Bearer '.length);
    const verified = await minter.verify(token, { expectedAudience: 'git-proxy' });
    expect(verified.expiresAt - Math.floor(Date.now() / 1000)).toBeGreaterThan(890);
    const wrong = await minter.mint(
      {
        org_id: verified.orgId,
        workspace_id: workspaceId,
        session_id: sessionId,
        mcp_server_names: [],
        credential_ids: [],
        vault_ids: [],
      },
      { audience: 'git-creds', repoUrls: [REPO] },
    );
    expect((await advertise(wrong.token)).statusCode).toBe(401);
    const raw = await app.inject({
      method: 'POST',
      url: '/v1/git-creds',
      headers: { authorization: `Bearer ${token}` },
      payload: { protocol: 'https', host: 'github.com', path: 'fixture/repo.git' },
    });
    expect(raw.statusCode).toBe(401);
    expect(upstream).not.toHaveBeenCalled();
  });

  it('rejects an expired token', async () => {
    const cap = await capability();
    vi.useFakeTimers({ toFake: ['Date'] });
    try {
      vi.setSystemTime((cap.expiresAt + 1) * 1000);
      expect(
        (await advertise(cap.authorizationHeader.slice('Authorization: Bearer '.length)))
          .statusCode,
      ).toBe(401);
    } finally {
      vi.useRealTimers();
    }
    expect(upstream).not.toHaveBeenCalled();
  });

  it('rejects cross-resource tokens and preserves binary request bytes without forwarding client headers', async () => {
    const cap = await capability();
    const token = cap.authorizationHeader.slice('Authorization: Bearer '.length);
    expect((await advertise(token, newId('sesrsc'))).statusCode).toBe(404);
    const bytes = Buffer.from('0014command=ls-refs\n00010009peel\n0000');
    const response = await app.inject({
      method: 'POST',
      url: `${cap.remoteUrl}/git-upload-pack`,
      headers: {
        authorization: `Bearer ${token}`,
        cookie: 'do-not-forward',
        'git-protocol': 'version=2',
        'content-type': 'application/x-git-upload-pack-request',
        'x-other': 'do-not-forward',
      },
      payload: bytes,
    });
    expect(response.statusCode).toBe(200);
    expect(upstream.mock.calls[0]![0]).toEqual({
      repoUrl: REPO,
      pat: PAT,
      method: 'POST',
      body: bytes,
      gitProtocol: 'version=2',
      signal: expect.any(AbortSignal),
    });
    expect(response.headers['cache-control']).toBe('no-store');
  });

  it.each([
    'resource_detached',
    'resource_deleted',
    'session_archived',
    'session_terminated',
    'workspace_archived',
    'credential_archived',
    'credential_rotated',
    'repo_rebound',
    'credential_rebound',
  ] as const)('revalidates %s before resolving the PAT', async (change) => {
    const { db } = await getTestDb();
    const cap = await capability();
    if (change === 'resource_detached')
      await db
        .update(sessionResources)
        .set({ detachedAt: new Date() })
        .where(eq(sessionResources.id, resourceId));
    if (change === 'resource_deleted')
      await db
        .update(sessionResources)
        .set({ deletedAt: new Date() })
        .where(eq(sessionResources.id, resourceId));
    if (change === 'session_archived')
      await db.update(sessions).set({ archivedAt: new Date() }).where(eq(sessions.id, sessionId));
    if (change === 'session_terminated')
      await db.update(sessions).set({ status: 'terminated' }).where(eq(sessions.id, sessionId));
    if (change === 'workspace_archived')
      await db
        .update(workspaces)
        .set({ status: 'archived', archivedAt: new Date(), updatedAt: new Date() })
        .where(eq(workspaces.id, workspaceId));
    if (change === 'credential_archived')
      await db
        .update(gitCredentials)
        .set({ archivedAt: new Date() })
        .where(eq(gitCredentials.id, credentialId));
    if (change === 'credential_rotated')
      await db
        .update(gitCredentials)
        .set({ secretRef: 'env:ROTATED', updatedAt: new Date(Date.now() + 1000) })
        .where(eq(gitCredentials.id, credentialId));
    if (change === 'repo_rebound')
      await db
        .update(sessionResources)
        .set({
          repoRef: { url: 'https://github.com/fixture/other.git', git_credential_id: credentialId },
        })
        .where(eq(sessionResources.id, resourceId));
    if (change === 'credential_rebound')
      await db
        .update(sessionResources)
        .set({ repoRef: { url: REPO, git_credential_id: newId('gitcred') } })
        .where(eq(sessionResources.id, resourceId));
    expect(
      (await advertise(cap.authorizationHeader.slice('Authorization: Bearer '.length))).statusCode,
    ).toBe(404);
    expect(upstream).not.toHaveBeenCalled();
    expect(resolveSecret).not.toHaveBeenCalled();
  });

  it('rejects receive-pack, invalid protocol headers, extra query fields and oversize bodies', async () => {
    const cap = await capability();
    const authorization = cap.authorizationHeader.slice('Authorization: '.length);
    expect(
      (
        await app.inject({
          url: `${cap.remoteUrl}/info/refs?service=git-receive-pack`,
          headers: { authorization },
        })
      ).statusCode,
    ).toBe(400);
    expect(
      (
        await app.inject({
          url: `${cap.remoteUrl}/info/refs?service=git-upload-pack&other=true`,
          headers: { authorization },
        })
      ).statusCode,
    ).toBe(400);
    expect(
      (
        await app.inject({
          url: `${cap.remoteUrl}/info/refs?service=git-upload-pack`,
          headers: { authorization, 'git-protocol': 'evil' },
        })
      ).statusCode,
    ).toBe(400);
    expect(
      (
        await app.inject({
          method: 'POST',
          url: `${cap.remoteUrl}/git-receive-pack`,
          headers: { authorization },
        })
      ).statusCode,
    ).not.toBe(200);
    const oversized = await app.inject({
      method: 'POST',
      url: `${cap.remoteUrl}/git-upload-pack`,
      headers: { authorization, 'content-type': 'application/x-git-upload-pack-request' },
      payload: Buffer.alloc(1024 * 1024 + 1),
    });
    expect(oversized.statusCode).toBe(413);
    expect(gitProxyContract.uploadPack.responses[413].parse(oversized.json())).toEqual({
      type: 'error',
      error: { type: 'request_too_large', message: 'Git request body is too large' },
      request_id: expect.any(String),
    });
    expect(oversized.headers['cache-control']).toBe('no-store');
    expect(upstream).not.toHaveBeenCalled();
  });

  it.each([
    {
      contentType: 'application/x-untrusted',
      body: PAT,
      code: 415,
      message: 'Git upload-pack bytes are required',
    },
    {
      contentType: 'application/json',
      body: '{invalid-json',
      code: 400,
      message: 'Invalid Git proxy request',
    },
  ] as const)(
    'returns a fixed Claude envelope for pre-handler parser error $code',
    async ({ contentType, body, code, message }) => {
      const cap = await capability();
      const response = await app.inject({
        method: 'POST',
        url: `${cap.remoteUrl}/git-upload-pack`,
        headers: {
          authorization: cap.authorizationHeader.slice('Authorization: '.length),
          'content-type': contentType,
        },
        payload: body,
      });
      expect(response.statusCode).toBe(code);
      expect(gitProxyContract.uploadPack.responses[code].parse(response.json())).toEqual({
        type: 'error',
        error: { type: 'invalid_request_error', message },
        request_id: expect.any(String),
      });
      expect(response.headers['cache-control']).toBe('no-store');
      expect(response.body).not.toContain(PAT);
      expect(upstream).not.toHaveBeenCalled();
      expect(resolveSecret).not.toHaveBeenCalled();
    },
  );

  it('collapses upstream diagnostics into a fixed opaque error', async () => {
    upstream.mockRejectedValue(new Error(`upstream leaked ${PAT}`));
    const response = await advertise();
    expect(response.statusCode).toBe(502);
    expect(response.body).not.toContain(PAT);
    expect(response.json().error.message).toBe('Git upstream request failed');
  });

  it('does not treat an owner ending in .git as the same repository owner', async () => {
    const { db } = await getTestDb();
    await db
      .update(gitCredentials)
      .set({ repoUrl: 'https://github.com/fixture.git/repo.git' })
      .where(eq(gitCredentials.id, credentialId));
    await expect(capability()).rejects.toThrow('attached Git resource is unavailable');
    expect(resolveSecret).not.toHaveBeenCalled();
  });

  it('aborts the upstream read when the Git client disconnects', async () => {
    const cap = await capability();
    let entered!: () => void;
    const started = new Promise<void>((resolve) => {
      entered = resolve;
    });
    let disconnected!: () => void;
    const aborted = new Promise<void>((resolve) => {
      disconnected = resolve;
    });
    upstream.mockImplementation(
      ({ signal }) =>
        new Promise((_resolve, reject) => {
          signal.addEventListener(
            'abort',
            () => {
              disconnected();
              reject(new Error('cancelled'));
            },
            { once: true },
          );
          entered();
        }),
    );
    const client = httpRequest(`${cap.remoteUrl}/info/refs?service=git-upload-pack`, {
      headers: { authorization: cap.authorizationHeader.slice('Authorization: '.length) },
    });
    client.on('error', () => {});
    client.end();
    await started;
    client.destroy();
    await aborted;
  });

  it.each(['workspace', 'session', 'organization'] as const)(
    'rejects cross-%s claims even with a correctly signed capability',
    async (scope) => {
      const cap = await capability();
      const verified = await minter.verify(
        cap.authorizationHeader.slice('Authorization: Bearer '.length),
      );
      const token = await minter.mint(
        {
          org_id: scope === 'organization' ? 'org_other' : verified.orgId,
          workspace_id: scope === 'workspace' ? uniqueWorkspace('other') : workspaceId,
          session_id: scope === 'session' ? newId('ses') : sessionId,
          mcp_server_names: [],
          vault_ids: [],
          credential_ids: [],
        },
        { audience: 'git-proxy', gitProxy: verified.gitProxy! },
      );
      expect((await advertise(token.token)).statusCode).toBe(404);
      expect(upstream).not.toHaveBeenCalled();
    },
  );
});
