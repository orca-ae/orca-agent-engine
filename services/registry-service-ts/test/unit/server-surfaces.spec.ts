// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { FileStore } from '@orca/file-store';
import { InMemorySkillStore } from '@orca/skill-store';
import type { TranscriptStore } from '@orca/transcript-store';
import { request as httpRequest } from 'node:http';
import type { AddressInfo } from 'node:net';
import { afterEach, describe, expect, it } from 'vitest';
import { WebSocket } from 'ws';
import type { SessionJwtMinter } from '../../src/auth/session-jwt.js';
import { StaticInternalAuthVerifier, staticTokenSource } from '../../src/auth/internal-auth.js';
import {
  LOGICAL_CREDENTIAL_ID_MAX_LENGTH,
  LOGICAL_CREDENTIAL_ID_PREFIX,
} from '../../src/domain/provider-credential.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import {
  buildAdminApp,
  buildCombinedTestApp,
  buildInternalApp,
  buildPublicApp,
  type BuildAppOptions,
} from '../../src/server.js';

const apps: Array<ReturnType<typeof buildPublicApp>> = [];
const INTERNAL_TOKEN = 'test-internal-service-token-at-least-32-chars';

afterEach(async () => {
  await Promise.all(apps.splice(0).map(async (app) => app.close()));
});

function options(): BuildAppOptions {
  return {
    db: {} as DbClient,
    oidc: { allowedIssuers: [], audience: 'test' },
    store: {} as TranscriptStore,
    sse: { bufferSize: 1, dropAgeMs: 1, heartbeatMs: 1 },
    jwtMinter: {} as SessionJwtMinter,
    fileStore: {} as FileStore,
    skillStore: new InMemorySkillStore(),
  };
}

function internalVerifier(): StaticInternalAuthVerifier {
  return new StaticInternalAuthVerifier(staticTokenSource(INTERNAL_TOKEN));
}

function h2cUpgradeRequest(
  url: string,
  contentType: string,
  body: string,
): Promise<{ statusCode: number; body: string }> {
  const target = new URL(url);
  return new Promise((resolve, reject) => {
    const req = httpRequest(
      {
        host: target.hostname,
        port: target.port,
        path: target.pathname,
        method: 'POST',
        headers: {
          connection: 'Upgrade, HTTP2-Settings',
          upgrade: 'h2c',
          'http2-settings': 'AAEAAEAAAAIAAAABAAMAAABkAAQBAAAAAAUAAEAA',
          'content-type': contentType,
          'content-length': Buffer.byteLength(body),
        },
      },
      (res) => {
        const chunks: Buffer[] = [];
        res.on('data', (chunk: Buffer) => chunks.push(chunk));
        res.on('end', () => {
          resolve({
            statusCode: res.statusCode ?? 0,
            body: Buffer.concat(chunks).toString('utf8'),
          });
        });
      },
    );
    req.on('error', reject);
    req.end(body);
  });
}

function listeningUrl(app: ReturnType<typeof buildPublicApp>): string {
  const address = app.server.address() as AddressInfo;
  return `http://127.0.0.1:${address.port}`;
}

describe('Registry listener surfaces', () => {
  it('parses JSON bodies when an HTTP/1.1 client offers an h2c upgrade', async () => {
    const app = buildPublicApp(options());
    app.post('/healthz', async (req) => req.body);
    apps.push(app);
    await app.listen({ host: '127.0.0.1', port: 0 });

    const response = await h2cUpgradeRequest(
      `${listeningUrl(app)}/healthz`,
      'application/json',
      JSON.stringify({ name: 'h2c-probe' }),
    );

    expect(response.statusCode).toBe(200);
    expect(JSON.parse(response.body)).toEqual({ name: 'h2c-probe' });
  });

  it('parses multipart bodies when an HTTP/1.1 client offers an h2c upgrade', async () => {
    const boundary = '----orca-h2c-probe';
    const body = [
      `--${boundary}`,
      'Content-Disposition: form-data; name="file"; filename="probe.txt"',
      'Content-Type: text/plain',
      '',
      'multipart payload',
      `--${boundary}--`,
      '',
    ].join('\r\n');
    const app = buildPublicApp(options());
    app.post('/readyz', async (req, reply) => {
      const file = await req.file();
      if (!file) return reply.code(400).send({ error: 'no file part provided' });
      return {
        filename: file.filename,
        content: (await file.toBuffer()).toString('utf8'),
      };
    });
    apps.push(app);
    await app.listen({ host: '127.0.0.1', port: 0 });

    const response = await h2cUpgradeRequest(
      `${listeningUrl(app)}/readyz`,
      `multipart/form-data; boundary=${boundary}`,
      body,
    );

    expect(response.statusCode).toBe(200);
    expect(JSON.parse(response.body)).toEqual({
      filename: 'probe.txt',
      content: 'multipart payload',
    });
  });

  it('continues to accept WebSocket upgrades on the public listener', async () => {
    const app = buildPublicApp(options());
    apps.push(app);
    await app.listen({ host: '127.0.0.1', port: 0 });

    const socket = new WebSocket(
      `${listeningUrl(app).replace('http://', 'ws://')}/v1/tunnels/runners/runner_upgrade_test`,
    );
    await new Promise<void>((resolve, reject) => {
      socket.once('open', resolve);
      socket.once('error', reject);
    });
    socket.close();
    await new Promise<void>((resolve) => socket.once('close', resolve));
  });

  it('injects an explicit internal principal on the test-only combined surface', async () => {
    const app = buildCombinedTestApp(options());
    app.get('/internal/test-principal', async (req) => req.internalAuth ?? null);
    apps.push(app);
    await app.ready();

    const response = await app.inject({ method: 'GET', url: '/internal/test-principal' });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({
      caller: 'shared',
      subject: 'combined-test-internal',
    });
  });

  it('does not register internal routes on the public app', async () => {
    const app = buildPublicApp(options());
    apps.push(app);
    await app.ready();

    const response = await app.inject({
      method: 'POST',
      url: '/internal/v1/workspaces/ws_a/sessions/ses_a/executions:prepare',
      payload: {},
    });

    expect(response.statusCode).toBe(404);
  });

  it('keeps observability context resolver internal and never cacheable, including early failures', async () => {
    const publicApp = buildPublicApp(options());
    const internalApp = buildInternalApp(options(), internalVerifier());
    const adminApp = buildAdminApp({
      db: options().db,
      oidc: { allowedIssuers: [], audience: 'admin-test' },
      platformOidc: { allowedIssuers: [], audience: 'platform-test' },
      store: options().store,
    });
    apps.push(publicApp, internalApp, adminApp);
    await Promise.all([publicApp.ready(), internalApp.ready(), adminApp.ready()]);
    const path =
      '/internal/v1/workspaces/ws_context/sessions/ses_context/agent-observability/context/resolve';

    // Path handling is intentionally broader than the POST-only capability
    // rule. A wrong method keeps its normal 403/404 routing and authorization
    // result, but it is still a tenant/session context URL and never cacheable.
    for (const method of ['GET', 'PUT'] as const) {
      const publicWrongMethod = await publicApp.inject({ method, url: path });
      expect(publicWrongMethod.statusCode).toBe(404);
      expect(publicWrongMethod.headers['cache-control']).toBe('private, no-store');

      const adminWrongMethod = await adminApp.inject({ method, url: path });
      expect(adminWrongMethod.statusCode).toBe(404);
      expect(adminWrongMethod.headers['cache-control']).toBe('private, no-store');

      const internalWrongMethod = await internalApp.inject({
        method,
        url: path,
        headers: { authorization: `Bearer ${INTERNAL_TOKEN}` },
      });
      expect(internalWrongMethod.statusCode).toBe(404);
      expect(internalWrongMethod.headers['cache-control']).toBe('private, no-store');
    }

    const overlongPath = `/internal/v1/workspaces/${'w'.repeat(129)}/sessions/ses_context/agent-observability/context/resolve`;
    for (const method of ['GET', 'PUT'] as const) {
      const publicOverlong = await publicApp.inject({ method, url: overlongPath });
      expect(publicOverlong.statusCode).toBe(404);
      expect(publicOverlong.headers['cache-control']).toBe('private, no-store');

      const adminOverlong = await adminApp.inject({ method, url: overlongPath });
      expect(adminOverlong.statusCode).toBe(404);
      expect(adminOverlong.headers['cache-control']).toBe('private, no-store');

      const internalOverlong = await internalApp.inject({
        method,
        url: overlongPath,
        headers: { authorization: `Bearer ${INTERNAL_TOKEN}` },
      });
      expect(internalOverlong.statusCode).toBe(400);
      expect(internalOverlong.headers['cache-control']).toBe('private, no-store');
    }

    const publicResponse = await publicApp.inject({ method: 'POST', url: path, payload: {} });
    expect(publicResponse.statusCode).toBe(404);
    expect(publicResponse.headers['cache-control']).toBe('private, no-store');

    const adminResponse = await adminApp.inject({ method: 'POST', url: path, payload: {} });
    expect(adminResponse.statusCode).toBe(404);
    expect(adminResponse.headers['cache-control']).toBe('private, no-store');

    const unauthorized = await internalApp.inject({ method: 'POST', url: path, payload: {} });
    expect(unauthorized.statusCode).toBe(401);
    expect(unauthorized.headers['cache-control']).toBe('private, no-store');

    const malformed = await internalApp.inject({
      method: 'POST',
      url: path,
      headers: {
        authorization: `Bearer ${INTERNAL_TOKEN}`,
        'content-type': 'application/json',
      },
      payload: '{',
    });
    expect(malformed.statusCode).toBe(400);
    expect(malformed.headers['cache-control']).toBe('private, no-store');

    const unavailable = await internalApp.inject({
      method: 'POST',
      url: path,
      headers: { authorization: `Bearer ${INTERNAL_TOKEN}` },
      payload: {},
    });
    expect(unavailable.statusCode).toBe(503);
    expect(unavailable.headers['cache-control']).toBe('private, no-store');
  });

  it('uses the internal context contract as its selector parser', async () => {
    const app = buildInternalApp(options(), internalVerifier());
    apps.push(app);
    await app.ready();
    const headers = { authorization: `Bearer ${INTERNAL_TOKEN}` };
    const maxSessionId = `ses_${'s'.repeat(124)}`;
    const resolve = (workspaceId: string, sessionId: string) =>
      app.inject({
        method: 'POST',
        url: `/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/agent-observability/context/resolve`,
        headers,
        payload: {},
      });

    for (const [workspaceId, sessionId] of [
      ['ws.invalid', 'ses_context'],
      ['w'.repeat(129), 'ses_context'],
      ['ws_context', 'sesn_context'],
      ['ws_context', `${maxSessionId}s`],
    ] as const) {
      const response = await resolve(workspaceId, sessionId);
      expect(response.statusCode).toBe(400);
      expect(response.headers['cache-control']).toBe('private, no-store');
      expect(response.json()).toEqual({ error: 'invalid agent observability context request' });
    }

    const maximum = await resolve('ws_context', maxSessionId);
    expect(maximum.statusCode).toBe(503);
    expect(maximum.headers['cache-control']).toBe('private, no-store');
  });

  it('does not register public tenant routes on the internal app', async () => {
    const app = buildInternalApp(options(), internalVerifier());
    apps.push(app);
    await app.ready();

    const response = await app.inject({
      method: 'GET',
      url: '/v1/agents/agt_a',
      headers: { authorization: `Bearer ${INTERNAL_TOKEN}` },
    });

    expect(response.statusCode).toBe(404);
  });

  it('routes the full logical credential ID contract and rejects overlength parameters', async () => {
    const app = buildInternalApp(options(), internalVerifier());
    apps.push(app);
    await app.ready();
    const maximum = `${LOGICAL_CREDENTIAL_ID_PREFIX}${'a'.repeat(
      LOGICAL_CREDENTIAL_ID_MAX_LENGTH - LOGICAL_CREDENTIAL_ID_PREFIX.length,
    )}`;
    const overlength = `${maximum}a`;
    const resolve = (id: string) =>
      app.inject({
        method: 'POST',
        url: `/internal/v1/workspaces/ws_a/sessions/invalid/vault-credentials/${id}/resolve`,
        headers: {
          authorization: `Bearer ${INTERNAL_TOKEN}`,
          'content-type': 'application/json',
        },
        payload: { credential_id: id, vault_id: id },
      });

    const maximumResponse = await resolve(maximum);
    expect(maximumResponse.statusCode).toBe(400);
    expect(maximumResponse.json()).toEqual({ error: 'invalid workspace or session id' });
    await expect(resolve(overlength)).resolves.toMatchObject({ statusCode: 404 });
  });

  it('keeps probes available on both surfaces', async () => {
    const publicApp = buildPublicApp(options());
    const internalApp = buildInternalApp(options(), internalVerifier());
    apps.push(publicApp, internalApp);
    await Promise.all([publicApp.ready(), internalApp.ready()]);

    await expect(publicApp.inject({ method: 'GET', url: '/healthz' })).resolves.toMatchObject({
      statusCode: 200,
    });
    await expect(internalApp.inject({ method: 'GET', url: '/healthz' })).resolves.toMatchObject({
      statusCode: 200,
    });
  });

  it('keeps admin and workspace credentials on mutually exclusive listeners', async () => {
    const publicApp = buildPublicApp(options());
    const adminApp = buildAdminApp({
      db: options().db,
      oidc: { allowedIssuers: [], audience: 'admin-test' },
      platformOidc: { allowedIssuers: [], audience: 'platform-test' },
      store: options().store,
    });
    apps.push(publicApp, adminApp);
    await Promise.all([publicApp.ready(), adminApp.ready()]);

    const adminKeyOnPublic = await publicApp.inject({
      method: 'GET',
      url: '/v1/agents',
      headers: { 'x-api-key': 'orca_admin_not_a_workspace_key' },
    });
    const platformKeyOnPublic = await publicApp.inject({
      method: 'GET',
      url: '/v1/agents',
      headers: { 'x-api-key': 'orca_platform_not_a_workspace_key_but_long_enough' },
    });
    const workspaceKeyOnAdmin = await adminApp.inject({
      method: 'GET',
      url: '/v1/organizations/me',
      headers: { 'x-api-key': 'orca_not_an_admin_key' },
    });
    const organizationKeyOnPlatform = await adminApp.inject({
      method: 'POST',
      url: '/v1/platform/organizations',
      headers: { 'x-api-key': 'orca_admin_not_a_platform_key_but_long_enough' },
      payload: { name: 'Must not be created' },
    });

    expect(adminKeyOnPublic.statusCode).toBe(401);
    expect(platformKeyOnPublic.statusCode).toBe(401);
    expect(workspaceKeyOnAdmin.statusCode).toBe(401);
    expect(organizationKeyOnPlatform.statusCode).toBe(401);
  });

  it('mounts observability state and workspace mutation only on the organization admin listener', async () => {
    const publicApp = buildPublicApp(options());
    const internalApp = buildInternalApp(options(), internalVerifier());
    const adminApp = buildAdminApp({
      db: options().db,
      oidc: { allowedIssuers: [], audience: 'admin-test' },
      platformOidc: { allowedIssuers: [], audience: 'platform-test' },
      store: options().store,
    });
    apps.push(publicApp, internalApp, adminApp);
    await Promise.all([publicApp.ready(), internalApp.ready(), adminApp.ready()]);

    const organizationRoute = '/v1/organizations/agent_observability';
    const workspaceRoute = '/v1/organizations/workspaces/:workspaceId/agent_observability';
    expect(adminApp.hasRoute({ method: 'GET', url: organizationRoute })).toBe(true);
    expect(adminApp.hasRoute({ method: 'GET', url: workspaceRoute })).toBe(true);
    expect(adminApp.hasRoute({ method: 'PUT', url: workspaceRoute })).toBe(true);
    expect(publicApp.hasRoute({ method: 'GET', url: organizationRoute })).toBe(false);
    expect(publicApp.hasRoute({ method: 'GET', url: workspaceRoute })).toBe(false);
    expect(publicApp.hasRoute({ method: 'PUT', url: workspaceRoute })).toBe(false);
    expect(internalApp.hasRoute({ method: 'GET', url: organizationRoute })).toBe(false);
    expect(internalApp.hasRoute({ method: 'GET', url: workspaceRoute })).toBe(false);
    expect(internalApp.hasRoute({ method: 'PUT', url: workspaceRoute })).toBe(false);
  });

  it('understands /api/v1 on every listener that serves /v1', async () => {
    // "`/api/v1` means `/v1`" has to be one fact about the deployment rather
    // than a per-listener quirk a caller has to memorise, so the admin listener
    // carries the same rewrite as the public one. Before it did, these 404'd —
    // reaching the route and being refused for want of credentials is the
    // difference being asserted.
    const publicApp = buildPublicApp(options());
    const adminApp = buildAdminApp({
      db: options().db,
      oidc: { allowedIssuers: [], audience: 'admin-test' },
      platformOidc: { allowedIssuers: [], audience: 'platform-test' },
      store: options().store,
    });
    apps.push(publicApp, adminApp);
    await Promise.all([publicApp.ready(), adminApp.ready()]);

    const cases: Array<[ReturnType<typeof buildPublicApp>, string, string]> = [
      [publicApp, '/v1/agents', '/api/v1/agents'],
      [adminApp, '/v1/organizations/me', '/api/v1/organizations/me'],
      [adminApp, '/v1/platform/organizations', '/api/v1/platform/organizations'],
    ];
    for (const [app, canonical, alias] of cases) {
      const direct = await app.inject({ method: 'GET', url: canonical });
      const aliased = await app.inject({ method: 'GET', url: alias });
      expect(aliased.statusCode, alias).toBe(direct.statusCode);
      expect(aliased.statusCode, alias).toBe(401);
    }

    // The discovery routes stay off the admin listener: it is a control plane,
    // not the API surface a client probes for capabilities. Asserted on the
    // route table rather than on a status code — the admin listener answers an
    // unmatched path with its authenticator's 401, not a 404, so a status code
    // cannot tell "not routed" from "not authorized" here.
    expect(adminApp.hasRoute({ method: 'GET', url: '/apis' })).toBe(false);
    expect(publicApp.hasRoute({ method: 'GET', url: '/apis' })).toBe(true);
  });

  it('serves metrics only on the internal listener', async () => {
    const publicApp = buildPublicApp(options());
    const internalApp = buildInternalApp(options(), internalVerifier());
    apps.push(publicApp, internalApp);
    await Promise.all([publicApp.ready(), internalApp.ready()]);

    await expect(publicApp.inject({ method: 'GET', url: '/metrics' })).resolves.toMatchObject({
      statusCode: 404,
    });
    await expect(internalApp.inject({ method: 'GET', url: '/metrics' })).resolves.toMatchObject({
      statusCode: 200,
    });
  });

  it('accepts forwarded client addresses only from explicitly trusted proxies', async () => {
    const directApp = buildPublicApp(options());
    directApp.get('/metrics', async (req) => ({ ip: req.ip }));

    const proxiedApp = buildPublicApp({
      ...options(),
      publicTrustProxy: ['10.42.0.0/16'],
    });
    proxiedApp.get('/metrics', async (req) => ({ ip: req.ip }));

    apps.push(directApp, proxiedApp);
    await Promise.all([directApp.ready(), proxiedApp.ready()]);

    const request = {
      method: 'GET' as const,
      url: '/metrics',
      remoteAddress: '10.42.1.10',
      headers: { 'x-forwarded-for': '198.51.100.25' },
    };
    expect((await directApp.inject(request)).json()).toEqual({ ip: '10.42.1.10' });
    expect((await proxiedApp.inject(request)).json()).toEqual({ ip: '198.51.100.25' });
  });
});
