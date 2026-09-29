// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { createServer, request as httpRequest, type Server } from 'node:http';
import { exportJWK, generateKeyPair, SignJWT } from 'jose';
import type { KeyLike } from 'jose';
import type { FastifyInstance } from 'fastify';
import { InMemorySkillStore } from '@orca/skill-store';
import { and, eq, inArray, like } from 'drizzle-orm';
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
  createTestWorkspace,
  TEST_ORGANIZATION_ID,
} from './fixtures.js';
import {
  buildAdminApp,
  buildCombinedTestApp,
  buildInternalApp,
  buildPublicApp,
} from '../../src/server.js';
import { StaticInternalAuthVerifier, staticTokenSource } from '../../src/auth/internal-auth.js';
import {
  fingerprintApiKey,
  hashApiKey,
  LEGACY_API_KEY_FALLBACK_MAX_CANDIDATES,
} from '../../src/auth/api-key.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import { apiKeys } from '../../src/persistence/postgres/schema.js';

const ADMIN_PROXY_AUTHORIZATION_HEADER = 'x-orca-registry-authorization';

interface TestIssuer {
  server: Server;
  issuer: string;
  kid: string;
  privateKey: KeyLike;
}

/**
 * A real RS256 key pair behind a real JWKS endpoint, so the tests below can
 * mint tokens the production OIDC verifier genuinely accepts.
 *
 * This is what makes the fall-through cases mean anything. A previous version
 * of this file passed another `orca_`-prefixed api key as its "Bearer" — a
 * value `buildOidcAuth` could never validate — so the fall-through had nothing
 * to succeed with and the test asserted 401 against a path incapable of
 * returning anything else.
 *
 * `listen(0)` gives a fresh port per issuer, which also keeps `oidc.ts`'s
 * module-level `ISSUER_CACHE` from serving one block's key set to another.
 */
async function startTestIssuer(kid: string): Promise<TestIssuer> {
  const { publicKey, privateKey } = await generateKeyPair('RS256');
  const jwk = { ...(await exportJWK(publicKey)), kid, alg: 'RS256' };
  const server = createServer((req, res) => {
    if (req.url === '/.well-known/jwks.json') {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ keys: [jwk] }));
      return;
    }
    res.writeHead(404).end();
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  if (address === null || typeof address === 'string') {
    throw new Error('missing JWKS server port');
  }
  return { server, issuer: `http://127.0.0.1:${address.port}`, kid, privateKey };
}

async function stopTestIssuer(issuer: TestIssuer): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    issuer.server.close((error) => (error ? reject(error) : resolve()));
  });
}

/**
 * A GET over a real socket, so header parsing is Node's and not
 * `light-my-request`'s. `node:http` writes an array header value as repeated
 * header lines, which is the only way to present a genuinely duplicated
 * `x-api-key` to the server.
 */
function rawGet(
  port: number,
  path: string,
  headers: Record<string, string | string[]>,
): Promise<{ statusCode: number; body: string }> {
  return new Promise((resolve, reject) => {
    const req = httpRequest({ host: '127.0.0.1', port, path, method: 'GET', headers }, (res) => {
      let body = '';
      res.setEncoding('utf8');
      res.on('data', (chunk: string) => (body += chunk));
      res.on('end', () => resolve({ statusCode: res.statusCode ?? 0, body }));
    });
    req.on('error', reject);
    req.end();
  });
}

function expectAdminUnauthenticated(
  response: { statusCode: number; body: string; json(): unknown },
  secrets: readonly string[] = [],
): void {
  expect(response.statusCode).toBe(401);
  expect(response.json()).toEqual({ error: 'unauthenticated' });
  for (const secret of secrets) expect(response.body).not.toContain(secret);
}

describe('Auth (integration)', () => {
  let app: FastifyInstance;
  let workspaceId: string;
  let apiKey: string;
  let db: DbClient;

  function legacyFallbackMetricValue(body: string, result: string): number {
    const prefix = `registry_service_legacy_api_key_fallback_total{result="${result}"} `;
    const line = body.split('\n').find((candidate) => candidate.startsWith(prefix));
    return line ? Number(line.slice(prefix.length)) : 0;
  }

  async function withIsolatedLegacyCandidates<T>(run: () => Promise<T>): Promise<T> {
    const existing = await db
      .select({ id: apiKeys.id })
      .from(apiKeys)
      .where(and(eq(apiKeys.status, 'active'), like(apiKeys.keyFingerprint, 'legacy:%')));
    const existingIds = existing.map((row) => row.id);
    if (existingIds.length > 0) {
      await db.update(apiKeys).set({ status: 'inactive' }).where(inArray(apiKeys.id, existingIds));
    }
    try {
      return await run();
    } finally {
      if (existingIds.length > 0) {
        await db.update(apiKeys).set({ status: 'active' }).where(inArray(apiKeys.id, existingIds));
      }
    }
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
    await app.ready();
    workspaceId = uniqueWorkspace('auth');
    apiKey = await createTestApiKey(db, workspaceId);
  });
  afterAll(async () => {
    await app.close();
    await closeTestDb();
  });

  it('valid x-api-key → 200', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/v1/skills',
      headers: { 'x-api-key': apiKey },
    });
    expect(res.statusCode).toBe(200);
  });

  it('missing auth → 401', async () => {
    const res = await app.inject({ method: 'GET', url: '/v1/skills' });
    expect(res.statusCode).toBe(401);
    expect(res.json()).toMatchObject({
      type: 'error',
      error: {
        type: 'authentication_error',
        message: 'unauthenticated',
      },
      request_id: expect.any(String),
    });
  });

  it('unknown api-key → 401', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/v1/skills',
      headers: { 'x-api-key': 'orca_unknown_xxxxxxxx' },
    });
    expect(res.statusCode).toBe(401);
  });

  /**
   * `apiKeyAuth` used to return `null` for both "no `x-api-key` was presented"
   * and "an `x-api-key` was presented and did not authenticate", so `buildAuth`
   * could not tell them apart and ran OIDC in both cases. A request carrying a
   * bad `x-api-key` plus a valid OIDC Bearer therefore authenticated as *the
   * Bearer's* workspace — one the caller never named, and one a gateway or
   * wrapper may have selected on their behalf without their knowledge.
   *
   * The first test in this block is the control that makes the rest mean
   * something: it proves the token really does authenticate here, so a 401 in
   * the others can only come from the api-key check refusing to fall through,
   * not from a fixture that never worked.
   */
  describe('an explicitly supplied api key must not fall through to OIDC', () => {
    let oidcApp: FastifyInstance;
    let testIssuer: TestIssuer;
    let signToken: () => Promise<string>;

    const buildOidcApp = () =>
      buildCombinedTestApp({
        db,
        oidc: { allowedIssuers: [testIssuer.issuer], audience: 'orca-managed-agents' },
        store: buildStubStore(),
        sse: STUB_SSE_CONFIG,
        jwtMinter: buildTestJwtMinter(),
        fileStore: buildStubFileStore(),
      });

    beforeAll(async () => {
      testIssuer = await startTestIssuer('oidc-fallthrough');
      const oidcWorkspaceId = uniqueWorkspace('auth-oidc');
      await createTestWorkspace(db, oidcWorkspaceId);

      signToken = () =>
        new SignJWT({ workspace_id: oidcWorkspaceId })
          .setProtectedHeader({ alg: 'RS256', kid: testIssuer.kid })
          .setIssuer(testIssuer.issuer)
          .setAudience('orca-managed-agents')
          .setSubject('user_oidc_fallthrough')
          .setIssuedAt()
          .setExpirationTime('5m')
          .sign(testIssuer.privateKey);

      oidcApp = buildOidcApp();
      await oidcApp.ready();
    });

    afterAll(async () => {
      await oidcApp.close();
      await stopTestIssuer(testIssuer);
    });

    it('authenticates a genuinely valid signed OIDC bearer when no api key is supplied', async () => {
      const res = await oidcApp.inject({
        method: 'GET',
        url: '/v1/skills',
        headers: { authorization: `Bearer ${await signToken()}` },
      });
      expect(res.statusCode).toBe(200);
    });

    // A well-formed key that simply is not in this deployment — rotated, or
    // from another environment — is the same caller mistake as a typo'd one and
    // has the same consequence if OIDC rescues it. A present `x-api-key` is
    // authoritative whatever it contains.
    it.each([
      ['a malformed x-api-key', 'not-orca-shaped'],
      ['an unknown but well-formed x-api-key', 'orca_not_a_real_key_aaaaaaaa'],
      ['an empty x-api-key', ''],
      ['a whitespace-only x-api-key', '   '],
      ['an admin key on the public listener', 'orca_admin_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'],
    ])('rejects %s even when the bearer is a valid OIDC token', async (_label, key) => {
      const res = await oidcApp.inject({
        method: 'GET',
        url: '/v1/skills',
        headers: { 'x-api-key': key, authorization: `Bearer ${await signToken()}` },
      });
      expect(res.statusCode).toBe(401);
    });

    /**
     * `light-my-request` hands `inject` header values through untouched, so the
     * cases above cannot say what a real listener does with two `x-api-key`
     * lines. This one confirms it rather than assuming it, over a real socket:
     * Node joins the duplicates into a single comma-separated string, which is
     * not a valid key, so the request is rejected instead of being read as
     * "absent" and rescued by the Bearer.
     */
    it('rejects duplicated x-api-key headers over a real socket', async () => {
      const seenApiKeyHeader: Array<string | string[] | undefined> = [];
      const echo = createServer((req, res) => {
        seenApiKeyHeader.push(req.headers['x-api-key']);
        res.writeHead(204).end();
      });
      await new Promise<void>((resolve) => echo.listen(0, '127.0.0.1', resolve));
      const echoAddress = echo.address();
      if (echoAddress === null || typeof echoAddress === 'string') {
        throw new Error('missing echo server port');
      }

      const socketApp = buildOidcApp();
      await socketApp.listen({ port: 0, host: '127.0.0.1' });
      try {
        await rawGet(echoAddress.port, '/', { 'x-api-key': ['orca_one', 'orca_two'] });
        // The mechanism: two header lines arrive as one string, never an array.
        expect(seenApiKeyHeader).toEqual(['orca_one, orca_two']);

        const appAddress = socketApp.server.address();
        if (appAddress === null || typeof appAddress === 'string') {
          throw new Error('missing app port');
        }
        const bearer = `Bearer ${await signToken()}`;
        // Control on the same socket: the bearer alone still authenticates.
        expect(
          (await rawGet(appAddress.port, '/v1/skills', { authorization: bearer })).statusCode,
        ).toBe(200);
        const duplicated = await rawGet(appAddress.port, '/v1/skills', {
          authorization: bearer,
          'x-api-key': ['orca_one', 'orca_two'],
        });
        expect(duplicated.statusCode).toBe(401);
      } finally {
        await socketApp.close();
        await new Promise<void>((resolve, reject) => {
          echo.close((error) => (error ? reject(error) : resolve()));
        });
      }
    });
  });

  /**
   * An unknown but well-formed key runs the legacy-fallback verification, which
   * is rate limited to two attempts per source per minute keyed on `req.ip`.
   * When that limiter tripped, `apiKeyAuth` *threw* rather than returning a
   * verdict, and the catch left the verdict permissive — so OIDC ran anyway and
   * the request authenticated as the Bearer's workspace. Two throwaway requests
   * bought an attacker the fall-through the block above exists to prevent, and
   * the 429 was swallowed along with it.
   *
   * A single-request test structurally cannot see this: the first two requests
   * reject correctly and only the third slips through. So this drives the whole
   * sequence and asserts that none of them authenticates. `req.ip` is
   * attacker-controlled, so the threshold is not a defence.
   */
  describe('a key that could not be validated must deny rather than delegate', () => {
    let rateLimitedApp: FastifyInstance;
    let testIssuer: TestIssuer;
    let signToken: () => Promise<string>;

    beforeAll(async () => {
      testIssuer = await startTestIssuer('ratelimit-fallthrough');
      const oidcWorkspaceId = uniqueWorkspace('auth-ratelimit');
      await createTestWorkspace(db, oidcWorkspaceId);

      signToken = () =>
        new SignJWT({ workspace_id: oidcWorkspaceId })
          .setProtectedHeader({ alg: 'RS256', kid: testIssuer.kid })
          .setIssuer(testIssuer.issuer)
          .setAudience('orca-managed-agents')
          .setSubject('user_ratelimit_fallthrough')
          .setIssuedAt()
          .setExpirationTime('5m')
          .sign(testIssuer.privateKey);

      // A dedicated app keeps the limiter budget fresh: it is created per
      // `buildAuth`, and every other block's requests spend some of it.
      rateLimitedApp = buildCombinedTestApp({
        db,
        oidc: { allowedIssuers: [testIssuer.issuer], audience: 'orca-managed-agents' },
        store: buildStubStore(),
        sse: STUB_SSE_CONFIG,
        jwtMinter: buildTestJwtMinter(),
        fileStore: buildStubFileStore(),
      });
      await rateLimitedApp.ready();
    });

    afterAll(async () => {
      await rateLimitedApp.close();
      await stopTestIssuer(testIssuer);
    });

    it('authenticates a genuinely valid signed OIDC bearer when no api key is supplied', async () => {
      const res = await rateLimitedApp.inject({
        method: 'GET',
        url: '/v1/skills',
        remoteAddress: '198.51.100.78',
        headers: { authorization: `Bearer ${await signToken()}` },
      });
      expect(res.statusCode).toBe(200);
    });

    it('never falls through to OIDC once the legacy-fallback limiter trips', async () => {
      const statuses: number[] = [];
      for (let attempt = 0; attempt < 5; attempt += 1) {
        const res = await rateLimitedApp.inject({
          method: 'GET',
          url: '/v1/skills',
          remoteAddress: '198.51.100.77',
          headers: {
            'x-api-key': `orca_unknown_ratelimit_${attempt}`,
            authorization: `Bearer ${await signToken()}`,
          },
        });
        statuses.push(res.statusCode);
      }
      // 401 while the limiter has budget, 429 once it does not. Never 200.
      expect(statuses).not.toContain(200);
      expect(statuses.every((status) => status === 401 || status === 429)).toBe(true);
    });
  });

  /**
   * The organization-admin listener had the identical fall-through
   * (`(await apiKey(req)) ?? (await oidc(req))`) with strictly more privilege
   * at stake: the rescuing credential names an *organization*, not a workspace.
   */
  describe('the admin listener must not fall through to OIDC either', () => {
    let adminApp: FastifyInstance;
    let testIssuer: TestIssuer;
    let signAdminToken: () => Promise<string>;

    beforeAll(async () => {
      testIssuer = await startTestIssuer('admin-fallthrough');
      // Creates TEST_ORGANIZATION_ID, which `buildAdminAuth` looks up and
      // requires to be active.
      await createTestWorkspace(db, uniqueWorkspace('admin-oidc'));

      signAdminToken = () =>
        new SignJWT({ organization_id: TEST_ORGANIZATION_ID, scopes: ['org:admin'] })
          .setProtectedHeader({ alg: 'RS256', kid: testIssuer.kid })
          .setIssuer(testIssuer.issuer)
          .setAudience('orca-admin')
          .setSubject('user_admin_fallthrough')
          .setIssuedAt()
          .setExpirationTime('5m')
          .sign(testIssuer.privateKey);

      adminApp = buildAdminApp({
        db,
        oidc: { allowedIssuers: [testIssuer.issuer], audience: 'orca-admin' },
        platformOidc: { allowedIssuers: [], audience: 'orca-platform' },
        store: buildStubStore(),
      });
      await adminApp.ready();
    });

    afterAll(async () => {
      await adminApp.close();
      await stopTestIssuer(testIssuer);
    });

    const listWorkspaces = (headers: Record<string, string>) =>
      adminApp.inject({ method: 'GET', url: '/v1/organizations/workspaces', headers });

    it('authenticates a genuinely valid signed admin OIDC bearer on its own', async () => {
      const res = await listWorkspaces({ authorization: `Bearer ${await signAdminToken()}` });
      expect(res.statusCode).toBe(200);
    });

    it.each([
      ['a malformed x-api-key', 'not-orca-shaped'],
      ['an unknown but well-formed admin key', 'orca_admin_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'],
      ['a workspace key on the admin listener', 'orca_workspace_key_not_admin'],
      ['an empty x-api-key', ''],
    ])('rejects %s even with a valid admin OIDC bearer', async (_label, key) => {
      const res = await listWorkspaces({
        'x-api-key': key,
        authorization: `Bearer ${await signAdminToken()}`,
      });
      expect(res.statusCode).toBe(401);
    });
  });

  /**
   * The platform listener is a fourth instance of the same shape, not named in
   * the brief for this change but identical in kind: `(await apiKey(req)) ??
   * (await oidc(req))` on the deployment-wide control plane.
   */
  describe('the platform listener must not fall through to OIDC either', () => {
    let adminApp: FastifyInstance;
    let testIssuer: TestIssuer;
    let signPlatformToken: () => Promise<string>;

    beforeAll(async () => {
      testIssuer = await startTestIssuer('platform-fallthrough');
      signPlatformToken = () =>
        new SignJWT({ scopes: ['platform:admin'] })
          .setProtectedHeader({ alg: 'RS256', kid: testIssuer.kid })
          .setIssuer(testIssuer.issuer)
          .setAudience('orca-platform')
          .setSubject('user_platform_fallthrough')
          .setIssuedAt()
          .setExpirationTime('5m')
          .sign(testIssuer.privateKey);

      adminApp = buildAdminApp({
        db,
        oidc: { allowedIssuers: [], audience: 'orca-admin' },
        platformOidc: { allowedIssuers: [testIssuer.issuer], audience: 'orca-platform' },
        store: buildStubStore(),
      });
      await adminApp.ready();
    });

    afterAll(async () => {
      await adminApp.close();
      await stopTestIssuer(testIssuer);
    });

    // A platform-authenticated request for a non-existent organization is a
    // 404 — an authenticated verdict that mutates nothing. 401 means the
    // credential never got that far.
    const createWorkspaceInMissingOrg = (headers: Record<string, string>) =>
      adminApp.inject({
        method: 'POST',
        url: '/v1/platform/organizations/org_missing_fallthrough/workspaces',
        headers,
        payload: { name: 'must not be created' },
      });

    it('authenticates a genuinely valid signed platform OIDC bearer on its own', async () => {
      const res = await createWorkspaceInMissingOrg({
        authorization: `Bearer ${await signPlatformToken()}`,
      });
      expect(res.statusCode).toBe(404);
    });

    it.each([
      ['a malformed x-api-key', 'not-orca-shaped'],
      ['an unknown but well-formed platform key', 'orca_platform_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'],
      ['an admin key on the platform listener', 'orca_admin_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'],
      ['an empty x-api-key', ''],
    ])('rejects %s even with a valid platform OIDC bearer', async (_label, key) => {
      const res = await createWorkspaceInMissingOrg({
        'x-api-key': key,
        authorization: `Bearer ${await signPlatformToken()}`,
      });
      expect(res.statusCode).toBe(401);
    });
  });

  describe('admin listener proxy authorization carrier', () => {
    let adminApp: FastifyInstance;
    let testIssuer: TestIssuer;
    let adminToken: string;
    let platformToken: string;

    type HeaderValue = string | string[];
    type Headers = Record<string, HeaderValue>;

    beforeAll(async () => {
      testIssuer = await startTestIssuer('admin-proxy-authorization');
      await createTestWorkspace(db, uniqueWorkspace('admin-proxy-authorization'));

      adminToken = await new SignJWT({
        organization_id: TEST_ORGANIZATION_ID,
        scopes: ['org:admin'],
      })
        .setProtectedHeader({ alg: 'RS256', kid: testIssuer.kid })
        .setIssuer(testIssuer.issuer)
        .setAudience('orca-admin')
        .setSubject('user_admin_proxy_authorization')
        .setIssuedAt()
        .setExpirationTime('5m')
        .sign(testIssuer.privateKey);

      platformToken = await new SignJWT({ scopes: ['platform:admin'] })
        .setProtectedHeader({ alg: 'RS256', kid: testIssuer.kid })
        .setIssuer(testIssuer.issuer)
        .setAudience('orca-platform')
        .setSubject('user_platform_proxy_authorization')
        .setIssuedAt()
        .setExpirationTime('5m')
        .sign(testIssuer.privateKey);

      adminApp = buildAdminApp({
        db,
        oidc: { allowedIssuers: [testIssuer.issuer], audience: 'orca-admin' },
        platformOidc: { allowedIssuers: [testIssuer.issuer], audience: 'orca-platform' },
        store: buildStubStore(),
      });
      await adminApp.ready();
    });

    afterAll(async () => {
      await adminApp.close();
      await stopTestIssuer(testIssuer);
    });

    const organizationRequest = (headers: Headers) =>
      adminApp.inject({ method: 'GET', url: '/v1/organizations/me', headers });
    const platformRequest = (headers: Headers) =>
      adminApp.inject({
        method: 'POST',
        url: '/v1/platform/organizations/org_missing_proxy_authorization/workspaces',
        headers,
        payload: { name: 'must not be created' },
      });
    const planes = () => [
      {
        label: 'organization-admin',
        token: adminToken,
        authenticatedStatus: 200,
        request: organizationRequest,
      },
      {
        label: 'platform',
        token: platformToken,
        authenticatedStatus: 404,
        request: platformRequest,
      },
    ];

    it('accepts a valid alternate bearer on organization-admin and platform routes', async () => {
      for (const plane of planes()) {
        const response = await plane.request({
          [ADMIN_PROXY_AUTHORIZATION_HEADER]: `Bearer ${plane.token}`,
        });
        expect(response.statusCode, plane.label).toBe(plane.authenticatedStatus);
      }
    });

    it('keeps standard Authorization working on both admin planes', async () => {
      for (const plane of planes()) {
        const response = await plane.request({ authorization: `Bearer ${plane.token}` });
        expect(response.statusCode, plane.label).toBe(plane.authenticatedStatus);
      }
    });

    it('rejects requests carrying both authorization headers', async () => {
      for (const plane of planes()) {
        const response = await plane.request({
          authorization: `Bearer ${plane.token}`,
          [ADMIN_PROXY_AUTHORIZATION_HEADER]: `Bearer ${plane.token}`,
        });
        expectAdminUnauthenticated(response, [plane.token]);
      }
    });

    it('rejects malformed and multi-valued alternate headers', async () => {
      const malformedSecret = 'proxy-malformed-secret-marker';
      for (const plane of planes()) {
        const malformed = await plane.request({
          [ADMIN_PROXY_AUTHORIZATION_HEADER]: `Basic ${malformedSecret}`,
        });
        expectAdminUnauthenticated(malformed, [malformedSecret]);

        const bearer = `Bearer ${plane.token}`;
        const multiValued = await plane.request({
          [ADMIN_PROXY_AUTHORIZATION_HEADER]: [bearer, bearer],
        });
        expectAdminUnauthenticated(multiValued, [plane.token]);
      }
    });

    it('does not let alternate OIDC rescue an invalid x-api-key', async () => {
      for (const plane of planes()) {
        const response = await plane.request({
          'x-api-key': 'invalid-admin-api-key',
          [ADMIN_PROXY_AUTHORIZATION_HEADER]: `Bearer ${plane.token}`,
        });
        expectAdminUnauthenticated(response, [plane.token]);
      }
    });

    it('rejects an invalid alternate JWT without exposing it in the error', async () => {
      const invalidToken = 'invalid.jwt.proxy-secret-marker';
      for (const plane of planes()) {
        const response = await plane.request({
          [ADMIN_PROXY_AUTHORIZATION_HEADER]: `Bearer ${invalidToken}`,
        });
        expectAdminUnauthenticated(response, [invalidToken]);
      }
    });

    it('does not accept the alternate carrier on public or internal listeners', async () => {
      const workspaceId = uniqueWorkspace('proxy-auth-boundary');
      await createTestWorkspace(db, workspaceId);
      const workspaceToken = await new SignJWT({ workspace_id: workspaceId })
        .setProtectedHeader({ alg: 'RS256', kid: testIssuer.kid })
        .setIssuer(testIssuer.issuer)
        .setAudience('orca-managed-agents')
        .setSubject('user_proxy_auth_boundary')
        .setIssuedAt()
        .setExpirationTime('5m')
        .sign(testIssuer.privateKey);
      const internalToken = 'proxy-auth-boundary-internal-token-at-least-32-chars';
      const publicApp = buildPublicApp({
        db,
        oidc: { allowedIssuers: [testIssuer.issuer], audience: 'orca-managed-agents' },
        store: buildStubStore(),
        sse: STUB_SSE_CONFIG,
        jwtMinter: buildTestJwtMinter(),
        fileStore: buildStubFileStore(),
        skillStore: new InMemorySkillStore(),
      });
      const internalApp = buildInternalApp(
        {
          db,
          oidc: { allowedIssuers: [], audience: 'unused-internal-audience' },
          store: buildStubStore(),
          sse: STUB_SSE_CONFIG,
          jwtMinter: buildTestJwtMinter(),
          fileStore: buildStubFileStore(),
        },
        new StaticInternalAuthVerifier(staticTokenSource(internalToken)),
      );
      await Promise.all([publicApp.ready(), internalApp.ready()]);

      try {
        const publicStandard = await publicApp.inject({
          method: 'GET',
          url: '/v1/skills',
          headers: { authorization: `Bearer ${workspaceToken}` },
        });
        const publicAlternate = await publicApp.inject({
          method: 'GET',
          url: '/v1/skills',
          headers: { [ADMIN_PROXY_AUTHORIZATION_HEADER]: `Bearer ${workspaceToken}` },
        });
        expect(publicStandard.statusCode).toBe(200);
        expect(publicAlternate.statusCode).toBe(401);

        const internalPath =
          '/internal/v1/workspaces/ws_proxy_auth_boundary/sessions/ses_missing/executions:prepare';
        const internalStandard = await internalApp.inject({
          method: 'POST',
          url: internalPath,
          headers: { authorization: `Bearer ${internalToken}` },
          payload: {},
        });
        const internalAlternate = await internalApp.inject({
          method: 'POST',
          url: internalPath,
          headers: { [ADMIN_PROXY_AUTHORIZATION_HEADER]: `Bearer ${internalToken}` },
          payload: {},
        });
        expect(internalStandard.statusCode).not.toBe(401);
        expectAdminUnauthenticated(internalAlternate, [internalToken]);
      } finally {
        await Promise.all([publicApp.close(), internalApp.close()]);
      }
    });
  });

  it('authenticates a legacy api key and upgrades its fingerprint', async () => {
    await withIsolatedLegacyCandidates(async () => {
      const plaintext = `orca_legacy_${Date.now()}`;
      const workspaceId = uniqueWorkspace('legacy_upgrade');
      const apiKeyId = `apikey_legacy_upgrade_${Date.now()}`;
      await createTestWorkspace(db, workspaceId);
      await db.insert(apiKeys).values({
        id: apiKeyId,
        workspaceId,
        hashedKey: await hashApiKey(plaintext),
        keyFingerprint: `legacy:${apiKeyId}`,
        principal: 'legacy-upgrade-test',
        scopes: [],
        revokedAt: null,
      });

      const first = await app.inject({
        method: 'GET',
        url: '/v1/skills',
        remoteAddress: '198.51.100.41',
        headers: { 'x-api-key': plaintext },
      });
      expect(first.statusCode).toBe(200);

      const upgraded = await db
        .select({ keyFingerprint: apiKeys.keyFingerprint, lastUsedAt: apiKeys.lastUsedAt })
        .from(apiKeys)
        .where(eq(apiKeys.id, apiKeyId))
        .limit(1);
      expect(upgraded[0]).toEqual({
        keyFingerprint: fingerprintApiKey(plaintext),
        lastUsedAt: expect.any(Date),
      });

      const second = await app.inject({
        method: 'GET',
        url: '/v1/skills',
        remoteAddress: '198.51.100.41',
        headers: { 'x-api-key': plaintext },
      });
      expect(second.statusCode).toBe(200);
    });
  });

  it('fails closed before Argon2 verification when the legacy candidate cap is exceeded', async () => {
    await withIsolatedLegacyCandidates(async () => {
      const plaintext = `orca_legacy_overflow_${Date.now()}`;
      const workspaceId = uniqueWorkspace('legacy_overflow');
      const ids = Array.from(
        { length: LEGACY_API_KEY_FALLBACK_MAX_CANDIDATES + 1 },
        (_, index) => `apikey_legacy_overflow_${Date.now()}_${index}`,
      );
      await createTestWorkspace(db, workspaceId);
      await db.insert(apiKeys).values(
        await Promise.all(
          ids.map(async (id, index) => ({
            id,
            workspaceId,
            hashedKey: index === 0 ? await hashApiKey(plaintext) : 'not-an-argon2-hash',
            keyFingerprint: `legacy:${id}`,
            principal: `legacy-overflow-${index}`,
            scopes: [],
            revokedAt: null,
          })),
        ),
      );

      try {
        const beforeMetrics = await app.inject({ method: 'GET', url: '/metrics' });
        const beforeCapExceeded = legacyFallbackMetricValue(
          beforeMetrics.body,
          'candidate_cap_exceeded',
        );
        const res = await app.inject({
          method: 'GET',
          url: '/v1/skills',
          remoteAddress: '198.51.100.42',
          headers: { 'x-api-key': plaintext },
        });

        expect(res.statusCode).toBe(401);
        const metrics = await app.inject({ method: 'GET', url: '/metrics' });
        expect(legacyFallbackMetricValue(metrics.body, 'candidate_cap_exceeded')).toBe(
          beforeCapExceeded + 1,
        );
        const target = await db
          .select({ keyFingerprint: apiKeys.keyFingerprint })
          .from(apiKeys)
          .where(eq(apiKeys.id, ids[0]!))
          .limit(1);
        expect(target[0]?.keyFingerprint).toBe(`legacy:${ids[0]}`);
      } finally {
        await db.delete(apiKeys).where(inArray(apiKeys.id, ids));
      }
    });
  });

  it('returns an API-compatible 429 when legacy verification quota is exhausted', async () => {
    await withIsolatedLegacyCandidates(async () => {
      const plaintext = `orca_legacy_throttled_${Date.now()}`;
      const workspaceId = uniqueWorkspace('legacy_throttled');
      const apiKeyId = `apikey_legacy_throttled_${Date.now()}`;
      await createTestWorkspace(db, workspaceId);
      await db.insert(apiKeys).values({
        id: apiKeyId,
        workspaceId,
        hashedKey: await hashApiKey(plaintext),
        keyFingerprint: `legacy:${apiKeyId}`,
        principal: 'legacy-throttled-test',
        scopes: [],
        revokedAt: null,
      });

      const throttledApp = buildCombinedTestApp({
        db,
        oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
        store: buildStubStore(),
        sse: STUB_SSE_CONFIG,
        jwtMinter: buildTestJwtMinter(),
        fileStore: buildStubFileStore(),
      });
      await throttledApp.ready();

      try {
        for (let attempt = 0; attempt < 2; attempt += 1) {
          const rejected = await throttledApp.inject({
            method: 'GET',
            url: '/v1/skills',
            remoteAddress: '198.51.100.43',
            headers: { 'x-api-key': `orca_invalid_${attempt}` },
          });
          expect(rejected.statusCode).toBe(401);
        }

        const throttled = await throttledApp.inject({
          method: 'GET',
          url: '/v1/skills',
          remoteAddress: '198.51.100.43',
          headers: { 'x-api-key': plaintext },
        });
        expect(throttled.statusCode).toBe(429);
        expect(Number(throttled.headers['retry-after'])).toBeGreaterThan(0);
        expect(throttled.json()).toMatchObject({
          type: 'error',
          error: {
            type: 'rate_limit_error',
            message: 'legacy api-key verification is temporarily rate limited',
          },
          request_id: expect.any(String),
        });

        const target = await db
          .select({ keyFingerprint: apiKeys.keyFingerprint })
          .from(apiKeys)
          .where(eq(apiKeys.id, apiKeyId))
          .limit(1);
        expect(target[0]?.keyFingerprint).toBe(`legacy:${apiKeyId}`);
      } finally {
        await throttledApp.close();
        await db.delete(apiKeys).where(eq(apiKeys.id, apiKeyId));
      }
    });
  });

  it('does not run legacy fallback verification for upgraded api-key fingerprints', async () => {
    const plaintext = `orca_nonlegacy_fallback_${Date.now()}`;
    const workspaceId = uniqueWorkspace('nonlegacy_fallback');
    await createTestApiKey(db, workspaceId);
    await db.insert(apiKeys).values({
      id: `apikey_nonlegacy_fallback_${Date.now()}`,
      workspaceId,
      hashedKey: await hashApiKey(plaintext),
      keyFingerprint: fingerprintApiKey(`${plaintext}_different_lookup_key`),
      principal: 'nonlegacy-fallback-test',
      scopes: [],
      revokedAt: null,
    });

    const res = await app.inject({
      method: 'GET',
      url: '/v1/skills',
      headers: { 'x-api-key': plaintext },
    });

    expect(res.statusCode).toBe(401);
  });

  it('cannot bind the same plaintext api key to two workspaces', async () => {
    const plaintext = `orca_duplicate_${Date.now()}`;
    const hashedKey = await hashApiKey(plaintext);
    const keyFingerprint = fingerprintApiKey(plaintext);
    const firstWorkspaceId = uniqueWorkspace('duplicate_a');
    const secondWorkspaceId = uniqueWorkspace('duplicate_b');
    await createTestApiKey(db, firstWorkspaceId);
    await createTestApiKey(db, secondWorkspaceId);
    await db.insert(apiKeys).values({
      id: `apikey_duplicate_a_${Date.now()}`,
      workspaceId: firstWorkspaceId,
      hashedKey,
      keyFingerprint,
      principal: 'duplicate-test',
      scopes: [],
      revokedAt: null,
    });

    await expect(
      db.insert(apiKeys).values({
        id: `apikey_duplicate_b_${Date.now()}`,
        workspaceId: secondWorkspaceId,
        hashedKey: await hashApiKey(plaintext),
        keyFingerprint,
        principal: 'duplicate-test',
        scopes: [],
        revokedAt: null,
      }),
    ).rejects.toMatchObject({ code: '23505' });
  });

  it('Bearer token without configured issuers → 401', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/v1/skills',
      headers: { authorization: 'Bearer not-a-real-jwt' },
    });
    expect(res.statusCode).toBe(401);
  });

  it('rejects explicit public workspace selectors instead of silently ignoring them', async () => {
    const bodyResponse = await app.inject({
      method: 'POST',
      url: '/v1/skills',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        workspace_id: uniqueWorkspace('forged_body'),
        name: 'must-not-create',
        slug: `must-not-create-${Date.now()}`,
      },
    });
    expect(bodyResponse.statusCode).toBe(400);
    expect(bodyResponse.json()).toMatchObject({
      type: 'error',
      error: {
        type: 'invalid_request_error',
        message: 'workspace_id is derived from authentication and must not be supplied',
      },
      request_id: expect.any(String),
    });

    const queryResponse = await app.inject({
      method: 'GET',
      url: `/v1/skills?workspace_id=${encodeURIComponent(uniqueWorkspace('forged_query'))}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(queryResponse.statusCode).toBe(400);
    expect(queryResponse.json()).toMatchObject({
      type: 'error',
      error: {
        type: 'invalid_request_error',
        message: 'workspace_id is derived from authentication and must not be supplied',
      },
      request_id: expect.any(String),
    });
  });

  it('/internal/* path skips app-layer auth (route may 404 since not registered)', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/internal/v1/workspaces/ws_unknown/sessions/ses_unknown/vault-credentials/vcrd_xxx/resolve',
      payload: { credential_id: 'vcrd_xxx', vault_id: 'vcrd_xxx' },
    });
    expect(res.statusCode).not.toBe(401);
  });

  it('keeps probes and combined-surface metrics unauthenticated', async () => {
    expect((await app.inject({ method: 'GET', url: '/healthz' })).statusCode).toBe(200);
    expect((await app.inject({ method: 'GET', url: '/readyz' })).statusCode).toBe(200);
    expect((await app.inject({ method: 'GET', url: '/metrics' })).statusCode).toBe(200);
  });

  it('serves capability discovery to an authenticated caller', async () => {
    // The unit specs build the app with an unusable `db`, so this is the only
    // place the authenticated path runs all the way through: a real key, a real
    // workspace lookup, and the body a client actually reads.
    const versions = await app.inject({
      method: 'GET',
      url: '/api',
      headers: { 'x-api-key': apiKey },
    });
    expect(versions.statusCode).toBe(200);
    expect(versions.json()).toEqual({
      kind: 'APIVersions',
      versions: ['v1'],
      preferred_version: 'v1',
    });

    const groups = await app.inject({
      method: 'GET',
      url: '/apis',
      headers: { 'x-api-key': apiKey },
    });
    expect(groups.statusCode).toBe(200);
    // The claim here is that an authenticated caller gets the group document at
    // all; `test/unit/discovery-routes.spec.ts` pins the exact payload. Naming
    // the groups rather than the whole shape keeps this test about auth, so
    // adding a group does not fail it in two places.
    const body = groups.json() as { kind: string; groups: Array<{ name: string }> };
    expect(body.kind).toBe('APIGroupList');
    expect(body.groups.map((group) => group.name)).toEqual(
      expect.arrayContaining(['policy.runorca.ai', 'pricing.runorca.ai', 'runtime.runorca.ai']),
    );
  });

  it('refuses capability discovery without a key', async () => {
    // A 401 rather than a 404 is itself useful: it says the base URL is right
    // and the credential is missing, which is exactly the ambiguity these routes
    // exist to resolve.
    for (const url of ['/api', '/apis']) {
      const res = await app.inject({ method: 'GET', url });
      expect(res.statusCode, url).toBe(401);
      expect(res.json(), url).toEqual({
        type: 'error',
        error: { type: 'authentication_error', message: 'unauthenticated' },
        request_id: expect.any(String),
      });
    }
  });
});
