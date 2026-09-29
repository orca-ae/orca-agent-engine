// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import Fastify from 'fastify';
import { afterEach, describe, expect, it } from 'vitest';
import { rewriteApiV1Alias, rewriteApiV1Path } from '../../src/middleware/api-v1-alias.js';

describe('the /api/v1 alias, as a function', () => {
  it('strips the /api prefix from core paths and keeps the query string', () => {
    expect(rewriteApiV1Path('/api/v1')).toBe('/v1');
    expect(rewriteApiV1Path('/api/v1/agents')).toBe('/v1/agents');
    expect(rewriteApiV1Path('/api/v1/agents/agt_1/versions')).toBe('/v1/agents/agt_1/versions');
    expect(rewriteApiV1Path('/api/v1/agents?limit=10&page=abc')).toBe(
      '/v1/agents?limit=10&page=abc',
    );
  });

  it('leaves the discovery routes alone — /api is a route, not a prefix', () => {
    // Stripping `/api` here would answer the core-version probe with whatever
    // `/` or the empty path routes to, which is the one behaviour this alias
    // must not have.
    expect(rewriteApiV1Path('/api')).toBe('/api');
    expect(rewriteApiV1Path('/apis')).toBe('/apis');
    expect(rewriteApiV1Path('/apis/example.orca.dev/v1/things')).toBe(
      '/apis/example.orca.dev/v1/things',
    );
  });

  it('rewrites nothing else', () => {
    expect(rewriteApiV1Path('/v1/agents')).toBe('/v1/agents');
    expect(rewriteApiV1Path('/healthz')).toBe('/healthz');
    expect(rewriteApiV1Path('/internal/v1/anything')).toBe('/internal/v1/anything');
    // Versions this service does not serve must 404 as themselves rather than
    // be silently downgraded to the one it does.
    expect(rewriteApiV1Path('/api/v2/agents')).toBe('/api/v2/agents');
    expect(rewriteApiV1Path('/api/v1beta/agents')).toBe('/api/v1beta/agents');
    // Not a segment boundary.
    expect(rewriteApiV1Path('/apiv1/agents')).toBe('/apiv1/agents');
    expect(rewriteApiV1Path('/api/')).toBe('/api/');
    expect(rewriteApiV1Path('/')).toBe('/');
  });

  it('answers a request with no URL with something routable', () => {
    expect(rewriteApiV1Alias({})).toBe('/');
    expect(rewriteApiV1Alias({ url: '/api/v1/files' })).toBe('/v1/files');
  });
});

describe('the /api/v1 alias, wired into Fastify', () => {
  const apps: ReturnType<typeof Fastify>[] = [];

  afterEach(async () => {
    await Promise.all(apps.splice(0).map((app) => app.close()));
  });

  function build() {
    const app = Fastify({ rewriteUrl: rewriteApiV1Alias });
    apps.push(app);
    app.get('/v1/agents', async (req) => ({ hit: 'core', seenUrl: req.url }));
    app.get('/api', async () => ({ hit: 'discovery' }));
    return app;
  }

  it('routes an alias request to the canonical route', async () => {
    const app = build();
    const response = await app.inject({ method: 'GET', url: '/api/v1/agents' });
    expect(response.statusCode).toBe(200);
    expect(response.json().hit).toBe('core');
  });

  it('rewrites before routing, so every later hook sees the canonical URL', async () => {
    // The claim the rest of the edge depends on: `rewriteUrl` is the only hook
    // that runs before the router, so by the time a preHandler or an onSend hook
    // reads `req.url` the `/api` prefix is already gone. Asserted rather than
    // assumed — the auth allowlist and the error envelope both key off this URL.
    const app = build();
    const response = await app.inject({ method: 'GET', url: '/api/v1/agents?limit=2' });
    expect(response.json().seenUrl).toBe('/v1/agents?limit=2');
  });

  it('serves /api itself instead of stripping it', async () => {
    const app = build();
    const response = await app.inject({ method: 'GET', url: '/api' });
    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({ hit: 'discovery' });
  });

  it('does not invent a route for a version this deployment does not serve', async () => {
    const app = build();
    await expect(app.inject({ method: 'GET', url: '/api/v2/agents' })).resolves.toMatchObject({
      statusCode: 404,
    });
  });

  it('leaves one canonical path for every downstream req.url consumer', async () => {
    // Why the alias is a rewrite and not a second route mount. Four things key
    // off `req.url` or the matched route: `rejectExplicitWorkspaceSelector`
    // (only inspects `/v1/`), `isClaudeEnvelopePath`, the idempotency scope
    // key, and the unauthenticated allowlist. A second mount would have to
    // teach all four about the alias, and forgetting any one of them is a
    // security or correctness bug rather than a cosmetic gap — most sharply
    // the workspace selector, which would let `POST /api/v1/agents` carry a
    // `workspace_id` the server never authorized.
    const app = Fastify({ rewriteUrl: rewriteApiV1Alias });
    apps.push(app);
    const gatedPaths: string[] = [];
    const scopes: string[] = [];
    app.addHook('preHandler', async (req) => {
      const path = req.url.split('?')[0] ?? '';
      // The exact shape of the `rejectExplicitWorkspaceSelector` guard.
      if (path.startsWith('/v1/')) gatedPaths.push(path);
      // The exact shape of the idempotency scope key.
      scopes.push(`${req.method} ${req.routeOptions.url ?? req.url}`);
    });
    app.post('/v1/agents', async () => ({ ok: true }));

    await app.inject({ method: 'POST', url: '/v1/agents', payload: {} });
    await app.inject({ method: 'POST', url: '/api/v1/agents', payload: {} });

    // The `/v1/`-gated hook fired for both, so the alias cannot slip past it.
    expect(gatedPaths).toEqual(['/v1/agents', '/v1/agents']);
    // One idempotency scope, so replaying a key against the alias replays the
    // same logical write rather than creating a second resource.
    expect(new Set(scopes)).toEqual(new Set(['POST /v1/agents']));
  });
});
