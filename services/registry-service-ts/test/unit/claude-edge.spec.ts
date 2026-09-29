// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { FileStore } from '@orca/file-store';
import { InMemorySkillStore } from '@orca/skill-store';
import type { TranscriptStore } from '@orca/transcript-store';
import Fastify from 'fastify';
import { afterEach, describe, expect, it } from 'vitest';
import type { SessionJwtMinter } from '../../src/auth/session-jwt.js';
import { rewriteApiV1Alias } from '../../src/middleware/api-v1-alias.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import { buildPublicApp } from '../../src/server.js';
import {
  errorEnvelope,
  errorTypeForStatus,
  isClaudeEnvelopePath,
  registerClaudePublicEdge,
} from '../../src/middleware/claude-edge.js';

describe('Claude public edge', () => {
  const apps: ReturnType<typeof Fastify>[] = [];

  afterEach(async () => {
    await Promise.all(apps.splice(0).map((app) => app.close()));
  });

  it('wraps flat public errors with a request id in the body', async () => {
    const app = Fastify();
    apps.push(app);
    registerClaudePublicEdge(app);
    app.get('/v1/failure', async (_req, reply) => reply.code(401).send({ error: 'bad key' }));

    const response = await app.inject({ method: 'GET', url: '/v1/failure' });
    expect(response.statusCode).toBe(401);
    expect(response.json()).toMatchObject({
      type: 'error',
      error: { type: 'authentication_error', message: 'bad key' },
      request_id: expect.any(String),
    });
  });

  it('normalizes default not-found and uncaught errors without leaking exception details', async () => {
    const app = Fastify();
    apps.push(app);
    registerClaudePublicEdge(app);
    app.get('/v1/throws', async () => {
      throw new Error('database password must not leak');
    });

    const missing = await app.inject({ method: 'GET', url: '/v1/missing' });
    expect(missing.json().error.type).toBe('not_found_error');
    expect(missing.json().request_id).toEqual(expect.any(String));

    const thrown = await app.inject({ method: 'GET', url: '/v1/throws' });
    expect(thrown.json()).toMatchObject({
      type: 'error',
      error: { type: 'api_error', message: 'Internal server error' },
      request_id: expect.any(String),
    });
    expect(thrown.body).not.toContain('database password');
  });

  it('preserves a canonical Memory error but refreshes its request id', async () => {
    const app = Fastify();
    apps.push(app);
    registerClaudePublicEdge(app);
    app.post('/v1/memory_stores/test', async (_req, reply) =>
      reply.code(409).send({
        type: 'error',
        error: {
          type: 'memory_path_conflict_error',
          message: 'path exists',
          conflicting_memory_id: 'mem_other',
        },
        request_id: 'stale-id',
      }),
    );

    const response = await app.inject({ method: 'POST', url: '/v1/memory_stores/test' });
    expect(response.json()).toMatchObject({
      type: 'error',
      error: {
        type: 'memory_path_conflict_error',
        message: 'path exists',
        conflicting_memory_id: 'mem_other',
      },
      request_id: expect.any(String),
    });
  });

  it('does not rewrite internal or probe responses', async () => {
    const app = Fastify();
    apps.push(app);
    registerClaudePublicEdge(app);
    app.get('/internal/failure', async (_req, reply) => reply.code(400).send({ error: 'legacy' }));
    // The prefix list says "the core API and the API-group tree", not "anything
    // a public client can reach" — and the probes are the reason the two
    // descriptions are not the same. An orchestrator reads the status code; an
    // `error.type` and a `request_id` would dress a liveness check up as an API
    // call. Asserted so the comment and the code cannot drift apart.
    app.get('/healthz', async (_req, reply) => reply.code(503).send({ status: 'draining' }));

    const internal = await app.inject({ method: 'GET', url: '/internal/failure' });
    expect(internal.json()).toEqual({ error: 'legacy' });

    const probe = await app.inject({ method: 'GET', url: '/healthz' });
    expect(probe.statusCode).toBe(503);
    expect(probe.json()).toEqual({ status: 'draining' });
  });

  it('matches whole path segments, so /apis is not a suffix of /api', () => {
    expect(isClaudeEnvelopePath('/v1')).toBe(true);
    expect(isClaudeEnvelopePath('/v1/agents?limit=1')).toBe(true);
    expect(isClaudeEnvelopePath('/api')).toBe(true);
    expect(isClaudeEnvelopePath('/apis')).toBe(true);
    expect(isClaudeEnvelopePath('/apis/example.orca.dev/v1/things')).toBe(true);
    expect(isClaudeEnvelopePath('/v1x/agents')).toBe(false);
    expect(isClaudeEnvelopePath('/apiary')).toBe(false);
    expect(isClaudeEnvelopePath('/healthz')).toBe(false);
    expect(isClaudeEnvelopePath('/readyz')).toBe(false);
    expect(isClaudeEnvelopePath('/internal/v1/anything')).toBe(false);
  });

  describe('past /v1', () => {
    /**
     * One app carrying the same two pieces production wires together: the
     * `/api/v1` rewrite in the Fastify factory and the envelope hook on top.
     * Every assertion below is about how those two compose.
     */
    function build() {
      const app = Fastify({ rewriteUrl: rewriteApiV1Alias });
      apps.push(app);
      registerClaudePublicEdge(app);
      app.get('/v1/authfail', async (_req, reply) => reply.code(401).send({ error: 'bad key' }));
      app.get('/v1/forbidden', async (_req, reply) =>
        reply.code(403).send({ error: 'not your workspace' }),
      );
      app.post('/v1/validated', {
        schema: {
          body: { type: 'object', required: ['name'], properties: { name: { type: 'string' } } },
        },
        handler: async () => ({ ok: true }),
      });
      app.get('/v1/boom', async () => {
        throw new Error('connection string must not leak');
      });
      app.get('/apis/example.orca.dev/v1/things', async (_req, reply) =>
        reply.code(403).send({ error: 'group not enabled' }),
      );
      app.get('/apis', async () => ({ kind: 'APIGroupList', groups: [] }));
      return app;
    }

    const fullEnvelope = (type: string, message: string) => ({
      type: 'error',
      error: { type, message },
      request_id: expect.any(String),
    });

    it('answers an /api/v1 failure with the envelope, because the rewrite ran first', async () => {
      // The claim being tested is about framework ordering, not about the
      // prefix list: `rewriteUrl` is documented as running before routing, so
      // `/api/v1/authfail` must reach the `/v1/authfail` handler and be
      // enveloped by the `/v1` root. Assumptions about hook order have been
      // wrong here before, so this is asserted against a real instance.
      const app = build();
      const response = await app.inject({ method: 'GET', url: '/api/v1/authfail' });
      expect(response.statusCode).toBe(401);
      expect(response.json()).toEqual(fullEnvelope('authentication_error', 'bad key'));
    });

    it('envelopes a miss under /api/v1 as a /v1 miss', async () => {
      const app = build();
      const response = await app.inject({ method: 'GET', url: '/api/v1/nonexistent' });
      expect(response.statusCode).toBe(404);
      expect(response.json()).toEqual(
        fullEnvelope('not_found_error', 'Route GET:/v1/nonexistent not found'),
      );
    });

    it('envelopes extension-group paths', async () => {
      const app = build();
      const denied = await app.inject({
        method: 'GET',
        url: '/apis/example.orca.dev/v1/things',
      });
      expect(denied.statusCode).toBe(403);
      expect(denied.json()).toEqual(fullEnvelope('permission_error', 'group not enabled'));

      const missing = await app.inject({ method: 'GET', url: '/apis/nope/v1/things' });
      expect(missing.statusCode).toBe(404);
      expect(missing.json().type).toBe('error');
      expect(missing.json().error.type).toBe('not_found_error');
    });

    it('covers the failure classes a client can hit', async () => {
      const app = build();

      const forbidden = await app.inject({
        method: 'GET',
        url: '/apis/example.orca.dev/v1/things',
      });
      expect(forbidden.json()).toEqual(fullEnvelope('permission_error', 'group not enabled'));

      const invalid = await app.inject({ method: 'POST', url: '/v1/validated', payload: {} });
      expect(invalid.statusCode).toBe(400);
      expect(invalid.json()).toEqual(
        fullEnvelope('invalid_request_error', "body must have required property 'name'"),
      );

      const failed = await app.inject({ method: 'GET', url: '/v1/boom' });
      expect(failed.statusCode).toBe(500);
      expect(failed.json()).toEqual(fullEnvelope('api_error', 'Internal server error'));
      expect(failed.body).not.toContain('connection string');
    });

    it('leaves a successful discovery response exactly as the handler wrote it', async () => {
      // Negative control. The hook keys off `statusCode >= 400`; widening the
      // prefix list must not turn a 200 into an envelope.
      const app = build();
      const response = await app.inject({ method: 'GET', url: '/apis' });
      expect(response.statusCode).toBe(200);
      expect(response.json()).toEqual({ kind: 'APIGroupList', groups: [] });
    });
  });

  describe('on the real public listener', () => {
    function options() {
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

    it('envelopes an unauthenticated call, whether it arrives at /v1 or /api/v1', async () => {
      const app = buildPublicApp(options());
      apps.push(app);
      await app.ready();

      for (const url of ['/v1/agents', '/api/v1/agents']) {
        const response = await app.inject({ method: 'GET', url });
        expect(response.statusCode, url).toBe(401);
        expect(response.json(), url).toEqual({
          type: 'error',
          error: { type: 'authentication_error', message: 'unauthenticated' },
          request_id: expect.any(String),
        });
      }
    });

    it('envelopes an unauthenticated discovery call', async () => {
      // The `/api` and `/apis` prefixes carried only 404s when discovery was
      // unauthenticated. They now carry the 401 too, so the entry is load-bearing
      // on the path a first-time client actually takes.
      const app = buildPublicApp(options());
      apps.push(app);
      await app.ready();

      for (const url of ['/api', '/apis']) {
        const response = await app.inject({ method: 'GET', url });
        expect(response.statusCode, url).toBe(401);
        expect(response.json(), url).toEqual({
          type: 'error',
          error: { type: 'authentication_error', message: 'unauthenticated' },
          request_id: expect.any(String),
        });
      }
    });
  });

  it('does not race handlers that send a successful response without returning reply', async () => {
    const app = Fastify();
    apps.push(app);
    registerClaudePublicEdge(app);
    app.get('/v1/success', async (_req, reply) => {
      reply.send({ ok: true });
    });

    const response = await app.inject({ method: 'GET', url: '/v1/success' });
    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({ ok: true });
  });

  it('maps relevant HTTP statuses to the Claude error union', () => {
    expect(errorTypeForStatus(400)).toBe('invalid_request_error');
    expect(errorTypeForStatus(403)).toBe('permission_error');
    expect(errorTypeForStatus(409)).toBe('conflict_error');
    expect(errorTypeForStatus(413)).toBe('request_too_large');
    expect(errorTypeForStatus(429)).toBe('rate_limit_error');
    expect(errorTypeForStatus(503)).toBe('overloaded_error');
    expect(errorTypeForStatus(529)).toBe('overloaded_error');
    expect(errorEnvelope(400, null, undefined).request_id).toBeNull();
  });

  it('prefers Fastify detail messages over generic HTTP labels', () => {
    expect(
      errorEnvelope(400, 'req_test', {
        statusCode: 400,
        error: 'Bad Request',
        message: 'body.name must be a string',
      }),
    ).toMatchObject({
      error: {
        type: 'invalid_request_error',
        message: 'body.name must be a string',
      },
    });
  });
});
