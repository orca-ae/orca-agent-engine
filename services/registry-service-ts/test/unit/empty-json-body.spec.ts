// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import Fastify, { type FastifyInstance } from 'fastify';

// Reproduces the empty-JSON-body content-type parser behavior in isolation: a request
// with `Content-Type: application/json` and an empty (zero-byte) body must be
// accepted (parsed as `{}`) instead of failing with FST_ERR_CTP_EMPTY_JSON_BODY,
// while a malformed non-empty body still returns 400.
function buildTestApp(): FastifyInstance {
  const app = Fastify({ logger: false });
  app.addContentTypeParser('application/json', { parseAs: 'string' }, (_req, body, done) => {
    const text = (body as string) ?? '';
    if (text.trim().length === 0) {
      done(null, {});
      return;
    }
    try {
      done(null, JSON.parse(text));
    } catch (err) {
      (err as { statusCode?: number }).statusCode = 400;
      done(err as Error, undefined);
    }
  });
  app.post('/v1/agents/:id/archive', async (req) => ({ body: req.body }));
  app.delete('/v1/agents/:id', async (req) => ({ body: req.body }));
  return app;
}

describe('empty application/json body parser', () => {
  it('accepts a zero-byte body on POST archive and parses it as {}', async () => {
    const app = buildTestApp();
    const res = await app.inject({
      method: 'POST',
      url: '/v1/agents/agent_123/archive',
      headers: { 'content-type': 'application/json' },
    });
    expect(res.statusCode).toBe(200);
    expect(res.json()).toEqual({ body: {} });
    await app.close();
  });

  it('accepts a zero-byte body on DELETE and parses it as {}', async () => {
    const app = buildTestApp();
    const res = await app.inject({
      method: 'DELETE',
      url: '/v1/agents/agent_123',
      headers: { 'content-type': 'application/json' },
    });
    expect(res.statusCode).toBe(200);
    expect(res.json()).toEqual({ body: {} });
    await app.close();
  });

  it('treats a whitespace-only body as {}', async () => {
    const app = buildTestApp();
    const res = await app.inject({
      method: 'POST',
      url: '/v1/agents/agent_123/archive',
      headers: { 'content-type': 'application/json' },
      payload: '   \n  ',
    });
    expect(res.statusCode).toBe(200);
    expect(res.json()).toEqual({ body: {} });
    await app.close();
  });

  it('still rejects a malformed non-empty JSON body with 400', async () => {
    const app = buildTestApp();
    const res = await app.inject({
      method: 'POST',
      url: '/v1/agents/agent_123/archive',
      headers: { 'content-type': 'application/json' },
      payload: '{bad',
    });
    expect(res.statusCode).toBe(400);
    await app.close();
  });

  it('still parses a well-formed non-empty JSON body', async () => {
    const app = buildTestApp();
    const res = await app.inject({
      method: 'POST',
      url: '/v1/agents/agent_123/archive',
      headers: { 'content-type': 'application/json' },
      payload: JSON.stringify({ reason: 'cleanup' }),
    });
    expect(res.statusCode).toBe(200);
    expect(res.json()).toEqual({ body: { reason: 'cleanup' } });
    await app.close();
  });
});
