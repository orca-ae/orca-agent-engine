// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { FastifyInstance } from 'fastify';
import {
  getTestDb,
  closeTestDb,
  buildStubStore,
  STUB_SSE_CONFIG,
  buildTestJwtMinter,
  buildStubFileStore,
} from './setup.js';
import { uniqueWorkspace, createTestApiKey } from './fixtures.js';
import { buildCombinedTestApp } from '../../src/server.js';

describe('Idempotency (integration)', () => {
  let app: FastifyInstance;
  let apiKey: string;
  const model = { provider: 'anthropic', id: 'claude-3-5-sonnet-20240620' };

  beforeAll(async () => {
    const { db } = await getTestDb();
    app = buildCombinedTestApp({
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store: buildStubStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
    });
    await app.ready();
    apiKey = await createTestApiKey(db, uniqueWorkspace('idem'));
  });
  afterAll(async () => {
    await app.close();
    await closeTestDb();
  });

  it('replays the same response on retry with the same Idempotency-Key', async () => {
    const idem = `idem-${Date.now()}`;
    const payload = { name: `Idempotent agent ${Date.now()}`, model };
    const headers = {
      'x-api-key': apiKey,
      'content-type': 'application/json',
      'idempotency-key': idem,
      'orca-beta': '1',
    };

    const first = await app.inject({ method: 'POST', url: '/v1/agents', headers, payload });
    expect(first.statusCode).toBe(200);
    const firstBody = first.body;

    const second = await app.inject({ method: 'POST', url: '/v1/agents', headers, payload });
    expect(second.statusCode).toBe(200);
    expect(second.body).toBe(firstBody); // byte-for-byte replay
  });

  it('returns 409 when reusing the key with a different body', async () => {
    const idem = `idem-conflict-${Date.now()}`;
    const headers = {
      'x-api-key': apiKey,
      'content-type': 'application/json',
      'idempotency-key': idem,
      'orca-beta': '1',
    };

    const first = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers,
      payload: { name: `Agent A ${Date.now()}`, model },
    });
    expect(first.statusCode).toBe(200);

    const second = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers,
      payload: { name: `Agent B ${Date.now()}`, model },
    });
    expect(second.statusCode).toBe(409);
    expect(second.json()).toMatchObject({
      type: 'error',
      error: {
        type: 'conflict_error',
        message: expect.stringMatching(/idempotency/),
      },
      request_id: expect.any(String),
    });
  });
});
