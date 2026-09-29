// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { FastifyInstance } from 'fastify';
import type { AddressInfo } from 'node:net';
import {
  getTestDb,
  closeTestDb,
  buildTestStore,
  STUB_SSE_CONFIG,
  buildTestJwtMinter,
  buildStubFileStore,
} from './setup.js';
import {
  uniqueWorkspace,
  createTestApiKey,
  createTestAgent,
  createTestSession,
} from './fixtures.js';
import { buildCombinedTestApp } from '../../src/server.js';
import type { KafkaTranscriptStore } from '@orca/transcript-store';

describe('events idempotency (integration)', () => {
  let app: FastifyInstance;
  let baseURL: string;
  let apiKey: string;
  let store: KafkaTranscriptStore;

  beforeAll(async () => {
    store = buildTestStore();
    const { db } = await getTestDb();
    app = buildCombinedTestApp({
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store,
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
    });
    await app.listen({ host: '127.0.0.1', port: 0 });
    baseURL = `http://127.0.0.1:${(app.server.address() as AddressInfo).port}`;
    apiKey = await createTestApiKey(db, uniqueWorkspace('idem-evt'));
  });

  afterAll(async () => {
    await app.close();
    await store.close();
    await closeTestDb();
  });

  it('replaying POST /events 3x produces a single set of events', async () => {
    const agentId = await createTestAgent(baseURL, apiKey);
    const sessionId = await createTestSession(baseURL, apiKey, agentId);
    const idemKey = `idem-evt-${Date.now()}`;
    const body = JSON.stringify({
      events: [{ type: 'user.message', content: [{ type: 'text', text: 'replay' }] }],
      request_id: idemKey,
    });
    const responses: Array<{ data: Array<{ id: string }> }> = [];
    for (let i = 0; i < 3; i++) {
      const r = await fetch(`${baseURL}/v1/sessions/${sessionId}/events`, {
        method: 'POST',
        headers: {
          'x-api-key': apiKey,
          'content-type': 'application/json',
          'idempotency-key': idemKey,
        },
        body,
      });
      responses.push((await r.json()) as { data: Array<{ id: string }> });
    }
    expect(responses[1]).toEqual(responses[0]);
    expect(responses[2]).toEqual(responses[0]);

    const list = await fetch(`${baseURL}/v1/sessions/${sessionId}/events`, {
      headers: { 'x-api-key': apiKey },
    });
    const got = (await list.json()) as { data: Array<{ id: string }> };
    expect(got.data).toHaveLength(1);
    expect(got.data[0]!.id).toBe(responses[0]!.data[0]!.id);
  }, 30000);
});
