// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { FastifyInstance } from 'fastify';
import type { AddressInfo } from 'node:net';
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

describe('Anthropic SDK round-trip (integration)', () => {
  let app: FastifyInstance;
  let baseURL: string;
  let apiKey: string;

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
    await app.listen({ host: '127.0.0.1', port: 0 });
    const addr = app.server.address() as AddressInfo;
    baseURL = `http://127.0.0.1:${addr.port}`;
    apiKey = await createTestApiKey(db, uniqueWorkspace('sdk'));
  });

  afterAll(async () => {
    await app.close();
    await closeTestDb();
  });

  it('SDK-shaped POST + GET round-trips an agent', async () => {
    const Anthropic = (await import('@anthropic-ai/sdk')).default;
    const client = new Anthropic({ baseURL, apiKey });
    const created = await client.beta.agents.create({
      name: `sdk-${Date.now()}`,
      model: 'claude-sonnet-4-6',
      tools: [{ type: 'agent_toolset_20260401' }],
    });

    // Explicit Orca compatibility exception: resource ID prefixes remain in
    // their established self-hosted form even on the Claude response model.
    expect(created.id).toMatch(/^agt_/);
    expect(created.type).toBe('agent');
    expect(created.model).toMatchObject({ id: 'claude-sonnet-4-6', speed: 'standard' });
    expect(created.tools[0]).toMatchObject({
      type: 'agent_toolset_20260401',
      default_config: { enabled: true, permission_policy: { type: 'always_allow' } },
    });

    const got = await client.beta.agents.retrieve(created.id);
    expect(got.id).toBe(created.id);
    expect(got.tools[0]?.type).toBe('agent_toolset_20260401');
  }, 30000);
});
