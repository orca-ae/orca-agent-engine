// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { readFileSync } from 'node:fs';
import type { AddressInfo } from 'node:net';
import { afterAll, expect, it, vi } from 'vitest';
import { and, eq } from 'drizzle-orm';
import { importSPKI, jwtVerify } from 'jose';
import { SessionJwtMinter } from '../../src/auth/session-jwt.js';
import { buildSnapshotRecordLoader } from '../../src/api/snapshot-loader.js';
import { AgentSnapshotResolver } from '../../src/domain/agent-snapshot-resolver.js';
import * as harnessBinding from '../../src/domain/session-harness-binding.js';
import { agentVersions, sessions } from '../../src/persistence/postgres/schema.js';
import { buildCombinedTestApp } from '../../src/server.js';
import {
  buildStubFileStore,
  buildStubStore,
  closeTestDb,
  getTestDb,
  STUB_SSE_CONFIG,
} from './setup.js';
import { createTestApiKey, createTestEnvironment, uniqueWorkspace } from './fixtures.js';

const privateKeyPem = readFileSync(
  new URL('../fixtures/session-jwt-private.pem', import.meta.url),
  'utf8',
);
const publicKeyPem = readFileSync(
  new URL('../fixtures/session-jwt-public.pem', import.meta.url),
  'utf8',
);
const route = 'llm-pi-anthropic-anthropic-messages';

afterAll(closeTestDb);

it.each([
  ['pi', 'ai-gateway', true],
  ['pi', 'configured-gateway', true],
  ['pi', 'ai-gateway', false],
  ['pi', 'configured-gateway', false],
  ['claude_code', 'ai-gateway', true],
  ['claude_code', 'configured-gateway', true],
  ['claude_code', 'ai-gateway', false],
  ['claude_code', 'configured-gateway', false],
] as const)('keeps %s scopes for %s (policy=%s)', async (harness, audience, scopedPolicy) => {
  const { db } = await getTestDb();
  const minter = new SessionJwtMinter({
    privateKeyPem,
    issuer: 'orca-registry',
    audience,
    ttlSecs: 300,
  });
  const llmPolicy = { routes: [route], models: ['claude-*'] };
  const app = buildCombinedTestApp({
    db,
    oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
    store: buildStubStore(),
    sse: STUB_SSE_CONFIG,
    jwtMinter: minter,
    ...(scopedPolicy ? { sessionJwtLlmPolicy: llmPolicy } : {}),
    fileStore: buildStubFileStore(),
  });
  await app.listen({ host: '127.0.0.1', port: 0 });
  try {
    const base = 'http://127.0.0.1:' + (app.server.address() as AddressInfo).port;
    const workspaceId = uniqueWorkspace('native_pi_mint');
    const apiKey = await createTestApiKey(db, workspaceId);
    const post = async (path: string, body: unknown) => {
      const response = await fetch(base + path, {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json', 'orca-beta': 'true' },
        body: JSON.stringify(body),
      });
      const result = (await response.json()) as { id: string };
      expect(response.status, JSON.stringify(result)).toBe(200);
      return result;
    };
    const child = await post('/v1/agents', {
      name: 'Roster Sonnet',
      model: { provider: 'anthropic', id: 'claude-sonnet-4-6' },
      tools: [],
      mcp_servers: [],
      skills: [],
    });
    const primary = await post('/v1/agents', {
      name: 'Primary Opus',
      model: { provider: 'anthropic', id: 'claude-opus-4-6' },
      metadata: { harness, mode: 'colocated' },
      multiagent: { type: 'coordinator', agents: [child.id] },
      tools: [],
      mcp_servers: [],
      skills: [],
    });
    const environmentId = await createTestEnvironment(base, apiKey);
    const session = await post('/v1/sessions', {
      agent: { type: 'agent_with_overrides', id: primary.id, model: 'claude-haiku-4-5' },
      environment_id: environmentId,
    });
    const [stored] = await db
      .select({ overrides: sessions.agentOverrides })
      .from(sessions)
      .where(eq(sessions.id, session.id));
    expect(stored?.overrides).toMatchObject({
      model: { provider: 'anthropic', id: 'claude-haiku-4-5' },
    });
    const publicKey = await importSPKI(publicKeyPem, 'RS256');
    const mint = async (requestedAudience?: string) => {
      const response = await fetch(
        base + '/internal/v1/workspaces/' + workspaceId + '/sessions/' + session.id + '/mint-jwt',
        {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({
            ...(requestedAudience ? { audience: requestedAudience } : {}),
            harness: 'pi',
            ttl_secs: 86400,
            org_id: 'caller-organization',
            llm_routes: ['caller-route'],
            llm_models: ['caller-model'],
          }),
        },
      );
      expect(response.status).toBe(200);
      const { token } = (await response.json()) as { token: string };
      return (
        await jwtVerify(token, publicKey, {
          audience: requestedAudience ?? audience,
          issuer: 'orca-registry',
        })
      ).payload;
    };
    const minted = await mint();
    const explicitDefault = await mint(audience);
    expect(explicitDefault.llm_routes).toEqual(minted.llm_routes);
    expect(explicitDefault.llm_models).toEqual(minted.llm_models);
    expect(explicitDefault.agent_id).toEqual(minted.agent_id);
    expect(explicitDefault.exp! - explicitDefault.iat!).toBe(minted.exp! - minted.iat!);
    if (harness === 'pi') {
      expect(minted.agent_id).toBe(primary.id);
      expect(minted.workspace_id).toBe(workspaceId);
      expect(minted.session_id).toBe(session.id);
      expect(minted.guardrail_ids).toEqual([]);
      expect(minted.runtime_config_revision).toBe('1');
    }
    if (harness === 'pi' && scopedPolicy) {
      const resolver = new AgentSnapshotResolver({
        loader: buildSnapshotRecordLoader(db),
        minter,
        llmPolicy,
        gatewayMcpUrl: 'https://gateway.example/v1/mcp',
        gatewayLlmUrl: 'https://gateway.example/v1/llm',
      });
      const snapshot = await resolver.resolve(session.id);
      if (snapshot?.egress.mode !== 'gateway') throw new Error('expected gateway');
      const expected = (await jwtVerify(snapshot.egress.gateway.llm_jwt!, publicKey, { audience }))
        .payload;
      expect(minted.llm_routes).toEqual(expected.llm_routes);
      expect(minted.llm_models).toEqual(['claude-haiku-4-5']);
      expect(minted.llm_models).toEqual(expected.llm_models);
      expect(minted.exp! - minted.iat!).toBe(660);
      await post('/v1/sessions/' + session.id, { agent: { model: { id: 'claude-opus-4-6' } } });
      expect((await mint()).llm_models).toEqual(['claude-opus-4-6']);
    } else if (audience === 'ai-gateway' && scopedPolicy) {
      expect(minted.llm_models).toEqual(['claude-haiku-4-5', 'claude-sonnet-4-6']);
      expect(minted.exp! - minted.iat!).toBe(300);
    } else {
      expect(minted.llm_routes).toBeUndefined();
      expect(minted.llm_models).toBeUndefined();
      expect(minted.exp! - minted.iat!).toBe(300);
      if (harness !== 'pi' && audience !== 'ai-gateway') expect(minted.agent_id).toBeUndefined();
    }
    for (const explicit of ['git-creds', 'llm-proxy']) {
      const token = await mint(explicit);
      expect(token.llm_routes).toBeUndefined();
      expect(token.llm_models).toBeUndefined();
      expect(token.agent_id).toBeUndefined();
      expect(token.exp! - token.iat!).toBe(300);
    }
    if (audience !== 'ai-gateway') {
      const token = await mint('ai-gateway');
      expect(token.llm_routes).toEqual(harness === 'pi' || !scopedPolicy ? undefined : [route]);
    }
    const pinned = and(
      eq(agentVersions.workspaceId, workspaceId),
      eq(agentVersions.agentId, primary.id),
      eq(agentVersions.version, 1),
    );
    const [version] = await db.select().from(agentVersions).where(pinned);
    if (!version) throw new Error('missing test Agent version');
    // Simulate an unresolvable binding while retaining the valid MCP snapshot.
    await db
      .update(agentVersions)
      .set({
        snapshot: {
          ...(version.snapshot as Record<string, unknown>),
          metadata: { harness: 'unknown_legacy_harness' },
        },
      })
      .where(pinned);
    for (const explicit of ['git-creds', 'llm-proxy']) {
      const token = await mint(explicit);
      expect(token.llm_routes).toBeUndefined();
      expect(token.llm_models).toBeUndefined();
    }
    if (!scopedPolicy) expect((await mint()).llm_models).toBeUndefined();
    if (scopedPolicy && (harness === 'pi' || audience === 'ai-gateway')) {
      const rejected = await fetch(
        base + '/internal/v1/workspaces/' + workspaceId + '/sessions/' + session.id + '/mint-jwt',
        { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{}' },
      );
      expect(rejected.status).toBe(409);
      const error = await rejected.json();
      expect(error).toMatchObject({ error: 'invalid_runtime_binding' });
      expect(error).not.toHaveProperty('token');
      const failure = vi
        .spyOn(harnessBinding, 'loadSessionHarnessBinding')
        .mockRejectedValueOnce(new Error('simulated binding query failure'));
      try {
        const unavailable = await fetch(
          base + '/internal/v1/workspaces/' + workspaceId + '/sessions/' + session.id + '/mint-jwt',
          { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{}' },
        );
        expect(unavailable.status).toBe(500);
        expect(await unavailable.json()).not.toHaveProperty('token');
      } finally {
        failure.mockRestore();
      }
    }
  } finally {
    await app.close();
  }
});
