// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createServer, type Server } from 'node:http';
import { exportJWK, generateKeyPair, SignJWT } from 'jose';
import type { KeyLike } from 'jose';
import type { FastifyInstance } from 'fastify';
import { and, eq } from 'drizzle-orm';
import type { Event, TranscriptStore } from '@orca/transcript-store';
import {
  buildStubFileStore,
  buildTestJwtMinter,
  closeTestDb,
  getTestDb,
  STUB_SSE_CONFIG,
} from './setup.js';
import { createTestApiKey, uniqueWorkspace } from './fixtures.js';
import { buildCombinedTestApp } from '../../src/server.js';
import { oidcTranscriptUserId } from '../../src/domain/events.js';
import { reconcileSessionLifecycleOutbox } from '../../src/domain/session-lifecycle-outbox.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import { sessionLifecycleOutbox } from '../../src/persistence/postgres/schema.js';

interface TestIssuer {
  server: Server;
  issuer: string;
  kid: string;
  privateKey: KeyLike;
}

async function startTestIssuer(): Promise<TestIssuer> {
  const { publicKey, privateKey } = await generateKeyPair('RS256');
  const kid = `session-event-attribution-${Date.now()}`;
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
  if (address === null || typeof address === 'string') throw new Error('missing JWKS server port');
  return { server, issuer: `http://127.0.0.1:${address.port}`, kid, privateKey };
}

async function stopTestIssuer(issuer: TestIssuer): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    issuer.server.close((error) => (error ? reject(error) : resolve()));
  });
}

describe('session event OIDC user attribution (integration)', () => {
  let app: FastifyInstance;
  let db: DbClient;
  let apiKey: string;
  let agentId: string;
  let environmentId: string;
  let issuer: TestIssuer;
  const workspaceId = uniqueWorkspace('session_event_attribution');
  const oidcUserId = 'user_verified_oidc_subject';
  const appendedEvents: Event[] = [];
  let failInitialAppend = false;

  const store: TranscriptStore = {
    async append(_workspaceId, _sessionId, events) {
      if (failInitialAppend && events.some((event) => event.idempotencyKey.includes(':initial:'))) {
        throw new Error('initial transcript append unavailable');
      }
      appendedEvents.push(...events);
      return events.map((event) => event.id);
    },
    async *read(workspaceId, sessionId) {
      for (const event of appendedEvents) {
        if (event.workspaceId === workspaceId && event.sessionId === sessionId) yield event;
      }
    },
    async *tail() {
      // This witness captures append and read paths only.
    },
    async archive() {},
    async close() {},
  };

  async function oidcToken(): Promise<string> {
    return new SignJWT({ workspace_id: workspaceId })
      .setProtectedHeader({ alg: 'RS256', kid: issuer.kid })
      .setIssuer(issuer.issuer)
      .setAudience('orca-managed-agents')
      .setSubject(oidcUserId)
      .setIssuedAt()
      .setExpirationTime('5m')
      .sign(issuer.privateKey);
  }

  async function principalOnlyOidcToken(): Promise<string> {
    return new SignJWT({ workspace_id: workspaceId, principal: 'legacy_oidc_principal' })
      .setProtectedHeader({ alg: 'RS256', kid: issuer.kid })
      .setIssuer(issuer.issuer)
      .setAudience('orca-managed-agents')
      .setIssuedAt()
      .setExpirationTime('5m')
      .sign(issuer.privateKey);
  }

  beforeAll(async () => {
    ({ db } = await getTestDb());
    issuer = await startTestIssuer();
    app = buildCombinedTestApp({
      db,
      oidc: { allowedIssuers: [issuer.issuer], audience: 'orca-managed-agents' },
      store,
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
    });
    await app.ready();
    apiKey = await createTestApiKey(db, workspaceId);

    const agent = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        name: `attribution-agent-${Date.now()}`,
        model: { provider: 'anthropic', id: 'claude-3-5-sonnet-20240620' },
        tools: [],
        mcp_servers: [],
        skills: [],
        metadata: {},
      },
    });
    expect(agent.statusCode).toBe(200);
    agentId = (agent.json() as { id: string }).id;

    const environment = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { name: `attribution-environment-${Date.now()}`, config: { type: 'cloud' } },
    });
    expect(environment.statusCode).toBe(200);
    environmentId = (environment.json() as { id: string }).id;
  });

  afterAll(async () => {
    await app?.close();
    if (issuer) await stopTestIssuer(issuer);
    await closeTestDb();
  });

  it('stamps verified OIDC subjects through initial-event recovery and event append', async () => {
    const expectedTranscriptUserId = oidcTranscriptUserId(issuer.issuer, oidcUserId)!;
    failInitialAppend = true;
    const create = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: {
        authorization: `Bearer ${await oidcToken()}`,
        'content-type': 'application/json',
        user_id: 'user_header_spoof_initial',
      },
      payload: {
        agent_id: agentId,
        environment_id: environmentId,
        initial_events: [
          { type: 'user.message', content: [{ type: 'text', text: 'OIDC initial event' }] },
        ],
      },
    });
    failInitialAppend = false;

    expect(create.statusCode).toBe(200);
    const sessionId = (create.json() as { id: string }).id;
    const [pending] = await db
      .select({
        id: sessionLifecycleOutbox.id,
        events: sessionLifecycleOutbox.events,
        publishedAt: sessionLifecycleOutbox.publishedAt,
      })
      .from(sessionLifecycleOutbox)
      .where(
        and(
          eq(sessionLifecycleOutbox.workspaceId, workspaceId),
          eq(sessionLifecycleOutbox.sessionId, sessionId),
          eq(sessionLifecycleOutbox.kind, 'session.initial_events'),
        ),
      )
      .limit(1);
    expect(pending?.publishedAt).toBeNull();
    const [durableInitialEvent] = pending!.events as Array<Record<string, unknown>>;
    expect(durableInitialEvent).toMatchObject({ userId: expectedTranscriptUserId });

    expect(await reconcileSessionLifecycleOutbox(db, store, { eventIds: [pending!.id] })).toEqual({
      processed: 1,
      published: 1,
      failed: 0,
    });
    expect(appendedEvents.find((event) => event.id === pending!.id)?.userId).toBe(
      expectedTranscriptUserId,
    );

    const post = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${sessionId}/events`,
      headers: {
        authorization: `Bearer ${await oidcToken()}`,
        'content-type': 'application/json',
      },
      payload: {
        events: [{ type: 'user.message', content: [{ type: 'text', text: 'OIDC POST event' }] }],
      },
    });
    expect(post.statusCode).toBe(200);
    const postedEvent = (post.json() as { data: Array<Record<string, unknown> & { id: string }> })
      .data[0]!;
    expect(postedEvent).not.toHaveProperty('userId');
    expect(postedEvent).not.toHaveProperty('user_id');
    expect(appendedEvents.find((event) => event.id === postedEvent.id)?.userId).toBe(
      expectedTranscriptUserId,
    );

    const listed = await app.inject({
      method: 'GET',
      url: `/v1/sessions/${sessionId}/events`,
      headers: { authorization: `Bearer ${await oidcToken()}` },
    });
    expect(listed.statusCode).toBe(200);
    const listedEvent = (
      listed.json() as { data: Array<Record<string, unknown> & { id: string }> }
    ).data.find((event) => event.id === postedEvent.id);
    expect(listedEvent).toBeDefined();
    expect(listedEvent).not.toHaveProperty('userId');
    expect(listedEvent).not.toHaveProperty('user_id');

    const spoofed = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${sessionId}/events`,
      headers: {
        authorization: `Bearer ${await oidcToken()}`,
        'content-type': 'application/json',
        'orca-beta': 'managed-agents-2026-04-01',
        user_id: 'user_header_spoof_post',
      },
      payload: {
        events: [
          {
            type: 'user.message',
            content: [{ type: 'text', text: 'spoof isolation' }],
            user_id: 'user_payload_spoof_post',
          },
        ],
      },
    });
    expect(spoofed.statusCode).toBe(200);
    const spoofedEvent = (
      spoofed.json() as { events: Array<Record<string, unknown> & { id: string }> }
    ).events[0]!;
    expect(spoofedEvent).toMatchObject({ user_id: 'user_payload_spoof_post' });
    expect(spoofedEvent).not.toHaveProperty('userId');
    expect(JSON.stringify(spoofedEvent)).not.toContain(oidcUserId);
    expect(appendedEvents.find((event) => event.id === spoofedEvent.id)?.userId).toBe(
      expectedTranscriptUserId,
    );
  });

  it('leaves API-key initial and POST events unattributed', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: {
        'x-api-key': apiKey,
        'content-type': 'application/json',
        user_id: 'user_header_spoof_api_key_initial',
      },
      payload: {
        agent_id: agentId,
        environment_id: environmentId,
        initial_events: [
          { type: 'user.message', content: [{ type: 'text', text: 'API-key initial event' }] },
        ],
      },
    });
    expect(create.statusCode).toBe(200);
    const sessionId = (create.json() as { id: string }).id;
    const initialEvent = appendedEvents.find(
      (event) => event.sessionId === sessionId && event.idempotencyKey.includes(':initial:'),
    );
    expect(initialEvent).toBeDefined();
    expect(initialEvent).not.toHaveProperty('userId');

    const post = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${sessionId}/events`,
      headers: {
        'x-api-key': apiKey,
        'content-type': 'application/json',
        'orca-beta': 'managed-agents-2026-04-01',
        user_id: 'user_header_spoof_api_key_post',
      },
      payload: {
        events: [
          {
            type: 'user.message',
            content: [{ type: 'text', text: 'API-key POST event' }],
            user_id: 'user_payload_spoof_api_key_post',
          },
        ],
      },
    });
    expect(post.statusCode).toBe(200);
    const postedEvent = (post.json() as { events: Array<{ id: string }> }).events[0]!;
    expect(appendedEvents.find((event) => event.id === postedEvent.id)).not.toHaveProperty(
      'userId',
    );
  });

  it('does not attribute a principal-only OIDC token', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: {
        authorization: `Bearer ${await principalOnlyOidcToken()}`,
        'content-type': 'application/json',
      },
      payload: {
        agent_id: agentId,
        environment_id: environmentId,
        initial_events: [
          { type: 'user.message', content: [{ type: 'text', text: 'principal-only initial' }] },
        ],
      },
    });
    expect(create.statusCode).toBe(200);
    const sessionId = (create.json() as { id: string }).id;
    const initialEvent = appendedEvents.find(
      (event) => event.sessionId === sessionId && event.idempotencyKey.includes(':initial:'),
    );
    expect(initialEvent).toBeDefined();
    expect(initialEvent).not.toHaveProperty('userId');

    const post = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${sessionId}/events`,
      headers: {
        authorization: `Bearer ${await principalOnlyOidcToken()}`,
        'content-type': 'application/json',
      },
      payload: {
        events: [
          { type: 'user.message', content: [{ type: 'text', text: 'principal-only POST' }] },
        ],
      },
    });
    expect(post.statusCode).toBe(200);
    const postedEvent = (post.json() as { data: Array<{ id: string }> }).data[0]!;
    expect(appendedEvents.find((event) => event.id === postedEvent.id)).not.toHaveProperty(
      'userId',
    );
  });
});
