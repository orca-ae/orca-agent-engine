// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Layer A.1: transcript visibility.
 *
 * Drives the public registry API while injecting one harness-internal replay
 * event through the configured transcript backend. This catches regressions
 * where registry list-events exposes SDK replay state to clients.
 */
import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { Kafka } from 'kafkajs';
import { Pool } from 'pg';
import {
  KafkaTranscriptStore,
  PostgresTranscriptStore,
  PulsarTranscriptStore,
  applyPostgresTranscriptMigrations,
  type Event,
  type TranscriptStore,
} from '@orca/transcript-store';
import {
  apiCall,
  buildClientFromConfig,
  ensureStackReachable,
  type OrcaClientConfig,
} from '../src/client.js';
import { seedWorkspaceApiKey } from '../src/seed.js';
import { createTestEnvironment, deleteTestEnvironment } from './environment-helpers.js';

interface AgentResponse {
  id: string;
}

interface SessionResponse {
  id: string;
}

interface EventsResponse {
  data: Array<{ id: string; type: string }>;
  next_page: string | null;
}

interface AppendEventsResponse {
  data: Array<{ id: string; type: string }>;
}

const INTERNAL_KIND = 'harness.claude.session_entry';

describe('Layer A.1: transcript visibility (live registry + transcript backend)', () => {
  let cfg: OrcaClientConfig;
  let workspaceId: string;
  let environmentId: string;
  let store: TranscriptStore | null = null;
  let registryPool: Pool | null = null;
  const created: { sessions: string[]; agents: string[] } = { sessions: [], agents: [] };

  beforeAll(async () => {
    const seeded = await seedWorkspaceApiKey();
    workspaceId = seeded.workspaceId;
    cfg = buildClientFromConfig({ apiKey: seeded.apiKey });
    await ensureStackReachable(cfg);
    environmentId = await createTestEnvironment(cfg, 'transcript-visibility-env');
    store = await buildTranscriptStore();
    registryPool = new Pool({
      connectionString:
        process.env['DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/registry',
      max: 1,
    });
  }, 30_000);

  afterAll(async () => {
    for (const id of created.sessions.splice(0)) {
      await apiCall(cfg, `/v1/sessions/${id}`, {
        method: 'DELETE',
        body: JSON.stringify({}),
      }).catch(() => {});
    }
    for (const id of created.agents.splice(0)) {
      await apiCall(cfg, `/v1/agents/${id}`, {
        method: 'DELETE',
        body: JSON.stringify({}),
      }).catch(() => {});
    }
    if (cfg && environmentId) {
      await deleteTestEnvironment(cfg, environmentId).catch(() => {});
    }
    await store?.close().catch(() => {});
    await registryPool?.end().catch(() => {});
  });

  it('GET /events returns public events but filters internal replay events from the Postgres read model', async () => {
    const agentRes = await apiCall(cfg, '/v1/agents', {
      method: 'POST',
      body: JSON.stringify({
        name: `visibility-${Date.now()}`,
        model: { provider: 'anthropic', id: 'claude-3-5-sonnet-20240620' },
        system: '',
        tools: [],
        mcp_servers: [],
        skills: [],
        metadata: {},
      }),
    });
    expect(agentRes.status).toBe(200);
    const agentId = agentRes.json<AgentResponse>().id;
    created.agents.push(agentId);

    const sessionRes = await apiCall(cfg, '/v1/sessions', {
      method: 'POST',
      body: JSON.stringify({ environment_id: environmentId, agent_id: agentId }),
    });
    expect(sessionRes.status).toBe(200);
    const sessionId = sessionRes.json<SessionResponse>().id;
    created.sessions.push(sessionId);

    const publicRes = await apiCall(cfg, `/v1/sessions/${sessionId}/events`, {
      method: 'POST',
      body: JSON.stringify({
        events: [
          {
            type: 'user.message',
            content: [{ type: 'text', text: 'transcript visibility' }],
          },
        ],
        request_id: `visibility-${randomUUID()}`,
      }),
    });
    expect(publicRes.status).toBe(200);
    const publicEvent = publicRes.json<AppendEventsResponse>().data[0]!;

    const internalEventId = randomUUID();
    const internalPayload = {
      type: INTERNAL_KIND,
      sdk_entry: {
        type: 'assistant',
        uuid: internalEventId,
        content: [{ type: 'text', text: 'internal replay only' }],
      },
    };
    await store!.append(workspaceId, sessionId, [
      {
        id: internalEventId,
        workspaceId,
        sessionId,
        subpath: '',
        seq: 0,
        producedAt: new Date().toISOString(),
        producedBy: 'harness',
        kind: INTERNAL_KIND,
        payload: Buffer.from(JSON.stringify(internalPayload), 'utf8'),
        idempotencyKey: '',
      } satisfies Event,
    ]);
    await insertInternalIndexRow(registryPool!, {
      workspaceId,
      sessionId,
      eventId: internalEventId,
      kind: INTERNAL_KIND,
      payload: internalPayload,
    });

    const listed = await waitForPublicEvents(cfg, sessionId, publicEvent.id, internalEventId);
    expect(listed.data.map((event) => event.id)).toContain(publicEvent.id);
    expect(listed.data.map((event) => event.id)).not.toContain(internalEventId);
    expect(listed.data.every((event) => event.type !== INTERNAL_KIND)).toBe(true);

    const indexed = await findIndexedEvent(registryPool!, workspaceId, sessionId, internalEventId);
    expect(indexed).toEqual({
      event_id: internalEventId,
      kind: INTERNAL_KIND,
      visibility: 'internal',
    });
  }, 30_000);
});

async function waitForPublicEvents(
  cfg: OrcaClientConfig,
  sessionId: string,
  publicEventId: string,
  internalEventId: string,
): Promise<EventsResponse> {
  return waitFor(async () => {
    const listRes = await apiCall(cfg, `/v1/sessions/${sessionId}/events`, { method: 'GET' });
    expect(listRes.status).toBe(200);
    const listed = listRes.json<EventsResponse>();
    const ids = listed.data.map((event) => event.id);
    if (ids.includes(publicEventId) && !ids.includes(internalEventId)) return listed;
    return null;
  });
}

async function insertInternalIndexRow(
  registryPool: Pool,
  input: {
    workspaceId: string;
    sessionId: string;
    eventId: string;
    kind: string;
    payload: unknown;
  },
): Promise<void> {
  await registryPool.query(
    `INSERT INTO session_events_index
       (workspace_id, session_id, seq, event_id, subpath, produced_at, produced_by, kind, visibility, payload)
     VALUES ($1, $2, $3, $4, '', $5, 'harness', $6, 'internal', $7::jsonb)
     ON CONFLICT DO NOTHING`,
    [
      input.workspaceId,
      input.sessionId,
      1,
      input.eventId,
      new Date().toISOString(),
      input.kind,
      JSON.stringify(input.payload),
    ],
  );
}

async function findIndexedEvent(
  registryPool: Pool,
  workspaceId: string,
  sessionId: string,
  eventId: string,
) {
  const indexed = await registryPool.query<{
    event_id: string;
    kind: string;
    visibility: string;
  }>(
    `SELECT event_id, kind, visibility
     FROM session_events_index
     WHERE workspace_id = $1 AND session_id = $2 AND event_id = $3`,
    [workspaceId, sessionId, eventId],
  );
  return indexed.rows[0] ?? null;
}

async function waitFor<T>(probe: () => Promise<T | null>): Promise<T> {
  const deadline = Date.now() + 10_000;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      const value = await probe();
      if (value !== null) return value;
    } catch (err) {
      lastError = err;
    }
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  if (lastError) throw lastError;
  throw new Error('timed out waiting for transcript events index');
}

async function buildTranscriptStore(): Promise<TranscriptStore> {
  const backend = (process.env['TRANSCRIPT_STORE_BACKEND'] ?? 'kafka').toLowerCase();
  switch (backend) {
    case 'kafka': {
      const brokers = (process.env['KAFKA_BROKERS'] ?? 'localhost:9092')
        .split(',')
        .map((broker) => broker.trim())
        .filter(Boolean);
      return new KafkaTranscriptStore({
        kafka: new Kafka({ brokers, clientId: `e2e-transcript-visibility-${process.pid}` }),
      });
    }
    case 'postgres': {
      const pool = new Pool({
        connectionString:
          process.env['TRANSCRIPT_STORE_DATABASE_URL'] ??
          'postgres://orca:orca@localhost:5432/transcriptstore',
        max: 1,
      });
      await applyPostgresTranscriptMigrations(pool);
      return new PostgresTranscriptStore({ pool });
    }
    case 'pulsar':
      return new PulsarTranscriptStore({
        serviceUrl: process.env['PULSAR_SERVICE_URL'] ?? 'pulsar://localhost:6650',
        tenant: process.env['PULSAR_TENANT'] ?? 'public',
        namespace: process.env['PULSAR_NAMESPACE'] ?? 'default',
        topicPrefix: process.env['PULSAR_TOPIC_PREFIX'] ?? 'orca',
      });
    default:
      throw new Error(`unsupported TRANSCRIPT_STORE_BACKEND=${backend}`);
  }
}
