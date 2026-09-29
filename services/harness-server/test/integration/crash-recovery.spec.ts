// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { FastifyInstance } from 'fastify';
import type { AddressInfo } from 'node:net';
import { Kafka } from 'kafkajs';
import { KafkaTranscriptStore, sessionTopicName } from '@orca/transcript-store';
import { Dispatcher } from '../../src/runner/dispatcher.js';
import { FakeHarness } from './fake-harness.js';

import { buildCombinedTestApp } from '../../../registry-service-ts/src/server.ts';
import { getTestDb, closeTestDb } from '../../../registry-service-ts/test/integration/setup.ts';
import {
  uniqueWorkspace,
  createTestApiKey,
  createTestAgent,
  createTestSession,
} from '../../../registry-service-ts/test/integration/fixtures.ts';

describe('harness-server crash recovery (integration)', () => {
  let app: FastifyInstance;
  let baseURL: string;
  let apiKey: string;
  let workspaceId: string;
  let store: KafkaTranscriptStore;
  let kafka: Kafka;
  const groupId = `harness-crash-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;

  beforeAll(async () => {
    kafka = new Kafka({
      clientId: 'crash-it',
      brokers: [process.env['KAFKA_BROKERS'] ?? 'localhost:9092'],
      metadataMaxAge: 1000,
    });
    store = new KafkaTranscriptStore({ kafka });
    const { db } = await getTestDb();
    app = buildCombinedTestApp({
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store,
      sse: { bufferSize: 256, dropAgeMs: 5000, heartbeatMs: 15000 },
    });
    await app.listen({ host: '127.0.0.1', port: 0 });
    baseURL = `http://127.0.0.1:${(app.server.address() as AddressInfo).port}`;
    workspaceId = uniqueWorkspace('crash');
    apiKey = await createTestApiKey(db, workspaceId);
  }, 60000);

  afterAll(async () => {
    await store.close();
    await app.close();
    await closeTestDb();
  }, 60000);

  async function preCreateTopic(ws: string, ses: string): Promise<void> {
    const admin = kafka.admin();
    await admin.connect();
    try {
      await admin.createTopics({
        waitForLeaders: true,
        topics: [{ topic: sessionTopicName(ws, ses), numPartitions: 1 }],
      });
    } finally {
      await admin.disconnect();
    }
  }

  function exactSessionTopicPattern(ws: string, ses: string): RegExp {
    return new RegExp(`^${escapeRegex(sessionTopicName(ws, ses))}$`);
  }

  it('processes a durable user.message after dispatcher restart', async () => {
    const agentId = await createTestAgent(baseURL, apiKey);
    const sessionId = await createTestSession(baseURL, apiKey, agentId);
    await preCreateTopic(workspaceId, sessionId);
    const topicPattern = exactSessionTopicPattern(workspaceId, sessionId);

    // Simulate harness downtime: client event is durably appended before any
    // dispatcher is running. The replacement dispatcher must consume it from
    // the session topic and produce the reply.
    const appendResponse = await fetch(`${baseURL}/v1/sessions/${sessionId}/events`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        events: [{ type: 'user.message', content: [{ type: 'text', text: 'recover me' }] }],
      }),
    });
    expect(appendResponse.ok).toBe(true);

    const dispatcher = new Dispatcher({
      kafka,
      groupId,
      topicPattern,
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => new FakeHarness(),
    });
    await dispatcher.start();

    try {
      await expect(waitForAgentMessage(workspaceId, sessionId, store)).resolves.toBe(true);
    } finally {
      await dispatcher.stop();
    }
  }, 90000);
});

async function waitForAgentMessage(
  workspaceId: string,
  sessionId: string,
  store: KafkaTranscriptStore,
): Promise<boolean> {
  const ac = new AbortController();
  const timeout = setTimeout(() => ac.abort(), 60000);
  try {
    for await (const event of store.tail(workspaceId, sessionId, {
      fromCursor: '0',
      subpath: '',
      signal: ac.signal,
    })) {
      if (event.producedBy === 'harness' && event.kind === 'agent.message') return true;
    }
  } finally {
    clearTimeout(timeout);
    ac.abort();
  }
  return false;
}

function escapeRegex(input: string): string {
  return input.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}
