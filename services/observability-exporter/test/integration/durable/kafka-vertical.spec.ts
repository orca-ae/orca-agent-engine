// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash, randomBytes } from 'node:crypto';
import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import {
  KafkaSessionEventSource,
  KafkaTranscriptStore,
  sessionTopicName,
} from '@orca/transcript-store';
import { Kafka } from 'kafkajs';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  applyObservabilityExporterMigrations,
  ObservabilityExporterRepository,
} from '../../../src/persistence.js';
import { RegistryObservabilityClient } from '../../../src/registry-client.js';
import { KafkaObservabilityExporterRuntime } from '../../../src/runtime.js';
import { TRANSCRIPT_SECRET, completedPrimaryTurnEvents } from '../../support/events.js';
import { basicRegistrySecret, enabledRegistryContext } from '../../support/registry.js';

const KAFKA_BROKERS = (process.env['KAFKA_BROKERS'] ?? 'localhost:9092')
  .split(',')
  .map((value) => value.trim())
  .filter(Boolean);
const ADMIN_DATABASE_URL =
  process.env['OBSERVABILITY_EXPORTER_TEST_ADMIN_DATABASE_URL'] ??
  'postgres://orca:orca@localhost:5432/postgres';

interface CapturedRequest {
  url: string | undefined;
  authorization: string | undefined;
  body: string;
}

describe('Kafka Transcript → Registry-authorized OTLP → mock collector', () => {
  let adminPool: Pool;
  let pool: Pool;
  let databaseName: string;
  let collector: Server;
  let collectorEndpoint: string;
  const collectorRequests: CapturedRequest[] = [];

  beforeAll(async () => {
    adminPool = new Pool({ connectionString: ADMIN_DATABASE_URL, max: 1 });
    databaseName = `observability_kafka_${process.pid}_${randomBytes(4).toString('hex')}`;
    await adminPool.query(`CREATE DATABASE "${databaseName}"`);
    const databaseUrl = new URL(ADMIN_DATABASE_URL);
    databaseUrl.pathname = `/${databaseName}`;
    pool = new Pool({ connectionString: databaseUrl.toString(), max: 4 });
    await applyObservabilityExporterMigrations(pool);
    collector = createServer(async (request, response) => {
      const chunks: Buffer[] = [];
      for await (const chunk of request) {
        chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
      }
      collectorRequests.push({
        url: request.url,
        authorization: request.headers.authorization,
        body: Buffer.concat(chunks).toString('utf8'),
      });
      response.writeHead(200, { 'content-type': 'application/json' });
      response.end('{}');
    });
    await listen(collector);
    const address = collector.address() as AddressInfo;
    collectorEndpoint = `http://127.0.0.1:${address.port}/api/public/otel/v1/traces`;
  });

  afterAll(async () => {
    await close(collector).catch(() => undefined);
    await pool?.end().catch(() => undefined);
    await adminPool?.query(`DROP DATABASE IF EXISTS "${databaseName}"`).catch(() => undefined);
    await adminPool?.end().catch(() => undefined);
  });

  it('acks source events after durable inbox commit, replays Kafka order, then sends one row', async () => {
    const suffix = randomBytes(5).toString('hex');
    const workspaceId = `ws_obs_${suffix}`;
    const sessionId = `ses_obs_${suffix}`;
    const topic = sessionTopicName(workspaceId, sessionId);
    const sourceGroupId = `observability-vertical-${suffix}`;
    const kafka = new Kafka({
      clientId: `observability-vertical-${suffix}`,
      brokers: KAFKA_BROKERS,
    });
    const admin = kafka.admin();
    await admin.connect();
    await admin.createTopics({
      waitForLeaders: true,
      topics: [{ topic, numPartitions: 1, replicationFactor: 1 }],
    });
    const store = new KafkaTranscriptStore({ kafka });
    const source = new KafkaSessionEventSource({
      kafka,
      groupId: sourceGroupId,
      topicPattern: new RegExp(`^${escapeRegExp(topic)}$`),
      topicDiscoveryIntervalMs: 25,
    });
    const repository = new ObservabilityExporterRepository(pool);
    const acceptEvent = repository.acceptEvent.bind(repository);
    let acceptAttempts = 0;
    repository.acceptEvent = async (sourceEvent) => {
      acceptAttempts += 1;
      if (acceptAttempts === 1) throw new Error('injected inbox transaction failure');
      return acceptEvent(sourceEvent);
    };
    let contextCalls = 0;
    let secretCalls = 0;
    const registryClient = new RegistryObservabilityClient({
      internalBaseUrl: 'http://registry.test',
      tokenProvider: async () => 'x'.repeat(32),
      fetchImpl: async (input) => {
        const url = String(input);
        if (url.endsWith('/agent-observability/context/resolve')) {
          contextCalls += 1;
          const context = enabledRegistryContext(workspaceId, sessionId);
          context.organization_id = 'org_kafka_vertical';
          (context.binding as { id: string; version: number }).id = 'aob_kafka_vertical';
          (context.binding as { id: string; version: number }).version = 1;
          return jsonResponse(context);
        }
        if (url.endsWith('/agent-observability/secret/resolve')) {
          secretCalls += 1;
          const secret = basicRegistrySecret();
          secret.binding_id = 'aob_kafka_vertical';
          secret.binding_version = 1;
          secret.credential_version = 4;
          secret.bundle = {
            adapter_type: 'otlp_http',
            auth: { type: 'basic', username: 'pk-kafka', password: 'sk-kafka' },
          };
          return jsonResponse(secret);
        }
        return new Response('', { status: 404 });
      },
    });
    const runtimeFor = (
      workerId: string,
      runtimeRepository: ObservabilityExporterRepository,
      projectorBatchSize = 1_000,
    ) =>
      new KafkaObservabilityExporterRuntime({
        repository: runtimeRepository,
        transcriptStore: store,
        eventSource: source,
        registryClient,
        workerId,
        projectorLeaseMs: 30_000,
        projectorBatchSize,
        projectorBatchBytes: 8 * 1024 * 1024,
        deliveryLeaseMs: 180_000,
        registryRequestTimeoutMs: 2_000,
        projectorPollMs: 10,
        deliveryPollMs: 10,
        otlpFetchImpl: async (_input, init) =>
          await fetch(collectorEndpoint, { ...init, redirect: 'manual' }),
      });
    const ingestionRuntime = runtimeFor(`vertical-ingest-${suffix}`, repository);
    const events = completedPrimaryTurnEvents('model_observation_kind', `_${suffix}`);
    const now = Date.now();
    for (const [index, event] of events.entries()) {
      event.workspaceId = workspaceId;
      event.sessionId = sessionId;
      event.producedAt = new Date(now + index).toISOString();
    }

    try {
      const poisonProducer = kafka.producer();
      await poisonProducer.connect();
      try {
        await poisonProducer.send({
          topic,
          messages: [
            {
              key: 'poison-route',
              value: '{}',
              headers: {
                id: 'poison-route',
                workspace_id: 'ws_forged',
                session_id: sessionId,
                produced_by: 'client',
                kind: 'user.message',
              },
            },
          ],
        });
      } finally {
        await poisonProducer.disconnect();
      }
      await ingestionRuntime.startIngestion();
      await store.append(workspaceId, sessionId, events);
      await waitFor(async () => {
        const count = await pool.query<{ count: string }>(
          `
          SELECT count(*)::text AS count
          FROM observability_exporter_event_inbox
          WHERE workspace_id = $1 AND session_id = $2
        `,
          [workspaceId, sessionId],
        );
        return count.rows[0]?.count === String(events.length);
      });
      expect(acceptAttempts).toBeGreaterThan(events.length);
      await ingestionRuntime.stopIngestion();

      const consumerGroupId = `${sourceGroupId}-${createHash('sha1').update(topic).digest('hex').slice(0, 16)}`;
      await waitFor(async () => {
        const offsets = await admin.fetchOffsets({ groupId: consumerGroupId, topics: [topic] });
        return Number(offsets[0]?.partitions[0]?.offset ?? '-1') >= events.length + 1;
      });

      // Reconstruct both stages from only Kafka + durable Postgres state.
      const projectorRuntime = runtimeFor(
        `vertical-project-${suffix}`,
        new ObservabilityExporterRepository(pool),
        1,
      );
      await expect(projectorRuntime.projectOnce()).resolves.toBe(true);
      const skippedPoison = await pool.query<{ next_seq: string }>(
        `SELECT next_seq::text
           FROM observability_exporter_session_state
          WHERE workspace_id = $1 AND session_id = $2`,
        [workspaceId, sessionId],
      );
      expect(skippedPoison.rows[0]?.next_seq).toBe('1');

      const resumedProjectorRuntime = runtimeFor(
        `vertical-project-resumed-${suffix}`,
        new ObservabilityExporterRepository(pool),
      );
      await expect(resumedProjectorRuntime.projectOnce()).resolves.toBe(true);
      const pending = await pool.query<{ count: string }>(
        `SELECT count(*)::text AS count
           FROM observability_exporter_trace_outbox
          WHERE workspace_id = $1 AND session_id = $2 AND status = 'pending'`,
        [workspaceId, sessionId],
      );
      expect(pending.rows[0]?.count).toBe('1');

      const deliveryRuntime = runtimeFor(
        `vertical-deliver-${suffix}`,
        new ObservabilityExporterRepository(pool),
      );
      await expect(deliveryRuntime.deliverOnce()).resolves.toBe(true);

      expect(contextCalls).toBe(1);
      expect(secretCalls).toBe(1);
      expect(collectorRequests).toHaveLength(1);
      const request = collectorRequests[0]!;
      expect(request.url).toBe('/api/public/otel/v1/traces');
      expect(request.authorization).toBe(
        `Basic ${Buffer.from('pk-kafka:sk-kafka').toString('base64')}`,
      );
      expect(request.body).toContain('orca.agent.turn');
      expect(request.body).not.toContain(TRANSCRIPT_SECRET);
      expect(request.body).not.toContain('queued and unaccepted');
    } finally {
      await ingestionRuntime.stopIngestion().catch(() => undefined);
      await store.close().catch(() => undefined);
      await admin.disconnect().catch(() => undefined);
      await deleteTopic(kafka, topic);
    }
  }, 60_000);
});

function jsonResponse(value: unknown): Response {
  return new Response(JSON.stringify(value), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  });
}

async function waitFor(condition: () => Promise<boolean>, timeoutMs = 20_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await condition()) return;
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error('timed out waiting for Kafka inbox commit');
}

async function deleteTopic(kafka: Kafka, topic: string): Promise<void> {
  const admin = kafka.admin();
  await admin.connect();
  try {
    await admin.deleteTopics({ topics: [topic], timeout: 5_000 });
  } catch {
    // A broker can disable topic deletion. The unique topic remains harmless.
  } finally {
    await admin.disconnect();
  }
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&');
}

function listen(server: Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      server.off('error', reject);
      resolve();
    });
  });
}

function close(server: Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.close((error) => (error === undefined ? resolve() : reject(error)));
  });
}
