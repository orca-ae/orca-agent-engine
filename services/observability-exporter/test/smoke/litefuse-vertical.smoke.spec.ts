// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomBytes } from 'node:crypto';
import { isIP } from 'node:net';
import {
  KafkaSessionEventSource,
  KafkaTranscriptStore,
  sessionTopicName,
} from '@orca/transcript-store';
import { Kafka, type Admin } from 'kafkajs';
import { Pool } from 'pg';
import { describe, expect, it } from 'vitest';
import { createHardenedOtlpFetch } from '../../src/egress.js';
import {
  applyObservabilityExporterMigrations,
  ObservabilityExporterRepository,
} from '../../src/persistence.js';
import { projectCanonicalTurns } from '../../src/projector.js';
import { RegistryObservabilityClient } from '../../src/registry-client.js';
import { KafkaObservabilityExporterRuntime } from '../../src/runtime.js';
import { completedPrimaryTurnEvents } from '../support/events.js';
import {
  assertCanonicalHttpsLitefuseEndpoint,
  litefuseSmokeTimeoutMs,
  requiredLitefuseSmokeCredentials,
  waitForLitefuseProjectedTrace,
} from '../support/litefuse.js';
import { basicRegistrySecret, enabledRegistryContext } from '../support/registry.js';

const KAFKA_BROKERS = (process.env['KAFKA_BROKERS'] ?? 'localhost:9092')
  .split(',')
  .map((value) => value.trim())
  .filter(Boolean);
const ADMIN_DATABASE_URL =
  process.env['OBSERVABILITY_EXPORTER_TEST_ADMIN_DATABASE_URL'] ??
  'postgres://orca:orca@localhost:5432/postgres';
const DATABASE_CONNECTION_TIMEOUT_MS = 10_000;
const DATABASE_QUERY_TIMEOUT_MS = 30_000;
const CLEANUP_STEP_TIMEOUT_MS = 35_000;

describe('Kafka Transcript → durable exporter → Litefuse', () => {
  it('delivers one metadata-only canonical turn through hardened public HTTPS egress', async () => {
    const credentials = requiredLitefuseSmokeCredentials();
    assertCanonicalHttpsLitefuseEndpoint(credentials.endpoint);
    const smokeOtlpFetch = buildSmokeOtlpFetch(credentials.endpoint);

    const suffix = randomBytes(8).toString('hex');
    const workspaceId = `ws_litefuse_${suffix}`;
    const sessionId = `ses_litefuse_${suffix}`;
    const userId = `user_litefuse_${suffix}`;
    const databaseName = `observability_litefuse_${process.pid}_${suffix}`;
    const topic = sessionTopicName(workspaceId, sessionId);
    const sourceGroupId = `observability-litefuse-${suffix}`;
    const registryToken = 'registry-litefuse-smoke-token-0001';
    const registryBasePath = `/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/agent-observability`;
    const registryRequests: string[] = [];
    const consumerGroupIds = new Set<string>();
    const adminPool = smokePool(ADMIN_DATABASE_URL, 1);
    let pool: Pool | undefined;
    let admin: Admin | undefined;
    let store: KafkaTranscriptStore | undefined;
    let source: KafkaSessionEventSource | undefined;
    let ingestionRuntime: KafkaObservabilityExporterRuntime | undefined;
    let ingestionStarted = false;
    let primaryFailed = false;
    let primaryError: unknown;
    let verifiedTraceId: string | undefined;

    try {
      await adminPool.query(`CREATE DATABASE "${databaseName}"`);
      const databaseUrl = new URL(ADMIN_DATABASE_URL);
      databaseUrl.pathname = `/${databaseName}`;
      const exporterPool = smokePool(databaseUrl.toString(), 4);
      pool = exporterPool;
      await applyObservabilityExporterMigrations(exporterPool);

      const kafka = new Kafka({
        clientId: `observability-litefuse-${suffix}`,
        brokers: KAFKA_BROKERS,
        connectionTimeout: 10_000,
        requestTimeout: 30_000,
        retry: { retries: 3 },
      });
      const createConsumer = kafka.consumer.bind(kafka);
      kafka.consumer = (config) => {
        consumerGroupIds.add(config.groupId);
        return createConsumer(config);
      };
      admin = kafka.admin();
      await admin.connect();
      await admin.createTopics({
        waitForLeaders: true,
        topics: [{ topic, numPartitions: 1, replicationFactor: 1 }],
      });
      const transcriptStore = new KafkaTranscriptStore({ kafka });
      store = transcriptStore;
      const eventSource = new KafkaSessionEventSource({
        kafka,
        groupId: sourceGroupId,
        topicPattern: new RegExp('^' + escapeRegExp(topic) + '$'),
        topicDiscoveryIntervalMs: 25,
      });
      source = eventSource;

      const bindingId = `aob_litefuse_${suffix}`;
      const registryClient = new RegistryObservabilityClient({
        internalBaseUrl: 'http://registry.test',
        tokenProvider: async () => registryToken,
        fetchImpl: async (input, init) => {
          const url = new URL(String(input));
          const headers = new Headers(init?.headers);
          if (
            init?.method !== 'POST' ||
            init.body !== '{}' ||
            headers.get('accept') !== 'application/json' ||
            headers.get('content-type') !== 'application/json' ||
            headers.get('authorization') !== `Bearer ${registryToken}`
          ) {
            return new Response('', { status: 400 });
          }
          registryRequests.push(url.pathname);
          if (url.pathname === `${registryBasePath}/context/resolve`) {
            return jsonResponse(
              registryContext({
                workspaceId,
                sessionId,
                bindingId,
                endpoint: credentials.endpoint,
                suffix,
              }),
            );
          }
          if (url.pathname === `${registryBasePath}/secret/resolve`) {
            return jsonResponse(
              registrySecret({
                bindingId,
                publicKey: credentials.publicKey,
                secretKey: credentials.secretKey,
              }),
            );
          }
          return new Response('', { status: 404 });
        },
      });
      const runtimeFor = (workerId: string, repository: ObservabilityExporterRepository) =>
        new KafkaObservabilityExporterRuntime({
          repository,
          transcriptStore,
          eventSource,
          registryClient,
          workerId,
          projectorLeaseMs: 30_000,
          projectorBatchSize: 1_000,
          projectorBatchBytes: 8 * 1024 * 1024,
          deliveryLeaseMs: 180_000,
          registryRequestTimeoutMs: 2_000,
          projectorPollMs: 10,
          deliveryPollMs: 10,
          ...(smokeOtlpFetch === undefined ? {} : { otlpFetchImpl: smokeOtlpFetch }),
        });

      const events = completedPrimaryTurnEvents('model_observation_kind', `_${suffix}`);
      const producedAt = Date.now();
      for (const [index, event] of events.entries()) {
        event.workspaceId = workspaceId;
        event.sessionId = sessionId;
        event.producedAt = new Date(producedAt + index).toISOString();
      }
      const acceptedUserEvent = events.find((event) => event.id === `evt_user_turn_${suffix}`);
      if (acceptedUserEvent === undefined) {
        throw new Error('synthetic accepted user event is missing');
      }
      acceptedUserEvent.userId = userId;
      const expectedTrace = projectCanonicalTurns(events)[0];
      if (expectedTrace === undefined) throw new Error('synthetic canonical trace is missing');

      ingestionRuntime = runtimeFor(
        `litefuse-ingest-${suffix}`,
        new ObservabilityExporterRepository(exporterPool),
      );
      await ingestionRuntime.startIngestion();
      ingestionStarted = true;
      await transcriptStore.append(workspaceId, sessionId, events);
      await waitFor(async () => {
        const result = await exporterPool.query<{ count: string }>(
          'SELECT count(*)::text AS count FROM observability_exporter_event_inbox ' +
            'WHERE workspace_id = $1 AND session_id = $2',
          [workspaceId, sessionId],
        );
        return result.rows[0]?.count === String(events.length);
      }, 'durable Kafka inbox commit');
      await ingestionRuntime.stopIngestion();
      ingestionStarted = false;

      const projectorRuntime = runtimeFor(
        `litefuse-project-${suffix}`,
        new ObservabilityExporterRepository(exporterPool),
      );
      await expect(projectorRuntime.projectOnce()).resolves.toBe(true);
      const pending = await exporterPool.query<{ count: string }>(
        'SELECT count(*)::text AS count FROM observability_exporter_trace_outbox ' +
          "WHERE workspace_id = $1 AND session_id = $2 AND status = 'pending'",
        [workspaceId, sessionId],
      );
      expect(pending.rows[0]?.count).toBe('1');

      const deliveryRuntime = runtimeFor(
        `litefuse-deliver-${suffix}`,
        new ObservabilityExporterRepository(exporterPool),
      );
      await waitForDelivered(
        deliveryRuntime,
        exporterPool,
        workspaceId,
        sessionId,
        litefuseSmokeTimeoutMs(),
      );
      expect(registryRequests[0]).toBe(`${registryBasePath}/context/resolve`);
      expect(
        registryRequests.filter((path) => path === `${registryBasePath}/secret/resolve`).length,
      ).toBeGreaterThanOrEqual(1);
      expect(
        registryRequests.every(
          (path) =>
            path === `${registryBasePath}/context/resolve` ||
            path === `${registryBasePath}/secret/resolve`,
        ),
      ).toBe(true);

      await waitForLitefuseProjectedTrace({ credentials, trace: expectedTrace });
      verifiedTraceId = expectedTrace.traceId;
    } catch (error) {
      primaryFailed = true;
      primaryError = error;
    }

    const cleanupErrors: string[] = [];
    if (ingestionStarted && ingestionRuntime !== undefined) {
      await cleanupStep('ingestion_stop', () => ingestionRuntime!.stopIngestion(), cleanupErrors);
    }
    if (source !== undefined) {
      await cleanupStep('source_stop', () => source!.stop(), cleanupErrors);
    }
    if (store !== undefined) {
      await cleanupStep('store_close', () => store!.close(), cleanupErrors);
    }
    if (admin !== undefined) {
      await cleanupStep(
        'consumer_group_delete',
        () => deleteConsumerGroups(admin!, consumerGroupIds),
        cleanupErrors,
      );
      await cleanupStep(
        'topic_delete',
        async () => {
          await admin!.deleteTopics({ topics: [topic], timeout: 5_000 });
          await waitFor(
            async () => !(await admin!.listTopics()).includes(topic),
            'Kafka topic deletion',
            15_000,
          );
        },
        cleanupErrors,
      );
      await cleanupStep('admin_disconnect', () => admin!.disconnect(), cleanupErrors);
    }
    if (pool !== undefined) {
      await cleanupStep('exporter_pool_end', () => pool!.end(), cleanupErrors);
    }
    await cleanupStep(
      'database_drop',
      async () => {
        await adminPool.query(`DROP DATABASE IF EXISTS "${databaseName}"`);
      },
      cleanupErrors,
    );
    await cleanupStep('admin_pool_end', () => adminPool.end(), cleanupErrors);

    if (primaryFailed) {
      if (cleanupErrors.length > 0) {
        console.error(
          JSON.stringify({ component: 'litefuse-vertical-smoke-cleanup', codes: cleanupErrors }),
        );
      }
      throw primaryError;
    }
    if (cleanupErrors.length > 0) {
      throw new Error(`Litefuse smoke cleanup failed: ${cleanupErrors.join(',')}`);
    }
    console.info(`Litefuse durable vertical smoke verified trace ${verifiedTraceId}`);
  });
});

function registryContext(input: {
  workspaceId: string;
  sessionId: string;
  bindingId: string;
  endpoint: string;
  suffix: string;
}): Record<string, unknown> {
  const context = enabledRegistryContext(input.workspaceId, input.sessionId);
  context.organization_id = `org_litefuse_${input.suffix}`;
  const epochs = enabledEpochs();
  context.epochs = { pinned: epochs, current: { ...epochs } };
  const binding = context.binding as Record<string, unknown>;
  binding.id = input.bindingId;
  binding.version = 1;
  binding.current_credential_version = 1;
  const target = binding.target as Record<string, unknown>;
  target.endpoint_url = input.endpoint;
  target.external_project_id = `litefuse-smoke-${input.suffix}`;
  const config = binding.config as Record<string, unknown>;
  config.timeout_ms = 60_000;
  return context;
}

function enabledEpochs(): Record<string, number> {
  return {
    organization_selection_epoch: 1,
    workspace_selection_epoch: 1,
    organization_default_revocation_epoch: 1,
    organization_revocation_epoch: 1,
    workspace_revocation_epoch: 1,
    binding_revocation_epoch: 1,
    platform_capture_restriction_epoch: 1,
    organization_capture_restriction_epoch: 1,
    workspace_capture_restriction_epoch: 1,
    session_revocation_epoch: 0,
  };
}

function registrySecret(input: {
  bindingId: string;
  publicKey: string;
  secretKey: string;
}): Record<string, unknown> {
  const secret = basicRegistrySecret();
  secret.binding_id = input.bindingId;
  secret.binding_version = 1;
  secret.credential_version = 1;
  secret.bundle = {
    adapter_type: 'otlp_http',
    auth: { type: 'basic', username: input.publicKey, password: input.secretKey },
  };
  return secret;
}

function jsonResponse(value: unknown): Response {
  return new Response(JSON.stringify(value), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  });
}

async function waitFor(
  condition: () => Promise<boolean>,
  description: string,
  timeoutMs = 30_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await condition()) return;
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error(`timed out waiting for ${description}`);
}

async function waitForDelivered(
  runtime: KafkaObservabilityExporterRuntime,
  pool: Pool,
  workspaceId: string,
  sessionId: string,
  timeoutMs: number,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    await runtime.deliverOnce(AbortSignal.timeout(Math.max(1, deadline - Date.now())));
    const result = await pool.query<{ status: string; suppression_reason: string | null }>(
      `SELECT status, suppression_reason
         FROM observability_exporter_trace_outbox
        WHERE workspace_id = $1 AND session_id = $2`,
      [workspaceId, sessionId],
    );
    const row = result.rows[0];
    if (row?.status === 'delivered') return;
    if (row?.status === 'suppressed') {
      throw new Error(`Litefuse delivery was suppressed: ${row.suppression_reason ?? 'unknown'}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`timed out waiting for Litefuse delivery after ${timeoutMs}ms`);
}

async function deleteConsumerGroups(
  admin: Admin,
  expectedGroupIds: ReadonlySet<string>,
): Promise<void> {
  const listed = await admin.listGroups();
  const groupIds = listed.groups
    .map((group) => group.groupId)
    .filter((groupId) => expectedGroupIds.has(groupId));
  if (groupIds.length === 0) return;
  const results = await admin.deleteGroups(groupIds);
  const failed = results.find((result) => (result.errorCode ?? 0) !== 0);
  if (failed !== undefined) {
    throw new Error(`Kafka consumer-group deletion failed with code ${failed.errorCode}`);
  }
}

async function cleanupStep(
  code: string,
  action: () => Promise<unknown>,
  errors: string[],
): Promise<void> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([
      action(),
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(() => reject(new Error('cleanup timeout')), CLEANUP_STEP_TIMEOUT_MS);
      }),
    ]);
  } catch {
    errors.push(code);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&');
}

function smokePool(connectionString: string, max: number): Pool {
  return new Pool({
    connectionString,
    max,
    connectionTimeoutMillis: DATABASE_CONNECTION_TIMEOUT_MS,
    statement_timeout: DATABASE_QUERY_TIMEOUT_MS,
    query_timeout: DATABASE_QUERY_TIMEOUT_MS + 1_000,
  });
}

function buildSmokeOtlpFetch(endpoint: string): typeof fetch | undefined {
  const address = process.env['LITEFUSE_SMOKE_RESOLVED_IP'];
  if (address === undefined || address === '') return undefined;
  const family = isIP(address);
  if (family !== 4 && family !== 6) {
    throw new Error('LITEFUSE_SMOKE_RESOLVED_IP must be one IPv4 or IPv6 address');
  }
  const expectedHostname = new URL(endpoint).hostname;
  return createHardenedOtlpFetch({
    resolve: async (hostname) => {
      if (hostname !== expectedHostname) {
        throw new Error('Litefuse smoke resolver received an unexpected hostname');
      }
      return [{ address, family }];
    },
  });
}
