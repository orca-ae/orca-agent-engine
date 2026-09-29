// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomBytes } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Kafka, logLevel } from 'kafkajs';
import { expect, it } from 'vitest';
import { KafkaOnlyObservabilityExporterRuntime } from '../../src/kafka-runtime.js';
import { initialCanonicalProjectionState, reduceCanonicalEventBatch } from '../../src/projector.js';
import { RegistryObservabilityClient } from '../../src/registry-client.js';
import { TRACE_SAMPLING_VERSION } from '../../src/sampling.js';
import { IO_CANARY, rawPrimaryTurnEvents } from '../support/raw-events.js';
import { basicRegistrySecret, enabledRegistryContext } from '../support/registry.js';
import { waitForKafkaTopics } from '../support/kafka.js';
import {
  assertCanonicalHttpsLitefuseEndpoint,
  requiredLitefuseSmokeCredentials,
  waitForLitefuseProjectedTrace,
} from '../support/litefuse.js';

it('delivers raw root/tool I/O through real Kafka and verifies Litefuse observations', async () => {
  // Opt-in synthetic smoke. Missing credentials fail before creating any infrastructure.
  const credentials = requiredLitefuseSmokeCredentials();
  assertCanonicalHttpsLitefuseEndpoint(credentials.endpoint);
  const suffix = randomBytes(8).toString('hex');
  const workspaceId = 'ws_io_smoke_' + suffix;
  const sessionId = 'ses_io_smoke_' + suffix;
  const groupId = 'obs-io-smoke-' + suffix;
  const topic = `orca.${workspaceId}.sessions.${sessionId}.events`;
  const checkpointTopic = groupId + '-checkpoints';
  const deliveryTopic = groupId + '-delivery';
  const topics = [topic, checkpointTopic, deliveryTopic];
  const kafka = new Kafka({
    clientId: groupId,
    logLevel: logLevel.ERROR,
    brokers: (process.env['KAFKA_BROKERS'] ?? 'localhost:9092')
      .split(',')
      .map((broker) => broker.trim())
      .filter(Boolean),
  });
  const admin = kafka.admin();
  const producer = kafka.producer({ allowAutoTopicCreation: false });
  const stateDirectory = await mkdtemp(join(tmpdir(), 'orca-io-smoke-'));
  const context = enabledRegistryContext(workspaceId, sessionId);
  const binding = context.binding as {
    target: Record<string, unknown>;
    config: Record<string, unknown>;
  };
  binding.target.endpoint_url = credentials.endpoint;
  binding.config.capture_mode = 'raw_io';
  binding.config.environment = 'test';
  binding.config.release = 'rio-attribution-approval-smoke';
  context.capture = {
    pinned_mode: 'raw_io',
    effective_mode: 'raw_io',
    current_ceilings: {
      platform: 'raw_io',
      organization: 'raw_io',
      workspace: 'raw_io',
    },
  };
  const epochs = context.epochs as { current: unknown; pinned: unknown };
  epochs.current = epochs.pinned;
  const secret = {
    ...basicRegistrySecret(),
    effective_capture_mode: 'raw_io',
    bundle: {
      adapter_type: 'otlp_http',
      auth: { type: 'basic', username: credentials.publicKey, password: credentials.secretKey },
    },
  };
  // Real Registry client/parser against contract-shaped authority responses, not a live Registry.
  const registryClient = new RegistryObservabilityClient({
    internalBaseUrl: 'http://registry.smoke',
    tokenProvider: async () => 'synthetic-service-token-'.repeat(2),
    fetchImpl: async (input) => {
      const path = new URL(String(input)).pathname;
      if (path.endsWith('/context/resolve')) return Response.json(context);
      if (path.endsWith('/secret/resolve')) return Response.json(secret);
      throw new Error('unexpected smoke Registry operation');
    },
  });
  const runtime = new KafkaOnlyObservabilityExporterRuntime({
    kafka,
    groupId,
    sessions: [{ topic, workspaceId, sessionId }],
    checkpointTopic,
    deliveryTopic,
    registryClient,
    stateDirectory,
    // No fetch override: use the production hardened, public-only OTLP transport.
  });
  let primaryError: unknown;
  let verifiedTraceId: string | undefined;
  try {
    await admin.connect();
    await admin.createTopics({
      waitForLeaders: false,
      topics: [
        { topic, numPartitions: 1, replicationFactor: 1 },
        {
          topic: checkpointTopic,
          numPartitions: 1,
          replicationFactor: 1,
          configEntries: [{ name: 'cleanup.policy', value: 'compact' }],
        },
        {
          topic: deliveryTopic,
          numPartitions: 1,
          replicationFactor: 1,
          configEntries: [
            { name: 'cleanup.policy', value: 'delete' },
            { name: 'retention.ms', value: '-1' },
            { name: 'retention.bytes', value: '-1' },
          ],
        },
      ],
    });
    await waitForKafkaTopics(admin, topics);
    await producer.connect();
    const now = Date.now();
    const events = rawPrimaryTurnEvents('_' + suffix).map((entry, index) => ({
      ...entry,
      workspaceId,
      sessionId,
      producedAt: new Date(now + index).toISOString(),
    }));
    const [expected] = reduceCanonicalEventBatch(
      initialCanonicalProjectionState(),
      events,
      new Set(),
      {
        algorithmVersion: TRACE_SAMPLING_VERSION,
        bindingId: 'aob_registry',
        bindingVersion: 2,
        sampleRate: 1,
      },
      'raw_io',
    ).completedTraces;
    expect(expected?.root.io?.input?.json).toBeDefined();
    expect(JSON.stringify(expected)).toContain(IO_CANARY);
    const pinned = await registryClient.resolveContext({ workspaceId, sessionId });
    if (pinned.status !== 'enabled') throw new Error('synthetic smoke context was not enabled');
    expect(
      expected!.spans.find((span) => span.observationType === 'tool')?.metadata[
        'orca.tool.last_approval.result'
      ],
    ).toBe('allow');
    await runtime.start();
    await producer.send({
      topic,
      acks: -1,
      messages: events.map((entry) => ({
        partition: 0,
        key: entry.id,
        value: Buffer.from(entry.payload),
        headers: {
          id: entry.id,
          workspace_id: workspaceId,
          session_id: sessionId,
          subpath: entry.subpath,
          produced_at: entry.producedAt,
          produced_by: entry.producedBy,
          kind: entry.kind,
          idempotency_key: entry.idempotencyKey,
          ...(entry.userId === undefined ? {} : { user_id: entry.userId }),
        },
      })),
    });
    await waitForLitefuseProjectedTrace({
      credentials,
      trace: expected!,
      context: pinned.deliveryContext,
    });
    expect(runtime.status().state).toBe('running');
    verifiedTraceId = expected!.traceId;
  } catch (error) {
    primaryError = error;
  }

  const cleanupErrors: string[] = [];
  const cleanup = async (code: string, run: () => Promise<unknown>) => {
    try {
      await run();
    } catch {
      cleanupErrors.push(code);
    }
  };
  await cleanup('runtime_stop', () => runtime.stop());
  await cleanup('producer_disconnect', () => producer.disconnect());
  await cleanup('groups_delete', async () => {
    const { groups } = await admin.listGroups();
    const owned = groups.map((group) => group.groupId).filter((id) => id.startsWith(groupId));
    if (owned.length) await admin.deleteGroups(owned);
  });
  await cleanup('topics_delete', async () => {
    const existing = new Set(await admin.listTopics());
    const owned = topics.filter((name) => existing.has(name));
    if (owned.length) await admin.deleteTopics({ topics: owned });
  });
  await cleanup('admin_disconnect', () => admin.disconnect());
  await cleanup('scratch_delete', () => rm(stateDirectory, { recursive: true, force: true }));
  if (primaryError !== undefined) throw primaryError;
  if (cleanupErrors.length)
    throw new Error('Litefuse Kafka smoke cleanup failed: ' + cleanupErrors.join(','));
  console.info('Litefuse Kafka raw smoke verified trace ' + verifiedTraceId);
}, 320_000);
