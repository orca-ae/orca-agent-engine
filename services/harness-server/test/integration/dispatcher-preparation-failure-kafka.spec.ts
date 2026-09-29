// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { AddressInfo } from 'node:net';
import Fastify from 'fastify';
import { Kafka } from 'kafkajs';
import { describe, expect, it, vi } from 'vitest';
import { KafkaTranscriptStore, sessionTopicName, type Event } from '@orca/transcript-store';
import { projectCanonicalTurns } from '@orca/observability-exporter';
import { RegistryClient } from '../../src/clients/registry.js';
import { Dispatcher } from '../../src/runner/dispatcher.js';

describe('Dispatcher permanent preparation failure with real Kafka', () => {
  it('commits a failed turn and its queued interrupt, and does not repeat either after restart', async () => {
    const suffix = `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    const workspaceId = `ws_prepare_${suffix}`;
    const sessionId = `ses_prepare_${suffix}`;
    const topic = sessionTopicName(workspaceId, sessionId);
    const groupId = `prepare-failure-${suffix}`;
    const kafka = (name: string) =>
      new Kafka({
        clientId: `${name}-${suffix}`,
        brokers: [process.env['KAFKA_BROKERS'] ?? 'localhost:9092'],
      });
    const admin = kafka('admin').admin();
    const producer = new KafkaTranscriptStore({ kafka: kafka('producer') });
    const app = Fastify();
    let prepareCalls = 0;
    const turnInspections: unknown[] = [];
    const states: unknown[] = [];
    const sessionBase = `/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}`;
    app.get(`${sessionBase}/execution-owner`, async () => ({ owner: 'harness-server' }));
    app.post(`${sessionBase}/harness-turn`, async (request, reply) => {
      turnInspections.push(request.body);
      // No native receipt exists for this legacy Claude binding. Send JSON null,
      // as the real Registry does, rather than a missing-route 404 or empty body.
      return reply.type('application/json').send('null');
    });
    // Exercise RegistryClient's real HTTP error classification. Registry's
    // archived-Agent binding itself is covered by internal-prepare-execution.spec.ts.
    app.post(`${sessionBase}/executions:prepare`, async (_request, reply) => {
      prepareCalls += 1;
      return reply.code(409).send({
        error: 'invalid_runtime_binding',
        resource_type: 'agent_version',
        resource_id: 'agt_archived@1',
      });
    });
    app.patch(`${sessionBase}/state`, async (request) => {
      states.push(request.body);
      return { id: sessionId, workspace_id: workspaceId, ...(request.body as object) };
    });
    const harnessFactory = vi.fn(() => {
      throw new Error('Neither the failed preparation nor interrupt may create a harness');
    });
    const stores: KafkaTranscriptStore[] = [];
    let dispatcher: Dispatcher | undefined;
    let createdTopic = false;
    const message = sourceEvent('user.message', `evt_message_${suffix}`);
    const interrupt = sourceEvent('user.interrupt', `evt_interrupt_${suffix}`);

    function sourceEvent(kind: string, id: string): Event {
      return {
        id,
        workspaceId,
        sessionId,
        subpath: '',
        seq: 0,
        producedAt: new Date().toISOString(),
        producedBy: 'client',
        kind,
        payload: Buffer.from(
          JSON.stringify(
            kind === 'user.message' ? { content: [{ type: 'text', text: 'run' }] } : {},
          ),
        ),
        idempotencyKey: '',
      };
    }

    function newDispatcher(): Dispatcher {
      const store = new KafkaTranscriptStore({ kafka: kafka(`store-${stores.length}`) });
      stores.push(store);
      const port = (app.server.address() as AddressInfo).port;
      return new Dispatcher({
        kafka: kafka(`dispatcher-${stores.length}`),
        groupId,
        store,
        registry: new RegistryClient(`http://127.0.0.1:${port}`, async () => 'test-service-token'),
        topicPattern: new RegExp(`^${topic.replaceAll('.', '\\.')}$`),
        topicRediscoverIntervalMs: 60_000,
        anthropicApiKey: 'unused',
        modelDefault: 'unused',
        harnessFactory,
      });
    }

    async function waitForOffset(offset: number): Promise<void> {
      await vi.waitFor(
        async () => {
          const committed = await admin.fetchOffsets({ groupId, topics: [topic] });
          expect(Number(committed[0]?.partitions[0]?.offset ?? '-1')).toBeGreaterThanOrEqual(
            offset,
          );
        },
        { timeout: 45_000, interval: 100 },
      );
    }

    try {
      await app.listen({ host: '127.0.0.1', port: 0 });
      await admin.connect();
      await admin.createTopics({ topics: [{ topic, numPartitions: 1, replicationFactor: 1 }] });
      createdTopic = true;
      await producer.append(workspaceId, sessionId, [message, interrupt]);
      dispatcher = newDispatcher();
      await dispatcher.start();
      // Offset 2 is reachable only after the first failed delivery settles and
      // resumes the partition so the queued interrupt can run.
      await waitForOffset(2);
      expect(prepareCalls).toBe(1);
      expect(turnInspections).toEqual([
        { action: { type: 'inspect' } },
        { action: { type: 'inspect' } },
      ]);
      expect(harnessFactory).not.toHaveBeenCalled();
      expect(states).toEqual([
        { status: 'idle', sandbox_handle_id: null },
        { status: 'idle', sandbox_handle_id: null },
      ]);
      await dispatcher.stop();
      dispatcher = undefined;

      const events: Event[] = [];
      for await (const event of producer.read(workspaceId, sessionId, {
        fromCursor: '',
        maxEvents: 0,
        subpath: '*',
      }))
        events.push(event);
      const payloads = (kind: string) =>
        events
          .filter((event) => event.kind === kind)
          .map((event) => JSON.parse(Buffer.from(event.payload).toString('utf8')));
      expect(payloads('session.error')).toEqual([
        expect.objectContaining({
          error: expect.objectContaining({ type: 'setup_failed' }),
          retry_status: { will_retry: false },
        }),
      ]);
      expect(payloads('session.status_idle')).toEqual([
        { stop_reason: { type: 'retries_exhausted' } },
        { stop_reason: { type: 'end_turn' } },
      ]);
      expect(payloads('session.user_event_processed')).toEqual([
        { user_event_id: message.id },
        { user_event_id: interrupt.id },
      ]);
      expect(payloads('session.user_event_completed')).toEqual([
        { user_event_id: message.id },
        { user_event_id: interrupt.id },
      ]);
      const traces = projectCanonicalTurns(events);
      expect(traces).toHaveLength(1);
      expect(traces[0]).toMatchObject({
        anchorEventId: message.id,
        root: { status: 'error', metadata: { 'orca.turn.terminal_reason': 'retries_exhausted' } },
      });

      // Force redelivery with a new Dispatcher AND a new transcript client;
      // completion must be recovered from Kafka, not an in-process cache.
      await admin.setOffsets({ groupId, topic, partitions: [{ partition: 0, offset: '0' }] });
      turnInspections.length = 0;
      dispatcher = newDispatcher();
      await dispatcher.start();
      await waitForOffset(events.length);
      expect(prepareCalls).toBe(1);
      expect(turnInspections).toEqual([
        { action: { type: 'inspect' } },
        { action: { type: 'inspect' } },
      ]);
      expect(states).toHaveLength(2);
      expect(harnessFactory).not.toHaveBeenCalled();
      const offsets = await admin.fetchTopicOffsets(topic);
      expect(Number(offsets[0]?.high)).toBe(events.length);
    } finally {
      await dispatcher?.stop();
      await Promise.all([...stores, producer].map((store) => store.close()));
      await app.close();
      try {
        if (createdTopic) await admin.deleteTopics({ topics: [topic] });
      } finally {
        await admin.disconnect();
      }
    }
  }, 120_000);
});
