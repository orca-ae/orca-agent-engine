// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import { execFile } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import type { Event } from '../../src/types.js';
import { SessionEventBarrierError } from '../../src/types.js';
import {
  normalizePulsarModule,
  pulsarTopicName,
  PulsarSessionEventSource,
  PulsarTranscriptStore,
} from '../../src/pulsar-store.js';

const describeIfPulsar = process.env['PULSAR_SERVICE_URL'] ? describe : describe.skip;

describeIfPulsar('PulsarTranscriptStore (library, integration)', () => {
  it('bounds native backlog admission and handler concurrency across session keys', async () => {
    const { ws, ses } = uniqueIds('pulsar_capacity');
    const serviceUrl = process.env['PULSAR_SERVICE_URL'];
    if (!serviceUrl) throw new Error('PULSAR_SERVICE_URL is required');
    const topicPrefix = `capacity${Date.now()}`;
    const native = normalizePulsarModule(await import('pulsar-client'));
    const client = new native.Client({ serviceUrl });
    const store = new PulsarTranscriptStore({ serviceUrl, topicPrefix });
    let received = 0;
    let acknowledged = 0;
    let peakPending = 0;
    const source = new PulsarSessionEventSource({
      topicPrefix,
      subscription: `capacity-${ws}`,
      receiveTimeoutMs: 50,
      topicRediscoverIntervalMs: 0,
      maxConcurrentHandlers: 2,
      maxPendingDeliveries: 5,
      client: {
        createProducer: (config) => client.createProducer(config),
        createReader: (config) => client.createReader(config),
        close: () => client.close(),
        subscribe: async (config) => {
          const consumer = await client.subscribe(config);
          return {
            receive: async (timeout) => {
              const message = await consumer.receive(timeout);
              received += 1;
              peakPending = Math.max(peakPending, received - acknowledged);
              return message;
            },
            acknowledge: async (message) => {
              const result = await consumer.acknowledge(message);
              acknowledged += 1;
              return result;
            },
            negativeAcknowledge: (message) => consumer.negativeAcknowledge(message),
            close: () => consumer.close(),
            unsubscribe: () => consumer.unsubscribe(),
          };
        },
      },
    });
    const events: Event[][] = Array.from({ length: 12 }, (_, session) =>
      [0, 1].map((index) => ({
        id: `evt_capacity_${session}_${index}`,
        workspaceId: ws,
        sessionId: `${ses}_${session}`,
        subpath: '',
        seq: 0,
        producedAt: new Date().toISOString(),
        producedBy: 'client',
        kind: 'user.message',
        payload: Buffer.from('{}'),
        idempotencyKey: '',
      })),
    );
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let active = 0;
    let peakActive = 0;
    const seen: Event[] = [];
    try {
      for (const pair of events) await store.append(ws, pair[0]!.sessionId, pair);
      await source.start(async (event) => {
        active += 1;
        peakActive = Math.max(peakActive, active);
        seen.push(event);
        await gate;
        await new Promise((resolve) => setTimeout(resolve, 10));
        active -= 1;
      });
      await vi.waitFor(() => expect(received).toBeGreaterThanOrEqual(5), { timeout: 15_000 });
      await new Promise((resolve) => setTimeout(resolve, 100));
      expect(received).toBe(5);
      expect(active).toBe(2);
      expect(acknowledged).toBe(0);
      release();
      await vi.waitFor(() => expect(acknowledged).toBe(events.flat().length), { timeout: 15_000 });
      expect(peakPending).toBeLessThanOrEqual(5);
      expect(peakActive).toBe(2);
      for (const pair of events)
        expect(
          seen.filter(({ sessionId }) => sessionId === pair[0]!.sessionId).map(({ id }) => id),
        ).toEqual(pair.map(({ id }) => id));
    } finally {
      release();
      await source.stop();
      await store.close();
    }
  }, 30_000);

  it('rejects an injected reader failure after a native prefix and recovers the complete transcript with a fresh client', async () => {
    const { ws, ses } = uniqueIds('pulsar_read_failure');
    const serviceUrl = process.env['PULSAR_SERVICE_URL'];
    if (!serviceUrl) throw new Error('PULSAR_SERVICE_URL is required');
    const topicPrefix = `readfailure${Date.now()}`;
    const native = normalizePulsarModule(await import('pulsar-client'));
    const client = new native.Client({ serviceUrl });
    const writer = new PulsarTranscriptStore({ serviceUrl, topicPrefix });
    const failingStore = new PulsarTranscriptStore({
      topicPrefix,
      pulsar: native,
      client: {
        createProducer: (config) => client.createProducer(config),
        subscribe: (config) => client.subscribe(config),
        close: () => client.close(),
        createReader: async (config) => {
          const reader = await client.createReader(config);
          let reads = 0;
          return {
            hasNext: () => reader.hasNext(),
            readNext: async (timeout) => {
              // hasNext just confirmed that the committed backlog continues.
              // Inject at the binding boundary for deterministic prefix failure.
              // The native error paths are exercised separately in a child
              // process so a binding regression cannot crash this test runner.
              if (++reads === 2) throw new Error('injected reader failure after native prefix');
              return reader.readNext(timeout);
            },
            close: () => reader.close(),
          };
        },
      },
    });
    const freshStore = new PulsarTranscriptStore({ serviceUrl, topicPrefix });
    let audit: Awaited<ReturnType<typeof client.subscribe>> | undefined;
    const events: Event[] = [
      'session.user_event_processed',
      'session.error',
      'session.user_event_completed',
    ].map((kind, i) => ({
      id: `evt_read_failure_${i}`,
      workspaceId: ws,
      sessionId: ses,
      subpath: '',
      seq: 0,
      producedAt: new Date().toISOString(),
      producedBy: 'harness',
      kind,
      payload: Buffer.from('{}'),
      idempotencyKey: '',
    }));
    const seen: Event[] = [];
    const read = async (store: PulsarTranscriptStore) => {
      for await (const event of store.read(ws, ses, { fromCursor: '', maxEvents: 0, subpath: '*' }))
        seen.push(event);
    };
    try {
      await writer.append(ws, ses, events);
      audit = await client.subscribe({
        topic: pulsarTopicName({ workspaceId: ws, sessionId: ses, topicPrefix }),
        subscription: `read-audit-${ws}`,
        subscriptionType: 'Exclusive',
        subscriptionInitialPosition: 'Earliest',
      });
      await expect(read(failingStore)).rejects.toThrow(
        'injected reader failure after native prefix',
      );
      expect(seen.map(({ id }) => id)).toEqual([events[0]!.id]);
      seen.length = 0;
      await read(freshStore);
      expect(seen.map(({ id }) => id)).toEqual(events.map(({ id }) => id));
    } finally {
      await audit?.unsubscribe().catch(() => null);
      await audit?.close().catch(() => null);
      await failingStore.close();
      await freshStore.close();
      await writer.close();
    }
  }, 15_000);

  it.each(['repair', 'handoff'])(
    'keeps other sessions moving and queued interrupts behind %s across native consumers',
    async (mode) => {
      const { ws, ses } = uniqueIds('pulsar_replicas');
      const serviceUrl = process.env['PULSAR_SERVICE_URL'];
      if (!serviceUrl) throw new Error('PULSAR_SERVICE_URL is required');
      const topicPrefix = `replicas${Date.now()}`;
      const options = {
        serviceUrl,
        topicPrefix,
        subscription: `replicas-${ws}`,
        maxConcurrentHandlers: 1,
        maxPendingDeliveries: 4,
        nAckRedeliverTimeoutMs: 50,
        receiveTimeoutMs: 50,
        topicRediscoverIntervalMs: 0,
      };
      const store = new PulsarTranscriptStore({ serviceUrl, topicPrefix, readTimeoutMs: 50 });
      const sourceA = new PulsarSessionEventSource(options);
      const native = normalizePulsarModule(await import('pulsar-client'));
      const auditClient = new native.Client({ serviceUrl });
      let audit: Awaited<ReturnType<typeof auditClient.subscribe>> | undefined;
      const clientB = new native.Client({ serviceUrl });
      let joined = false;
      const sourceB = new PulsarSessionEventSource({
        ...options,
        client: {
          createProducer: (config) => clientB.createProducer(config),
          createReader: (config) => clientB.createReader(config),
          close: () => clientB.close(),
          subscribe: async (config) => {
            const consumer = await clientB.subscribe(config);
            joined = true;
            return consumer;
          },
        },
      });
      const event = (id: string, kind: string, producedBy: string): Event => ({
        id,
        kind,
        producedBy,
        workspaceId: ws,
        sessionId: ses,
        subpath: '',
        seq: 0,
        producedAt: new Date().toISOString(),
        idempotencyKey: '',
        payload: Buffer.from('{}'),
      });
      const first = event('evt_replica_message', 'user.message', 'client');
      const processed = event('evt_replica_processed', 'session.user_event_processed', 'harness');
      const failure = event('evt_replica_error', 'session.error', 'harness');
      const interrupts = Array.from({ length: 16 }, (_, i) =>
        event(`evt_replica_interrupt_${i}`, 'user.interrupt', 'client'),
      );
      const otherSessions = [
        { ...first, id: 'evt_other_workspace', workspaceId: `${ws}_other` },
        { ...first, id: 'evt_other_session', sessionId: `${ses}_other` },
      ];
      const otherSeen: string[] = [];
      let available = false;
      let attempts = 0;
      const seen: string[] = [];
      const repair = async (): Promise<void> => {
        attempts += 1;
        if (!available) throw new SessionEventBarrierError('partial outcome unavailable', repair);
        await store.append(ws, ses, [failure]);
      };
      const handler = async (input: Event): Promise<void> => {
        if (input.workspaceId !== ws || input.sessionId !== ses) {
          otherSeen.push(input.id);
          return;
        }
        seen.push(input.id);
        if (input.id === first.id) {
          await store.append(ws, ses, [processed]);
          await repair();
        } else {
          await store.append(ws, ses, [
            event(`${input.id}_idle`, 'session.status_idle', 'harness'),
          ]);
        }
      };
      try {
        await store.append(ws, ses, [first]);
        // All topics exist before the sole consumer subscribes, so progress
        // cannot be attributed to a different replica or a pattern refresh.
        for (const input of otherSessions)
          await store.append(input.workspaceId, input.sessionId, [input]);
        // Retain an unacknowledged audit subscription while owners come and go:
        // zero-retention brokers can otherwise delete acknowledged ledgers.
        audit = await auditClient.subscribe({
          topic: pulsarTopicName({ workspaceId: ws, sessionId: ses, topicPrefix }),
          subscription: `replicas-audit-${ws}`,
          subscriptionType: 'Exclusive',
          subscriptionInitialPosition: 'Earliest',
        });
        await sourceA.start(handler);
        await vi.waitFor(() => expect(attempts).toBeGreaterThan(0), { timeout: 15_000 });
        await vi.waitFor(
          () => expect(otherSeen.sort()).toEqual(otherSessions.map(({ id }) => id).sort()),
          { timeout: 5_000 },
        );
        expect(seen).toEqual([first.id]);
        await sourceB.start(handler);
        await vi.waitFor(() => expect(joined).toBe(true), { timeout: 15_000 });
        // Separate broker batches expose Shared distribution across both consumers.
        for (const interrupt of interrupts) await store.append(ws, ses, [interrupt]);
        await new Promise((resolve) => setTimeout(resolve, 600));
        expect(seen).toEqual([first.id]);
        if (mode === 'handoff') {
          // The replacement must redeliver the unfinished source before interrupts.
          await sourceA.stop();
          await vi.waitFor(() => expect(seen).toHaveLength(2), { timeout: 15_000 });
          expect(seen).toEqual([first.id, first.id]);
        }
        available = true;
        const sourceDeliveries = mode === 'handoff' ? [first.id, first.id] : [first.id];
        await vi.waitFor(
          () => expect(seen).toHaveLength(sourceDeliveries.length + interrupts.length),
          { timeout: 15_000 },
        );
        await sourceA.stop();
        await sourceB.stop();
        const events: Record<string, string>[] = [];
        for (let i = 0; i < 3 + 2 * interrupts.length; i++) {
          events.push((await audit.receive(1_000)).getProperties());
        }
        expect(seen).toEqual([...sourceDeliveries, ...interrupts.map(({ id }) => id)]);
        const outcomes = events.filter(({ produced_by }) => produced_by === 'harness');
        expect(outcomes.map(({ kind }) => kind)).toEqual([
          'session.user_event_processed',
          'session.error',
          ...interrupts.map(() => 'session.status_idle'),
        ]);
      } finally {
        available = true;
        await sourceA.stop();
        await sourceB.stop();
        await audit?.unsubscribe().catch(() => null);
        await audit?.close().catch(() => null);
        await auditClient.close();
        await store.close();
      }
    },
    40_000,
  );

  it('repairs a partial outcome before delivering a queued event with native Pulsar', async () => {
    const { ws, ses } = uniqueIds('pulsar_barrier');
    const topicPrefix = `barrier${Date.now()}`;
    const serviceUrl = process.env['PULSAR_SERVICE_URL'];
    if (!serviceUrl) throw new Error('PULSAR_SERVICE_URL is required');
    const store = new PulsarTranscriptStore({
      serviceUrl,
      topicPrefix,
      readTimeoutMs: 50,
    });
    const source = new PulsarSessionEventSource({
      serviceUrl,
      topicPrefix,
      subscription: `barrier-${Date.now()}`,
      nAckRedeliverTimeoutMs: 100,
      receiveTimeoutMs: 50,
      topicRediscoverIntervalMs: 0,
    });
    const event = (id: string, kind: string, producedBy: string): Event => ({
      id,
      kind,
      producedBy,
      workspaceId: ws,
      sessionId: ses,
      subpath: '',
      seq: 0,
      producedAt: new Date().toISOString(),
      idempotencyKey: '',
      payload: Buffer.from('{}'),
    });
    const first = event('evt_barrier_message', 'user.message', 'client');
    const next = event('evt_barrier_interrupt', 'user.interrupt', 'client');
    const processed = event('evt_barrier_processed', 'session.user_event_processed', 'harness');
    const failure = event('evt_barrier_error', 'session.error', 'harness');
    const idle = event('evt_barrier_idle', 'session.status_idle', 'harness');
    const seen: string[] = [];
    let available = false;
    let attempts = 0;
    const repair = async (): Promise<void> => {
      attempts += 1;
      if (!available) throw new SessionEventBarrierError('outcome append failed', repair);
      await store.append(ws, ses, [failure]);
    };
    try {
      // Both source events are already queued before the first outcome starts.
      await store.append(ws, ses, [first, next]);
      await source.start(async (input) => {
        seen.push(input.id);
        if (input.id === first.id) {
          await store.append(ws, ses, [processed]);
          await repair();
        } else {
          await store.append(ws, ses, [idle]);
        }
      });
      await vi.waitFor(() => expect(attempts).toBeGreaterThanOrEqual(7), { timeout: 15_000 });
      expect(seen).toEqual([first.id]);
      available = true;
      await vi.waitFor(() => expect(seen).toEqual([first.id, next.id]), { timeout: 15_000 });
      await source.stop();
      const events: Event[] = [];
      for await (const persisted of store.read(ws, ses, {
        fromCursor: '',
        maxEvents: 0,
        subpath: '*',
      })) {
        events.push(persisted);
      }
      expect(events.map(({ id }) => id)).toEqual([
        first.id,
        next.id,
        processed.id,
        failure.id,
        idle.id,
      ]);
    } finally {
      await source.stop();
      await store.close();
    }
  }, 40_000);

  it('keeps concurrent first appends alive through native client garbage collection', async () => {
    const { stdout } = await promisify(execFile)(
      process.execPath,
      [
        '--expose-gc',
        fileURLToPath(new URL('../fixtures/pulsar-client-lifecycle.mjs', import.meta.url)),
      ],
      { timeout: 45_000 },
    );

    expect(stdout).toContain('Verified 48 concurrent session appends and reads');
  }, 50_000);

  it('rejects native Reader errors without crashing the process', async () => {
    const { stdout } = await promisify(execFile)(
      process.execPath,
      [
        '--expose-gc',
        fileURLToPath(new URL('../fixtures/pulsar-reader-errors.mjs', import.meta.url)),
      ],
      { timeout: 45_000 },
    );

    expect(stdout).toContain('Verified native Reader timeout, recovery and closed-reader errors');
  }, 50_000);

  it('appends and reads back events against a real Pulsar broker', async () => {
    const { ws, ses } = uniqueIds('pulsar_it');
    const serviceUrl = process.env['PULSAR_SERVICE_URL'];
    if (!serviceUrl) throw new Error('PULSAR_SERVICE_URL is required');
    const store = new PulsarTranscriptStore({
      serviceUrl,
      tenant: process.env['PULSAR_TENANT'] ?? 'public',
      namespace: process.env['PULSAR_NAMESPACE'] ?? 'default',
      topicPrefix: process.env['PULSAR_TOPIC_PREFIX'] ?? 'orca',
      readTimeoutMs: 500,
    });
    try {
      const events: Event[] = [1, 2, 3].map((i) => ({
        id: `evt_pulsar_it_${i}_${Date.now()}`,
        workspaceId: ws,
        sessionId: ses,
        subpath: '',
        seq: 0,
        producedAt: new Date().toISOString(),
        producedBy: 'harness',
        kind: 'agent.message',
        payload: new Uint8Array([i]),
        idempotencyKey: '',
      }));
      events[0]!.userId = 'user_pulsar_it_1';
      events[1]!.userId = '';
      await store.append(ws, ses, events);

      const seen: Event[] = [];
      for await (const event of store.read(ws, ses, {
        fromCursor: '',
        maxEvents: 0,
        subpath: '',
      })) {
        seen.push(event);
      }

      expect(seen.map((event) => event.id)).toEqual(events.map((event) => event.id));
      expect(seen.map((event) => Buffer.from(event.payload)[0])).toEqual([1, 2, 3]);
      expect(seen.map((event) => event.userId)).toEqual(['user_pulsar_it_1', undefined, undefined]);
    } finally {
      await store.close();
    }
  }, 30000);

  it('uses live-process LRU dedup for a root and child Event.id', async () => {
    const { ws, ses } = uniqueIds('pulsar_dedup');
    const serviceUrl = process.env['PULSAR_SERVICE_URL'];
    if (!serviceUrl) throw new Error('PULSAR_SERVICE_URL is required');
    const store = new PulsarTranscriptStore({
      serviceUrl,
      tenant: process.env['PULSAR_TENANT'] ?? 'public',
      namespace: process.env['PULSAR_NAMESPACE'] ?? 'default',
      topicPrefix: process.env['PULSAR_TOPIC_PREFIX'] ?? 'orca',
      readTimeoutMs: 500,
    });
    const eventId = `evt_pulsar_dedup_${Date.now()}`;
    const rootEvent: Event = {
      id: eventId,
      workspaceId: ws,
      sessionId: ses,
      subpath: '',
      seq: 0,
      producedAt: new Date().toISOString(),
      producedBy: 'harness',
      kind: 'agent.message',
      payload: new Uint8Array([1]),
      idempotencyKey: 'pulsar-dedup-root',
    };
    const childEvent: Event = {
      ...rootEvent,
      subpath: 'subagents/pulsar-dedup-child',
      seq: 0,
      payload: new Uint8Array([2]),
      idempotencyKey: 'pulsar-dedup-child',
    };

    try {
      const [rootId] = await store.append(ws, ses, [rootEvent]);
      const [childId] = await store.append(ws, ses, [childEvent]);

      expect(rootId).toBe(eventId);
      expect(childId).toBe(rootId);
      expect(childEvent.seq).toBe(rootEvent.seq);

      const seen: Event[] = [];
      for await (const event of store.read(ws, ses, {
        fromCursor: '',
        maxEvents: 0,
        subpath: '*',
      })) {
        seen.push(event);
      }

      expect(seen).toHaveLength(1);
      expect(seen[0]).toMatchObject({
        id: rootId,
        seq: rootEvent.seq,
        subpath: '',
        idempotencyKey: rootEvent.idempotencyKey,
      });
      expect(seen[0]!.payload).toEqual(rootEvent.payload);
    } finally {
      await store.close();
    }
  }, 30000);

  it('tails can idle after existing events without crashing the node process', async () => {
    const { ws, ses } = uniqueIds('pulsar_tail_idle');
    const serviceUrl = process.env['PULSAR_SERVICE_URL'];
    if (!serviceUrl) throw new Error('PULSAR_SERVICE_URL is required');
    const store = new PulsarTranscriptStore({
      serviceUrl,
      tenant: process.env['PULSAR_TENANT'] ?? 'public',
      namespace: process.env['PULSAR_NAMESPACE'] ?? 'default',
      topicPrefix: process.env['PULSAR_TOPIC_PREFIX'] ?? 'orca',
      tailReadTimeoutMs: 50,
    });
    const ac = new AbortController();
    try {
      const existing: Event = {
        id: `evt_pulsar_tail_idle_${Date.now()}`,
        workspaceId: ws,
        sessionId: ses,
        subpath: '',
        seq: 0,
        producedAt: new Date(Date.now() - 1000).toISOString(),
        producedBy: 'client',
        kind: 'session.input',
        payload: new Uint8Array([1]),
        idempotencyKey: '',
      };
      await store.append(ws, ses, [existing]);

      setTimeout(() => ac.abort(), 150);
      const seen: Event[] = [];
      for await (const event of store.tail(ws, ses, {
        fromCursor: '',
        subpath: '',
        signal: ac.signal,
      })) {
        seen.push(event);
      }

      expect(seen).toEqual([]);
    } finally {
      ac.abort();
      await store.close();
    }
  }, 30000);

  it('subscribes to existing session topics through a real pattern consumer', async () => {
    const { ws, ses } = uniqueIds('pulsar_pattern');
    const serviceUrl = process.env['PULSAR_SERVICE_URL'];
    if (!serviceUrl) throw new Error('PULSAR_SERVICE_URL is required');
    const tenant = process.env['PULSAR_TENANT'] ?? 'public';
    const namespace = process.env['PULSAR_NAMESPACE'] ?? 'default';
    const topicPrefix = `${process.env['PULSAR_TOPIC_PREFIX'] ?? 'orca'}_pattern_${Date.now()}`;
    const store = new PulsarTranscriptStore({ serviceUrl, tenant, namespace, topicPrefix });
    const source = new PulsarSessionEventSource({
      serviceUrl,
      tenant,
      namespace,
      topicPrefix,
      subscription: `pulsar-pattern-it-${Date.now()}`,
      receiveTimeoutMs: 50,
      topicRediscoverIntervalMs: 0,
    });
    const event: Event = {
      id: `evt_pulsar_pattern_${Date.now()}`,
      workspaceId: ws,
      sessionId: ses,
      subpath: '',
      seq: 0,
      producedAt: new Date().toISOString(),
      producedBy: 'client',
      kind: 'user.message',
      payload: new Uint8Array([1]),
      idempotencyKey: '',
    };
    try {
      await store.append(ws, ses, [event]);
      const received = new Promise<Event>((resolve, reject) => {
        const timeout = setTimeout(
          () => reject(new Error('pattern consumer did not receive the session event')),
          10_000,
        );
        void source.start(async (seen) => {
          clearTimeout(timeout);
          resolve(seen);
        });
      });

      await expect(received).resolves.toMatchObject({
        id: event.id,
        workspaceId: ws,
        sessionId: ses,
      });
    } finally {
      await source.stop();
      await store.close();
    }
  }, 30000);
});

function uniqueIds(prefix: string): { ws: string; ses: string } {
  const suffix = Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
  return { ws: `ws_${prefix}_${suffix}`, ses: `ses_${prefix}_${suffix}` };
}
