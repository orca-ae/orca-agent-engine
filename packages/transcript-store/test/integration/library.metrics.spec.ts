// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { Event } from '../../src/types.js';
import type { KafkaTranscriptStore } from '../../src/kafka-store.js';
import { registry } from '../../src/metrics.js';
import { deleteTopicsForSession, makeStore, uniqueIds } from './setup.js';

async function getCounterValue(name: string, labels: Record<string, string>): Promise<number> {
  const m = registry.getSingleMetric(name);
  if (!m) return 0;
  const data = await m.get();
  const match = data.values.find((v) =>
    Object.entries(labels).every(([k, val]) => v.labels?.[k] === val),
  );
  return (match?.value as number | undefined) ?? 0;
}

describe('KafkaTranscriptStore metrics (library, integration)', () => {
  let store: KafkaTranscriptStore;

  beforeAll(() => {
    store = makeStore();
  });
  afterAll(async () => {
    await store.close();
  });

  it('exercises append + read + tail + archive and bumps the status=ok counters', async () => {
    const { ws, ses } = uniqueIds('metrics');
    const ac = new AbortController();
    try {
      const beforeAppend = await getCounterValue('transcript_store_append_total', { status: 'ok' });
      const beforeRead = await getCounterValue('transcript_store_read_total', { status: 'ok' });
      const beforeTail = await getCounterValue('transcript_store_tail_total', { status: 'ok' });
      const beforeArchive = await getCounterValue('transcript_store_archive_total', {
        status: 'ok',
      });
      const beforeSubAgent = await getCounterValue('transcript_store_subagent_message_rate', {
        workspace_id: ws,
        produced_by: 'harness',
      });

      // Append a parent + a subagent event.
      const parent: Event = {
        id: `evt_metric_parent_${Date.now()}`,
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
      const subagent: Event = {
        id: `evt_metric_sub_${Date.now()}`,
        workspaceId: ws,
        sessionId: ses,
        subpath: 'subagents/a/0',
        seq: 0,
        producedAt: new Date().toISOString(),
        producedBy: 'harness',
        kind: 'agent.message',
        payload: new Uint8Array([2]),
        idempotencyKey: '',
      };
      await store.append(ws, ses, [parent, subagent]);

      // Read (drains all events, end-of-stream bumps read_total).
      const readSeen: Event[] = [];
      for await (const e of store.read(ws, ses, {
        fromCursor: '',
        maxEvents: 0,
        subpath: '*',
      })) {
        readSeen.push(e);
      }
      expect(readSeen).toHaveLength(2);

      // Tail briefly, then cancel — counter is incremented at start. The tail is
      // from-now (empty cursor = current head), so it observes only events
      // appended AFTER its consumer subscribes. The async generator is lazy: the
      // underlying consumer does not subscribe until the first `next()` is
      // awaited, so START pulling first, give it time to join + seek to the head,
      // and only THEN append the live event the tail should deliver.
      let markReady: () => void;
      const ready = new Promise<void>((resolve) => {
        markReady = resolve;
      });
      const iter = store.tail(ws, ses, {
        fromCursor: '',
        subpath: '*',
        onReady: () => markReady(),
        signal: ac.signal,
      });
      const reader = iter[Symbol.asyncIterator]();
      const firstPromise = reader.next(); // begins the consumer subscribe from-now
      // Gate on the contract signal (joined + head seek applied), not a sleep.
      await ready;
      const live: Event = {
        id: `evt_metric_live_${Date.now()}`,
        workspaceId: ws,
        sessionId: ses,
        subpath: '',
        seq: 0,
        producedAt: new Date().toISOString(),
        producedBy: 'client',
        kind: 'user.message',
        payload: new Uint8Array([3]),
        idempotencyKey: '',
      };
      await store.append(ws, ses, [live]);
      const first = await firstPromise;
      expect(first.done).toBe(false);
      expect(first.value?.id).toBe(live.id);
      ac.abort();
      // Drain to completion to release the iterator.
      // eslint-disable-next-line no-empty
      while (!(await reader.next()).done) {}

      // Archive (sentinel append).
      await store.archive(ws, ses);

      const afterAppend = await getCounterValue('transcript_store_append_total', { status: 'ok' });
      const afterRead = await getCounterValue('transcript_store_read_total', { status: 'ok' });
      const afterTail = await getCounterValue('transcript_store_tail_total', { status: 'ok' });
      const afterArchive = await getCounterValue('transcript_store_archive_total', {
        status: 'ok',
      });
      const afterSubAgent = await getCounterValue('transcript_store_subagent_message_rate', {
        workspace_id: ws,
        produced_by: 'harness',
      });

      // append called once (parent+sub batch); archive uses a separate counter.
      expect(afterAppend - beforeAppend).toBeGreaterThanOrEqual(1);
      expect(afterRead - beforeRead).toBeGreaterThanOrEqual(1);
      expect(afterTail - beforeTail).toBeGreaterThanOrEqual(1);
      expect(afterArchive - beforeArchive).toBeGreaterThanOrEqual(1);
      // Only the subagent event should have bumped the subagent_message_rate
      // counter for this workspace+produced_by combination.
      expect(afterSubAgent - beforeSubAgent).toBe(1);
    } finally {
      ac.abort();
      await deleteTopicsForSession(ws, ses);
    }
  }, 45000);
});
