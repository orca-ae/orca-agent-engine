// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { KafkaTranscriptStore } from '../../src/kafka-store.js';
import { dedupHits, registry } from '../../src/metrics.js';
import { deleteTopicsForSession, makeKafka, makeStore, uniqueIds } from './setup.js';

describe('KafkaTranscriptStore.append dedup (library, integration)', () => {
  let store: KafkaTranscriptStore;

  beforeAll(() => {
    store = makeStore();
  });
  afterAll(async () => {
    await store.close();
  });

  it('dedupes the same event id across root and child subpaths', async () => {
    const { ws, ses } = uniqueIds('dedup');
    const topicName = `orca.${ws}.sessions.${ses}.events`;
    try {
      const eventId = `evt_dedup_${Date.now()}`;
      const rootEvent = {
        id: eventId,
        workspaceId: ws,
        sessionId: ses,
        subpath: '',
        seq: 0,
        producedAt: new Date().toISOString(),
        producedBy: 'harness',
        kind: 'agent.message',
        payload: new Uint8Array([1]),
        idempotencyKey: '',
      };
      const childEvent = {
        ...rootEvent,
        subpath: 'subagents/dedup-child',
        seq: 0,
      };

      // Snapshot dedup metric before
      const before = (
        await registry.getSingleMetric('transcript_store_dedup_hits_total')!.get()
      ).values[0]?.value as number | undefined;

      await store.append(ws, ses, [rootEvent]);
      await store.append(ws, ses, [childEvent]);
      expect(childEvent.seq).toBe(rootEvent.seq);

      // Verify exactly one message landed via the admin watermark.
      const admin = makeKafka('verify').admin();
      await admin.connect();
      let high: string;
      try {
        const offsets = await admin.fetchTopicOffsets(topicName);
        expect(offsets).toHaveLength(1);
        high = offsets[0]!.high;
      } finally {
        await admin.disconnect();
      }
      expect(Number(high)).toBe(1);

      // Verify dedup metric bumped by exactly one.
      const after = (
        await registry.getSingleMetric('transcript_store_dedup_hits_total')!.get()
      ).values[0]?.value as number | undefined;
      // Reference dedupHits to keep the import live (vitest tree-shake noop).
      void dedupHits;
      expect((after ?? 0) - (before ?? 0)).toBe(1);
    } finally {
      await deleteTopicsForSession(ws, ses);
    }
  }, 30000);
});
