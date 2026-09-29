// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { Event } from '../../src/types.js';
import type { KafkaTranscriptStore } from '../../src/kafka-store.js';
import { deleteTopicsForSession, makeStore, uniqueIds } from './setup.js';

describe('KafkaTranscriptStore.read with cursor (library, integration)', () => {
  let store: KafkaTranscriptStore;

  beforeAll(() => {
    store = makeStore();
  });
  afterAll(async () => {
    await store.close();
  });

  it('reads from a non-zero cursor and gets only the suffix', async () => {
    const { ws, ses } = uniqueIds('cursor');
    try {
      const events: Event[] = [10, 20, 30, 40, 50].map((i) => ({
        id: `evt_cur_${i}_${Date.now()}`,
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
      await store.append(ws, ses, events);

      // First read all 5 to learn the offsets.
      const all: Event[] = [];
      for await (const e of store.read(ws, ses, { fromCursor: '', maxEvents: 0, subpath: '' })) {
        all.push(e);
      }
      expect(all).toHaveLength(5);
      const cursor = String(all[1]!.seq); // resume from the 2nd event

      const suffix: Event[] = [];
      for await (const e of store.read(ws, ses, {
        fromCursor: cursor,
        maxEvents: 0,
        subpath: '',
      })) {
        suffix.push(e);
      }
      expect(suffix).toHaveLength(4);
      expect(String(suffix[0]!.seq)).toBe(cursor);
      expect(String(suffix[3]!.seq)).toBe(String(Number(cursor) + 3));
    } finally {
      await deleteTopicsForSession(ws, ses);
    }
  }, 30000);
});
