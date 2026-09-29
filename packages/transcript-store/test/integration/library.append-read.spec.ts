// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { Event } from '../../src/types.js';
import type { KafkaTranscriptStore } from '../../src/kafka-store.js';
import { deleteTopicsForSession, makeStore, uniqueIds } from './setup.js';

describe('KafkaTranscriptStore.append + read (library, integration)', () => {
  let store: KafkaTranscriptStore;

  beforeAll(() => {
    store = makeStore();
  });
  afterAll(async () => {
    await store.close();
  });

  it('appends a batch and returns one id per input', async () => {
    const { ws, ses } = uniqueIds('append');
    try {
      const events: Event[] = [1, 2, 3].map((i) => ({
        id: `evt_append_${i}_${Date.now()}`,
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
      const ids = await store.append(ws, ses, events);
      expect(ids).toHaveLength(3);
      expect(ids).toEqual(events.map((e) => e.id));
      expect(events.map((e) => e.seq)).toEqual([0, 1, 2]);

      const duplicate = { ...events[1]!, seq: 0 };
      await store.append(ws, ses, [duplicate]);
      expect(duplicate.seq).toBe(events[1]!.seq);
    } finally {
      await deleteTopicsForSession(ws, ses);
    }
  }, 30000);

  it('appends and reads back via read()', async () => {
    const { ws, ses } = uniqueIds('readback');
    try {
      const events: Event[] = [1, 2, 3, 4, 5].map((i) => ({
        id: `evt_rb_${i}_${Date.now()}`,
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

      const seen: Event[] = [];
      for await (const e of store.read(ws, ses, { fromCursor: '', maxEvents: 0, subpath: '' })) {
        seen.push(e);
      }
      expect(seen).toHaveLength(5);
      expect(seen.map((e) => Buffer.from(e.payload)[0])).toEqual([1, 2, 3, 4, 5]);
    } finally {
      await deleteTopicsForSession(ws, ses);
    }
  }, 30000);

  it('round-trips optional user attribution without inventing absent values', async () => {
    const { ws, ses } = uniqueIds('user_id');
    try {
      const events: Event[] = [1, 2, 3].map((i) => ({
        id: `evt_user_id_${i}_${Date.now()}`,
        workspaceId: ws,
        sessionId: ses,
        subpath: '',
        seq: 0,
        producedAt: new Date().toISOString(),
        producedBy: 'client',
        kind: 'user.message',
        payload: new Uint8Array([i]),
        idempotencyKey: '',
      }));
      events[0]!.userId = 'user_kafka_1';
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

      expect(seen.map((event) => event.userId)).toEqual(['user_kafka_1', undefined, undefined]);
    } finally {
      await deleteTopicsForSession(ws, ses);
    }
  }, 30000);
});
