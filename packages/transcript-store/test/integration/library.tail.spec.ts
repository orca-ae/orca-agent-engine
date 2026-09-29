// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { Event } from '../../src/types.js';
import type { KafkaTranscriptStore } from '../../src/kafka-store.js';
import { deleteTopicsForSession, makeKafka, makeStore, uniqueIds } from './setup.js';
import { createEmptyTopic } from '../support/empty-kafka-topic.js';

function makeEvent(ws: string, ses: string, marker: number): Event {
  return {
    id: `evt_tail_${marker}_${Date.now()}_${Math.random().toString(36).slice(2)}`,
    workspaceId: ws,
    sessionId: ses,
    subpath: '',
    seq: 0,
    producedAt: new Date().toISOString(),
    producedBy: 'harness',
    kind: 'agent.message',
    payload: new Uint8Array([marker]),
    idempotencyKey: '',
  };
}

describe('KafkaTranscriptStore.tail (library, integration)', () => {
  let store: KafkaTranscriptStore;

  beforeAll(() => {
    store = makeStore();
  });
  afterAll(async () => {
    await store.close();
  });

  it('from-now: an empty cursor delivers ONLY events appended after the tail starts', async () => {
    const { ws, ses } = uniqueIds('tail');
    const ac = new AbortController();
    try {
      // Pre-seed events that already exist BEFORE the tail subscribes. The
      // from-now contract (TailOptions.fromCursor "" = current head) means a
      // tail must NOT replay these.
      await store.append(
        ws,
        ses,
        [1, 2, 3].map((i) => makeEvent(ws, ses, i)),
      );

      const seen: Event[] = [];
      // Gate on the contract signal, not a fixed sleep: `onReady` fires after
      // the consumer has joined AND applied its head seek (consumer.ts), which
      // is exactly the ordering guarantee this test needs before appending —
      // deterministic on a slow CI broker where a sleep could lose the race.
      let markReady: () => void;
      const ready = new Promise<void>((resolve) => {
        markReady = resolve;
      });
      const tailDone = (async () => {
        for await (const e of store.tail(ws, ses, {
          fromCursor: '',
          subpath: '',
          onReady: () => markReady(),
          signal: ac.signal,
        })) {
          seen.push(e);
          if (seen.length === 2) {
            ac.abort();
            break;
          }
        }
      })();

      await ready;
      await store.append(ws, ses, [makeEvent(ws, ses, 4), makeEvent(ws, ses, 5)]);

      await tailDone;
      // Only the post-subscribe events (markers 4,5) are delivered — the
      // pre-seeded 1,2,3 are NOT replayed.
      expect(seen.map((e) => Buffer.from(e.payload)[0])).toEqual([4, 5]);
    } finally {
      ac.abort();
      await deleteTopicsForSession(ws, ses);
    }
  }, 30000);

  it('from-now on an EMPTY topic does not end and streams the first appends live', async () => {
    const { ws, ses } = uniqueIds('tailnew');
    const ac = new AbortController();
    try {
      // The topic exists but is empty: head is offset 0. A from-now tail must
      // NOT end here (bounded read ends on an empty topic; tail follows live) and
      // must deliver the first appends as they land.
      await createEmptyTopic(makeKafka().admin(), `orca.${ws}.sessions.${ses}.events`);

      const seen: Event[] = [];
      let markReady: () => void;
      const ready = new Promise<void>((resolve) => {
        markReady = resolve;
      });
      const tailDone = (async () => {
        for await (const e of store.tail(ws, ses, {
          fromCursor: '',
          subpath: '',
          onReady: () => markReady(),
          signal: ac.signal,
        })) {
          seen.push(e);
          if (seen.length === 3) {
            ac.abort();
            break;
          }
        }
      })();

      // Same ready gate as above: positioned-and-ready before the first append.
      await ready;
      await store.append(
        ws,
        ses,
        [1, 2, 3].map((i) => makeEvent(ws, ses, i)),
      );

      await tailDone;
      expect(seen.map((e) => Buffer.from(e.payload)[0])).toEqual([1, 2, 3]);
    } finally {
      ac.abort();
      await deleteTopicsForSession(ws, ses);
    }
  }, 30000);

  it('an explicit cursor replays forward from that offset (inclusive)', async () => {
    const { ws, ses } = uniqueIds('tailcur');
    const ac = new AbortController();
    try {
      const appended = await store.append(
        ws,
        ses,
        [10, 20, 30, 40].map((i) => makeEvent(ws, ses, i)),
      );
      void appended;

      // Learn the offsets via a bounded read.
      const all: Event[] = [];
      for await (const e of store.read(ws, ses, { fromCursor: '', maxEvents: 0, subpath: '' })) {
        all.push(e);
      }
      expect(all).toHaveLength(4);
      const cursor = String(all[1]!.seq); // resume from the 2nd event, inclusive

      const seen: Event[] = [];
      for await (const e of store.tail(ws, ses, {
        fromCursor: cursor,
        subpath: '',
        signal: ac.signal,
      })) {
        seen.push(e);
        if (seen.length === 3) {
          ac.abort();
          break;
        }
      }
      // Explicit cursor is a replay-from-offset (NOT from-now): markers 20,30,40.
      expect(seen.map((e) => Buffer.from(e.payload)[0])).toEqual([20, 30, 40]);
      expect(String(seen[0]!.seq)).toBe(cursor);
    } finally {
      ac.abort();
      await deleteTopicsForSession(ws, ses);
    }
  }, 30000);
});
