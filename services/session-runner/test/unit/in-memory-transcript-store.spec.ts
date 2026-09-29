// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the in-memory, tunnel-fed transcript store — the self-hosted
// runner's history substrate (NO Kafka, NO Postgres, NO network).
//
// A self-hosted runner is outbound-WSS-only behind a NAT: it has no direct
// transcript backend. This store is what the claude provider's SDK `SessionStore`
// adapter reads `load()` from and writes `append()` to in-process, so cross-turn
// conversation continuity holds WITHIN the live runner without any broker. It is
// populated by two sources, both asserted here:
//   1. the agent's own emitted turns — the adapter's `append()`;
//   2. the registry-pushed recovery replay — `ingest()` (the tunnel feed).
// `read()` then serves both back in order, honoring the cursor / subpath / bound
// the `TranscriptStore` contract defines.

import { describe, expect, it } from 'vitest';
import type { Event } from '@orca/transcript-store-types';
import { InMemoryTranscriptStore } from '../../src/transcript/in-memory-transcript-store.js';

const WS = 'ws_mem';
const SES = 'ses_mem';

/** A minimal transcript Event with sane defaults for the fields the store ignores. */
function event(overrides: Partial<Event> & Pick<Event, 'id'>): Event {
  return {
    workspaceId: WS,
    sessionId: SES,
    subpath: '',
    seq: 0,
    producedAt: '2026-01-01T00:00:00.000Z',
    producedBy: 'harness',
    kind: 'harness.claude.session_entry',
    payload: new Uint8Array(0),
    idempotencyKey: '',
    ...overrides,
  };
}

/** Drain a read into an array (the bounded read ends on its own at the watermark). */
async function drain(it: AsyncIterable<Event>): Promise<Event[]> {
  const out: Event[] = [];
  for await (const e of it) {
    out.push(e);
  }
  return out;
}

describe('InMemoryTranscriptStore — append + read round-trip', () => {
  it('appends a batch and reads it back in order from the beginning', async () => {
    const store = new InMemoryTranscriptStore();
    await store.append(WS, SES, [event({ id: 'a' }), event({ id: 'b' })]);
    await store.append(WS, SES, [event({ id: 'c' })]);

    const read = await drain(store.read(WS, SES, { fromCursor: '', maxEvents: 0, subpath: '' }));
    expect(read.map((e) => e.id)).toEqual(['a', 'b', 'c']);
  });

  it('returns one id per input position and stamps a monotonic seq on append', async () => {
    const store = new InMemoryTranscriptStore();
    const ids = await store.append(WS, SES, [event({ id: 'a' }), event({ id: 'b' })]);
    expect(ids).toEqual(['a', 'b']);

    const read = await drain(store.read(WS, SES, { fromCursor: '', maxEvents: 0, subpath: '' }));
    // seq is store-assigned (the inputs carried seq 0) and strictly increasing.
    expect(read.map((e) => e.seq)).toEqual([0, 1]);
  });

  it('DEDUPS a re-appended id (the existing id round-trips, no duplicate stored)', async () => {
    const store = new InMemoryTranscriptStore();
    await store.append(WS, SES, [event({ id: 'a' })]);
    const ids = await store.append(WS, SES, [event({ id: 'a' }), event({ id: 'b' })]);
    // The contract: existing ids round-trip on dedup; only the new one is stored.
    expect(ids).toEqual(['a', 'b']);

    const read = await drain(store.read(WS, SES, { fromCursor: '', maxEvents: 0, subpath: '' }));
    expect(read.map((e) => e.id)).toEqual(['a', 'b']); // 'a' stored once
  });

  it('isolates events by (workspace, session)', async () => {
    const store = new InMemoryTranscriptStore();
    await store.append(WS, SES, [event({ id: 'a' })]);
    await store.append('ws_other', SES, [event({ id: 'x', workspaceId: 'ws_other' })]);
    await store.append(WS, 'ses_other', [event({ id: 'y', sessionId: 'ses_other' })]);

    const read = await drain(store.read(WS, SES, { fromCursor: '', maxEvents: 0, subpath: '' }));
    expect(read.map((e) => e.id)).toEqual(['a']);
  });
});

describe('InMemoryTranscriptStore — read cursor + bound', () => {
  it('reads only events AFTER fromCursor (the seq string of the last consumed event)', async () => {
    const store = new InMemoryTranscriptStore();
    await store.append(WS, SES, [event({ id: 'a' }), event({ id: 'b' }), event({ id: 'c' })]);
    const all = await drain(store.read(WS, SES, { fromCursor: '', maxEvents: 0, subpath: '' }));
    const cursor = String(all[0]!.seq); // resume strictly after the first event

    const rest = await drain(
      store.read(WS, SES, { fromCursor: cursor, maxEvents: 0, subpath: '' }),
    );
    expect(rest.map((e) => e.id)).toEqual(['b', 'c']);
  });

  it('bounds the read at maxEvents (>0); 0 is unbounded', async () => {
    const store = new InMemoryTranscriptStore();
    await store.append(WS, SES, [event({ id: 'a' }), event({ id: 'b' }), event({ id: 'c' })]);
    const two = await drain(store.read(WS, SES, { fromCursor: '', maxEvents: 2, subpath: '' }));
    expect(two.map((e) => e.id)).toEqual(['a', 'b']);
  });
});

describe('InMemoryTranscriptStore — subpath scoping', () => {
  it('"" reads parent-only; "*" reads all; an exact subpath matches one', async () => {
    const store = new InMemoryTranscriptStore();
    await store.append(WS, SES, [
      event({ id: 'p1', subpath: '' }),
      event({ id: 's1', subpath: 'subagents/x' }),
      event({ id: 'p2', subpath: '' }),
    ]);

    const parent = await drain(store.read(WS, SES, { fromCursor: '', maxEvents: 0, subpath: '' }));
    expect(parent.map((e) => e.id)).toEqual(['p1', 'p2']);

    const all = await drain(store.read(WS, SES, { fromCursor: '', maxEvents: 0, subpath: '*' }));
    expect(all.map((e) => e.id)).toEqual(['p1', 's1', 'p2']);

    const one = await drain(
      store.read(WS, SES, { fromCursor: '', maxEvents: 0, subpath: 'subagents/x' }),
    );
    expect(one.map((e) => e.id)).toEqual(['s1']);
  });
});

describe('InMemoryTranscriptStore — tunnel feed (ingest) + adapter feed share one log', () => {
  it('ingest() makes a registry-pushed event readable, in append order with the agent turns', async () => {
    const store = new InMemoryTranscriptStore();
    // The agent's own turn lands via append…
    await store.append(WS, SES, [event({ id: 'turn-1' })]);
    // …and a recovery-replayed event lands via the tunnel feed.
    store.ingest(event({ id: 'replayed-1', kind: 'agent.message' }));

    const read = await drain(store.read(WS, SES, { fromCursor: '', maxEvents: 0, subpath: '*' }));
    expect(read.map((e) => e.id)).toEqual(['turn-1', 'replayed-1']);
  });

  it('ingest() DEDUPS by id (a re-pushed replayed event is a no-op)', async () => {
    const store = new InMemoryTranscriptStore();
    store.ingest(event({ id: 'r1' }));
    store.ingest(event({ id: 'r1' })); // re-push (flapping reconnect): no duplicate

    const read = await drain(store.read(WS, SES, { fromCursor: '', maxEvents: 0, subpath: '*' }));
    expect(read.map((e) => e.id)).toEqual(['r1']);
  });
});

describe('InMemoryTranscriptStore — archive + close', () => {
  it('archive() appends a session.archived sentinel readable on the parent log', async () => {
    const store = new InMemoryTranscriptStore();
    await store.archive(WS, SES);
    const read = await drain(store.read(WS, SES, { fromCursor: '', maxEvents: 0, subpath: '' }));
    expect(read).toHaveLength(1);
    expect(read[0]!.kind).toBe('session.archived');
  });

  it('close() is a no-op (no connections to release) and read still works after', async () => {
    const store = new InMemoryTranscriptStore();
    await store.append(WS, SES, [event({ id: 'a' })]);
    await expect(store.close()).resolves.toBeUndefined();
    const read = await drain(store.read(WS, SES, { fromCursor: '', maxEvents: 0, subpath: '' }));
    expect(read.map((e) => e.id)).toEqual(['a']);
  });
});

describe('InMemoryTranscriptStore — tail (from-now follow)', () => {
  it('delivers events appended AFTER the tail subscribed, then ends on abort', async () => {
    const store = new InMemoryTranscriptStore();
    await store.append(WS, SES, [event({ id: 'before' })]);

    const ac = new AbortController();
    const seen: string[] = [];
    const tailDone = (async () => {
      for await (const e of store.tail(WS, SES, {
        fromCursor: '',
        subpath: '',
        signal: ac.signal,
      })) {
        seen.push(e.id);
        if (e.id === 'after-2') {
          ac.abort();
        }
      }
    })();

    // Appends after the subscribe are delivered; the pre-existing 'before' is not.
    await store.append(WS, SES, [event({ id: 'after-1' })]);
    await store.append(WS, SES, [event({ id: 'after-2' })]);
    await tailDone;

    expect(seen).toEqual(['after-1', 'after-2']);
  });
});
