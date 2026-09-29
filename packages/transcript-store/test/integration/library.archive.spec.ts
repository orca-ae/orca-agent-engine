// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { Event } from '../../src/types.js';
import type { KafkaTranscriptStore } from '../../src/kafka-store.js';
import { deleteTopicsForSession, makeStore, uniqueIds } from './setup.js';

describe('KafkaTranscriptStore.archive (library, integration)', () => {
  let store: KafkaTranscriptStore;

  beforeAll(() => {
    store = makeStore();
  });
  afterAll(async () => {
    await store.close();
  });

  it('emits a session.archived sentinel readable via read()', async () => {
    const { ws, ses } = uniqueIds('archive');
    try {
      await store.archive(ws, ses);

      const seen: Event[] = [];
      for await (const e of store.read(ws, ses, { fromCursor: '', maxEvents: 0, subpath: '' })) {
        seen.push(e);
      }
      expect(seen).toHaveLength(1);
      expect(seen[0]!.kind).toBe('session.archived');
      expect(seen[0]!.producedBy).toBe('transcript-store');
      expect(Buffer.from(seen[0]!.payload).length).toBe(0);
    } finally {
      await deleteTopicsForSession(ws, ses);
    }
  }, 30000);
});
