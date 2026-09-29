// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, describe, expect, it } from 'vitest';
import { KafkaDiskIndex } from '../../src/kafka-disk-index.js';
import { encodeKafkaBlob, type KafkaBlobValue } from '../../src/kafka-blob.js';
import { boundedAgentEventId, eventIdentityKey } from '../../src/event-identity.js';
import {
  initialKafkaCheckpoint,
  kafkaTranscriptHash,
  projectKafkaEvents,
} from '../../src/kafka-state.js';
import {
  collectKafkaMembership,
  initialKafkaState,
  kafkaStateKeys,
  loadKafkaState,
  migrateKafkaCheckpoint,
  projectKafkaState,
  readKafkaStateBlob,
  validateKafkaState,
  type KafkaStateProjection,
} from '../../src/kafka-state-v2.js';
import type { PinnedDeliveryContext } from '../../src/types.js';
import { completedPrimaryTurnEvents, event, SESSION_ID, WORKSPACE_ID } from '../support/events.js';

const route = { topic: 'transcript', workspaceId: WORKSPACE_ID, sessionId: SESSION_ID };
const key = JSON.stringify([WORKSPACE_ID, SESSION_ID]);
const context: PinnedDeliveryContext = {
  organizationId: 'org_test',
  bindingId: 'aob_test',
  bindingVersion: 1,
  adapterType: 'otlp_http',
  endpointKind: 'traces_endpoint',
  endpointClass: 'public',
  endpointUrl: 'https://collector.example/api/public/otel/v1/traces',
  semanticProfile: 'langfuse',
  protocol: 'http/json',
  compression: 'none',
  timeoutMs: 1000,
  captureMode: 'metadata_only',
  sampleRate: 1,
  configSchemaVersion: 1,
};
const messages = completedPrimaryTurnEvents().map((event) => ({
  offset: String(event.seq),
  event,
}));
const indexes: KafkaDiskIndex[] = [];
async function open() {
  const index = await KafkaDiskIndex.open({});
  indexes.push(index);
  return index;
}
let offset = 0;
async function apply(index: KafkaDiskIndex, result: KafkaStateProjection) {
  for (let start = 0; start < result.records.length; start += 256) {
    await index.apply(
      result.records
        .slice(start, start + 256)
        .map((entry) => ({ ...entry, offset: String(offset++) })),
    );
  }
  return loadKafkaState(result.headValue, route, key, index);
}
afterEach(async () => {
  await Promise.all(indexes.splice(0).map((index) => index.close()));
});

describe('Kafka service-private v2 state', () => {
  it('excludes only completion markers from membership collection', () => {
    const marker = event(1, 'session.user_event_completed', { user_event_id: 'evt_user_turn' });
    const acceptance = event(2, 'session.user_event_processed', { user_event_id: 'evt_user_turn' });
    const reducer = initialKafkaCheckpoint(route, context).reducer;
    expect(collectKafkaMembership(reducer, [{ offset: '1', event: marker }])).toEqual({
      identities: [],
      accepted: [],
    });
    expect(collectKafkaMembership(reducer, [{ offset: '2', event: acceptance }])).toEqual({
      identities: [eventIdentityKey(acceptance.id)],
      accepted: [boundedAgentEventId(acceptance.id), 'evt_user_turn'],
    });
  });

  it('preserves legacy completion identities across migration, commit and replay', async () => {
    const marker = event(1, 'session.user_event_completed', { user_event_id: 'evt_user_turn' });
    const legacy = {
      ...initialKafkaCheckpoint(route, context),
      nextOffset: '2',
      identities: [
        { key: eventIdentityKey(marker.id), hash: kafkaTranscriptHash(marker), offset: '1' },
      ],
    };
    const index = await open();
    const state = await apply(index, migrateKafkaCheckpoint(JSON.stringify(legacy), route, key));
    const identityKey = kafkaStateKeys(key).identity(eventIdentityKey(marker.id));
    const before = await index.read([identityKey]);
    const replay = { ...marker, seq: 2, producedAt: '2026-01-02T00:00:00.000Z' };
    const batch = [{ offset: '2', event: replay }];
    const projected = await projectKafkaState(state, batch, key, index);
    expect(projected.deliveries).toEqual([]);
    expect(projected.head.nextOffset).toBe('3');
    expect(projected.head.identityCount).toBe(1);
    expect(state.head.nextOffset).toBe('2');
    const restored = await apply(index, projected);
    await validateKafkaState(restored, key, index);
    expect(await index.read([identityKey])).toEqual(before);
    const repeated = await projectKafkaState(restored, batch, key, index);
    expect(repeated.deliveries).toEqual([]);
    expect(repeated.head.nextOffset).toBe('3');
    expect(repeated.head.identityCount).toBe(1);
  });

  it('replays raw state across every split without changing persisted delivery content', async () => {
    const raw = { ...context, captureMode: 'raw_io' };
    const oracle = projectKafkaEvents(initialKafkaCheckpoint(route, raw), messages, 'raw_io');
    expect(oracle.deliveries[0]!.trace.schemaVersion).toBe('orca.observability.projected-trace.v2');
    for (let split = 1; split < messages.length; split++) {
      const index = await open();
      const initial = await apply(index, initialKafkaState(route, raw, key));
      const first = await projectKafkaState(
        initial,
        messages.slice(0, split),
        key,
        index,
        undefined,
        'raw_io',
      );
      const restored = await apply(index, first);
      const second = await projectKafkaState(restored, messages, key, index, undefined, 'raw_io');
      expect(second.deliveries).toEqual(oracle.deliveries);
      const final = await apply(index, second);
      await validateKafkaState(final, key, index);
      expect(
        (await projectKafkaState(final, messages, key, index, undefined, 'raw_io')).deliveries,
      ).toEqual([]);
    }
  });

  it('never upgrades metadata pins even when direct callers request content', async () => {
    const index = await open();
    const initial = await apply(index, initialKafkaState(route, context, key));
    const projected = await projectKafkaState(initial, messages, key, index, undefined, 'raw_io');
    expect(projected.deliveries[0]!.trace.schemaVersion).toBe(
      'orca.observability.projected-trace.v1',
    );
    expect(JSON.stringify(projected)).not.toContain('content-that-must-not-be-exported');
  });

  it('does not retain unknown top-level head fields in subsequent checkpoints', async () => {
    const index = await open();
    const initial = initialKafkaState(route, null, key);
    const raw = JSON.stringify({ ...initial.head, unrecognized: 'must-not-be-republished' });
    await index.apply([{ key, value: raw, offset: String(offset++) }]);
    const loaded = await loadKafkaState(raw, route, key, index);
    const result = await projectKafkaState(loaded, [], key, index);
    expect(result.headValue).not.toContain('must-not-be-republished');
    expect(result.head).not.toHaveProperty('unrecognized');
  });

  it('rejects incomplete positional membership reads before projecting', async () => {
    const index = await open();
    const initial = await apply(index, initialKafkaState(route, null, key));
    const incomplete = {
      read: async (keys: string[], expected?: { key: string; value: string }) => {
        const rows = await index.read(keys, expected);
        return keys.length ? rows.slice(0, -1) : rows;
      },
      countPrefix: (prefix: string) => index.countPrefix(prefix),
    };
    await expect(projectKafkaState(initial, messages.slice(0, 1), key, incomplete)).rejects.toThrow(
      'membership read',
    );
  });

  it('stops hostile 1KiB/1024-chunk manifests after one bounded read, not 65MB', async () => {
    const blob: KafkaBlobValue = {
      kind: 'chunks',
      bytes: 1024,
      count: 1024,
      keyPrefix: 'R/hostile',
      sha256: '0'.repeat(64),
    };
    const value = JSON.stringify({ version: 1, data: Buffer.alloc(48_000).toString('base64') });
    const batches: number[] = [];
    let receivedBytes = 0;
    const index = {
      read: async (keys: string[]) => {
        if (keys.length) batches.push(keys.length);
        receivedBytes += keys.length * Buffer.byteLength(value);
        return keys.map(() => value);
      },
    };
    await expect(readKafkaStateBlob(index, blob, blob.keyPrefix)).rejects.toThrow(
      'oversized chunk',
    );
    expect(batches).toEqual([1]);
    expect(receivedBytes).toBeLessThan(65_536);
  });

  it('checks cumulative raw bytes and canonical envelopes before reading further chunks', async () => {
    const blob: KafkaBlobValue = {
      kind: 'chunks',
      bytes: 1024,
      count: 1024,
      keyPrefix: 'D/hostile',
      sha256: '0'.repeat(64),
    };
    for (const bad of [
      JSON.stringify({ version: 1, data: Buffer.alloc(2).toString('base64') }),
      JSON.stringify({ version: 1, data: 'Zh==' }),
      JSON.stringify({ version: 1, data: 'Zg==', extra: true }),
    ]) {
      let reads = 0;
      const index = {
        read: async (keys: string[]) =>
          keys.map(() => {
            reads++;
            return reads === 1 ? JSON.stringify({ version: 1, data: 'Zg==' }) : bad;
          }),
      };
      await expect(readKafkaStateBlob(index, blob, blob.keyPrefix)).rejects.toThrow(
        'Invalid Kafka blob',
      );
      expect(reads).toBe(2);
    }
  });

  it('roundtrips delivery blobs through bounded reads with optional expected head', async () => {
    const original = { text: 'test'.repeat(100_000) };
    const encoded = encodeKafkaBlob(original, 'D/test', { inlineBytes: 0 });
    const records = new Map(encoded.records.map((record) => [record.key, record.value]));
    const expected = { key: 'head', value: 'serialized' };
    let largest = 0;
    const index = {
      read: async (keys: string[], actual?: { key: string; value: string }) => {
        expect(actual).toEqual(expected);
        largest = Math.max(largest, keys.length);
        return keys.map((key) => records.get(key) ?? null);
      },
    };
    expect(await readKafkaStateBlob(index, encoded.blob, 'D/test', expected)).toEqual(original);
    expect(largest).toBeLessThanOrEqual(2);
  });

  it('matches the real v1 projector across every split and preserves replay membership', async () => {
    for (let split = 0; split <= messages.length; split++) {
      const index = await open();
      const initial = await apply(index, initialKafkaState(route, context, key));
      const first = await projectKafkaState(initial, messages.slice(0, split), key, index);
      const next = await apply(index, first);
      const second = await projectKafkaState(next, messages.slice(split), key, index);
      const state = await apply(index, second);
      const oracle = projectKafkaEvents(initialKafkaCheckpoint(route, context), messages);
      expect([...first.deliveries, ...second.deliveries]).toEqual(oracle.deliveries);
      expect(state.reducer).toEqual(oracle.checkpoint.reducer);
      expect(state.head.identityCount).toBe(oracle.checkpoint.identities.length);
      expect(state.head.acceptedCount).toBe(oracle.checkpoint.acceptedSourceIds.length);
      await validateKafkaState(state, key, index);
      const replay = event(
        30,
        'session.user_event_processed',
        { user_event_id: 'evt_user_turn' },
        { id: 'evt_new_accept' },
      );
      expect(
        (await projectKafkaState(state, [{ offset: '30', event: replay }], key, index)).deliveries,
      ).toEqual([]);
    }
  });

  it('migrates real v1 and authenticates the unchanged baseline after growth', async () => {
    const old = projectKafkaEvents(
      initialKafkaCheckpoint(route, context),
      messages.slice(0, 4),
    ).checkpoint;
    const imported = migrateKafkaCheckpoint(JSON.stringify(old), route, key, { inlineBytes: 0 });
    expect(imported.deliveries).toEqual([]);
    expect(imported.records.at(-1)).toEqual({ key, value: imported.headValue });
    expect(
      imported.records
        .slice(0, -1)
        .every((entry) => ['I/', 'A/', 'R/'].some((prefix) => entry.key.startsWith(prefix))),
    ).toBe(true);
    const index = await open();
    const state = await apply(index, imported);
    await validateKafkaState(state, key, index);
    const projected = await projectKafkaState(state, messages.slice(4), key, index, {
      inlineBytes: 0,
    });
    const grown = await apply(index, projected);
    expect(grown.head.identityCount).toBeGreaterThan(old.identities.length);
    expect(grown.head.importBaseline).toEqual(imported.head.importBaseline);
    await validateKafkaState(grown, key, index);
    const first = imported.records.find((entry) => entry.key.startsWith('I/'))!;
    await index.apply([
      {
        key: first.key,
        value: JSON.stringify({ ...JSON.parse(first.value), hash: '0'.repeat(64) }),
        offset: String(offset++),
      },
    ]);
    await expect(validateKafkaState(grown, key, index)).rejects.toThrow('import ledger');
  });

  it('preserves null suppression, exact first offsets, and baseline acceptance visibility', async () => {
    const old = projectKafkaEvents(initialKafkaCheckpoint(route, context), messages).checkpoint;
    old.deliveryContext = null;
    const imported = migrateKafkaCheckpoint(JSON.stringify(old), route, key);
    expect(imported.head.deliveryContext).toBeNull();
    const identity = JSON.parse(
      imported.records.find((entry) => entry.key.startsWith('I/'))!.value,
    );
    expect(identity).toMatchObject({
      ...old.identities[0],
      visibleOffset: old.nextOffset,
      imported: true,
    });
    const accepted = JSON.parse(
      imported.records.find((entry) => entry.key.startsWith('A/'))!.value,
    );
    expect(accepted).toEqual({
      id: old.acceptedSourceIds[0],
      visibleOffset: old.nextOffset,
      imported: true,
    });
    const index = await open();
    const state = await apply(index, imported);
    const result = await projectKafkaState(
      state,
      [{ offset: '40', event: event(40, 'session.status_idle') }],
      key,
      index,
    );
    expect(result.deliveries).toEqual([]);
    expect(result.head.deliveryContext).toBeNull();
  });

  it('rejects future identities rather than treating them as past or overwriting them', async () => {
    const index = await open();
    const state = await apply(index, initialKafkaState(route, null, key));
    const source = messages[0]!;
    const identityKey = eventIdentityKey(source.event.id);
    await index.apply([
      {
        key: kafkaStateKeys(key).identity(identityKey),
        value: JSON.stringify({
          key: identityKey,
          hash: '0'.repeat(64),
          offset: '99',
          visibleOffset: '100',
          imported: false,
        }),
        offset: String(offset++),
      },
    ]);
    await expect(projectKafkaState(state, [source], key, index)).rejects.toThrow('future');
  });

  it('rejects changed heads, duplicate source offsets, identity conflicts and missing ledgers', async () => {
    const index = await open();
    const state = await apply(index, initialKafkaState(route, null, key));
    await expect(
      projectKafkaState(state, [messages[0]!, messages[0]!], key, index),
    ).rejects.toThrow('ordering');
    const first = await projectKafkaState(state, messages.slice(0, 1), key, index);
    const current = await apply(index, first);
    await expect(loadKafkaState(state.headValue, route, key, index)).rejects.toThrow();
    await expect(
      projectKafkaState(
        current,
        [{ offset: '40', event: { ...messages[0]!.event, payload: Buffer.from('bad') } }],
        key,
        index,
      ),
    ).rejects.toThrow('identity conflict');
    const identity = first.records.find((entry) => entry.key.startsWith('I/'))!;
    await index.apply([{ key: identity.key, value: null, offset: String(offset++) }]);
    await expect(validateKafkaState(current, key, index)).rejects.toThrow('incomplete');
  });

  it('normalizes raw IDs once, and never re-normalizes pending digest IDs', () => {
    const raw = 'evt_' + 'x'.repeat(600);
    const normalized = boundedAgentEventId(raw)!;
    const reducer = projectKafkaEvents(initialKafkaCheckpoint(route, context), [
      { offset: '1', event: event(1, 'user.message', {}, { id: raw, producedBy: 'client' }) },
    ]).checkpoint.reducer;
    const candidates = collectKafkaMembership(reducer, [
      { offset: '2', event: event(2, 'session.user_event_processed', { user_event_id: raw }) },
    ]);
    expect(candidates.accepted).toContain(normalized);
    expect(candidates.accepted).not.toContain(boundedAgentEventId(normalized));
  });

  it('verifies a large bounded import with batched point reads, without loading growing history', async () => {
    const old = initialKafkaCheckpoint(route, null);
    old.acceptedSourceIds = Array.from({ length: 30_000 }, (_, i) => `evt_${i}`);
    const imported = migrateKafkaCheckpoint(JSON.stringify(old), route, key);
    const index = await open();
    const state = await apply(index, imported);
    let largest = 0;
    const boundedIndex = {
      read: async (keys: string[], expected?: { key: string; value: string }) => {
        largest = Math.max(largest, keys.length);
        return index.read(keys, expected);
      },
      countPrefix: (prefix: string) => index.countPrefix(prefix),
    };
    await validateKafkaState(state, key, boundedIndex);
    expect(largest).toBeLessThanOrEqual(256);
    expect(state.head.acceptedCount).toBe(30_000);
    const projected = await projectKafkaState(
      state,
      [{ offset: '1', event: event(1, 'session.status_running') }],
      key,
      boundedIndex,
    );
    await validateKafkaState(await apply(index, projected), key, boundedIndex);
    // Real SQLite import plus repeated authentication is a capacity regression, not a 5s SLO.
    // Coverage instrumentation on shared CI runners needs headroom without shrinking the ledger.
  }, 30_000);

  it('keeps late acceptance and raw digest-namespace IDs equivalent to v1', async () => {
    for (const raw of ['evt_' + 'x'.repeat(600), 'evt_digest_' + '1'.repeat(64), 'evt_\ud800']) {
      const index = await open();
      const original = initialKafkaCheckpoint(route, context);
      const initial = await apply(index, initialKafkaState(route, context, key));
      const inputs = [
        { offset: '1', event: event(1, 'user.message', {}, { id: raw, producedBy: 'client' }) },
        { offset: '2', event: event(2, 'session.user_event_processed', { user_event_id: raw }) },
        { offset: '3', event: event(3, 'session.status_idle') },
      ];
      const first = await projectKafkaState(initial, inputs, key, index);
      const state = await apply(index, first);
      const oracle = projectKafkaEvents(original, inputs);
      expect(first.deliveries).toEqual(oracle.deliveries);
      const replay = [
        { offset: '4', event: event(4, 'session.user_event_processed', { user_event_id: raw }) },
      ];
      expect((await projectKafkaState(state, replay, key, index)).deliveries).toEqual(
        projectKafkaEvents(oracle.checkpoint, replay).deliveries,
      );
    }
  });

  it('bounds v1 migration bytes and rejects wrong-scope chunks', async () => {
    expect(() => migrateKafkaCheckpoint(' '.repeat(524289), route, key)).toThrow('migration limit');
    const imported = migrateKafkaCheckpoint(
      JSON.stringify(initialKafkaCheckpoint(route, null)),
      route,
      key,
      { inlineBytes: 0 },
    );
    const index = await open();
    await apply(index, imported);
    const head = {
      ...imported.head,
      reducer: { ...imported.head.reducer, keyPrefix: 'R/foreign/state/' },
    };
    const raw = JSON.stringify(head);
    await index.apply([{ key, value: raw, offset: String(offset++) }]);
    await expect(loadKafkaState(raw, route, key, index)).rejects.toThrow('prefix');
  });
});
