// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import {
  RetryableSessionEventError,
  SessionEventBarrierError,
  type Event,
} from '../../src/types.js';
import {
  normalizePulsarModule,
  pulsarTopicName,
  PulsarSessionEventSource,
  PulsarTranscriptStore,
} from '../../src/pulsar-store.js';

type FakePulsarModule = ReturnType<typeof normalizePulsarModule>;

describe('PulsarTranscriptStore', () => {
  it('rejects tenant identifiers that can alter the Pulsar topic namespace', () => {
    expect(() =>
      pulsarTopicName({ workspaceId: 'ws_a.sessions.ses_x', sessionId: 'ses_y' }),
    ).toThrow(/invalid Pulsar workspaceId/);
    expect(() => pulsarTopicName({ workspaceId: 'ws_a', sessionId: 'ses_x/ses_y' })).toThrow(
      /invalid Pulsar sessionId/,
    );
    expect(() =>
      pulsarTopicName({ tenant: 'public/default', workspaceId: 'ws_a', sessionId: 'ses_y' }),
    ).toThrow(/invalid Pulsar tenant/);
  });

  it('rejects events whose embedded route differs from the append target', async () => {
    const fake = new FakePulsar();
    const store = new PulsarTranscriptStore({
      client: fake.client(),
      pulsar: fake.module(),
      readTimeoutMs: 10,
    });
    const [event] = makeEvents('ws_forged', 'ses_a', [1]);

    await expect(store.append('ws_a', 'ses_a', [event!])).rejects.toThrow(/event route mismatch/);
    await store.close();
  });

  it('normalizes pulsar-client CommonJS default exports under ESM import', () => {
    const module = new FakePulsar().module();

    expect(normalizePulsarModule({ default: module }).Client).toBe(module.Client);
    expect(normalizePulsarModule({ 'module.exports': module }).MessageId).toBe(module.MessageId);
    expect(normalizePulsarModule(module).Client).toBe(module.Client);
  });

  it('shares one owned client across concurrent first appends', async () => {
    const fake = new FakePulsar();
    const store = new PulsarTranscriptStore({ pulsar: fake.module() });
    const { ws, ses } = ids('concurrent_client');

    try {
      await Promise.all(
        [0, 1, 2, 3].map((i) => {
          const sessionId = `${ses}_${i}`;
          return store.append(ws, sessionId, makeEvents(ws, sessionId, [i]));
        }),
      );

      expect(fake.clientConfigs).toHaveLength(1);
    } finally {
      await store.close();
    }
  });

  it('passes token authentication to the Pulsar client', async () => {
    const fake = new FakePulsar();
    const store = new PulsarTranscriptStore({
      pulsar: fake.module(),
      auth: { type: 'token', token: 'pulsar-token' },
      readTimeoutMs: 10,
    });
    const { ws, ses } = ids('auth_token');
    const [event] = makeEvents(ws, ses, [1]);

    await store.append(ws, ses, [event!]);

    expect(fake.clientConfigs[0]?.authentication).toEqual({
      kind: 'token',
      params: { token: 'pulsar-token' },
    });
    await store.close();
  });

  it('passes OAuth2 authentication to the Pulsar client', async () => {
    const fake = new FakePulsar();
    const store = new PulsarTranscriptStore({
      pulsar: fake.module(),
      auth: {
        type: 'oauth2',
        issuerUrl: 'https://issuer.example',
        clientId: 'client-id',
        clientSecret: 'client-secret',
        audience: 'pulsar-audience',
        scope: 'produce consume',
      },
      readTimeoutMs: 10,
    });
    const { ws, ses } = ids('auth_oauth2');
    const [event] = makeEvents(ws, ses, [1]);

    await store.append(ws, ses, [event!]);

    expect(fake.clientConfigs[0]?.authentication).toEqual({
      kind: 'oauth2',
      params: {
        type: 'client_credentials',
        issuer_url: 'https://issuer.example',
        client_id: 'client-id',
        client_secret: 'client-secret',
        audience: 'pulsar-audience',
        scope: 'produce consume',
      },
    });
    await store.close();
  });

  it('dedupes duplicate IDs across subpaths and reads events in topic order', async () => {
    const fake = new FakePulsar({ messageTopicSuffix: '-partition-0' });
    const store = new PulsarTranscriptStore({
      client: fake.client(),
      pulsar: fake.module(),
      readTimeoutMs: 10,
    });
    const { ws, ses } = ids('read');
    const events = makeEvents(ws, ses, [1, 2, 3]);

    await store.append(ws, ses, events);
    expect(events.map((event) => event.seq)).toEqual([0, 1, 2]);
    const duplicate = { ...events[1]!, subpath: 'subagents/dedup-child', seq: 0 };
    await store.append(ws, ses, [duplicate]);
    expect(duplicate.seq).toBe(events[1]!.seq);

    const seen: Event[] = [];
    for await (const event of store.read(ws, ses, { fromCursor: '', maxEvents: 0, subpath: '*' })) {
      seen.push(event);
    }

    expect(seen.map((event) => event.id)).toEqual(events.map((event) => event.id));
    expect(seen.map((event) => event.seq)).toEqual([0, 1, 2]);
    expect(seen.map((event) => event.subpath)).toEqual(['', '', '']);
    await store.close();
  });

  it('preserves optional user attribution in properties and decodes legacy empty values as absent', async () => {
    const fake = new FakePulsar();
    const store = new PulsarTranscriptStore({
      client: fake.client(),
      pulsar: fake.module(),
      readTimeoutMs: 10,
    });
    const { ws, ses } = ids('user_id');
    const events = makeEvents(ws, ses, [1, 2, 3]);
    events[0]!.userId = 'user_pulsar_1';
    events[1]!.userId = '';
    const legacyEmpty = { ...events[2]!, id: `${events[2]!.id}_legacy_empty` };

    await store.append(ws, ses, events);
    fake.publishEvent(pulsarTopicName({ workspaceId: ws, sessionId: ses }), legacyEmpty, {
      user_id: '',
    });

    const seen: Event[] = [];
    for await (const event of store.read(ws, ses, { fromCursor: '', maxEvents: 0, subpath: '' })) {
      seen.push(event);
    }

    expect(fake.sentProperties[0]).toMatchObject({ user_id: 'user_pulsar_1' });
    expect(fake.sentProperties[1]).not.toHaveProperty('user_id');
    expect(fake.sentProperties[2]).not.toHaveProperty('user_id');
    expect(seen.map((event) => event.userId)).toEqual([
      'user_pulsar_1',
      undefined,
      undefined,
      undefined,
    ]);
    await store.close();
  });

  it('remembers successful sends when another event in the append batch fails', async () => {
    const fake = new FakePulsar({ rejectSendIndexes: [1] });
    const store = new PulsarTranscriptStore({
      client: fake.client(),
      pulsar: fake.module(),
      readTimeoutMs: 10,
    });
    const { ws, ses } = ids('partial_append');
    const events = makeEvents(ws, ses, [1, 2, 3]);

    await expect(store.append(ws, ses, events)).rejects.toThrow('send failed');
    await expect(store.append(ws, ses, events)).resolves.toEqual(events.map((event) => event.id));

    const seen: Event[] = [];
    for await (const event of store.read(ws, ses, { fromCursor: '', maxEvents: 0, subpath: '' })) {
      seen.push(event);
    }
    expect(seen).toHaveLength(3);
    expect(new Set(seen.map((event) => event.id))).toEqual(
      new Set(events.map((event) => event.id)),
    );
    expect(fake.sentPartitionKeys).toHaveLength(4);
    await store.close();
  });

  it('makes one append visible to a Pulsar handler as a single batch', async () => {
    const fake = new FakePulsar({
      waitForFirstUnbatchedAck: true,
      realisticBatchMessageIds: true,
    });
    const store = new PulsarTranscriptStore({
      client: fake.client(),
      pulsar: fake.module(),
      readTimeoutMs: 10,
    });
    const source = new PulsarSessionEventSource({
      client: fake.client(),
      subscription: 'pulsar-append-batch-unit',
      receiveTimeoutMs: 10,
      topicRediscoverIntervalMs: 0,
    });
    const { ws, ses } = ids('append_batch_visibility');
    const [userEvent, systemEvent] = makeEvents(ws, ses, [1, 2], 'client');
    userEvent!.kind = 'user.message';
    systemEvent!.kind = 'system.message';
    const duplicateUserEvent = { ...userEvent!, seq: -1 };
    let companion: Event | undefined;
    let resolveHandled: (() => void) | undefined;
    let rejectHandled: ((cause: unknown) => void) | undefined;
    const handled = new Promise<void>((resolve, reject) => {
      resolveHandled = resolve;
      rejectHandled = reject;
    });

    try {
      await source.start(async (event) => {
        if (event.id !== userEvent!.id) return;
        try {
          for await (const candidate of store.read(ws, ses, {
            fromCursor: String(event.seq + 1),
            maxEvents: 1,
            subpath: '*',
          })) {
            companion = candidate;
          }
          resolveHandled?.();
        } catch (cause) {
          rejectHandled?.(cause);
        }
      });
      await waitFor(() => fake.subscribeCount === 1);

      await store.append(ws, ses, [userEvent!, duplicateUserEvent, systemEvent!]);
      await handled;

      expect(systemEvent!.seq).toBe(userEvent!.seq + 1);
      expect(companion).toMatchObject({
        id: systemEvent!.id,
        kind: 'system.message',
        seq: systemEvent!.seq,
      });
      expect(duplicateUserEvent.seq).toBe(userEvent!.seq);
      expect(fake.producerConfigs[0]).toMatchObject({
        batchingEnabled: true,
        batchingType: 'KeyBasedBatching',
        batchingMaxPublishDelayMs: 60_000,
        batchingMaxMessages: 1_000,
        batchingMaxAllowedSizeInBytes: 2 * 1024 * 1024,
      });
      expect(fake.publishedBatchSizes).toEqual([2]);
      expect(fake.sentPartitionKeys).toEqual([ses, ses]);
    } finally {
      await source.stop();
      await store.close();
    }
  });

  it('waits for delayed broker delivery after hasNext confirms backlog', async () => {
    const fake = new FakePulsar();
    const client = fake.client();
    const createReader = client.createReader.bind(client);
    vi.spyOn(client, 'createReader').mockImplementation(async (config) => {
      const reader = await createReader(config);
      const readNext = reader.readNext.bind(reader);
      vi.spyOn(reader, 'readNext').mockImplementation(async (timeout) => {
        await new Promise((resolve) => setTimeout(resolve, 150));
        if ((timeout ?? 0) < 150) throw new Error('broker delivery timed out');
        return readNext(timeout);
      });
      return reader;
    });
    const store = new PulsarTranscriptStore({ client, pulsar: fake.module() });
    const { ws, ses } = ids('delayed_reader');
    const events = makeEvents(ws, ses, [1, 2]);
    try {
      await store.append(ws, ses, events);
      const seen: Event[] = [];
      for await (const event of store.read(ws, ses, { fromCursor: '', maxEvents: 0, subpath: '*' }))
        seen.push(event);
      expect(seen.map(({ id }) => id)).toEqual(events.map(({ id }) => id));
    } finally {
      await store.close();
    }
  });

  it('reads an uncapped bounded snapshot up to the reader high-watermark', async () => {
    const fake = new FakePulsar();
    const store = new PulsarTranscriptStore({
      client: fake.client(),
      pulsar: fake.module(),
      readTimeoutMs: 10,
    });
    const { ws, ses } = ids('read_uncapped_snapshot');
    const events = makeEvents(ws, ses, [1, 2]);

    await store.append(ws, ses, events);

    const seen: Event[] = [];
    for await (const event of store.read(ws, ses, { fromCursor: '', maxEvents: 0, subpath: '' })) {
      seen.push(event);
    }

    expect(seen.map((event) => event.id)).toEqual(events.map((event) => event.id));
    await store.close();
  });

  it.each(['timeout', 'connection closed'])(
    'rejects a partial recovery read on %s',
    async (reason) => {
      const fake = new FakePulsar();
      const module = fake.module();
      const client = new module.Client({ serviceUrl: 'unused' });
      const createReader = client.createReader.bind(client);
      const failure = new Error(reason);
      let fail = true;
      const closed = vi.fn();
      vi.spyOn(client, 'createReader').mockImplementation(async (config) => {
        const reader = await createReader(config);
        const readNext = reader.readNext.bind(reader);
        let read = 0;
        vi.spyOn(reader, 'readNext').mockImplementation(async (timeout) => {
          if (fail && ++read === 2) throw failure;
          return readNext(timeout);
        });
        const close = reader.close.bind(reader);
        vi.spyOn(reader, 'close').mockImplementation(async () => {
          closed();
          return close();
        });
        return reader;
      });
      const store = new PulsarTranscriptStore({ client, pulsar: module, readTimeoutMs: 1 });
      const { ws, ses } = ids('partial_recovery');
      const events = makeEvents(ws, ses, [1, 2, 3]);
      try {
        await store.append(ws, ses, events);
        const seen: Event[] = [];
        const read = async () => {
          for await (const event of store.read(ws, ses, {
            fromCursor: '',
            maxEvents: 0,
            subpath: '*',
          }))
            seen.push(event);
        };
        await expect(read()).rejects.toBe(failure);
        expect(seen.map(({ id }) => id)).toEqual([events[0]!.id]);
        expect(closed).toHaveBeenCalledTimes(1);
        fail = false;
        seen.length = 0;
        await read();
        expect(seen.map(({ id }) => id)).toEqual(events.map(({ id }) => id));
      } finally {
        await store.close();
      }
    },
  );

  it('does not include events appended after read starts', async () => {
    const fake = new FakePulsar();
    const store = new PulsarTranscriptStore({
      client: fake.client(),
      pulsar: fake.module(),
      readTimeoutMs: 100,
    });
    const { ws, ses } = ids('read_snapshot_boundary');
    const [first] = makeEvents(ws, ses, [1]);

    await store.append(ws, ses, [first!]);

    const seen: Event[] = [];
    const readDone = (async () => {
      for await (const event of store.read(ws, ses, {
        fromCursor: '',
        maxEvents: 0,
        subpath: '',
      })) {
        seen.push(event);
      }
    })();

    await waitFor(() => seen.length === 1);
    await sleep(10);
    const [second] = makeEvents(ws, ses, [2]);
    await store.append(ws, ses, [second!]);
    await readDone;

    expect(seen.map((event) => event.id)).toEqual([first!.id]);
    await store.close();
  });

  it('excludes post-start appends from the bounded snapshot', async () => {
    const fake = new FakePulsar();
    const store = new PulsarTranscriptStore({
      client: fake.client(),
      pulsar: fake.module(),
      readTimeoutMs: 100,
    });
    const { ws, ses } = ids('read_stale_append_boundary');
    const [first, second] = makeEvents(ws, ses, [1, 2]);

    await store.append(ws, ses, [first!]);

    const seen: Event[] = [];
    const readDone = (async () => {
      for await (const event of store.read(ws, ses, {
        fromCursor: '',
        maxEvents: 0,
        subpath: '',
      })) {
        seen.push(event);
      }
    })();

    await waitFor(() => seen.length === 1);
    await store.append(ws, ses, [second!]);
    await readDone;

    expect(seen.map((event) => event.id)).toEqual([first!.id]);
    await store.close();
  });

  it('includes pre-start appends even when the event timestamp is in the future', async () => {
    const fake = new FakePulsar();
    const store = new PulsarTranscriptStore({
      client: fake.client(),
      pulsar: fake.module(),
      readTimeoutMs: 10,
    });
    const { ws, ses } = ids('read_future_timestamp');
    const [event] = makeEvents(ws, ses, [1]);
    event!.producedAt = new Date(Date.now() + 60_000).toISOString();

    await store.append(ws, ses, [event!]);

    const seen: Event[] = [];
    for await (const readEvent of store.read(ws, ses, {
      fromCursor: '',
      maxEvents: 0,
      subpath: '',
    })) {
      seen.push(readEvent);
    }

    expect(seen.map((readEvent) => readEvent.id)).toEqual([event!.id]);
    await store.close();
  });

  it('reads a bounded snapshot after evicting a per-topic producer', async () => {
    const fake = new FakePulsar();
    const store = new PulsarTranscriptStore({
      client: fake.client(),
      pulsar: fake.module(),
      producerCacheCapacity: 1,
      readTimeoutMs: 100,
    });
    const { ws, ses } = ids('read_hwm_after_producer_eviction');
    const [first, second] = makeEvents(ws, ses, [1, 2]);
    const other = ids('read_hwm_evictor');
    const [evictor] = makeEvents(other.ws, other.ses, [3]);

    await store.append(ws, ses, [first!]);
    await store.append(other.ws, other.ses, [evictor!]);

    const seen: Event[] = [];
    const readDone = (async () => {
      for await (const event of store.read(ws, ses, {
        fromCursor: '',
        maxEvents: 0,
        subpath: '',
      })) {
        seen.push(event);
      }
    })();

    await waitFor(() => seen.length === 1);
    await store.append(ws, ses, [second!]);
    await readDone;

    expect(seen.map((event) => event.id)).toEqual([first!.id]);
    await store.close();
  });

  it('keeps large Pulsar ledger message ids monotonic', async () => {
    const fake = new FakePulsar({
      messageIdForIndex: (index) => `${10_000_000 + index}:0:0:0`,
    });
    const store = new PulsarTranscriptStore({
      client: fake.client(),
      pulsar: fake.module(),
      readTimeoutMs: 10,
    });
    const { ws, ses } = ids('large_ledger');
    const events = makeEvents(ws, ses, [1, 2]);

    await store.append(ws, ses, events);

    expect(events[0]!.seq).toBeGreaterThan(Number.MAX_SAFE_INTEGER);
    expect(events[1]!.seq).toBeGreaterThan(events[0]!.seq);
    await store.close();
  });

  it('uses the official Node client parenthesized Pulsar message id format for seqs', async () => {
    const fake = new FakePulsar({
      messageIdForIndex: (index) => `(55,${index},-1,-1)`,
    });
    const store = new PulsarTranscriptStore({
      client: fake.client(),
      pulsar: fake.module(),
      readTimeoutMs: 10,
    });
    const { ws, ses } = ids('parenthesized_message_id');
    const events = makeEvents(ws, ses, [1, 2, 3]);

    await store.append(ws, ses, events);

    expect(events.map((event) => event.seq)).toEqual([
      55_000_000_000, 55_000_010_000, 55_000_020_000,
    ]);

    const seen: Event[] = [];
    for await (const event of store.read(ws, ses, { fromCursor: '', maxEvents: 0, subpath: '' })) {
      seen.push(event);
    }

    expect(seen.map((event) => event.seq)).toEqual(events.map((event) => event.seq));
    await store.close();
  });

  it('treats three-part Pulsar message ids as unbatched', async () => {
    const fake = new FakePulsar({
      messageIdForIndex: (index) => `(55,${index},3)`,
    });
    const store = new PulsarTranscriptStore({
      client: fake.client(),
      pulsar: fake.module(),
      readTimeoutMs: 10,
    });
    const { ws, ses } = ids('three_part_message_id');
    const events = makeEvents(ws, ses, [1, 2]);

    await store.append(ws, ses, events);

    expect(events.map((event) => event.seq)).toEqual([55_000_000_000, 55_000_010_000]);
    await store.close();
  });

  it('does not drop existing messages when the broker clock is ahead', async () => {
    const fake = new FakePulsar({ publishTimestampOffsetMs: 60_000 });
    const store = new PulsarTranscriptStore({
      client: fake.client(),
      pulsar: fake.module(),
      readTimeoutMs: 10,
    });
    const { ws, ses } = ids('read_clock_skew');
    const events = makeEvents(ws, ses, [1, 2]);

    await store.append(ws, ses, events);

    const seen: Event[] = [];
    for await (const event of store.read(ws, ses, { fromCursor: '', maxEvents: 0, subpath: '' })) {
      seen.push(event);
    }

    expect(seen.map((event) => event.id)).toEqual(events.map((event) => event.id));
    await store.close();
  });

  it('caps per-topic dedup memory', async () => {
    const fake = new FakePulsar();
    const store = new PulsarTranscriptStore({
      client: fake.client(),
      pulsar: fake.module(),
      dedupCapacity: 1,
      readTimeoutMs: 10,
    });
    const { ws, ses } = ids('dedup_cap');
    const [first, second] = makeEvents(ws, ses, [1, 2]);

    await store.append(ws, ses, [first!]);
    await store.append(ws, ses, [second!]);
    await store.append(ws, ses, [first!]);

    const seen: Event[] = [];
    for await (const event of store.read(ws, ses, { fromCursor: '', maxEvents: 0, subpath: '' })) {
      seen.push(event);
    }

    expect(seen.map((event) => event.id)).toEqual([first!.id, second!.id, first!.id]);
    await store.close();
  });

  it('evicts and closes old per-topic producers', async () => {
    const fake = new FakePulsar();
    const store = new PulsarTranscriptStore({
      client: fake.client(),
      pulsar: fake.module(),
      producerCacheCapacity: 1,
      readTimeoutMs: 10,
    });
    const { ws, ses } = ids('producer_cap');
    const [first] = makeEvents(ws, ses, [1]);
    const [second] = makeEvents(ws, `${ses}_other`, [2]);

    await store.append(ws, ses, [first!]);
    await store.append(ws, `${ses}_other`, [second!]);

    expect(fake.closedProducers).toBe(1);
    await store.close();
  });

  it('tails events appended after subscription from partition topic names', async () => {
    const fake = new FakePulsar({ messageTopicSuffix: '-partition-1' });
    const store = new PulsarTranscriptStore({
      client: fake.client(),
      pulsar: fake.module(),
      tailReadTimeoutMs: 10,
    });
    const { ws, ses } = ids('tail');
    const ac = new AbortController();
    const received: Event[] = [];

    const done = (async () => {
      for await (const event of store.tail(ws, ses, {
        fromCursor: '',
        subpath: '',
        signal: ac.signal,
      })) {
        received.push(event);
        ac.abort();
      }
    })();

    await new Promise((resolve) => setTimeout(resolve, 20));
    const [event] = makeEvents(ws, ses, [9]);
    await store.append(ws, ses, [event!]);
    await done;

    expect(received.map((e) => e.id)).toEqual([event!.id]);
    await store.close();
  });

  it('feeds partition-topic client events through PulsarSessionEventSource', async () => {
    const fake = new FakePulsar({ messageTopicSuffix: '-partition-2' });
    const { ws, ses } = ids('source');
    const source = new PulsarSessionEventSource({
      client: fake.client(),
      subscription: 'harness-test',
      receiveTimeoutMs: 10,
    });
    const store = new PulsarTranscriptStore({
      client: fake.client(),
      pulsar: fake.module(),
      readTimeoutMs: 10,
    });
    const seen: Event[] = [];

    await source.start(async (event) => {
      seen.push(event);
    });
    expect(fake.consumerConfigs[0]?.topicsPattern).toBe(
      'persistent://public/default/orca\\.([A-Za-z0-9_-]+)\\.sessions\\.([A-Za-z0-9_-]+)\\.events(?:-partition-\\d+)?',
    );
    const events = makeEvents(ws, ses, [7, 8], 'client', 'user.message');
    await store.append(ws, ses, events);
    await waitFor(() => seen.length === 2);

    expect(fake.consumerConfigs[0]).toMatchObject({
      subscriptionType: 'KeyShared',
      keySharedPolicy: { keyShareMode: 'AutoSplit', allowOutOfOrderDelivery: false },
    });

    expect(seen.map((event) => event.id)).toEqual(events.map((event) => event.id));
    expect(seen.map((event) => event.seq)).toEqual([0, 1]);
    expect(fake.acknowledged).toEqual(expect.arrayContaining(events.map((event) => event.id)));
    await source.stop();
    await store.close();
  });

  it.each(['session.archived', 'session.deleted'] as const)(
    'feeds %s sentinels through the default event filter',
    async (kind) => {
      const fake = new FakePulsar();
      const { ws, ses } = ids(`source_lifecycle_${kind.replace('.', '_')}`);
      const source = new PulsarSessionEventSource({
        client: fake.client(),
        subscription: 'harness-test',
        receiveTimeoutMs: 10,
      });
      const store = new PulsarTranscriptStore({
        client: fake.client(),
        pulsar: fake.module(),
        readTimeoutMs: 10,
      });
      const seen: Event[] = [];

      await source.start(async (event) => {
        seen.push(event);
      });
      if (kind === 'session.archived') {
        await store.archive(ws, ses);
      } else {
        await store.append(ws, ses, makeEvents(ws, ses, [0], 'registry-service', kind));
      }
      await waitFor(() => seen.length === 1);

      expect(seen[0]).toMatchObject({
        workspaceId: ws,
        sessionId: ses,
        producedBy: kind === 'session.archived' ? 'transcript-store' : 'registry-service',
        kind,
      });
      expect(fake.acknowledged).toContain(seen[0]!.id);
      await source.stop();
      await store.close();
    },
  );

  it('acks and drops messages whose properties forge the actual topic route', async () => {
    const fake = new FakePulsar();
    const { ws, ses } = ids('source_forged_route');
    const source = new PulsarSessionEventSource({
      client: fake.client(),
      subscription: 'harness-test',
      receiveTimeoutMs: 10,
    });
    const handler = vi.fn(async () => undefined);
    const [event] = makeEvents(ws, ses, [19], 'client', 'user.message');
    const topic = pulsarTopicName({ workspaceId: ws, sessionId: ses });

    await source.start(handler);
    fake.publishEvent(topic, event!, { workspace_id: 'ws_forged' });
    await waitFor(() => fake.acknowledged.includes(event!.id));

    expect(handler).not.toHaveBeenCalled();
    expect(fake.nacked).not.toContain(event!.id);
    await source.stop();
  });

  it('retries event source startup failures', async () => {
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => undefined);
    const fake = new FakePulsar({ subscribeFailures: 1 });
    const { ws, ses } = ids('source_retry');
    const source = new PulsarSessionEventSource({
      client: fake.client(),
      subscription: 'harness-test',
      receiveTimeoutMs: 10,
    });
    const store = new PulsarTranscriptStore({
      client: fake.client(),
      pulsar: fake.module(),
      readTimeoutMs: 10,
    });
    const seen: Event[] = [];

    await source.start(async (event) => {
      seen.push(event);
    });
    expect(source.status()).toEqual({ ready: true, state: 'running' });
    await waitFor(() =>
      errorSpy.mock.calls.some(
        ([message]) => message === 'pulsar session event source subscribe failed',
      ),
    );
    expect(source.status()).toEqual({ ready: true, state: 'running' });
    const [event] = makeEvents(ws, ses, [9], 'client', 'user.message');
    await store.append(ws, ses, [event!]);
    await waitFor(() => seen.length === 1);
    await source.stop();
    expect(source.status()).toEqual({ ready: false, state: 'stopped' });
    await store.close();

    expect(errorSpy).toHaveBeenCalledWith(
      'pulsar session event source subscribe failed',
      expect.any(Error),
    );
    errorSpy.mockRestore();
  });

  it('logs and settles a run loop crash instead of leaving an unhandled rejection', async () => {
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => undefined);
    // A handler failure is caught inside run()'s loop, but negativeAcknowledge
    // throwing from that catch branch is not — it escapes run() and rejects
    // start()'s runPromise unless caught.
    const fake = new FakePulsar({ negativeAcknowledgeThrows: true });
    const { ws, ses } = ids('source_crash');
    const source = new PulsarSessionEventSource({
      client: fake.client(),
      subscription: 'harness-test',
      receiveTimeoutMs: 10,
      // The throwing nack leaves an outstanding-nack entry behind; keep the
      // quiescence window tiny so stop()'s bounded drain wait stays short.
      nAckRedeliverTimeoutMs: 1,
      maxConcurrentHandlers: 1,
      maxPendingDeliveries: 1,
    });
    const store = new PulsarTranscriptStore({
      client: fake.client(),
      pulsar: fake.module(),
      readTimeoutMs: 10,
    });

    await source.start(async () => {
      throw new Error('handler boom');
    });
    await store.append(ws, ses, makeEvents(ws, ses, [11, 12], 'client', 'user.message'));

    await waitFor(() =>
      errorSpy.mock.calls.some(
        ([message]) => message === 'PulsarSessionEventSource: run loop crashed',
      ),
    );

    expect(source.status()).toEqual({ ready: false, state: 'failed' });
    expect(JSON.stringify(source.status())).not.toContain('handler boom');
    await expect(source.whenFailed()).resolves.toBeUndefined();

    // stop() awaits the run promise; it must resolve instead of hanging or
    // rejecting even though the run loop crashed.
    await expect(source.stop()).resolves.toBeUndefined();
    await store.close();
    errorSpy.mockRestore();
  });

  it('does not resolve whenFailed for intentional stop', async () => {
    const fake = new FakePulsar();
    const source = new PulsarSessionEventSource({
      client: fake.client(),
      subscription: 'harness-intentional-stop',
      receiveTimeoutMs: 1,
    });
    const failed = vi.fn();
    void source.whenFailed().then(failed);

    await source.start(async () => undefined);
    await source.stop();
    await Promise.resolve();

    expect(failed).not.toHaveBeenCalled();
  });

  it('sanitizes terminal run-loop crash diagnostics', async () => {
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => undefined);
    const source = new PulsarSessionEventSource({
      client: new FakePulsar().client(),
      subscription: 'harness-terminal-log-sanitize',
    });
    (
      source as unknown as {
        run: (_handler: (event: Event) => Promise<void>) => Promise<void>;
      }
    ).run = async () => {
      const error = Object.assign(new Error('pulsar://token:secret@example.invalid'), {
        name: 'TokenLeakingError',
        code: 'ghp_0123456789abcdefghijklmnopqrstuvwxyzABCDEF',
      });
      throw error;
    };

    try {
      await source.start(async () => undefined);
      await source.whenFailed();

      expect(errorSpy).toHaveBeenCalledWith('PulsarSessionEventSource: run loop crashed', {
        name: 'Error',
        code: 'unknown',
      });
      expect(JSON.stringify(errorSpy.mock.calls)).not.toContain('secret');
      expect(JSON.stringify(errorSpy.mock.calls)).not.toContain('ghp_');
    } finally {
      await source.stop();
      errorSpy.mockRestore();
    }
  });

  it('refreshes idle pattern subscriptions to discover newly created session topics', async () => {
    const fake = new FakePulsar({ snapshotPatternTopics: true });
    const { ws, ses } = ids('source_refresh');
    const source = new PulsarSessionEventSource({
      client: fake.client(),
      subscription: 'harness-test',
      receiveTimeoutMs: 10,
      topicRediscoverIntervalMs: 20,
    });
    const store = new PulsarTranscriptStore({
      client: fake.client(),
      pulsar: fake.module(),
      readTimeoutMs: 10,
    });
    const seen: Event[] = [];

    await source.start(async (event) => {
      seen.push(event);
    });
    await new Promise((resolve) => setTimeout(resolve, 25));
    const [event] = makeEvents(ws, ses, [10], 'client', 'user.message');
    await store.append(ws, ses, [event!]);

    await waitFor(() => seen.length === 1);

    expect(seen.map((received) => received.id)).toEqual([event!.id]);
    expect(fake.subscribeCount).toBeGreaterThan(1);
    await source.stop();
    await store.close();
  });

  it('retains closed pattern consumers while native callbacks can still reference them', async () => {
    const fake = new FakePulsar({ snapshotPatternTopics: true });
    const source = new PulsarSessionEventSource({
      client: fake.client(),
      subscription: 'harness-test',
      receiveTimeoutMs: 5,
      topicRediscoverIntervalMs: 20,
    });

    await source.start(async () => undefined);
    await waitFor(() => fake.subscribeCount > 1);

    const retired = (
      source as unknown as {
        retiredConsumers: Array<{ consumer: unknown; releaseAt: number }>;
      }
    ).retiredConsumers;
    expect(fake.closedConsumers).toBeGreaterThan(0);
    expect(retired).toHaveLength(fake.closedConsumers);
    expect(retired.every(({ releaseAt }) => releaseAt > Date.now())).toBe(true);
    await source.stop();
  });

  it('waits for the native receive worker before closing on stop', async () => {
    const fake = new FakePulsar();
    const source = new PulsarSessionEventSource({
      client: fake.client(),
      subscription: 'harness-test',
      receiveTimeoutMs: 30,
      topicRediscoverIntervalMs: 0,
    });

    await source.start(async () => undefined);
    await waitFor(() => fake.activeConsumerReceives > 0);
    await source.stop();

    expect(fake.closedConsumers).toBe(1);
    expect(fake.closedWhileReceiving).toBe(0);
  });

  it('defaults to fast explicit rediscovery for pattern subscriptions', () => {
    const fake = new FakePulsar();
    const source = new PulsarSessionEventSource({
      client: fake.client(),
      subscription: 'harness-test',
    });

    expect(
      (source as unknown as { topicRediscoverIntervalMs: number }).topicRediscoverIntervalMs,
    ).toBe(5_000);
  });

  it('keeps a single subscription when rediscovery is disabled', async () => {
    const fake = new FakePulsar();
    const { ws, ses } = ids('no_rediscover');
    const source = new PulsarSessionEventSource({
      client: fake.client(),
      subscription: 'harness-test',
      receiveTimeoutMs: 5,
      topicRediscoverIntervalMs: 0,
    });
    const store = new PulsarTranscriptStore({
      client: fake.client(),
      pulsar: fake.module(),
      readTimeoutMs: 10,
    });
    const seen: Event[] = [];
    await source.start(async (event) => {
      seen.push(event);
    });
    const [event] = makeEvents(ws, ses, [12], 'client', 'user.message');
    await store.append(ws, ses, [event!]);
    await waitFor(() => seen.some((e) => e.id === event!.id));
    // Stay idle across several receive cycles; consumer must NOT recreate.
    await sleep(60);

    await source.stop();
    await store.close();
    expect(fake.subscribeCount).toBe(1);
    expect(fake.consumerConfigs[0]?.patternAutoDiscoveryPeriod).toBe(1);
  });

  it('acks and drops a poison message once the redelivery cap is reached', async () => {
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => undefined);
    const fake = new FakePulsar();
    const { ws, ses } = ids('poison');
    const source = new PulsarSessionEventSource({
      client: fake.client(),
      subscription: 'harness-test',
      receiveTimeoutMs: 10,
    });
    const store = new PulsarTranscriptStore({
      client: fake.client(),
      pulsar: fake.module(),
      readTimeoutMs: 10,
    });
    let handlerCalls = 0;

    await source.start(async () => {
      handlerCalls += 1;
      throw new Error('handler always fails');
    });
    const [event] = makeEvents(ws, ses, [13], 'client', 'user.tool_confirmation');
    await store.append(ws, ses, [event!]);

    // Nacked while below the cap (MAX_HANDLER_REDELIVERIES = 5), then acked
    // and dropped instead of looping forever.
    await waitFor(() => fake.acknowledged.includes(event!.id));

    expect(fake.nacked).toEqual([event!.id, event!.id, event!.id, event!.id, event!.id]);
    expect(handlerCalls).toBe(6);
    expect(fake.acknowledged.filter((id) => id === event!.id)).toHaveLength(1);
    expect(
      errorSpy.mock.calls.some(
        ([message]) =>
          typeof message === 'string' &&
          message.includes('dropping poison message after 5 redeliveries'),
      ),
    ).toBe(true);
    await source.stop();
    await store.close();
    errorSpy.mockRestore();
  });

  it('keeps infrastructure failures retryable after the poison-message cap', async () => {
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => undefined);
    const fake = new FakePulsar();
    const { ws, ses } = ids('retryable_infrastructure');
    const source = new PulsarSessionEventSource({
      client: fake.client(),
      subscription: 'harness-test',
      receiveTimeoutMs: 10,
      nAckRedeliverTimeoutMs: 1,
    });
    const store = new PulsarTranscriptStore({
      client: fake.client(),
      pulsar: fake.module(),
      readTimeoutMs: 10,
    });

    let handlerCalls = 0;
    await source.start(async () => {
      handlerCalls += 1;
      if (handlerCalls <= 7) {
        throw new RetryableSessionEventError('acceptance store unavailable');
      }
    });
    const [event] = makeEvents(ws, ses, [14], 'client', 'user.message');
    await store.append(ws, ses, [event!]);
    await waitFor(() => fake.acknowledged.includes(event!.id));

    expect(fake.nacked.filter((id) => id === event!.id)).toHaveLength(7);
    expect(handlerCalls).toBe(8);
    expect(fake.acknowledged.filter((id) => id === event!.id)).toHaveLength(1);
    await source.stop();
    await store.close();
    errorSpy.mockRestore();
  });

  it('defers idle consumer refresh while a nack is outstanding and resumes once its redelivery retires it', async () => {
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => undefined);
    const fake = new FakePulsar();
    // Park the redelivery so the outstanding-nack state is observable.
    fake.holdRedeliveries = true;
    const { ws, ses } = ids('nack_outstanding');
    const source = new PulsarSessionEventSource({
      client: fake.client(),
      subscription: 'harness-test',
      receiveTimeoutMs: 5,
      topicRediscoverIntervalMs: 20,
      // Quiescence window = 2 * 10_000 + 500 — far beyond this test's
      // runtime, so any refresh below is enabled by the outstanding-nack
      // counter draining, never by the window elapsing.
      nAckRedeliverTimeoutMs: 10_000,
    });
    const store = new PulsarTranscriptStore({
      client: fake.client(),
      pulsar: fake.module(),
      readTimeoutMs: 10,
    });
    let failedOnce = false;

    await source.start(async () => {
      if (!failedOnce) {
        failedOnce = true;
        throw new Error('fail once');
      }
    });
    const [event] = makeEvents(ws, ses, [14], 'client', 'user.message');
    await store.append(ws, ses, [event!]);
    await waitFor(() => fake.nacked.includes(event!.id));
    expect(fake.consumerConfigs[0]?.nAckRedeliverTimeoutMs).toBe(10_000);

    // Several rediscover intervals pass while idle, but the consumer must not
    // be torn down while the nacked message still sits in the native
    // negative-ack tracker (its redelivery has not come back yet).
    await sleep(150);
    expect(fake.subscribeCount).toBe(1);

    // The redelivery arrives, the handler succeeds, and the retired nack
    // re-enables the idle refresh long before the time window would elapse.
    fake.holdRedeliveries = false;
    await waitFor(() => fake.acknowledged.includes(event!.id));
    await waitFor(() => fake.subscribeCount > 1, 3000);
    await source.stop();
    await store.close();
    errorSpy.mockRestore();
  });

  it('refreshes after the quiescence window even when the nack never comes back here', async () => {
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => undefined);
    const fake = new FakePulsar();
    // After a key ownership change redelivery can go to another replica, so
    // the local outstanding-nack counter never drains; the time window must
    // bound the refresh deferral in that case.
    fake.holdRedeliveries = true;
    const { ws, ses } = ids('nack_window');
    const source = new PulsarSessionEventSource({
      client: fake.client(),
      subscription: 'harness-test',
      receiveTimeoutMs: 5,
      topicRediscoverIntervalMs: 20,
      // Quiescence window = 2 * 50 + 500 (slack) = 600ms.
      nAckRedeliverTimeoutMs: 50,
    });
    const store = new PulsarTranscriptStore({
      client: fake.client(),
      pulsar: fake.module(),
      readTimeoutMs: 10,
    });

    await source.start(async () => {
      throw new Error('handler always fails');
    });
    const [event] = makeEvents(ws, ses, [15], 'client', 'user.message');
    await store.append(ws, ses, [event!]);
    await waitFor(() => fake.nacked.includes(event!.id));

    await waitFor(() => fake.subscribeCount > 1, 3000);
    await source.stop();
    await store.close();
    errorSpy.mockRestore();
  });
});

describe('Pulsar session delivery lanes', () => {
  it.each(['drain', 'stop', 'defaults'])(
    'bounds active handlers and pending deliveries during %s',
    async (mode) => {
      const fake = new FakePulsar();
      const module = fake.module();
      const client = new module.Client({ serviceUrl: 'unused' });
      const subscribe = client.subscribe.bind(client);
      const concurrencyLimit = mode === 'defaults' ? 8 : 2;
      const pendingLimit = mode === 'defaults' ? 100 : 5;
      let received = 0;
      let peakPending = 0;
      vi.spyOn(client, 'subscribe').mockImplementation(async (config) => {
        const consumer = await subscribe(config);
        const receive = consumer.receive.bind(consumer);
        vi.spyOn(consumer, 'receive').mockImplementation(async (timeout) => {
          const message = await receive(timeout);
          received += 1;
          peakPending = Math.max(peakPending, received - fake.acknowledged.length);
          return message;
        });
        return consumer;
      });
      const source = new PulsarSessionEventSource({
        client,
        subscription: 'bounded',
        receiveTimeoutMs: 1,
        topicRediscoverIntervalMs: 0,
        ...(mode === 'defaults'
          ? {}
          : { maxConcurrentHandlers: concurrencyLimit, maxPendingDeliveries: pendingLimit }),
      });
      const store = new PulsarTranscriptStore({ client: fake.client(), pulsar: module });
      const { ws, ses } = ids('bounded');
      const events = Array.from({ length: mode === 'defaults' ? 64 : 12 }, (_, i) =>
        makeEvents(ws, `${ses}_${i}`, [i, i + 100], 'client', 'user.message'),
      );
      let release!: () => void;
      const gate = new Promise<void>((resolve) => {
        release = resolve;
      });
      let active = 0;
      let peakActive = 0;
      const activeSessions = new Set<string>();
      const seen: Event[] = [];
      let overlap = false;
      try {
        for (const pair of events) await store.append(ws, pair[0]!.sessionId, pair);
        await source.start(async (event) => {
          overlap ||= activeSessions.has(event.sessionId);
          activeSessions.add(event.sessionId);
          active += 1;
          peakActive = Math.max(peakActive, active);
          seen.push(event);
          await gate;
          await sleep(1);
          active -= 1;
          activeSessions.delete(event.sessionId);
        });
        await waitFor(() => received >= pendingLimit);
        await sleep(20);
        expect(received).toBe(pendingLimit);
        expect(active).toBe(concurrencyLimit);
        expect(fake.acknowledged).toEqual([]);
        if (mode === 'drain') {
          release();
          await waitFor(() => fake.acknowledged.length === events.flat().length);
          expect(peakActive).toBe(concurrencyLimit);
          expect(peakPending).toBeLessThanOrEqual(pendingLimit);
          expect(overlap).toBe(false);
          for (const pair of events)
            expect(
              seen.filter(({ sessionId }) => sessionId === pair[0]!.sessionId).map(({ id }) => id),
            ).toEqual(pair.map(({ id }) => id));
        }
        const stopping = source.stop();
        await sleep(10);
        release();
        await stopping;
        if (mode !== 'drain') {
          expect(received).toBe(pendingLimit);
          expect(seen).toHaveLength(concurrencyLimit);
          expect(fake.acknowledged).toEqual([]);
        }
        expect(fake.nacked).toEqual([]);
        expect(fake.closedWhileReceiving).toBe(0);
      } finally {
        release();
        await source.stop();
        await store.close();
      }
    },
  );

  it.each(['repair', 'stop'])(
    'lets other workspace/session lanes progress during %s',
    async (mode) => {
      const fake = new FakePulsar();
      const { ws, ses } = ids('lanes');
      const store = new PulsarTranscriptStore({ client: fake.client(), pulsar: fake.module() });
      const source = new PulsarSessionEventSource({
        client: fake.client(),
        subscription: 'lanes',
        receiveTimeoutMs: 1,
        nAckRedeliverTimeoutMs: 5,
        topicRediscoverIntervalMs: 5,
        maxConcurrentHandlers: 1,
        maxPendingDeliveries: 10,
      });
      const first = makeEvents(ws, ses, [1], 'client', 'user.message')[0]!;
      const queued = makeEvents(ws, ses, [2, 3], 'client', 'user.interrupt');
      const others = [
        makeEvents(`${ws}_other`, ses, [4], 'client', 'user.message')[0]!,
        makeEvents(ws, `${ses}_other`, [5], 'client', 'user.message')[0]!,
      ];
      let available = false;
      let attempts = 0;
      let releaseLast!: () => void;
      const lastHandler = new Promise<void>((resolve) => {
        releaseLast = resolve;
      });
      const seen: string[] = [];
      const repair = async (): Promise<void> => {
        attempts += 1;
        if (!available) throw new SessionEventBarrierError('session-specific failure', repair);
      };
      const error = vi.spyOn(console, 'error').mockImplementation(() => undefined);
      try {
        await store.append(ws, ses, [first, ...queued]);
        for (const event of others) await store.append(event.workspaceId, event.sessionId, [event]);
        await source.start(async (event) => {
          seen.push(event.id);
          if (event.id === first.id) await repair();
          if (event.id === queued.at(-1)!.id) await lastHandler;
        });
        await waitFor(() => others.every(({ id }) => fake.acknowledged.includes(id)));
        await waitFor(() => attempts > 5);
        expect(seen).toEqual([first.id, ...others.map(({ id }) => id)]);
        expect(fake.nacked).toEqual([]);
        expect(fake.closedConsumers).toBe(0);
        if (mode === 'repair') {
          available = true;
          await waitFor(() => seen.includes(queued.at(-1)!.id));
          expect(seen).toEqual([
            first.id,
            ...others.map(({ id }) => id),
            ...queued.map(({ id }) => id),
          ]);
        }
        const stopping = source.stop();
        if (mode === 'repair') {
          await sleep(20);
          expect(fake.closedConsumers).toBe(0);
        }
        releaseLast();
        await stopping;
        const stoppedAttempts = attempts;
        await sleep(20);
        expect(attempts).toBe(stoppedAttempts);
        if (mode === 'stop') expect(fake.acknowledged).toEqual(others.map(({ id }) => id));
        expect(fake.nacked).toEqual([]);
        expect(fake.closedWhileReceiving).toBe(0);
      } finally {
        releaseLast();
        await source.stop();
        await store.close();
        error.mockRestore();
      }
    },
  );
});

class FakePulsar {
  private readonly topics = new Map<string, FakeMessage[]>();
  readonly acknowledged: string[] = [];
  readonly nacked: string[] = [];
  readonly clientConfigs: Array<{ authentication?: unknown }> = [];
  readonly producerConfigs: Array<{
    batchingEnabled?: boolean;
    batchingMaxPublishDelayMs?: number;
    batchingMaxMessages?: number;
    batchingMaxAllowedSizeInBytes?: number;
  }> = [];
  readonly publishedBatchSizes: number[] = [];
  readonly sentPartitionKeys: Array<string | undefined> = [];
  readonly sentProperties: Array<Record<string, string> | undefined> = [];
  readonly consumerConfigs: Array<{
    nAckRedeliverTimeoutMs?: number;
    patternAutoDiscoveryPeriod?: number;
    topicsPattern?: string;
  }> = [];
  subscribeCount = 0;
  closedProducers = 0;
  closedConsumers = 0;
  activeConsumerReceives = 0;
  closedWhileReceiving = 0;
  private sendCallIndex = 0;
  /**
   * While true, nacked messages stay parked instead of being redelivered on
   * the next receive(), emulating the real nack delay so tests can observe
   * the outstanding-nack state deterministically.
   */
  holdRedeliveries = false;

  private subscribeFailuresRemaining: number;

  constructor(
    private readonly opts: {
      publishTimestampOffsetMs?: number;
      subscribeFailures?: number;
      messageIdForIndex?: (index: number) => string;
      snapshotPatternTopics?: boolean;
      negativeAcknowledgeThrows?: boolean;
      messageTopicSuffix?: string;
      waitForFirstUnbatchedAck?: boolean;
      realisticBatchMessageIds?: boolean;
      rejectSendIndexes?: number[];
    } = {},
  ) {
    this.subscribeFailuresRemaining = opts.subscribeFailures ?? 0;
  }

  module(): FakePulsarModule {
    const clientConfigs = this.clientConfigs;
    const makeClient = this.client.bind(this);
    return {
      Client: class {
        constructor(config: { authentication?: unknown }) {
          clientConfigs.push(config);
          return makeClient();
        }
      },
      MessageId: {
        earliest: () => new FakeMessageId('earliest'),
        latest: () => new FakeMessageId('latest'),
        deserialize: () => new FakeMessageId('earliest'),
      },
      AuthenticationToken: class {
        readonly kind = 'token';
        constructor(readonly params: unknown) {}
      },
      AuthenticationOauth2: class {
        readonly kind = 'oauth2';
        constructor(readonly params: unknown) {}
      },
    } as unknown as FakePulsarModule;
  }

  publishEvent(topic: string, event: Event, propertyOverrides: Record<string, string> = {}): void {
    const messageIndex = this.topic(topic).length;
    this.topic(topic).push(
      new FakeMessage(
        Buffer.from(event.payload),
        {
          id: event.id,
          workspace_id: event.workspaceId,
          session_id: event.sessionId,
          subpath: event.subpath,
          produced_at: event.producedAt,
          produced_by: event.producedBy,
          kind: event.kind,
          idempotency_key: event.idempotencyKey,
          ...propertyOverrides,
        },
        new FakeMessageId(String(messageIndex)),
        this.opts.publishTimestampOffsetMs ?? 0,
        `${topic}${this.opts.messageTopicSuffix ?? ''}`,
      ),
    );
  }

  client(): never {
    return {
      createProducer: async (config: {
        topic: string;
        batchingEnabled?: boolean;
        batchingMaxPublishDelayMs?: number;
        batchingMaxMessages?: number;
        batchingMaxAllowedSizeInBytes?: number;
      }) => {
        this.producerConfigs.push(config);
        const pending: Array<{
          message: {
            data: Buffer;
            properties?: Record<string, string>;
            partitionKey?: string;
          };
          resolve: (messageId: FakeMessageId) => void;
        }> = [];
        let batchEntry = 0;
        const publish = (
          message: {
            data: Buffer;
            properties?: Record<string, string>;
          },
          messageId?: FakeMessageId,
        ): FakeMessageId => {
          const messageIndex = this.topic(config.topic).length;
          const stored = new FakeMessage(
            message.data,
            message.properties ?? {},
            messageId ??
              new FakeMessageId(
                this.opts.messageIdForIndex?.(messageIndex) ?? String(messageIndex),
              ),
            this.opts.publishTimestampOffsetMs ?? 0,
            `${config.topic}${this.opts.messageTopicSuffix ?? ''}`,
          );
          this.topic(config.topic).push(stored);
          return stored.getMessageId();
        };
        return {
          send: async (message: {
            data: Buffer;
            properties?: Record<string, string>;
            partitionKey?: string;
          }) => {
            this.sentPartitionKeys.push(message.partitionKey);
            this.sentProperties.push(message.properties);
            const sendIndex = this.sendCallIndex++;
            if (this.opts.rejectSendIndexes?.includes(sendIndex)) {
              throw new Error('send failed');
            }
            if (config.batchingEnabled) {
              return new Promise<FakeMessageId>((resolve) => pending.push({ message, resolve }));
            }
            const messageId = publish(message);
            if (this.opts.waitForFirstUnbatchedAck && this.topic(config.topic).length === 1) {
              await waitFor(() => this.acknowledged.includes(message.properties?.['id'] ?? ''));
            }
            return messageId;
          },
          flush: async () => {
            if (pending.length > 0) {
              this.publishedBatchSizes.push(pending.length);
              const batch = pending.splice(0);
              for (let i = 0; i < batch.length; i++) {
                const entry = batch[i]!;
                const messageId = this.opts.realisticBatchMessageIds
                  ? new FakeMessageId(`(55,${batchEntry},-1,${i})`)
                  : undefined;
                entry.resolve(publish(entry.message, messageId));
              }
              batchEntry += 1;
            }
            return null;
          },
          close: async () => {
            this.closedProducers += 1;
            return null;
          },
        };
      },
      createReader: async ({
        topic,
        startMessageId,
      }: {
        topic: string;
        startMessageId: FakeMessageId;
      }) => new FakeReader(this.topic(topic), startMessageId.kind === 'latest'),
      subscribe: async (config: {
        topic?: string;
        topicsPattern?: string;
        nAckRedeliverTimeoutMs?: number;
        patternAutoDiscoveryPeriod?: number;
      }) => {
        if (this.subscribeFailuresRemaining > 0) {
          this.subscribeFailuresRemaining -= 1;
          throw new Error('subscribe failed');
        }
        this.subscribeCount += 1;
        this.consumerConfigs.push(config);
        const { topic, topicsPattern } = config;
        const pattern = topic
          ? new RegExp(`^${escapeRegExp(topic)}$`)
          : new RegExp(`^(?:${topicsPattern ?? ''})$`);
        const topicSnapshot =
          !topic && this.opts.snapshotPatternTopics
            ? [...this.topics.keys()].filter((candidate) => pattern.test(candidate))
            : undefined;
        return new FakeConsumer(
          this.topics,
          pattern,
          topic === undefined,
          this.acknowledged,
          this.nacked,
          topicSnapshot,
          this.opts.negativeAcknowledgeThrows,
          () => this.holdRedeliveries,
          () => {
            this.activeConsumerReceives += 1;
          },
          () => {
            this.activeConsumerReceives -= 1;
          },
          () => {
            this.closedConsumers += 1;
            if (this.activeConsumerReceives > 0) this.closedWhileReceiving += 1;
          },
        );
      },
      close: async () => null,
    } as never;
  }

  private topic(topic: string): FakeMessage[] {
    let messages = this.topics.get(topic);
    if (!messages) {
      messages = [];
      this.topics.set(topic, messages);
    }
    return messages;
  }
}

class FakeMessageId {
  constructor(readonly kind: string) {}

  serialize(): Buffer {
    return Buffer.from(this.kind);
  }

  toString(): string {
    return this.kind;
  }
}

class FakeMessage {
  private readonly publishTimestamp: number;
  private redeliveryCount = 0;

  constructor(
    private readonly data: Buffer,
    private readonly properties: Record<string, string>,
    private readonly id: FakeMessageId,
    publishTimestampOffsetMs: number,
    private readonly topicName = '',
  ) {
    this.publishTimestamp = Date.now() + publishTimestampOffsetMs;
  }

  getData(): Buffer {
    return this.data;
  }

  getProperties(): Record<string, string> {
    return this.properties;
  }

  getMessageId(): FakeMessageId {
    return this.id;
  }

  getPublishTimestamp(): number {
    return this.publishTimestamp;
  }

  getRedeliveryCount(): number {
    return this.redeliveryCount;
  }

  getTopicName(): string {
    return this.topicName;
  }

  markRedelivered(): void {
    this.redeliveryCount += 1;
  }
}

class FakeReader {
  private index: number;
  private readonly highWatermark: number;

  constructor(
    private readonly messages: FakeMessage[],
    latest: boolean,
  ) {
    this.index = latest ? messages.length : 0;
    this.highWatermark = messages.length;
  }

  hasNext(): boolean {
    return this.index < this.highWatermark;
  }

  async readNext(timeoutMs?: number): Promise<FakeMessage> {
    const started = Date.now();
    while (Date.now() - started < (timeoutMs ?? 100)) {
      const message = this.messages[this.index];
      if (message) {
        this.index += 1;
        return message;
      }
      await new Promise((resolve) => setTimeout(resolve, 1));
    }
    throw new Error('timeout');
  }

  async close(): Promise<null> {
    return null;
  }

  async unsubscribe(): Promise<null> {
    return null;
  }
}

class FakeConsumer {
  private readonly offsets = new Map<string, number>();
  private readonly redeliveryQueue: FakeMessage[] = [];

  constructor(
    private readonly topics: Map<string, FakeMessage[]>,
    private readonly pattern: RegExp,
    private readonly matchPhysicalTopic: boolean,
    private readonly acknowledged: string[],
    private readonly nacked: string[],
    private readonly topicSnapshot?: string[],
    private readonly negativeAcknowledgeThrows?: boolean,
    private readonly redeliveriesHeld?: () => boolean,
    private readonly onReceiveStart?: () => void,
    private readonly onReceiveEnd?: () => void,
    private readonly onClose?: () => void,
  ) {}

  async receive(timeoutMs?: number): Promise<FakeMessage> {
    this.onReceiveStart?.();
    try {
      const started = Date.now();
      while (Date.now() - started < (timeoutMs ?? 100)) {
        if (!this.redeliveriesHeld?.()) {
          const redelivery = this.redeliveryQueue.shift();
          if (redelivery) return redelivery;
        }
        for (const [topic, messages] of this.iterTopics()) {
          // Pulsar applies a topic pattern as a full match against the physical
          // topic name, which includes `-partition-N` for partitioned topics.
          // The map key remains the logical topic so reader tests share data.
          const candidateTopic = this.matchPhysicalTopic
            ? (messages[0]?.getTopicName() ?? topic)
            : topic;
          if (!this.pattern.test(candidateTopic)) continue;
          const offset = this.offsets.get(topic) ?? 0;
          const message = messages[offset];
          if (message) {
            this.offsets.set(topic, offset + 1);
            return message;
          }
        }
        await new Promise((resolve) => setTimeout(resolve, 1));
      }
      throw new Error('timeout');
    } finally {
      this.onReceiveEnd?.();
    }
  }

  async acknowledge(message: FakeMessage): Promise<null> {
    this.acknowledged.push(message.getProperties()['id'] ?? '');
    return null;
  }

  // Unlike real Pulsar, redelivery is immediate (no nack delay) unless the
  // test parks it via FakePulsar.holdRedeliveries.
  negativeAcknowledge(message: FakeMessage): void {
    if (this.negativeAcknowledgeThrows) throw new Error('negative acknowledge failed');
    message.markRedelivered();
    this.nacked.push(message.getProperties()['id'] ?? '');
    this.redeliveryQueue.push(message);
  }

  async close(): Promise<null> {
    this.onClose?.();
    return null;
  }

  async unsubscribe(): Promise<null> {
    return null;
  }

  private *iterTopics(): Iterable<[string, FakeMessage[]]> {
    if (!this.topicSnapshot) {
      yield* this.topics;
      return;
    }
    for (const topic of this.topicSnapshot) {
      const messages = this.topics.get(topic);
      if (messages) yield [topic, messages];
    }
  }
}

function makeEvents(
  workspaceId: string,
  sessionId: string,
  payloads: number[],
  producedBy = 'harness',
  kind = 'agent.message',
): Event[] {
  return payloads.map((payload) => ({
    id: `evt_pulsar_${payload}_${Date.now()}_${Math.random().toString(36).slice(2)}`,
    workspaceId,
    sessionId,
    subpath: '',
    seq: 0,
    producedAt: new Date().toISOString(),
    producedBy,
    kind,
    payload: new Uint8Array([payload]),
    idempotencyKey: '',
  }));
}

function ids(prefix: string): { ws: string; ses: string } {
  const suffix = `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`;
  return { ws: `ws_${prefix}_${suffix}`, ses: `ses_${prefix}_${suffix}` };
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

async function waitFor(predicate: () => boolean, timeoutMs = 1000): Promise<void> {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    if (predicate()) return;
    await sleep(10);
  }
  throw new Error('condition not met');
}

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}
