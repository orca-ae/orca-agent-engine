// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { ConsumerRunConfig, Kafka, Producer } from 'kafkajs';
import { KafkaSharedState } from '../../src/kafka-shared-state.js';

const mock = vi.hoisted(() => ({ open: vi.fn() }));
vi.mock('../../src/kafka-disk-index.js', () => ({ KafkaDiskIndex: { open: mock.open } }));
const states: KafkaSharedState[] = [];
afterEach(async () => {
  await Promise.all(states.splice(0).map((state) => state.close()));
  vi.useRealTimers();
});
beforeEach(() => vi.clearAllMocks());

function fixture(options: { maxDiskBytes?: number; timeoutMs?: number } = {}) {
  const values = new Map<string, string>();
  const index = {
    apply: vi.fn(async (records: Array<{ key: string; value: string | null }>) => {
      for (const record of records) {
        if (record.value === null) values.delete(record.key);
        else values.set(record.key, record.value);
      }
    }),
    read: vi.fn(async (keys: string[]) => keys.map((key) => values.get(key) ?? null)),
    stats: vi.fn(async () => ({
      databaseBytes: 12288,
      databaseLimitBytes: 32768,
      diskQuotaBytes: 131072,
    })),
    close: vi.fn(async () => {}),
  };
  mock.open.mockResolvedValue(index);
  let run!: ConsumerRunConfig;
  let crash!: () => void;
  const consumer = {
    events: { CRASH: 'crash' },
    on: vi.fn((_event, callback) => {
      crash = callback;
    }),
    connect: vi.fn(async () => {}),
    subscribe: vi.fn(async () => {}),
    run: vi.fn(async (config: ConsumerRunConfig) => {
      run = config;
    }),
    disconnect: vi.fn(async () => {}),
  };
  const kafka = { consumer: vi.fn(() => consumer) };
  const failure = vi.fn();
  const state = new KafkaSharedState({
    kafka: kafka as unknown as Kafka,
    topic: 'state',
    timeoutMs: 500,
    onFailure: failure,
    ...options,
  });
  states.push(state);
  let barrier: { key: string; value: string } | undefined;
  const transaction = {
    send: vi.fn(async ({ messages }) => {
      barrier = messages[0];
      return [{ baseOffset: '9' }];
    }),
    commit: vi.fn(async () => {}),
    abort: vi.fn(async () => {}),
  };
  const producer = { transaction: vi.fn(async () => transaction) } as unknown as Producer;
  async function feed(
    records: Array<{ key: string; value: string | null; offset: string }>,
    overrides: { heartbeat?: () => Promise<void>; isStale?: () => boolean } = {},
  ) {
    await run.eachBatch!({
      batch: {
        topic: 'state',
        partition: 0,
        messages: records.map((r) => ({
          ...r,
          key: Buffer.from(r.key),
          value: r.value === null ? null : Buffer.from(r.value),
        })),
      },
      heartbeat: async () => {},
      resolveOffset: () => {},
      isRunning: () => true,
      isStale: () => false,
      ...overrides,
    } as never);
  }
  return {
    state,
    kafka,
    index,
    consumer,
    transaction,
    producer,
    failure,
    feed,
    crash: () => crash(),
    barrier: () => barrier!,
  };
}

describe('shared Kafka state materialization', () => {
  it.each(['apply', 'snapshot'] as const)(
    'maintains reader heartbeats during slow %s',
    async (io) => {
      vi.useFakeTimers();
      const f = fixture({ timeoutMs: 120_000 });
      let release!: () => void;
      const gate = new Promise<void>((resolve) => {
        release = resolve;
      });
      const snapshot = vi.fn(async () => {
        if (io === 'snapshot') await gate;
      });
      if (io === 'apply') f.index.apply.mockImplementationOnce(async () => gate);
      const pending = f.state.barrier(f.producer, 'source', new AbortController().signal, snapshot);
      await vi.waitFor(() => expect(f.transaction.commit).toHaveBeenCalledOnce());
      const heartbeat = vi.fn(async () => {});
      const feed = f.feed([{ ...f.barrier(), offset: '9' }], { heartbeat });
      await vi.advanceTimersByTimeAsync(0);
      const initial = heartbeat.mock.calls.length;
      await vi.advanceTimersByTimeAsync(65_000);
      expect(heartbeat.mock.calls.length - initial).toBeGreaterThanOrEqual(20);
      expect(f.kafka.consumer).toHaveBeenCalledWith(
        expect.objectContaining({
          sessionTimeout: 60_000,
          heartbeatInterval: 3_000,
          groupId: expect.stringMatching(/^orca-exporter-restore-/u),
        }),
      );
      release();
      await feed;
      await pending;
      await f.state.close();
      expect(vi.getTimerCount()).toBe(0);
      expect(f.failure).not.toHaveBeenCalled();
    },
  );

  it('fails active snapshots and all waiters immediately on heartbeat failure', async () => {
    vi.useFakeTimers();
    const f = fixture({ timeoutMs: 120_000 });
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const snapshot = vi.fn(async () => gate);
    const pending = f.state.barrier(f.producer, 'source', new AbortController().signal, snapshot);
    await vi.waitFor(() => expect(f.transaction.commit).toHaveBeenCalledOnce());
    const reason = new Error('reader heartbeat lost');
    const heartbeat = vi
      .fn(async () => {})
      .mockResolvedValueOnce(undefined)
      .mockRejectedValue(reason);
    const rejected = expect(pending).rejects.toBe(reason);
    const waiting = expect(f.state.waitForOffset('20', new AbortController().signal)).rejects.toBe(
      reason,
    );
    const feed = f.feed([{ ...f.barrier(), offset: '9' }], { heartbeat });
    const feedRejected = expect(feed).rejects.toBe(reason);
    await vi.advanceTimersByTimeAsync(3_000);
    expect(snapshot).toHaveBeenCalledOnce();
    expect(f.failure).toHaveBeenCalledWith(reason);
    await rejected;
    await waiting;
    release();
    await feedRejected;
    await f.state.close();
    expect(vi.getTimerCount()).toBe(0);
  });

  it('propagates idle disk failure even when the failure callback throws', async () => {
    const f = fixture();
    await f.state.start();
    const reason = new Error('Kafka disk index worker unavailable');
    const rejected = expect(f.state.waitForOffset('20', new AbortController().signal)).rejects.toBe(
      reason,
    );
    f.failure.mockImplementation(() => {
      throw new Error('observer failed');
    });
    expect(() => mock.open.mock.calls[0]![0].onFailure(reason)).not.toThrow();
    await rejected;
    await expect(f.state.read(async () => null)).rejects.toBe(reason);
    await f.state.close();
    expect(f.failure).toHaveBeenCalledOnce();
    expect(f.failure).toHaveBeenCalledWith(reason);
    expect(f.index.close).toHaveBeenCalledOnce();
  });

  it.each(['close', 'stale', 'failure'] as const)(
    'drains one heartbeat without overlap on %s',
    async (end) => {
      vi.useFakeTimers();
      const f = fixture();
      await f.state.start();
      let releaseIO!: () => void;
      let releaseHeartbeat!: () => void;
      const io = new Promise<void>((resolve) => {
        releaseIO = resolve;
      });
      const heartbeatGate = new Promise<void>((resolve) => {
        releaseHeartbeat = resolve;
      });
      f.index.apply.mockImplementationOnce(async () => io);
      const heartbeat = vi.fn(async () => heartbeatGate);
      let stale = false;
      const feed = f.feed([{ key: 'head', value: 'one', offset: '0' }], {
        heartbeat,
        isStale: () => stale,
      });
      // Handle rejection before shutdown/failure, but inspect its identity below.
      const outcome = feed.then(
        () => undefined,
        (error: unknown) => error,
      );
      await vi.advanceTimersByTimeAsync(15_000);
      expect(heartbeat).toHaveBeenCalledOnce();
      // Flush joins the already-running heartbeat, rather than issuing another.
      releaseIO();
      await vi.advanceTimersByTimeAsync(0);
      expect(heartbeat).toHaveBeenCalledOnce();
      let closed = false;
      let closing: Promise<void> | undefined;
      if (end === 'close')
        closing = f.state.close().then(() => {
          closed = true;
        });
      else if (end === 'stale') stale = true;
      else f.crash();
      await vi.advanceTimersByTimeAsync(15_000);
      expect(heartbeat).toHaveBeenCalledOnce();
      expect(closed).toBe(false);
      releaseHeartbeat();
      const error = await outcome;
      if (end === 'stale') expect(error).toBeUndefined();
      else expect(error).toBeInstanceOf(Error);
      await closing;
      await f.state.close();
      await vi.advanceTimersByTimeAsync(15_000);
      expect(heartbeat).toHaveBeenCalledOnce();
      expect(vi.getTimerCount()).toBe(0);
      expect(f.failure).toHaveBeenCalledTimes(end === 'failure' ? 1 : 0);
    },
  );

  it('stops heartbeat scheduling when a snapshot batch becomes stale', async () => {
    vi.useFakeTimers();
    const f = fixture({ timeoutMs: 120_000 });
    const controller = new AbortController();
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const snapshot = vi.fn(async () => gate);
    const pending = f.state.barrier(f.producer, 'source', controller.signal, snapshot);
    await vi.waitFor(() => expect(f.transaction.commit).toHaveBeenCalledOnce());
    const heartbeat = vi.fn(async () => {});
    let stale = false;
    const feed = f.feed([{ ...f.barrier(), offset: '9' }], { heartbeat, isStale: () => stale });
    await vi.advanceTimersByTimeAsync(3_000);
    expect(snapshot).toHaveBeenCalledOnce();
    const calls = heartbeat.mock.calls.length;
    stale = true;
    await vi.advanceTimersByTimeAsync(9_000);
    expect(heartbeat).toHaveBeenCalledTimes(calls);
    const reason = new Error('assignment lost');
    const rejected = expect(pending).rejects.toBe(reason);
    controller.abort(reason);
    await rejected;
    release();
    await feed;
    await f.state.close();
    expect(vi.getTimerCount()).toBe(0);
    expect(f.failure).not.toHaveBeenCalled();
  });

  it.each(['close', 'failure'] as const)(
    'keeps a stale snapshot waiter reachable by %s',
    async (end) => {
      vi.useFakeTimers();
      const f = fixture({ timeoutMs: 120_000 });
      let release!: () => void;
      const gate = new Promise<void>((resolve) => {
        release = resolve;
      });
      const pending = f.state.barrier(
        f.producer,
        'source',
        new AbortController().signal,
        async () => gate,
      );
      const outcome = pending.then(
        () => undefined,
        (error: unknown) => error,
      );
      await vi.waitFor(() => expect(f.transaction.commit).toHaveBeenCalledOnce());
      let stale = false;
      const feed = f.feed([{ ...f.barrier(), offset: '9' }], { isStale: () => stale });
      await vi.advanceTimersByTimeAsync(0);
      stale = true;
      release();
      await feed;
      expect(await f.state.diagnostics()).toMatchObject({ waiters: 1 });
      if (end === 'failure') f.crash();
      else await f.state.close();
      expect(await outcome).toEqual(
        new Error(end === 'failure' ? 'Kafka state reader failed' : 'Kafka state reader closed'),
      );
      await f.state.close();
      expect(vi.getTimerCount()).toBe(0);
    },
  );

  it.each([undefined, 131072])(
    'reports diagnostics without starting for quota %s',
    async (maxDiskBytes) => {
      const f = fixture(maxDiskBytes === undefined ? {} : { maxDiskBytes });
      expect(await f.state.diagnostics()).toEqual({
        scannedBytes: 0,
        scannedRecords: 0,
        waiters: 0,
        databaseBytes: 0,
        databaseLimitBytes: 0,
        diskQuotaBytes: maxDiskBytes ?? 1024 * 1024 * 1024,
      });
      expect(mock.open).not.toHaveBeenCalled();
      expect(f.kafka.consumer).not.toHaveBeenCalled();
    },
  );

  it('counts scanned wire bytes, tombstones, repeated offsets and active waiters', async () => {
    const f = fixture();
    await f.state.start();
    const waiting = f.state.waitForOffset('2', new AbortController().signal);
    await f.feed([{ key: '中', value: '😀', offset: '0' }]);
    await f.feed([
      { key: '中', value: '😀', offset: '0' },
      { key: '中', value: null, offset: '1' },
    ]);
    expect(await f.state.diagnostics()).toEqual({
      scannedBytes: 17,
      scannedRecords: 3,
      waiters: 1,
      databaseBytes: 12288,
      databaseLimitBytes: 32768,
      diskQuotaBytes: 131072,
    });
    await f.feed([{ key: 'k', value: '', offset: '2' }]);
    await waiting;
    expect(await f.state.diagnostics()).toMatchObject({
      scannedBytes: 18,
      scannedRecords: 4,
      waiters: 0,
    });
  });

  it('serializes diagnostics with foreground reads', async () => {
    const f = fixture();
    await f.state.start();
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const read = f.state.read(async () => gate);
    const diagnostics = f.state.diagnostics();
    await Promise.resolve();
    expect(f.index.stats).not.toHaveBeenCalled();
    release();
    await read;
    await diagnostics;
    expect(f.index.stats).toHaveBeenCalledOnce();
  });

  it('preserves a leading BOM in Kafka keys and values rather than aliasing a different key', async () => {
    const f = fixture();
    await f.state.start();
    await f.feed([{ key: '\uFEFFhead', value: '\uFEFFvalue', offset: '0' }]);
    expect(await f.state.read((index) => index.read(['head', '\uFEFFhead']))).toEqual([
      null,
      '\uFEFFvalue',
    ]);
  });
  it('does not open an index or fence a producer after shutdown', async () => {
    const f = fixture();
    await expect(
      f.state.barrier(f.producer, 'source', AbortSignal.abort(), async () => null),
    ).rejects.toThrow();
    expect(mock.open).not.toHaveBeenCalled();
    expect(f.producer.transaction).not.toHaveBeenCalled();
  });

  it('bounds each record before allocating or applying state', async () => {
    const f = fixture();
    await f.state.start();
    await expect(
      f.feed([{ key: 'oversized', value: 'x'.repeat(530_000), offset: '0' }]),
    ).rejects.toThrow(
      'Kafka state record size exceeded kind=state-record actual=530009 limit=528384',
    );
    expect(await f.state.diagnostics()).toMatchObject({ scannedBytes: 530009, scannedRecords: 1 });
    expect(f.index.apply).not.toHaveBeenCalled();
    expect(f.failure).toHaveBeenCalledOnce();
  });
  it('preserves assignment-loss reasons while waiting for state', async () => {
    const f = fixture();
    await f.state.start();
    const controller = new AbortController();
    const reason = Object.assign(new Error('rebalance'), { type: 'REBALANCE_IN_PROGRESS' });
    const pending = f.state.waitForOffset('9', controller.signal);
    const rejected = expect(pending).rejects.toBe(reason);
    controller.abort(reason);
    await rejected;
    expect(f.failure).not.toHaveBeenCalled();
  });
  it('opens one reader for multiple owners and snapshots each barrier before later values', async () => {
    const f = fixture();
    const signal = new AbortController().signal;
    const first = f.state.barrier(f.producer, 'source', signal, (index) => index.read(['head']));
    await vi.waitFor(() => expect(f.transaction.commit).toHaveBeenCalledOnce());
    await f.feed([
      { key: 'head', value: 'before', offset: '0' },
      { ...f.barrier(), offset: '9' },
      { key: 'head', value: 'after', offset: '12' },
    ]);
    expect(await first).toEqual(['before']);
    expect(await f.state.read((index) => index.read(['head']))).toEqual(['after']);
    await f.state.start();
    expect(f.kafka.consumer).toHaveBeenCalledOnce();
    expect(f.consumer.subscribe).toHaveBeenCalledWith({ topic: 'state', fromBeginning: true });
    expect(f.consumer.run.mock.calls[0]![0]).toMatchObject({
      autoCommit: false,
      eachBatchAutoResolve: false,
    });
  });

  it('does not resolve a barrier from an offset hole or different nonce', async () => {
    vi.useFakeTimers();
    const f = fixture();
    const pending = f.state.barrier(f.producer, 'source', new AbortController().signal, (index) =>
      index.read(['head']),
    );
    const rejected = expect(pending).rejects.toThrow('timed out');
    await vi.waitFor(() => expect(f.transaction.commit).toHaveBeenCalledOnce());
    await f.feed([
      { key: 'barrier:source', value: 'replaced', offset: '9' },
      { key: 'other', value: 'later', offset: '15' },
    ]);
    await vi.advanceTimersByTimeAsync(501);
    await rejected;
  });

  it('aborts failed sends and never authenticates a snapshot', async () => {
    const f = fixture();
    f.transaction.send.mockRejectedValue(new Error('send failed'));
    const snapshot = vi.fn();
    await expect(
      f.state.barrier(f.producer, 'source', new AbortController().signal, snapshot),
    ).rejects.toThrow('send failed');
    expect(f.transaction.abort).toHaveBeenCalledOnce();
    expect(snapshot).not.toHaveBeenCalled();
  });

  it('waits for index apply after a projection commit and closes all waiting work', async () => {
    const f = fixture();
    await f.state.start();
    let done = false;
    const waiting = f.state.waitForOffset('12', new AbortController().signal).then(() => {
      done = true;
    });
    await f.feed([{ key: 'head', value: 'one', offset: '3' }]);
    expect(done).toBe(false);
    await f.feed([{ key: 'head', value: 'two', offset: '12' }]);
    await waiting;
    expect(done).toBe(true);
    const pending = f.state.waitForOffset('20', new AbortController().signal);
    const rejected = expect(pending).rejects.toThrow('closed');
    await f.state.close();
    await rejected;
    expect(f.index.close).toHaveBeenCalledOnce();
  });

  it('propagates reader and index failures rather than using partial cache', async () => {
    const f = fixture();
    await f.state.start();
    const pending = f.state.waitForOffset('20', new AbortController().signal);
    const rejected = expect(pending).rejects.toThrow('reader failed');
    f.crash();
    await rejected;
    expect(f.failure).toHaveBeenCalledOnce();
    await expect(f.state.read((index) => index.read(['head']))).rejects.toThrow('reader failed');
  });

  it('drains a late index open without creating a consumer after stop', async () => {
    const f = fixture();
    let resolve!: (value: unknown) => void;
    mock.open.mockReturnValue(
      new Promise((done) => {
        resolve = done;
      }),
    );
    const started = f.state.start();
    const rejected = expect(started).rejects.toThrow('closed');
    const stopped = f.state.close();
    resolve(f.index);
    await rejected;
    await stopped;
    expect(f.kafka.consumer).not.toHaveBeenCalled();
    expect(f.index.close).toHaveBeenCalledOnce();
  });
});
