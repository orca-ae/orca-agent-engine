// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { mkdtemp, readdir, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Worker } from 'node:worker_threads';
import Database from 'better-sqlite3';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { KafkaDiskIndex } from '../../src/kafka-disk-index.js';

describe('KafkaDiskIndex real scratch SQLite', () => {
  let directory: string;
  const indexes: KafkaDiskIndex[] = [];
  beforeEach(async () => {
    directory = await mkdtemp(join(tmpdir(), 'orca-index-test-'));
  });
  afterEach(async () => {
    await Promise.all(indexes.splice(0).map((index) => index.close()));
    await rm(directory, { recursive: true, force: true });
  });
  async function open(
    options: { maxBytes?: number; maxBatchBytes?: number; onFailure?: (error: Error) => void } = {},
  ) {
    const index = await KafkaDiskIndex.open({ directory, ...options });
    indexes.push(index);
    return index;
  }
  const record = (key: string, value: string | null, offset = '1') => ({ key, value, offset });
  const identityPrefix = `I/${'a'.repeat(64)}/`;
  const acceptedPrefix = `A/${'a'.repeat(64)}/`;

  it('uses a real database and preserves detached values, tombstones, and 64-bit offset ordering', async () => {
    const index = await open();
    await index.apply([record('head', 'one'), record('id/a', 'v1', '9007199254740992')]);
    const detached = await index.read(['head', 'id/a', 'missing'], { key: 'head', value: 'one' });
    await index.apply([record('id/a', 'v2', '9007199254740993'), record('head', 'two', '2')]);
    await index.apply([
      record('id/a', 'stale', '9007199254740992'),
      record('id/a', 'same-offset', '9007199254740993'),
    ]);
    expect(await index.read(['id/a'])).toEqual(['v2']);
    expect(detached).toEqual(['one', 'v1', null]);
    await index.apply([record('id/a', null, '9007199254740994')]);
    await index.apply([record('id/a', 'cannot resurrect', '9007199254740993')]);
    expect(await index.read(['id/a'])).toEqual([null]);
    expect(await index.countPrefix(identityPrefix)).toBe(0);
    const [scratch] = await readdir(directory);
    const db = new Database(join(directory, scratch!, 'index.sqlite'), { readonly: true });
    try {
      expect(db.pragma('journal_mode', { simple: true })).toBe('delete');
      expect(db.prepare('SELECT count(*) AS n FROM kv').get()).toEqual({ n: 2 });
    } finally {
      db.close();
    }
  });

  it('reports real worker database pages and the configured disk budget', async () => {
    const maxBytes = 1024 * 1024;
    const index = await open({ maxBytes });
    const before = await index.stats();
    expect(before.databaseBytes).toBeGreaterThan(0);
    expect(before.databaseBytes % 4096).toBe(0);
    expect(before.databaseLimitBytes).toBe(Math.floor((maxBytes - 65536) / (3 * 4096)) * 4096);
    expect(before.diskQuotaBytes).toBe(maxBytes);
    await index.apply([record('large', 'x'.repeat(32 * 1024))]);
    const after = await index.stats();
    const [scratch] = await readdir(directory);
    expect(after.databaseBytes).toBe((await stat(join(directory, scratch!, 'index.sqlite'))).size);
    expect(after.databaseBytes).toBeGreaterThan(before.databaseBytes);
    expect(after.databaseBytes).toBeLessThanOrEqual(after.databaseLimitBytes);
    expect(after.databaseLimitBytes).toBe(before.databaseLimitBytes);
    expect(after.diskQuotaBytes).toBe(maxBytes);
  });

  it('checks the expected head inside every bounded read transaction', async () => {
    const index = await open();
    await expect(index.read([], { key: 'head', value: 'old' })).rejects.toThrow(
      'Kafka state head changed',
    );
    await index.apply([record('head', 'old'), record('page', 'old page')]);
    const before = index.read(['page'], { key: 'head', value: 'old' });
    const write = index.apply([record('head', 'new', '2'), record('page', 'new page', '2')]);
    expect(await before).toEqual(['old page']);
    await write;
    await expect(index.read(['page'], { key: 'head', value: 'old' })).rejects.toThrow(
      'Kafka state head changed',
    );
    expect(await index.read(['page'], { key: 'head', value: 'new' })).toEqual(['new page']);
  });

  it('rolls back the entire write on disk-full and stays within the disk budget', async () => {
    const maxBytes = 128 * 1024;
    const index = await open({ maxBytes });
    await index.apply([record('stable', 'before')]);
    await expect(
      index.apply([record('stable', 'after', '2'), record('large', 'x'.repeat(64 * 1024))]),
    ).rejects.toThrow('Kafka disk index quota exceeded');
    expect(await index.read(['stable', 'large'])).toEqual(['before', null]);
    await index.apply([record('stable', 'retry', '2')]);
    expect(await index.read(['stable'])).toEqual(['retry']);
    const [scratch] = await readdir(directory);
    const files = await readdir(join(directory, scratch!));
    const sizes = await Promise.all(
      files.map(async (file) => (await stat(join(directory, scratch!, file))).size),
    );
    expect(sizes.reduce((sum, size) => sum + size, 0)).toBeLessThanOrEqual(maxBytes);
    expect(files.some((file) => file.endsWith('-wal'))).toBe(false);
  });

  it('updates ledger counts atomically with offset-fenced insert, delete, and resurrection', async () => {
    const index = await open();
    const key = `${identityPrefix}one`;
    expect(await index.countPrefix(identityPrefix)).toBe(0);
    await index.apply([record(key, null, '2'), record(key, 'stale', '1')]);
    expect(await index.countPrefix(identityPrefix)).toBe(0);
    await index.apply([
      record(key, 'live', '3'),
      record(key, 'updated', '4'),
      record(key, null, '3'),
    ]);
    expect(await index.countPrefix(identityPrefix)).toBe(1);
    await index.apply([record(key, null, '5'), record(key, null, '6'), record(key, 'stale', '5')]);
    expect(await index.countPrefix(identityPrefix)).toBe(0);
    const apply = index.apply([
      record(key, 'restored', '7'),
      record(key, null, '7'),
      record(`${acceptedPrefix}one`, 'accepted'),
      record(identityPrefix, 'prefix key itself'),
      record(`I/${'b'.repeat(64)}/other`, 'other scope'),
    ]);
    const countAfterApply = index.countPrefix(identityPrefix);
    await apply;
    expect(await countAfterApply).toBe(2);
    expect(await index.countPrefix(acceptedPrefix)).toBe(1);
    expect(await index.read([key, identityPrefix])).toEqual(['restored', 'prefix key itself']);
    const beforeDelete = index.countPrefix(identityPrefix);
    const deletion = index.apply([record(key, null, '8'), record(identityPrefix, null, '2')]);
    expect(await beforeDelete).toBe(2);
    await deletion;
    expect(await index.countPrefix(identityPrefix)).toBe(0);
  });

  it('rolls back derived ledger counts and KV together when a later batch write exhausts disk', async () => {
    const index = await open({ maxBytes: 128 * 1024 });
    const key = `${identityPrefix}existing`;
    const accepted = `${acceptedPrefix}new`;
    await index.apply([record(key, 'before')]);
    await expect(
      index.apply([
        record(key, null, '2'),
        record(accepted, 'new'),
        record('large', 'x'.repeat(64 * 1024)),
      ]),
    ).rejects.toThrow('Kafka disk index quota exceeded');
    expect(await index.countPrefix(identityPrefix)).toBe(1);
    expect(await index.countPrefix(acceptedPrefix)).toBe(0);
    expect(await index.read([key, accepted])).toEqual(['before', null]);
    await index.apply([record(key, null, '2'), record(accepted, 'retry')]);
    expect(await index.countPrefix(identityPrefix)).toBe(0);
    expect(await index.countPrefix(acceptedPrefix)).toBe(1);
  });

  it('preserves arbitrary Unicode keys but only counts exact lowercase v2 ledger prefixes', async () => {
    const index = await open();
    const keys = ['a/one', 'a/two', 'a0/other', 'a/\0', 'a/\ud800', 'a/😀', '\uffff', '\uffffx'];
    await index.apply(keys.map((key) => record(key, key)));
    expect(await index.read(keys)).toEqual(keys);
    await index.apply([
      record(`${identityPrefix}one`, 'identity'),
      record(`${acceptedPrefix}one`, 'accepted'),
    ]);
    expect(await index.countPrefix(identityPrefix)).toBe(1);
    expect(await index.countPrefix(acceptedPrefix)).toBe(1);
    for (const prefix of [
      '',
      'a/',
      'a/\ud800',
      '\uffff',
      'I/',
      identityPrefix.toUpperCase(),
      `${identityPrefix}extra`,
      `${identityPrefix}\n`,
      `I/${'a'.repeat(63)}/`,
      `I/${'a'.repeat(65)}/`,
      `I/${'g'.repeat(64)}/`,
      `X/${'a'.repeat(64)}/`,
    ]) {
      await expect(index.countPrefix(prefix)).rejects.toThrow(
        'Invalid Kafka disk index count prefix',
      );
    }
  });

  it('rejects invalid and unbounded work before mutating the database', async () => {
    const index = await open({ maxBatchBytes: 128 });
    await expect(
      index.apply([record('first', 'ok'), record('large', 'x'.repeat(100))]),
    ).rejects.toThrow('input limit');
    expect(await index.read(['first'])).toEqual([null]);
    for (const offset of ['-1', '1.5', '9223372036854775808', 'secret-payload']) {
      await expect(index.apply([record('k', 'v', offset)])).rejects.toThrow(
        'Kafka disk index input limit exceeded',
      );
    }
    await expect(index.read(Array.from({ length: 8193 }, () => 'k'))).rejects.toThrow(
      'input limit',
    );
    await expect(index.read(['x'.repeat(16385)])).rejects.toThrow('input limit');
    await expect(index.apply([record('v', 'x'.repeat(524289))])).rejects.toThrow('input limit');
    await expect(KafkaDiskIndex.open({ directory, maxBytes: NaN })).rejects.toThrow('Invalid');
    await expect(KafkaDiskIndex.open({ directory, maxBatchBytes: Infinity })).rejects.toThrow(
      'Invalid',
    );
  });

  it('bounds detached read results independently of the number of requested keys', async () => {
    const index = await open();
    await index.apply([record('large', 'x'.repeat(512 * 1024))]);
    await expect(index.read(Array.from({ length: 65 }, () => 'large'))).rejects.toThrow(
      'Kafka disk index read limit exceeded',
    );
    expect(await index.read(['absent'])).toEqual([null]);
  });

  it('bounds in-flight requests and drains accepted work on idempotent close', async () => {
    const index = await open();
    const a = index.apply([record('a', 'one')]);
    const b = index.read(['a']);
    await expect(index.read(['a'])).rejects.toThrow('Kafka disk index busy');
    const closed = index.close();
    expect(index.close()).toBe(closed);
    await a;
    expect(await b).toEqual(['one']);
    await closed;
    await expect(index.read([])).rejects.toThrow('Kafka disk index closed');
    expect(await readdir(directory)).toEqual([]);
  });

  it('settles outstanding requests when the worker crashes', async () => {
    const index = await open();
    const worker = (index as unknown as { worker: Worker }).worker;
    const termination = worker.terminate();
    const pending = Promise.allSettled([index.read(['k']), index.countPrefix(identityPrefix)]);
    await termination;
    expect((await pending).every((result) => result.status === 'rejected')).toBe(true);
    await index.close();
    expect(await readdir(directory)).toEqual([]);
  });

  it.each([false, true])(
    'notifies idle worker death once despite callback throwing=%s',
    async (throws) => {
      const onFailure = vi.fn(() => {
        if (throws) throw new Error('consumer callback failed');
      });
      const index = await open({ onFailure });
      const worker = (index as unknown as { worker: Worker }).worker;
      await worker.terminate();
      // No subsequent read/write is needed to discover an idle worker exit.
      expect(onFailure).toHaveBeenCalledTimes(1);
      expect(onFailure).toHaveBeenCalledWith(new Error('Kafka disk index worker unavailable'));
      await index.close();
      expect(onFailure).toHaveBeenCalledTimes(1);
      await expect(index.read([])).rejects.toThrow('Kafka disk index closed');
      expect(await readdir(directory)).toEqual([]);
    },
  );

  it('notifies an error followed by exit only once without exposing native errors', async () => {
    const onFailure = vi.fn();
    const index = await open({ onFailure });
    const worker = (index as unknown as { worker: Worker }).worker;
    worker.emit('error', new Error('private native path'));
    expect(onFailure).toHaveBeenCalledTimes(1);
    expect(onFailure).toHaveBeenCalledWith(new Error('Kafka disk index worker unavailable'));
    await index.close();
    expect(onFailure).toHaveBeenCalledTimes(1);
    expect(await readdir(directory)).toEqual([]);
  });

  it('does not notify expected shutdown or recoverable operation errors', async () => {
    const onFailure = vi.fn();
    const index = await open({ onFailure });
    await expect(index.read([], { key: 'missing', value: 'head' })).rejects.toThrow(
      'Kafka state head changed',
    );
    await index.close();
    expect(onFailure).not.toHaveBeenCalled();
    expect(await readdir(directory)).toEqual([]);
  });

  it('terminates after ignored close IPC and settles accepted requests', async () => {
    const onFailure = vi.fn();
    const index = await open({ onFailure });
    const worker = (index as unknown as { worker: Worker }).worker;
    const post = vi.spyOn(worker, 'postMessage').mockImplementation(() => undefined);
    const terminate = vi.spyOn(worker, 'terminate');
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      const pending = Promise.allSettled([index.read(['k']), index.countPrefix(identityPrefix)]);
      await expect(index.read([])).rejects.toThrow('Kafka disk index busy');
      const closed = index.close();
      expect(index.close()).toBe(closed);
      await Promise.race([
        closed,
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => reject(new Error('close IPC did not time out')), 2500);
        }),
      ]);
      expect(terminate).toHaveBeenCalledTimes(1);
      expect((await pending).every((result) => result.status === 'rejected')).toBe(true);
      expect(onFailure).not.toHaveBeenCalled();
      expect(await readdir(directory)).toEqual([]);
    } finally {
      clearTimeout(timer);
      post.mockRestore();
      terminate.mockRestore();
      await worker.terminate();
      await index.close();
    }
  });

  it('starts empty in a unique directory and never adopts or deletes another index', async () => {
    await writeFile(join(directory, 'keep-me'), 'old index or user data');
    const first = await open();
    await first.apply([record('old', 'value')]);
    const second = await open();
    expect(await second.read(['old'])).toEqual([null]);
    expect((await readdir(directory)).length).toBe(3);
    await first.close();
    expect((await readdir(directory)).length).toBe(2);
    await second.close();
    expect(await readdir(directory)).toEqual(['keep-me']);
    await expect(KafkaDiskIndex.open({ directory: join(directory, 'keep-me') })).rejects.toThrow(
      'Kafka disk index open failed',
    );
    expect(await readdir(directory)).toEqual(['keep-me']);
  });

  it('cleans partial open failures without deleting the parent or exposing worker errors', async () => {
    await writeFile(join(directory, 'keep-me'), 'not ours');
    const post = vi.spyOn(Worker.prototype, 'postMessage').mockImplementationOnce(() => {
      throw new Error('private native path or payload');
    });
    try {
      await expect(KafkaDiskIndex.open({ directory })).rejects.toThrow(
        'Kafka disk index open failed',
      );
    } finally {
      post.mockRestore();
    }
    expect(await readdir(directory)).toEqual(['keep-me']);
  });

  it('keeps a bounded working set for 100k real disk rows', async () => {
    const index = await open();
    const rss = process.memoryUsage().rss;
    for (let start = 0; start < 100_000; start += 1000) {
      await index.apply(
        Array.from({ length: 1000 }, (_, i) =>
          record(
            `${(start + i) % 2 === 0 ? identityPrefix : acceptedPrefix}${start + i}`,
            'x'.repeat(96),
          ),
        ),
      );
    }
    expect(await index.countPrefix(identityPrefix)).toBe(50_000);
    expect(await index.countPrefix(acceptedPrefix)).toBe(50_000);
    expect(
      await index.read([`${identityPrefix}0`, `${acceptedPrefix}99999`, `${identityPrefix}100000`]),
    ).toEqual(['x'.repeat(96), 'x'.repeat(96), null]);
    // RSS includes native SQLite and the worker, not just the parent JS heap.
    // A smoke guard, not a cross-platform peak-memory benchmark.
    expect(process.memoryUsage().rss - rss).toBeLessThan(192 * 1024 * 1024);
    await expect(index.countPrefix('I/')).rejects.toThrow('Invalid Kafka disk index count prefix');
    const [scratch] = await readdir(directory);
    const db = new Database(join(directory, scratch!, 'index.sqlite'));
    try {
      expect(db.prepare('SELECT n FROM ledger_counts ORDER BY prefix').all()).toEqual([
        { n: 50_000 },
        { n: 50_000 },
      ]);
      // Make the history table unavailable to prove the actual worker count
      // path uses only derived metadata, rather than relying on wall-clock timing.
      db.exec('ALTER TABLE kv RENAME TO unavailable_history');
      expect(await index.countPrefix(identityPrefix)).toBe(50_000);
      expect(await index.countPrefix(acceptedPrefix)).toBe(50_000);
      expect(await index.countPrefix(`I/${'f'.repeat(64)}/`)).toBe(0);
    } finally {
      db.exec('ALTER TABLE unavailable_history RENAME TO kv');
      db.close();
    }
  }, 30_000);
});
