// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { mkdtemp, mkdir, rm } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Worker } from 'node:worker_threads';

const DEFAULT_MAX_BYTES = 1024 * 1024 * 1024;
const DEFAULT_BATCH_BYTES = 4 * 1024 * 1024;
const MAX_KEYS = 8192;
const MAX_KEY_BYTES = 16 * 1024;
const MAX_VALUE_BYTES = 512 * 1024;
const MAX_IN_FLIGHT = 2;
const CLOSE_IPC_TIMEOUT_MS = 1000;

type RecordEntry = { key: string; value: string | null; offset: string };
type Pending = { resolve(value: unknown): void; reject(error: Error): void };

// Deliberately plain JavaScript: no TS loader, Function.toString(), worker asset
// copying, or experimental node:sqlite dependency in either source or dist.
const workerSource = String.raw`
const { parentPort, workerData } = require("node:worker_threads");
const Database = require(workerData.driver);
let db;
let lookup;
let upsert;
let liveKey;
let ledgerCount;
let incrementCount;
let decrementCount;
const ledgerPrefix = /^([IA]\/[0-9a-f]{64}\/)/;
const pageSize = 4096;
// Reserve rollback-journal pages/headers as well as database pages. No WAL.
const maxPages = Math.min(0xfffffffe, Math.floor((workerData.maxBytes - 65536) / (3 * pageSize)));
const encode = (value) => Buffer.from(value, "utf16le");
const decode = (value) => value === null ? null : value.toString("utf16le");
const quota = () => {
  if (db.pragma("page_count", { simple: true }) > maxPages) throw new Error("quota");
};
const operations = {
  open() {
    db = new Database(workerData.path);
    db.pragma("page_size = 4096");
    db.pragma("journal_mode = DELETE");
    db.pragma("synchronous = FULL");
    db.pragma("cache_size = -8192");
    db.pragma("mmap_size = 0");
    db.pragma("temp_store = FILE");
    db.pragma("max_page_count = " + maxPages);
    // NULL is a logical tombstone. Keep its offset fence, otherwise delayed
    // older records could resurrect a deleted key. No local restore cursor.
    db.exec("CREATE TABLE kv (key BLOB PRIMARY KEY, value BLOB, offset TEXT NOT NULL) WITHOUT ROWID");
    db.exec("CREATE TABLE ledger_counts (prefix TEXT PRIMARY KEY, n INTEGER NOT NULL CHECK(n >= 0 AND n <= 9007199254740991)) WITHOUT ROWID");
    lookup = db.prepare("SELECT value FROM kv WHERE key = ?");
    liveKey = db.prepare("SELECT value IS NOT NULL AS live FROM kv WHERE key = ?");
    ledgerCount = db.prepare("SELECT n FROM ledger_counts WHERE prefix = ?");
    incrementCount = db.prepare("INSERT INTO ledger_counts(prefix,n) VALUES(?,1) ON CONFLICT(prefix) DO UPDATE SET n=n+1");
    decrementCount = db.prepare("UPDATE ledger_counts SET n=n-1 WHERE prefix = ?");
    upsert = db.prepare(
      "INSERT INTO kv(key,value,offset) VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value, offset=excluded.offset " +
      "WHERE length(excluded.offset)>length(kv.offset) OR (length(excluded.offset)=length(kv.offset) AND excluded.offset>kv.offset)"
    );
    quota();
  },
  apply(records) {
    db.transaction(() => {
      for (const record of records) {
        const key = encode(record.key);
        const prefix = ledgerPrefix.exec(record.key)?.[1];
        const before = prefix ? (liveKey.get(key)?.live ?? 0) : 0;
        const changed = upsert.run(key, record.value === null ? null : encode(record.value), record.offset).changes;
        // Derived metadata and KV changes share the same SQLite transaction.
        // Ignored offsets and live-to-live updates cannot change cardinality.
        if (prefix && changed) {
          const delta = (record.value === null ? 0 : 1) - before;
          if (delta > 0) incrementCount.run(prefix);
          else if (delta < 0 && decrementCount.run(prefix).changes !== 1) throw new Error("count-limit");
        }
      }
      quota();
    })();
  },
  read({ keys, expected }) {
    return db.transaction(() => {
      if (expected) {
        const row = lookup.get(encode(expected.key));
        if (!row || decode(row.value) !== expected.value) throw new Error("head");
      }
      let bytes = 0;
      return keys.map((key) => {
        const row = lookup.get(encode(key));
        if (!row || row.value === null) return null;
        bytes += row.value.length;
        if (bytes > 64 * 1024 * 1024) throw new Error("read-limit");
        return decode(row.value);
      });
    })();
  },
  countPrefix(prefix) {
    if (ledgerPrefix.exec(prefix)?.[0] !== prefix) throw new Error("count-prefix");
    // Only exact v2 ledger prefixes are supported: one metadata point lookup,
    // never a range scan proportional to the scope historical I/A ledger size.
    return db.transaction(() => ledgerCount.get(prefix)?.n ?? 0)();
  },
  stats() {
    return {
      databaseBytes: db.pragma("page_count", { simple: true }) * pageSize,
      databaseLimitBytes: db.pragma("max_page_count", { simple: true }) * pageSize,
      diskQuotaBytes: workerData.maxBytes,
    };
  },
  close() {
    if (db) db.close();
    db = undefined;
  },
};
parentPort.on("message", ({ id, operation, data }) => {
  try {
    const value = operations[operation](data);
    parentPort.postMessage({ id, value });
  } catch (error) {
    // Never forward SQL, filesystem paths, keys, or native exception payloads.
    const message = error.message === "head" ? "Kafka state head changed"
      : error.code === "SQLITE_FULL" || error.message === "quota" ? "Kafka disk index quota exceeded"
      : error.message === "read-limit" ? "Kafka disk index read limit exceeded"
      : error.message === "count-limit" ? "Kafka disk index count limit exceeded"
      : error.message === "count-prefix" ? "Invalid Kafka disk index count prefix"
      : "Kafka disk index operation failed";
    parentPort.postMessage({ id, error: message });
  }
});
`;

/** Fresh process-local Kafka projection. Kafka, not this scratch database, is authoritative. */
export class KafkaDiskIndex {
  private readonly pending = new Map<number, Pending>();
  private nextId = 0;
  private closing = false;
  private failed = false;
  private closePromise: Promise<void> | undefined;

  private constructor(
    private readonly worker: Worker,
    private readonly directory: string,
    private readonly maxBatchBytes: number,
    private readonly onFailure?: (error: Error) => void,
  ) {
    worker.on('message', (message: { id: number; value?: unknown; error?: string }) => {
      const pending = this.pending.get(message.id);
      if (!pending) return;
      this.pending.delete(message.id);
      if (message.error) pending.reject(new Error(message.error));
      else pending.resolve(message.value);
    });
    worker.on('error', () => this.workerFailed());
    worker.on('exit', () => this.workerFailed());
  }

  static async open(
    options: {
      directory?: string;
      maxBytes?: number;
      maxBatchBytes?: number;
      /** Called once on unexpected worker failure, including while idle. Exceptions are ignored. */
      onFailure?: (error: Error) => void;
    } = {},
  ): Promise<KafkaDiskIndex> {
    const maxBytes = options.maxBytes ?? DEFAULT_MAX_BYTES;
    const maxBatchBytes = options.maxBatchBytes ?? DEFAULT_BATCH_BYTES;
    if (
      !Number.isSafeInteger(maxBytes) ||
      maxBytes < 128 * 1024 ||
      !Number.isSafeInteger(maxBatchBytes) ||
      maxBatchBytes < 1 ||
      maxBatchBytes > 32 * 1024 * 1024
    ) {
      throw new Error('Invalid Kafka disk index limits');
    }
    let directory: string | undefined;
    let index: KafkaDiskIndex | undefined;
    try {
      const parent = options.directory ?? tmpdir();
      await mkdir(parent, { recursive: true });
      directory = await mkdtemp(join(parent, 'orca-kafka-index-'));
      const driver = createRequire(import.meta.url).resolve('better-sqlite3');
      const worker = new Worker(workerSource, {
        eval: true,
        // This is CJS JavaScript regardless of parent --input-type or TS loaders.
        execArgv: [],
        workerData: { driver, path: join(directory, 'index.sqlite'), maxBytes },
      });
      index = new KafkaDiskIndex(worker, directory, maxBatchBytes, options.onFailure);
      await index.request('open');
      return index;
    } catch {
      if (index) await index.close();
      else if (directory) await rm(directory, { recursive: true, force: true });
      throw new Error('Kafka disk index open failed');
    }
  }

  async apply(records: readonly RecordEntry[]): Promise<void> {
    this.checkCount(records.length);
    let bytes = 0;
    const detached = records.map((record) => {
      bytes += this.keyBytes(record.key);
      if (record.value !== null) {
        if (typeof record.value !== 'string' || Buffer.byteLength(record.value) > MAX_VALUE_BYTES)
          this.invalid();
        bytes += Buffer.byteLength(record.value);
      }
      if (
        typeof record.offset !== 'string' ||
        !/^\d{1,19}$/.test(record.offset) ||
        BigInt(record.offset) > 9223372036854775807n
      )
        this.invalid();
      bytes += record.offset.length + 32;
      if (bytes > this.maxBatchBytes) this.invalid();
      return { key: record.key, value: record.value, offset: BigInt(record.offset).toString() };
    });
    await this.request('apply', detached);
  }

  async read(
    keys: readonly string[],
    expected?: { key: string; value: string },
  ): Promise<Array<string | null>> {
    this.checkCount(keys.length);
    let bytes = 0;
    for (const key of keys) {
      bytes += this.keyBytes(key) + 32;
      if (bytes > this.maxBatchBytes) this.invalid();
    }
    if (expected) {
      bytes += this.keyBytes(expected.key);
      if (typeof expected.value !== 'string' || Buffer.byteLength(expected.value) > MAX_VALUE_BYTES)
        this.invalid();
      bytes += Buffer.byteLength(expected.value);
      if (bytes > this.maxBatchBytes) this.invalid();
    }
    return (await this.request('read', { keys: [...keys], expected })) as Array<string | null>;
  }

  async countPrefix(prefix: string): Promise<number> {
    this.keyBytes(prefix);
    return (await this.request('countPrefix', prefix)) as number;
  }

  async stats(): Promise<{
    databaseBytes: number;
    databaseLimitBytes: number;
    diskQuotaBytes: number;
  }> {
    return (await this.request('stats')) as {
      databaseBytes: number;
      databaseLimitBytes: number;
      diskQuotaBytes: number;
    };
  }

  /**
   * Best-effort shutdown: allow close IPC one second, then terminate the worker.
   * Worker termination cannot guarantee preemption of arbitrary stuck native or
   * kernel I/O; directory removal waits for termination to avoid deleting live DB files.
   */
  close(): Promise<void> {
    if (this.closePromise) return this.closePromise;
    this.closing = true;
    this.closePromise = (async () => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        if (!this.failed) {
          await Promise.race([
            this.request('close', undefined, true),
            new Promise<never>((_, reject) => {
              timer = setTimeout(
                () => reject(new Error('Kafka disk index close timed out')),
                CLOSE_IPC_TIMEOUT_MS,
              );
            }),
          ]);
        }
      } catch {
        // Close IPC is best effort, including a dead or unresponsive worker.
      } finally {
        clearTimeout(timer);
        // Settle callers even if native I/O delays termination indefinitely.
        this.workerFailed();
        await this.worker.terminate();
        await rm(this.directory, { recursive: true, force: true });
      }
    })();
    return this.closePromise;
  }

  private workerFailed(): void {
    if (this.failed) return;
    this.failed = true;
    const error = new Error('Kafka disk index worker unavailable');
    for (const pending of this.pending.values()) pending.reject(error);
    this.pending.clear();
    // Unexpected exits also release only this instance's scratch directory.
    if (!this.closing) {
      void this.close().catch(() => undefined);
      try {
        this.onFailure?.(error);
      } catch {
        // Consumer notification must never derail worker/directory cleanup.
      }
    }
  }

  private request(operation: string, data?: unknown, closing = false): Promise<unknown> {
    if (this.failed || (this.closing && !closing))
      return Promise.reject(new Error('Kafka disk index closed'));
    if (!closing && this.pending.size >= MAX_IN_FLIGHT)
      return Promise.reject(new Error('Kafka disk index busy'));
    const id = ++this.nextId;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      try {
        this.worker.postMessage({ id, operation, data });
      } catch {
        this.pending.delete(id);
        reject(new Error('Kafka disk index worker unavailable'));
      }
    });
  }

  private keyBytes(key: string): number {
    if (typeof key !== 'string' || Buffer.byteLength(key) > MAX_KEY_BYTES) this.invalid();
    return Buffer.byteLength(key);
  }

  private checkCount(count: number): void {
    if (!Number.isSafeInteger(count) || count > MAX_KEYS) this.invalid();
  }

  private invalid(): never {
    throw new Error('Kafka disk index input limit exceeded');
  }
}
