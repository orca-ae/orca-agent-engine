// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Memory version watcher.
 *
 * Per-session polling watcher that detects writes to each attached
 * `memory_store` and registers them as new versions via
 * the workspace/session-scoped internal route. Two scan paths, picked by which wiring is
 * passed in:
 *   - **S3 / FUSE path:** when `s3` + `bucket` are set, the watcher paginates
 *     `ListObjectsV2` over the store's workspace-scoped `live/` prefix,
 *     fetches each live key's bytes, computes sha256, and diffs against the
 *     last-seen sha cache. Version-history blobs live outside that prefix.
 *   - **InMemory path:** when `sandbox` is set, the watcher walks the
 *     sandbox's mount path recursively via `sandbox.files.list` /
 *     `sandbox.files.read`, computes sha256, and diffs the same cache.
 *
 * Writes are surfaced asynchronously on a 2s polling cadence; there is no
 * synchronous libfuse write-through path. The registry's scoped memory-version
 * route is last-writer-
 * wins on cross-session conflicts — when it sets `conflict: true`, the
 * watcher emits a `session.memory_conflict` event so the SDK can surface the
 * mismatch to client code.
 *
 * Lifecycle:
 *   - `start()` seeds the (storeId|path) → lastSeenSha cache from the
 *     registry's `listSessionMemories` so writes that pre-existed at
 *     session-spawn don't re-register.
 *   - The interval timer calls `tick()` every `intervalMs`. Polls are
 *     serialized via the `inflight` flag so a slow poll doesn't pile up.
 *   - `stop()` cancels the timer, lets an active poll drain, runs one bounded
 *     pending-registration flush, and aborts shutdown work after
 *     `stopTimeoutMs`.
 *
 * The dispatcher starts one watcher per session after the memory mounts are
 * active; the SessionRunner owns it and stops it first during shutdown.
 */

import { createHash } from 'node:crypto';
import { GetObjectCommand, ListObjectsV2Command, type S3Client } from '@aws-sdk/client-s3';
import { v7 as uuidv7 } from 'uuid';
import type { Event, TranscriptStore } from '@orca/transcript-store';
import { buildMemoryStoreLivePrefix } from '../../auth/sts-creds.js';
import type { RegistryClient } from '../../clients/registry.js';
import {
  harnessMemoryVersionsRecordedTotal,
  harnessMemoryWatcherPollTotal,
  harnessMemoryWriteLagSeconds,
} from '../../metrics.js';
import type { SandboxHandle } from '../sandbox-runtime.js';

export interface WatchedStore {
  storeId: string;
  storeName: string;
  /**
   * Sandbox path the memory_store is mounted at (e.g.
   * `/mnt/memory/{store_name}/`). Used by the InMemory scan path to root the
   * recursive walk; ignored on the S3 path.
   */
  mountPath: string;
}

export interface MemoryVersionWatcherOptions {
  workspaceId: string;
  sessionId: string;
  stores: WatchedStore[];
  registry: RegistryClient;
  store: TranscriptStore;

  /** S3 path: required when mount.kind='s3'. */
  s3?: S3Client;
  bucket?: string;
  /** Default `memory/` — same prefix passed to `S3MemoryBlobStore`. */
  memoryKeyPrefix?: string;

  /** InMemory path: required when mount.kind='inmemory_local'. */
  sandbox?: SandboxHandle;

  /** Default 2000ms (2s). Set to 0 to disable the timer (manual `tick()`; used by tests). */
  intervalMs?: number;
  /** Default 30000ms. Bounds each S3 / sandbox / registry operation inside a poll. */
  operationTimeoutMs?: number;
  /** Default 5000ms. Bounds how long stop() waits for an in-flight poll to observe abort. */
  stopTimeoutMs?: number;
}

export interface PollResult {
  storesScanned: number;
  versionsRecorded: number;
  conflicts: number;
  errors: number;
}

interface ObservedFile {
  path: string;
  sha256: string;
  content: Buffer;
  /**
   * S3 `LastModified` for the underlying object. Set on the S3 scan path so
   * the per-key registration site can `.observe()` write-lag against the
   * harness clock; left undefined on the InMemory path because the sandbox
   * FS doesn't expose a meaningful equivalent, so only the S3 path observes
   * write lag.
   */
  lastModified?: Date;
}

interface PendingRegistration {
  sha256: string;
  content: Buffer;
  previousSha256: string | null;
  versionId: string;
  attemptedAtMs: number;
}

const DEFAULT_MEMORY_KEY_PREFIX = 'memory/';
const DEFAULT_INTERVAL_MS = 2000;
const DEFAULT_OPERATION_TIMEOUT_MS = 30_000;
const DEFAULT_STOP_TIMEOUT_MS = 5_000;

export class MemoryVersionWatcher {
  private timer: NodeJS.Timeout | null = null;
  private inflight = false;
  private stopped = false;
  private currentAbortController: AbortController | null = null;
  private forcePendingRetry = false;
  /** (storeId|path) → last-seen sha256. */
  private readonly cache = new Map<string, string>();
  /**
   * Registry writes are non-idempotent, so if a bounded POST times out during
   * an active poll, do not immediately retry the same live sha. Reconcile
   * against the registry first; retry only after a backoff window.
   */
  private readonly pendingRegistrations = new Map<string, PendingRegistration>();
  private readonly intervalMs: number;
  private readonly operationTimeoutMs: number;
  private readonly stopTimeoutMs: number;

  constructor(private readonly opts: MemoryVersionWatcherOptions) {
    this.intervalMs = opts.intervalMs ?? DEFAULT_INTERVAL_MS;
    this.operationTimeoutMs = opts.operationTimeoutMs ?? DEFAULT_OPERATION_TIMEOUT_MS;
    this.stopTimeoutMs = opts.stopTimeoutMs ?? DEFAULT_STOP_TIMEOUT_MS;
  }

  /**
   * Seed the cache from the registry's current state, then start the polling
   * loop. Seed failures are required setup failures: starting with an empty
   * cache would make pre-existing objects look like new writes on the first
   * poll.
   */
  async start(): Promise<void> {
    this.stopped = false;
    for (const s of this.opts.stores) {
      const memories = await this.opts.registry.listSessionMemories({
        workspaceId: this.opts.workspaceId,
        sessionId: this.opts.sessionId,
        storeId: s.storeId,
      });
      for (const m of memories) {
        this.cache.set(this.cacheKey(s.storeId, m.path), m.currentSha256);
      }
    }
    if (this.intervalMs > 0) {
      this.timer = setInterval(() => {
        this.tick().catch((e: unknown) => console.error('MemoryVersionWatcher tick failed', e));
      }, this.intervalMs);
    }
  }

  /**
   * Cancel the polling timer and wait for any in-flight poll to drain. Safe
   * to call multiple times; idempotent. SessionRunner.stop() (and the
   * dispatcher's setup-failure path) call this BEFORE the memory mounts are
   * deactivated so the watcher can finish an active pass against the live
   * state. If the active poll does not finish before
   * `stopTimeoutMs`, abort it and continue shutdown.
   *
   * S3 streams and registry fetches receive abort signals. The local sandbox
   * file API (`files.list`/`files.read`) does not currently accept signals,
   * so stop timeout can unblock teardown while that underlying sandbox call
   * finishes in the background.
   */
  async stop(): Promise<void> {
    this.stopped = true;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    const deadline = Date.now() + this.stopTimeoutMs;
    while (this.inflight) {
      if (Date.now() >= deadline) {
        this.currentAbortController?.abort();
        console.warn(
          `MemoryVersionWatcher.stop timed out after ${this.stopTimeoutMs}ms; continuing shutdown`,
        );
        // Once an active poll exceeded the shutdown deadline, skip the final
        // pending flush because the backing store state is no longer known.
        return;
      }
      await new Promise((r) => setTimeout(r, 50));
    }
    if (this.pendingRegistrations.size > 0 && Date.now() < deadline) {
      await this.flushPendingRegistrations(deadline);
    }
  }

  /**
   * Single poll cycle across all watched stores. Exposed for tests so they
   * can drive the watcher synchronously without the timer. Returns a summary
   * counter — useful for metrics and for tests that want to
   * assert "no errors, n registrations."
   *
   * Per-key errors are logged + counted; they DO NOT abort the tick. A
   * single slow store also doesn't block other stores — each store's scan is
   * wrapped in its own try/catch.
   */
  async tick(): Promise<PollResult> {
    if (this.stopped || this.inflight) {
      return { storesScanned: 0, versionsRecorded: 0, conflicts: 0, errors: 0 };
    }
    this.inflight = true;
    this.currentAbortController = new AbortController();
    const abortController = this.currentAbortController;
    const result: PollResult = {
      storesScanned: 0,
      versionsRecorded: 0,
      conflicts: 0,
      errors: 0,
    };
    // Top-level try wraps the per-store loop so a structural failure (e.g. a
    // throw escaping our per-store/per-key handlers) still increments
    // `harness_memory_watcher_poll_total{result="error"}` rather than
    // silently failing. Per existing structure this branch is unreachable
    // today, but the counter stays honest about poll outcomes.
    try {
      try {
        for (const s of this.opts.stores) {
          result.storesScanned += 1;
          let observed: ObservedFile[];
          try {
            observed = await this.scanStore(s, abortController.signal);
          } catch (e) {
            if (this.stopped && abortController.signal.aborted && isAbortOrTimeout(e)) break;
            result.errors += 1;
            console.error(`MemoryVersionWatcher: scanStore ${s.storeId} failed`, e);
            continue;
          }
          for (const o of observed) {
            const key = this.cacheKey(s.storeId, o.path);
            let lastSeen = this.cache.get(key);
            if (lastSeen === o.sha256) continue;
            let attempted: PendingRegistration | null = null;
            try {
              const pending = this.pendingRegistrations.get(key);
              if (pending) {
                const reconciled = await this.reconcilePendingRegistration(
                  s.storeId,
                  o.path,
                  pending,
                  abortController.signal,
                );
                if (reconciled) {
                  lastSeen = pending.sha256;
                } else {
                  if (
                    !this.forcePendingRetry &&
                    Date.now() - pending.attemptedAtMs < this.operationTimeoutMs
                  ) {
                    continue;
                  }
                  if (pending.sha256 !== o.sha256) {
                    this.pendingRegistrations.delete(key);
                  } else {
                    attempted = pending;
                    const pendingReg = await this.recordObservedVersion(
                      s.storeId,
                      o.path,
                      pending.content,
                      pending.sha256,
                      pending.previousSha256,
                      pending.versionId,
                      abortController.signal,
                    );
                    this.cache.set(key, pending.sha256);
                    this.pendingRegistrations.delete(key);
                    lastSeen = pending.sha256;
                    result.versionsRecorded += 1;
                    if (pendingReg.conflict) {
                      result.conflicts += 1;
                      harnessMemoryVersionsRecordedTotal.inc({
                        workspace_id: this.opts.workspaceId,
                        result: 'conflict',
                      });
                      await this.emitConflict(
                        s.storeId,
                        o.path,
                        pending.sha256,
                        pending.previousSha256,
                      );
                    } else {
                      harnessMemoryVersionsRecordedTotal.inc({
                        workspace_id: this.opts.workspaceId,
                        result: 'ok',
                      });
                    }
                  }
                }
              }
              if (lastSeen === o.sha256) continue;

              const versionId = this.versionIdForRegistration(
                s.storeId,
                o.path,
                o.sha256,
                lastSeen ?? null,
              );
              attempted = {
                sha256: o.sha256,
                content: o.content,
                previousSha256: lastSeen ?? null,
                versionId,
                attemptedAtMs: Date.now(),
              };
              const reg = await this.recordObservedVersion(
                s.storeId,
                o.path,
                o.content,
                o.sha256,
                lastSeen ?? null,
                versionId,
                abortController.signal,
              );
              this.cache.set(key, o.sha256);
              this.pendingRegistrations.delete(key);
              result.versionsRecorded += 1;
              if (o.lastModified) {
                const lagSeconds = (Date.now() - o.lastModified.getTime()) / 1000;
                if (lagSeconds >= 0) harnessMemoryWriteLagSeconds.observe(lagSeconds);
              }
              if (reg.conflict) {
                result.conflicts += 1;
                harnessMemoryVersionsRecordedTotal.inc({
                  workspace_id: this.opts.workspaceId,
                  result: 'conflict',
                });
                await this.emitConflict(s.storeId, o.path, o.sha256, lastSeen ?? null);
              } else {
                harnessMemoryVersionsRecordedTotal.inc({
                  workspace_id: this.opts.workspaceId,
                  result: 'ok',
                });
              }
            } catch (e) {
              if (this.stopped && abortController.signal.aborted && isAbortOrTimeout(e)) break;
              if (e instanceof MemoryWatcherTimeoutError && attempted) {
                this.pendingRegistrations.set(key, {
                  ...attempted,
                  attemptedAtMs: Date.now(),
                });
              }
              result.errors += 1;
              harnessMemoryVersionsRecordedTotal.inc({
                workspace_id: this.opts.workspaceId,
                result: 'error',
              });
              console.error(
                `MemoryVersionWatcher: recordMemoryVersion failed for ${s.storeId}/${o.path}`,
                e,
              );
            }
          }
        }
        harnessMemoryWatcherPollTotal.inc({ result: 'ok' });
      } catch (e) {
        harnessMemoryWatcherPollTotal.inc({ result: 'error' });
        throw e;
      }
    } finally {
      if (this.currentAbortController === abortController) {
        this.currentAbortController = null;
      }
      this.inflight = false;
    }
    return result;
  }

  private cacheKey(storeId: string, path: string): string {
    return `${storeId}|${path}`;
  }

  private versionIdForRegistration(
    storeId: string,
    path: string,
    sha256: string,
    previousSha256: string | null,
  ): string {
    const digest = createHash('sha256')
      .update(this.opts.workspaceId)
      .update('\0')
      .update(this.opts.sessionId)
      .update('\0')
      .update(storeId)
      .update('\0')
      .update(path)
      .update('\0')
      .update(sha256)
      .update('\0')
      .update(previousSha256 ?? '')
      .digest('hex')
      .slice(0, 32);
    return `memver_${digest}`;
  }

  private conflictVersionIdForRegistration(versionId: string): string {
    return `${versionId}_conflict`;
  }

  private async recordObservedVersion(
    storeId: string,
    path: string,
    content: Buffer,
    sha256: string,
    previousSha256: string | null,
    versionId: string,
    signal: AbortSignal,
  ): Promise<{ conflict: boolean }> {
    return await this.withBounds(
      (opSignal) =>
        this.opts.registry.recordSessionMemoryVersion({
          workspaceId: this.opts.workspaceId,
          sessionId: this.opts.sessionId,
          storeId,
          path,
          contentBase64: content.toString('base64'),
          contentSha256: sha256,
          previousSha256,
          versionId,
          signal: opSignal,
        }),
      signal,
      `recordMemoryVersion ${storeId}/${path}`,
      () => !this.stopped,
    );
  }

  private async scanStore(s: WatchedStore, signal: AbortSignal): Promise<ObservedFile[]> {
    if (this.opts.s3 && this.opts.bucket) {
      return this.scanStoreS3(s, signal);
    }
    if (this.opts.sandbox) {
      return this.scanStoreLocal(s, signal);
    }
    throw new Error(
      'MemoryVersionWatcher: neither S3 (s3+bucket) nor sandbox is configured; cannot scan',
    );
  }

  /**
   * S3 path: paginate `ListObjectsV2` over the store's `live/` prefix
   * (version blobs live outside it), fetch each live key's bytes, and hash.
   * Every poll cycle re-fetches — we don't trust the ETag because it isn't
   * guaranteed to be a sha for multipart uploads.
   */
  private async scanStoreS3(s: WatchedStore, signal: AbortSignal): Promise<ObservedFile[]> {
    const s3 = this.opts.s3!;
    const bucket = this.opts.bucket!;
    const keyPrefix = this.opts.memoryKeyPrefix ?? DEFAULT_MEMORY_KEY_PREFIX;
    const prefix = buildMemoryStoreLivePrefix(keyPrefix, this.opts.workspaceId, s.storeId);
    const out: ObservedFile[] = [];
    let continuationToken: string | undefined;
    do {
      const params: { Bucket: string; Prefix: string; ContinuationToken?: string } = {
        Bucket: bucket,
        Prefix: prefix,
      };
      if (continuationToken !== undefined) params.ContinuationToken = continuationToken;
      throwIfAborted(signal, `ListObjectsV2 ${s.storeId}`);
      const list = await this.withBounds(
        (opSignal) => s3.send(new ListObjectsV2Command(params), { abortSignal: opSignal }),
        signal,
        `ListObjectsV2 ${s.storeId}`,
      );
      const contents = list.Contents ?? [];
      for (const obj of contents) {
        const key = obj.Key;
        if (!key) continue;
        // Skip directory placeholders s3fs sometimes leaves behind.
        if (key.endsWith('/')) continue;
        throwIfAborted(signal, `GetObject ${key}`);
        const got = await this.withBounds(
          (opSignal) =>
            s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }), {
              abortSignal: opSignal,
            }),
          signal,
          `GetObject ${key}`,
        );
        const body = got.Body;
        if (!body) continue;
        const buf = await this.withBounds(
          (opSignal) => streamToBuffer(body as NodeJS.ReadableStream, opSignal),
          signal,
          `read S3 body ${key}`,
        );
        const path = key.startsWith(prefix) ? key.slice(prefix.length) : key;
        // Capture LastModified from the list result so the registration site
        // can observe write-lag. We prefer the list-call's
        // timestamp over a separate HeadObject — it's already in-hand, and
        // s3fs's PUTs trigger a corresponding LastModified update.
        const observed: ObservedFile = { path, sha256: sha256Hex(buf), content: buf };
        if (obj.LastModified !== undefined) observed.lastModified = obj.LastModified;
        out.push(observed);
      }
      continuationToken = list.IsTruncated ? list.NextContinuationToken : undefined;
    } while (continuationToken);
    return out;
  }

  /**
   * InMemory path: walk the sandbox FS rooted at `mountPath`. The sandbox
   * `files.read` impl throws on directories (EISDIR for the tmpdir backend
   * + matches the `output-indexer` walk pattern), so we attempt `read`
   * first; on failure we fall back to `list` and recurse. A directory that
   * doesn't exist yet (no writes have happened) is treated as empty rather
   * than an error.
   */
  private async scanStoreLocal(s: WatchedStore, signal: AbortSignal): Promise<ObservedFile[]> {
    const sandbox = this.opts.sandbox!;
    const out: ObservedFile[] = [];
    const root = s.mountPath.endsWith('/') ? s.mountPath.slice(0, -1) : s.mountPath;
    const rootPrefix = `${root}/`;

    const visit = async (dir: string): Promise<void> => {
      let entries: string[];
      try {
        throwIfAborted(signal, `sandbox.files.list ${dir}`);
        entries = await this.withBounds(
          () => sandbox.files.list(dir),
          signal,
          `sandbox.files.list ${dir}`,
        );
      } catch (e) {
        if (isAbortOrTimeout(e)) throw e;
        // Directory doesn't exist yet — no writes so far. Treat as empty.
        return;
      }
      for (const e of entries) {
        const full = `${dir}/${e}`;
        let buf: Buffer | null = null;
        try {
          throwIfAborted(signal, `sandbox.files.read ${full}`);
          buf = await this.withBounds(
            () => sandbox.files.read(full),
            signal,
            `sandbox.files.read ${full}`,
          );
        } catch (e) {
          if (isAbortOrTimeout(e)) throw e;
          // Likely a directory; recurse.
          await visit(full);
          continue;
        }
        if (buf === null) continue;
        const rel = full.startsWith(rootPrefix) ? full.slice(rootPrefix.length) : full;
        out.push({ path: rel, sha256: sha256Hex(buf), content: buf });
      }
    };

    await visit(root);
    return out;
  }

  private async withBounds<T>(
    operation: (signal: AbortSignal) => Promise<T> | T,
    signal: AbortSignal,
    label: string,
    shouldTimeout?: () => boolean,
  ): Promise<T> {
    return withAbortAndTimeout(operation, signal, this.operationTimeoutMs, label, shouldTimeout);
  }

  private async reconcilePendingRegistration(
    storeId: string,
    path: string,
    pending: PendingRegistration,
    signal: AbortSignal,
  ): Promise<boolean> {
    const memories = await this.withBounds(
      (opSignal) =>
        this.opts.registry.listSessionMemories({
          workspaceId: this.opts.workspaceId,
          sessionId: this.opts.sessionId,
          storeId,
          signal: opSignal,
        }),
      signal,
      `listMemories ${storeId} pending registration`,
    );
    const current = memories.find((m) => m.path === path);
    if (!current) return false;
    const versions = await this.withBounds(
      (opSignal) =>
        this.opts.registry.listSessionMemoryVersions({
          workspaceId: this.opts.workspaceId,
          sessionId: this.opts.sessionId,
          storeId,
          memoryId: current.id,
          signal: opSignal,
        }),
      signal,
      `listMemoryVersions ${storeId}/${path} pending registration`,
    );
    const acceptedVersionIds = new Set([
      pending.versionId,
      this.conflictVersionIdForRegistration(pending.versionId),
    ]);
    if (!versions.some((v) => acceptedVersionIds.has(v.id) && v.sha256 === pending.sha256)) {
      return false;
    }

    this.cache.set(this.cacheKey(storeId, path), pending.sha256);
    this.pendingRegistrations.delete(this.cacheKey(storeId, path));
    return true;
  }

  private async flushPendingRegistrations(deadline: number): Promise<void> {
    const remainingMs = deadline - Date.now();
    if (remainingMs <= 0) return;

    this.stopped = false;
    this.forcePendingRetry = true;
    // The final flush re-runs a full store scan rather than retrying only the
    // pending keys: the watcher has no targeted live-read path.
    const timeout = setTimeout(() => this.currentAbortController?.abort(), remainingMs);
    try {
      await this.tick();
    } catch (e) {
      if (!isAbortOrTimeout(e)) {
        console.error('MemoryVersionWatcher: final pending flush failed', e);
      }
    } finally {
      clearTimeout(timeout);
      this.forcePendingRetry = false;
      this.stopped = true;
    }
  }

  /**
   * Emit a `session.memory_conflict` transcript event so SDK consumers can
   * surface the mismatch back to client code. Best-effort: if the
   * TranscriptStore append fails, we log and move on rather than abort the
   * tick — the version is already registered server-side and the cache is
   * already updated, so swallowing the event is preferable to retrying and
   * potentially duplicating the registration.
   */
  private async emitConflict(
    storeId: string,
    path: string,
    observedSha: string,
    expectedSha: string | null,
  ): Promise<void> {
    const payload = {
      type: 'session.memory_conflict',
      store_id: storeId,
      path,
      observed_sha256: observedSha,
      expected_sha256: expectedSha,
      written_by_session_id: this.opts.sessionId,
    };
    const event: Event = {
      id: uuidv7(),
      workspaceId: this.opts.workspaceId,
      sessionId: this.opts.sessionId,
      subpath: '',
      seq: 0,
      producedAt: new Date().toISOString(),
      producedBy: 'harness',
      kind: 'session.memory_conflict',
      payload: Buffer.from(JSON.stringify(payload), 'utf8'),
      idempotencyKey: '',
    };
    try {
      await this.opts.store.append(this.opts.workspaceId, this.opts.sessionId, [event]);
    } catch (e) {
      console.error('MemoryVersionWatcher: emit session.memory_conflict failed', e);
    }
  }
}

function sha256Hex(buf: Buffer): string {
  return createHash('sha256').update(buf).digest('hex');
}

class MemoryWatcherAbortError extends Error {
  constructor(label: string) {
    super(`${label} aborted`);
    this.name = 'MemoryWatcherAbortError';
  }
}

class MemoryWatcherTimeoutError extends Error {
  constructor(label: string, timeoutMs: number) {
    super(`${label} timed out after ${timeoutMs}ms`);
    this.name = 'MemoryWatcherTimeoutError';
  }
}

function isAbortOrTimeout(err: unknown): boolean {
  return err instanceof MemoryWatcherAbortError || err instanceof MemoryWatcherTimeoutError;
}

function throwIfAborted(signal: AbortSignal, label: string): void {
  if (signal.aborted) throw new MemoryWatcherAbortError(label);
}

function withAbortAndTimeout<T>(
  operation: (signal: AbortSignal) => Promise<T> | T,
  signal: AbortSignal,
  timeoutMs: number,
  label: string,
  shouldTimeout: () => boolean = () => true,
): Promise<T> {
  if (signal.aborted) return Promise.reject(new MemoryWatcherAbortError(label));

  return new Promise<T>((resolve, reject) => {
    let settled = false;
    const timeoutController = new AbortController();
    const operationSignal = combineAbortSignals(signal, timeoutController.signal);
    const timeout = setTimeout(() => {
      if (!shouldTimeout()) return;
      timeoutController.abort();
      settle(() => reject(new MemoryWatcherTimeoutError(label, timeoutMs)));
    }, timeoutMs);
    const onAbort = (): void => {
      settle(() => reject(new MemoryWatcherAbortError(label)));
    };
    const settle = (fn: () => void): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      signal.removeEventListener('abort', onAbort);
      fn();
    };

    signal.addEventListener('abort', onAbort, { once: true });
    Promise.resolve()
      .then(() => operation(operationSignal))
      .then(
        (value) => settle(() => resolve(value)),
        (err: unknown) => settle(() => reject(err)),
      );
  });
}

function combineAbortSignals(first: AbortSignal, second: AbortSignal): AbortSignal {
  const maybeAny = (AbortSignal as { any?: (signals: AbortSignal[]) => AbortSignal }).any;
  if (maybeAny) return maybeAny([first, second]);
  const controller = new AbortController();
  const abort = (): void => controller.abort();
  if (first.aborted || second.aborted) {
    controller.abort();
    return controller.signal;
  }
  first.addEventListener('abort', abort, { once: true });
  second.addEventListener('abort', abort, { once: true });
  return controller.signal;
}

function streamToBuffer(stream: NodeJS.ReadableStream, signal?: AbortSignal): Promise<Buffer> {
  const chunks: Buffer[] = [];
  let onAbort: (() => void) | undefined;
  return new Promise<Buffer>((resolve, reject) => {
    if (signal?.aborted) {
      reject(new MemoryWatcherAbortError('read stream'));
      return;
    }
    onAbort = () => {
      stream.destroy(new MemoryWatcherAbortError('read stream'));
    };
    signal?.addEventListener('abort', onAbort, { once: true });
    stream.on('data', (chunk: Buffer | Uint8Array) => {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    });
    stream.once('error', (err) => {
      cleanup();
      reject(err);
    });
    stream.once('end', () => {
      cleanup();
      resolve(Buffer.concat(chunks));
    });
  });

  function cleanup(): void {
    if (onAbort) signal?.removeEventListener('abort', onAbort);
  }
}
