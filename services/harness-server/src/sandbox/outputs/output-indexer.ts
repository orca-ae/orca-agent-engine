// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Output indexer.
 *
 * Runs after agent tool results and again at session-end. Walks the session's
 * output mount — either the S3 prefix (FUSE path) or the sandbox's
 * `/mnt/session/outputs` directory (local path) — and registers each blob with
 * the registry as a `File` row
 * (`purpose='agent_output'`, `scope_id=ses_…`, `downloadable=true`), and
 * emits one `session.output_indexed` event per file via the TranscriptStore.
 *
 * The indexer is best-effort per object: a single failed `createFile` call
 * pushes an entry into `errors[]` and the loop continues. The 500 MB
 * per-object cap matches Anthropic's Files API contract — oversized blobs
 * are skipped (counted in `skipped`) rather than partially uploaded.
 *
 * Bandwidth note: in the FUSE path the indexer reads bytes from S3 only to
 * post them to the registry, which then re-uploads to the same S3 backend.
 * That redundancy is the v1 cost of keeping the registry the single source
 * of truth for `File` rows; a sha-shortcut path can come later if the cost
 * shows up in profiles.
 */

import { createHash } from 'node:crypto';
import { GetObjectCommand, ListObjectsV2Command, type S3Client } from '@aws-sdk/client-s3';
import { v7 as uuidv7 } from 'uuid';
import type { Event, TranscriptStore } from '@orca/transcript-store';
import type { RegistryClient, RegistryFileRecord } from '../../clients/registry.js';
import type { SandboxHandle } from '../sandbox-runtime.js';
import type { OutputMountHandle } from './output-mount.js';

/** 500 MB — same cap the public and scoped internal file routes enforce. */
const DEFAULT_MAX_BYTES_PER_FILE = 500 * 1024 * 1024;

export interface IndexSessionInput {
  workspaceId: string;
  sessionId: string;
  mount: OutputMountHandle;
  /** Required when `mount.kind === 's3'`. */
  s3?: S3Client;
  /**
   * Required when `mount.kind === 'inmemory_local'` so the indexer can walk
   * the sandbox FS at `mount.sandboxPath`. Omitted in the FUSE path.
   */
  sandbox?: SandboxHandle;
  registry: RegistryClient;
  store: TranscriptStore;
  /** Cancels a scan before its mount is torn down during bounded shutdown. */
  signal?: AbortSignal;
  /** Default 500 MB. Files larger than this are skipped + logged. */
  maxBytesPerFile?: number;
}

export interface IndexSessionResult {
  count: number;
  skipped: number;
  errors: Array<{ key: string; error: string }>;
}

export class OutputIndexer {
  /** One OutputIndexer is created per SessionRunner. */
  private readonly indexedContentByKey = new Map<string, string>();
  /** Registry succeeded but the readiness event has not reached the transcript yet. */
  private pendingIndexedEvents: Array<{ key: string; event: Event }> = [];

  async indexSession(input: IndexSessionInput): Promise<IndexSessionResult> {
    input.signal?.throwIfAborted();
    const result: IndexSessionResult = { count: 0, skipped: 0, errors: [] };
    const maxBytes = input.maxBytesPerFile ?? DEFAULT_MAX_BYTES_PER_FILE;
    await this.flushPendingIndexedEvents(input, result);

    if (input.mount.kind === 's3') {
      if (!input.s3 || !input.mount.s3) {
        throw new Error('OutputIndexer: mount.kind=s3 requires s3 client + mount.s3 config');
      }
      await this.indexS3(input, input.s3, input.mount.s3, maxBytes, result);
      return result;
    }

    if (input.mount.kind === 'inmemory_local') {
      if (!input.sandbox) {
        // Fail soft: surface a clear error in the result so the caller can log.
        // This matches the per-object failure model — a missing sandbox is a
        // wiring bug, not a reason to abort the whole indexer pass.
        result.errors.push({
          key: input.mount.sandboxPath,
          error: 'OutputIndexer: mount.kind=inmemory_local requires sandbox',
        });
        return result;
      }
      await this.indexInMemory(input, input.sandbox, maxBytes, result);
      return result;
    }

    return result;
  }

  /**
   * S3 path: paginate `ListObjectsV2` over the session prefix, fetch each
   * object via `GetObjectCommand` (capped at `maxBytes`), and POST to the
   * registry. The trailing-slash invariant on `prefix` (set by
   * `mountSessionOutputs`) keeps us from accidentally matching a wider
   * key namespace.
   */
  private async indexS3(
    input: IndexSessionInput,
    s3: S3Client,
    cfg: { bucket: string; prefix: string },
    maxBytes: number,
    result: IndexSessionResult,
  ): Promise<void> {
    let continuationToken: string | undefined;
    do {
      const params: {
        Bucket: string;
        Prefix: string;
        ContinuationToken?: string;
      } = { Bucket: cfg.bucket, Prefix: cfg.prefix };
      if (continuationToken !== undefined) params.ContinuationToken = continuationToken;
      const list = await s3.send(new ListObjectsV2Command(params), {
        abortSignal: input.signal,
      });
      const contents = list.Contents ?? [];

      for (const obj of contents) {
        const key = obj.Key;
        if (!key) continue;
        // Skip "directory" placeholders s3fs sometimes leaves behind.
        if (key.endsWith('/')) continue;

        const size = typeof obj.Size === 'number' ? obj.Size : 0;
        if (size > maxBytes) {
          console.warn(`OutputIndexer: skipping ${key} (${size} bytes > ${maxBytes} cap)`);
          result.skipped += 1;
          continue;
        }

        try {
          const get = await s3.send(new GetObjectCommand({ Bucket: cfg.bucket, Key: key }), {
            abortSignal: input.signal,
          });
          const body = get.Body;
          if (!body) {
            result.errors.push({ key, error: 'empty body' });
            continue;
          }
          const buffer = await collect(body as NodeJS.ReadableStream, input.signal);
          if (buffer.length > maxBytes) {
            // Defensive — the size from List can lag a multipart upload.
            console.warn(
              `OutputIndexer: skipping ${key} (${buffer.length} bytes > ${maxBytes} cap)`,
            );
            result.skipped += 1;
            continue;
          }
          const filename = key.startsWith(cfg.prefix) ? key.slice(cfg.prefix.length) : key;
          const contentSha256 = sha256(buffer);
          if (this.isAlreadyIndexed(filename, contentSha256)) continue;
          const mimeType =
            typeof get.ContentType === 'string' && get.ContentType.length > 0
              ? get.ContentType
              : 'application/octet-stream';

          const file = await withAbort(
            input.signal,
            async () =>
              await input.registry.createFile({
                workspaceId: input.workspaceId,
                sessionId: input.sessionId,
                content: buffer,
                filename,
                mimeType,
                ...(input.signal ? { signal: input.signal } : {}),
              }),
          );

          this.rememberIndexed(filename, contentSha256);
          result.count += 1;
          const emitError = await this.emitIndexedEvent(input, file, filename);
          if (emitError) result.errors.push({ key, error: emitError });
        } catch (e) {
          if (isAbortError(e)) throw e;
          const error = (e as Error).message ?? String(e);
          console.error(`OutputIndexer: failed to register ${key}`, e);
          result.errors.push({ key, error });
        }
      }

      continuationToken = list.IsTruncated ? list.NextContinuationToken : undefined;
    } while (continuationToken);
  }

  /**
   * InMemory path: walk the sandbox-side outputs dir and read each file via
   * `sandbox.files.read`. The sandbox's `files.list` returns one level of
   * entries, so we recurse so nested subdirs (e.g. `outputs/charts/foo.png`)
   * are picked up.
   */
  private async indexInMemory(
    input: IndexSessionInput,
    sandbox: SandboxHandle,
    maxBytes: number,
    result: IndexSessionResult,
  ): Promise<void> {
    const root = input.mount.sandboxPath;
    const queue: string[] = [];
    let topLevel: string[] = [];
    try {
      topLevel = await withAbort(input.signal, async () => await sandbox.files.list(root));
    } catch (e) {
      if (isAbortError(e)) throw e;
      // The dir may not exist (no outputs were written). Treat as empty.
      console.warn(`OutputIndexer: list ${root} failed (treating as empty):`, (e as Error).message);
      return;
    }
    for (const entry of topLevel) {
      queue.push(`${root}/${entry}`);
    }

    while (queue.length > 0) {
      const path = queue.shift()!;
      // Try reading first; if it's a file, this is a single roundtrip. If
      // it's a directory, the `files.read` impl throws (EISDIR for the local
      // tmpdir backend) and we fall through to `files.list`.
      let contents: Buffer | null = null;
      try {
        contents = await withAbort(input.signal, async () => await sandbox.files.read(path));
      } catch (readError) {
        if (isAbortError(readError)) throw readError;
        try {
          const entries = await withAbort(input.signal, async () => await sandbox.files.list(path));
          for (const entry of entries) queue.push(`${path}/${entry}`);
          continue;
        } catch (e) {
          if (isAbortError(e)) throw e;
          result.errors.push({ key: path, error: (e as Error).message });
          continue;
        }
      }

      if (contents === null) continue;
      if (contents.length > maxBytes) {
        console.warn(
          `OutputIndexer: skipping ${path} (${contents.length} bytes > ${maxBytes} cap)`,
        );
        result.skipped += 1;
        continue;
      }
      try {
        const filename = path.startsWith(`${root}/`) ? path.slice(root.length + 1) : path;
        const contentSha256 = sha256(contents);
        if (this.isAlreadyIndexed(filename, contentSha256)) continue;
        const file = await withAbort(
          input.signal,
          async () =>
            await input.registry.createFile({
              workspaceId: input.workspaceId,
              sessionId: input.sessionId,
              content: contents,
              filename,
              mimeType: 'application/octet-stream',
              ...(input.signal ? { signal: input.signal } : {}),
            }),
        );
        this.rememberIndexed(filename, contentSha256);
        result.count += 1;
        const emitError = await this.emitIndexedEvent(input, file, filename);
        if (emitError) result.errors.push({ key: path, error: emitError });
      } catch (e) {
        if (isAbortError(e)) throw e;
        const error = (e as Error).message ?? String(e);
        console.error(`OutputIndexer: failed to register ${path}`, e);
        result.errors.push({ key: path, error });
      }
    }
  }

  /**
   * Emit a `session.output_indexed` event so consumers (SDK, observability)
   * can correlate the registered `File.id` to its output-relative path.
   */
  private async emitIndexedEvent(
    input: IndexSessionInput,
    file: RegistryFileRecord,
    relativePath: string,
  ): Promise<string | null> {
    const eventId = uuidv7();
    const payload = {
      type: 'session.output_indexed',
      file_id: file.id,
      key: relativePath,
      sha256: file.sha256,
      size_bytes: file.size_bytes,
    };
    const event: Event = {
      id: eventId,
      workspaceId: input.workspaceId,
      sessionId: input.sessionId,
      subpath: '',
      seq: 0,
      producedAt: new Date().toISOString(),
      producedBy: 'harness',
      kind: 'session.output_indexed',
      payload: Buffer.from(JSON.stringify(payload), 'utf8'),
      // Retries use the same event object, so a response-lost ambiguity in
      // TranscriptStore.append cannot produce duplicate readiness events.
      idempotencyKey: eventId,
    };
    try {
      await withAbort(
        input.signal,
        async () => await input.store.append(input.workspaceId, input.sessionId, [event]),
      );
      return null;
    } catch (e) {
      if (isAbortError(e)) {
        this.pendingIndexedEvents.push({ key: relativePath, event });
        throw e;
      }
      const error = (e as Error).message ?? String(e);
      console.error('emit session.output_indexed failed', e);
      this.pendingIndexedEvents.push({ key: relativePath, event });
      return `emit session.output_indexed failed: ${error}`;
    }
  }

  /** Retry readiness events without re-registering their already-created File rows. */
  private async flushPendingIndexedEvents(
    input: IndexSessionInput,
    result: IndexSessionResult,
  ): Promise<void> {
    if (this.pendingIndexedEvents.length === 0) return;
    const pending = this.pendingIndexedEvents;
    this.pendingIndexedEvents = [];
    for (let index = 0; index < pending.length; index += 1) {
      const item = pending[index]!;
      try {
        await withAbort(
          input.signal,
          async () => await input.store.append(input.workspaceId, input.sessionId, [item.event]),
        );
      } catch (e) {
        if (isAbortError(e)) {
          // The local batch was removed from the instance queue before the
          // retry loop. Put the current item and every unattempted successor
          // back so cancellation cannot silently discard readiness events.
          this.pendingIndexedEvents.push(...pending.slice(index));
          throw e;
        }
        const error = (e as Error).message ?? String(e);
        this.pendingIndexedEvents.push(item);
        result.errors.push({
          key: item.key,
          error: `retry session.output_indexed failed: ${error}`,
        });
      }
    }
  }

  private isAlreadyIndexed(key: string, contentSha256: string): boolean {
    return this.indexedContentByKey.get(key) === contentSha256;
  }

  private rememberIndexed(key: string, contentSha256: string): void {
    this.indexedContentByKey.set(key, contentSha256);
  }
}

function sha256(content: Buffer): string {
  return createHash('sha256').update(content).digest('hex');
}

/**
 * Drain a Readable into a Buffer. The S3 SDK returns
 * `StreamingBlobPayloadOutputTypes` which on Node is a `Readable`; we use
 * `for await` so back-pressure stays correct.
 */
async function collect(stream: NodeJS.ReadableStream, signal?: AbortSignal): Promise<Buffer> {
  signal?.throwIfAborted();
  const chunks: Buffer[] = [];
  const destroy = (): void => {
    const destroyable = stream as NodeJS.ReadableStream & { destroy?: (error?: Error) => void };
    destroyable.destroy?.(abortError('output stream collection aborted'));
  };
  signal?.addEventListener('abort', destroy, { once: true });
  try {
    for await (const chunk of stream) {
      signal?.throwIfAborted();
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as Uint8Array));
    }
  } finally {
    signal?.removeEventListener('abort', destroy);
  }
  return Buffer.concat(chunks);
}

function withAbort<T>(signal: AbortSignal | undefined, operation: () => Promise<T>): Promise<T> {
  if (!signal) return operation();
  signal.throwIfAborted();
  return new Promise<T>((resolve, reject) => {
    const onAbort = (): void => reject(signal.reason ?? abortError('output indexing aborted'));
    signal.addEventListener('abort', onAbort, { once: true });
    let pending: Promise<T>;
    try {
      pending = operation();
    } catch (error) {
      signal.removeEventListener('abort', onAbort);
      reject(error);
      return;
    }
    void pending.then(
      (value) => {
        signal.removeEventListener('abort', onAbort);
        resolve(value);
      },
      (error: unknown) => {
        signal.removeEventListener('abort', onAbort);
        reject(error);
      },
    );
  });
}

function abortError(message: string): Error {
  const error = new Error(message);
  error.name = 'AbortError';
  return error;
}

function isAbortError(value: unknown): boolean {
  return value instanceof Error && value.name === 'AbortError';
}
