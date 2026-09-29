// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { FastifyInstance } from 'fastify';
import type { FileStore, FileRecord, FilePurpose } from '@orca/file-store';
import { Readable } from 'node:stream';
import { parsePositiveIntQueryParam } from './query-params.js';

interface ListQuery {
  limit?: string;
  after_id?: string;
  before_id?: string;
  scope_id?: string;
}

/**
 * Anthropic's Files API contract caps an individual upload at 500 MB. We
 * enforce the same cap so the SDK-compatibility surface matches; oversized
 * uploads get a 413, never a silent truncation.
 */
const MAX_FILE_BYTES = 500 * 1024 * 1024;

export interface FilesRouteOptions {
  /** Test seam; production uses Claude's 500 MiB upload limit. */
  maxFileBytes?: number;
}

class FileUploadTooLargeError extends Error {
  readonly code = 'ORCA_FILE_TOO_LARGE';

  constructor(readonly maxFileBytes: number) {
    super(`file exceeds ${maxFileBytes} byte limit`);
    this.name = 'FileUploadTooLargeError';
  }
}

function sizeLimitedStream(source: NodeJS.ReadableStream, maxFileBytes: number): Readable {
  async function* chunks(): AsyncGenerator<unknown> {
    let sizeBytes = 0;
    for await (const chunk of source as NodeJS.ReadableStream & AsyncIterable<unknown>) {
      const chunkSize =
        typeof chunk === 'string'
          ? Buffer.byteLength(chunk)
          : ArrayBuffer.isView(chunk)
            ? chunk.byteLength
            : Buffer.byteLength(String(chunk));
      sizeBytes += chunkSize;
      if (sizeBytes > maxFileBytes) throw new FileUploadTooLargeError(maxFileBytes);
      yield chunk;
    }
    // Busboy normally emits maxFileBytes + 1 bytes because the multipart
    // parser's own cap is deliberately one byte higher than ours. Retain this
    // check as a defensive fallback in case a future parser version truncates
    // before yielding that final byte.
    if ((source as NodeJS.ReadableStream & { truncated?: boolean }).truncated === true) {
      throw new FileUploadTooLargeError(maxFileBytes);
    }
  }
  return Readable.from(chunks());
}

function isOrcaBetaRequest(headers: Record<string, string | string[] | undefined>): boolean {
  const value = headers['orca-beta'];
  return Array.isArray(value) ? value.length > 0 : typeof value === 'string' && value.length > 0;
}

function toLegacyApi(r: FileRecord): {
  id: string;
  filename: string;
  mime_type: string;
  size_bytes: number;
  sha256: string;
  metadata: Record<string, string>;
  purpose: FilePurpose;
  scope_id: string | null;
  downloadable: boolean;
  archived_at: string | null;
  created_at: string;
  updated_at: string;
} {
  return {
    id: r.id,
    filename: r.filename,
    mime_type: r.mimeType,
    size_bytes: r.sizeBytes,
    sha256: r.sha256,
    metadata: r.metadata,
    purpose: r.purpose,
    scope_id: r.scopeId,
    downloadable: r.downloadable,
    archived_at: r.archivedAt?.toISOString() ?? null,
    created_at: r.createdAt.toISOString(),
    updated_at: r.updatedAt.toISOString(),
  };
}

function toClaudeApi(r: FileRecord) {
  return {
    id: r.id,
    created_at: r.createdAt.toISOString(),
    filename: r.filename,
    mime_type: r.mimeType,
    size_bytes: r.sizeBytes,
    type: 'file' as const,
    downloadable: r.downloadable,
    scope: r.scopeId === null ? null : { type: 'session' as const, id: r.scopeId },
  };
}

export function fileRecordToApi(r: FileRecord, orcaBeta: boolean) {
  return orcaBeta ? toLegacyApi(r) : toClaudeApi(r);
}

export function registerFilesRoutes(
  app: FastifyInstance,
  fileStore: FileStore,
  opts: FilesRouteOptions = {},
): void {
  const maxFileBytes = opts.maxFileBytes ?? MAX_FILE_BYTES;
  app.post('/v1/files', async (req, reply) => {
    const auth = req.auth!;
    // The official upload body contains only `file`. Stream the first file
    // part directly into the store and ignore every public metadata selector,
    // regardless of multipart order. In particular, callers cannot use
    // legacy `purpose`, `scope_id`, `downloadable`, or workspace fields to
    // mint an internally-scoped/downloadable record. The harness-only
    // `/internal/files` route owns that capability.
    const partsIterable = (
      req as unknown as {
        parts: (opts?: {
          limits?: { fileSize?: number };
          throwFileSizeLimit?: boolean;
        }) => AsyncIterableIterator<{
          type: 'file' | 'field';
          filename?: string;
          mimetype?: string;
          fieldname: string;
          file?: NodeJS.ReadableStream;
          value?: unknown;
        }>;
      }
    ).parts({
      // Give the route-level counting stream one extra byte so it can reject
      // before FileStore.create completes. Relying only on Busboy's
      // `truncated` flag would discover the overflow after a truncated object
      // may already have been persisted (or deduplicated to an existing row).
      limits: { fileSize: maxFileBytes + 1 },
      throwFileSizeLimit: false,
    });

    let created: FileRecord | null = null;

    try {
      for await (const part of partsIterable) {
        if (part.type === 'file' && part.fieldname === 'file') {
          if (created !== null) {
            // The contract accepts one file. Drain extra file streams so the
            // multipart iterator can finish, but never persist or validate
            // them. Applying the first-file size guard here could reject after
            // the accepted file has already been persisted, leaving a record
            // behind for an otherwise failed request.
            if (part.file) {
              for await (const _chunk of part.file as NodeJS.ReadableStream &
                AsyncIterable<unknown>) {
                // drain
              }
            }
            continue;
          }
          if (!part.file) {
            return reply.code(400).send({ error: 'missing file stream' });
          }
          created = await fileStore.create({
            workspaceId: auth.workspaceId,
            filename: part.filename ?? 'untitled',
            mimeType: part.mimetype ?? 'application/octet-stream',
            metadata: {},
            content: sizeLimitedStream(part.file, maxFileBytes),
            purpose: 'agent' satisfies FilePurpose,
            scopeId: null,
          });
        }
      }
    } catch (err) {
      // The route-level stream guard is the primary signal. Keep recognizing
      // @fastify/multipart's own 413 as a defensive fallback for parser-level
      // enforcement changes.
      const e = err as { code?: string; statusCode?: number; name?: string };
      if (
        e.code === 'ORCA_FILE_TOO_LARGE' ||
        e.code === 'FST_REQ_FILE_TOO_LARGE' ||
        e.statusCode === 413
      ) {
        return reply.code(413).send({ error: `file exceeds ${maxFileBytes} byte limit` });
      }
      throw err;
    }

    if (created === null) {
      return reply.code(400).send({ error: 'no file part provided' });
    }
    return reply.send(fileRecordToApi(created, isOrcaBetaRequest(req.headers)));
  });

  app.get('/v1/files/:id', async (req, reply) => {
    const auth = req.auth!;
    const { id } = req.params as { id: string };
    const r = await fileStore.get(auth.workspaceId, id);
    if (!r) return reply.code(404).send({ error: 'not found' });
    return reply.send(fileRecordToApi(r, isOrcaBetaRequest(req.headers)));
  });

  app.get('/v1/files', async (req, reply) => {
    const auth = req.auth!;
    const q = req.query as ListQuery;
    const parsedLimit = parsePositiveIntQueryParam(q.limit, 'limit', {
      defaultValue: 20,
      max: 1000,
    });
    if (!parsedLimit.ok) return reply.code(400).send({ error: parsedLimit.error });
    if (q.after_id && q.before_id) {
      return reply.code(400).send({ error: 'after_id and before_id are mutually exclusive' });
    }
    const limit = parsedLimit.value!;
    const opts: { limit: number; cursor?: string; beforeCursor?: string; scopeId?: string } = {
      limit,
    };
    if (q.after_id) opts.cursor = q.after_id;
    if (q.before_id) opts.beforeCursor = q.before_id;
    // Convention: `?scope_id=` (empty string) is treated as "no filter" — the
    // truthy guard below intentionally skips the empty-string case so the
    // response still includes both scoped and unscoped files. Only a
    // non-empty `scope_id` narrows the result set; passing an unknown id
    // returns an empty page (locked in by `files-purpose-scope.spec.ts`).
    if (q.scope_id) opts.scopeId = q.scope_id;
    const page = await fileStore.list(auth.workspaceId, opts);
    return reply.send({
      data: page.items.map((record) => fileRecordToApi(record, isOrcaBetaRequest(req.headers))),
      first_id: page.items[0]?.id ?? null,
      last_id: page.items[page.items.length - 1]?.id ?? null,
      has_more: page.nextCursor !== null,
    });
  });

  app.get('/v1/files/:id/content', async (req, reply) => {
    const auth = req.auth!;
    const { id } = req.params as { id: string };
    // Look up the record BEFORE opening the stream so we can enforce the
    // downloadable gate. Anthropic's Files API contract: user uploads
    // (`purpose='agent'`) are NOT retrievable via getContent — only agent
    // outputs (`purpose='agent_output'`) are. The store sets `downloadable`
    // appropriately at create time; this route is the enforcement point.
    const record = await fileStore.get(auth.workspaceId, id);
    if (!record) return reply.code(404).send({ error: 'not found' });
    if (!record.downloadable) {
      return reply.code(403).send({
        error:
          'this file is not downloadable (uploads are not retrievable per Anthropic Files API contract; only agent_output files can be downloaded)',
      });
    }
    const opened = await fileStore.open(auth.workspaceId, id);
    if (!opened) return reply.code(404).send({ error: 'not found' });
    reply.header('content-type', 'application/octet-stream');
    reply.header('content-length', String(opened.sizeBytes));
    return reply.send(opened.stream);
  });

  app.delete('/v1/files/:id', async (req, reply) => {
    const auth = req.auth!;
    const { id } = req.params as { id: string };
    try {
      await fileStore.delete(auth.workspaceId, id);
    } catch (err) {
      if ((err as Error).name === 'FileNotFoundError') {
        return reply.code(404).send({ error: 'not found' });
      }
      throw err;
    }
    return reply.send({ id, type: 'file_deleted' });
  });
}
