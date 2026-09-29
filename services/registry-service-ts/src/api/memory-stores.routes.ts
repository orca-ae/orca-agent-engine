// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { Readable } from 'node:stream';
import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import {
  MemoryConflictError,
  type MemoryActorAttribution,
  type MemoryRecord,
  type MemoryStore as MemoryStoreLib,
  type MemoryStoreRecord,
  type MemoryVersionRecord,
} from '@orca/memory-store';
import type { AuthenticatedPrincipal } from '../auth/principal.js';
import { mapReadItems } from '../domain/read-concurrency.js';
import { requestReadSignal } from '../middleware/read-admission.js';
import { parsePositiveIntQueryParam } from './query-params.js';
import {
  applyMetadataPatch,
  parseMetadata,
  parseMetadataPatch,
  toJsonMetadata,
  validateMetadataLimits,
} from './metadata.js';

interface ListQuery {
  limit?: string;
  page?: string;
  include_archived?: string;
  'created_at[gte]'?: string;
  'created_at[lte]'?: string;
}

interface ApiMemoryStore {
  id: string;
  name: string;
  description: string | null;
  metadata: Record<string, string>;
  archived_at: string | null;
  created_at: string;
  updated_at: string;
}

/**
 * Anthropic's Memory Tool API caps each memory at ~100 KB. We mirror the cap
 * here so the registry rejects oversize payloads with HTTP 413 before
 * touching the underlying store (which enforces the same cap defensively).
 */
const MAX_MEMORY_BYTES = 100 * 1024;
const MAX_MEMORY_PATH_BYTES = 1024;
const CONTROL_RE = /\p{Cc}/u;
const CONTROL_OR_FORMAT_RE = /[\p{Cc}\p{Cf}]/u;

function isOrcaBetaRequest(headers: Record<string, string | string[] | undefined>): boolean {
  const value = headers['orca-beta'];
  return Array.isArray(value) ? value.length > 0 : typeof value === 'string' && value.length > 0;
}

type MemoryView = 'basic' | 'full';

function publicActor(auth: AuthenticatedPrincipal): MemoryActorAttribution {
  if (auth.apiKeyId) return { apiKeyId: auth.apiKeyId };
  if (auth.userId) return { userId: auth.userId };
  return {};
}

function publicWriteAttribution(auth: AuthenticatedPrincipal) {
  const actor = publicActor(auth);
  return {
    ...(actor.apiKeyId !== undefined ? { writtenByApiKeyId: actor.apiKeyId } : {}),
    ...(actor.userId !== undefined ? { writtenByUserId: actor.userId } : {}),
  };
}

function createdByActor(version: MemoryVersionRecord) {
  if (version.writtenBySessionId) {
    return { type: 'session_actor' as const, session_id: version.writtenBySessionId };
  }
  if (version.writtenByApiKeyId) {
    return { type: 'api_actor' as const, api_key_id: version.writtenByApiKeyId };
  }
  if (version.writtenByUserId) {
    return { type: 'user_actor' as const, user_id: version.writtenByUserId };
  }
  return undefined;
}

function redactedByActor(version: MemoryVersionRecord) {
  if (version.redactedBySessionId) {
    return { type: 'session_actor' as const, session_id: version.redactedBySessionId };
  }
  if (version.redactedByApiKeyId) {
    return { type: 'api_actor' as const, api_key_id: version.redactedByApiKeyId };
  }
  if (version.redactedByUserId) {
    return { type: 'user_actor' as const, user_id: version.redactedByUserId };
  }
  return undefined;
}

function storeToApi(r: MemoryStoreRecord, orcaBeta: boolean) {
  if (orcaBeta) return toApi(r);
  return {
    id: r.id,
    created_at: r.createdAt.toISOString(),
    name: r.name,
    type: 'memory_store' as const,
    updated_at: r.updatedAt.toISOString(),
    archived_at: r.archivedAt?.toISOString() ?? null,
    description: r.description ?? '',
    metadata: r.metadata,
  };
}

function toApi(r: MemoryStoreRecord): ApiMemoryStore {
  return {
    id: r.id,
    name: r.name,
    description: r.description,
    metadata: r.metadata,
    archived_at: r.archivedAt ? r.archivedAt.toISOString() : null,
    created_at: r.createdAt.toISOString(),
    updated_at: r.updatedAt.toISOString(),
  };
}

function sha256Hex(content: Buffer): string {
  return createHash('sha256').update(content).digest('hex');
}

function absoluteMemoryPath(path: string): string {
  return `/${path}`;
}

function parseClaudeMemoryPath(path: unknown): { ok: true; path: string } | { ok: false } {
  if (typeof path !== 'string' || !path.startsWith('/') || path === '/') return { ok: false };
  if (path.normalize('NFC') !== path || CONTROL_OR_FORMAT_RE.test(path)) return { ok: false };
  if (Buffer.byteLength(path, 'utf8') > MAX_MEMORY_PATH_BYTES) return { ok: false };
  const segments = path.slice(1).split('/');
  if (segments.some((segment) => segment.length === 0 || segment === '.' || segment === '..')) {
    return { ok: false };
  }
  return { ok: true, path: segments.join('/') };
}

function unicodeCodePointLength(value: string): number {
  return Array.from(value).length;
}

function parsePathPrefix(value: unknown): { ok: true; prefix: string } | { ok: false } {
  if (value === undefined) return { ok: true, prefix: '/' };
  if (typeof value !== 'string' || !value.startsWith('/') || !value.endsWith('/')) {
    return { ok: false };
  }
  if (value === '/') return { ok: true, prefix: '/' };
  const parsed = parseClaudeMemoryPath(value.slice(0, -1));
  return parsed.ok ? { ok: true, prefix: `/${parsed.path}/` } : { ok: false };
}

function parseView(value: unknown, defaultView: MemoryView): MemoryView | null {
  if (value === undefined) return defaultView;
  return value === 'basic' || value === 'full' ? value : null;
}

function parseDepth(value: unknown): 0 | 1 | null {
  if (value === undefined || value === 0 || value === '0') return 0;
  if (value === 1 || value === '1') return 1;
  return null;
}

function parseBoolean(value: unknown, defaultValue: boolean): boolean | null {
  if (value === undefined) return defaultValue;
  if (value === true || value === 'true') return true;
  if (value === false || value === 'false') return false;
  return null;
}

function parseDateFilter(value: unknown): Date | null | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== 'string') return null;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
}

async function streamToUtf8(stream: NodeJS.ReadableStream, signal?: AbortSignal): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const chunk of signal ? Readable.from(stream, { signal }) : stream) {
    chunks.push(
      Buffer.isBuffer(chunk)
        ? chunk
        : typeof chunk === 'string'
          ? Buffer.from(chunk)
          : Buffer.from(chunk as unknown as Uint8Array),
    );
  }
  return Buffer.concat(chunks).toString('utf8');
}

async function memoryToClaudeApi(
  memoryStore: MemoryStoreLib,
  workspaceId: string,
  memory: MemoryRecord,
  view: MemoryView,
  prefetchedVersions?: MemoryVersionRecord[],
  signal?: AbortSignal,
) {
  const versions =
    prefetchedVersions ?? (await memoryStore.listVersions(workspaceId, memory.storeId, memory.id));
  const head = versions[0];
  const created = versions[versions.length - 1];
  if (!head || !created) throw new Error(`memory ${memory.id} has no version history`);
  let content: string | null = null;
  if (view === 'full') {
    const opened = await memoryStore.openMemory(workspaceId, memory.storeId, memory.id);
    if (!opened) throw new Error(`memory ${memory.id} content is missing`);
    content = await streamToUtf8(opened.stream, signal);
  }
  return {
    id: memory.id,
    content_sha256: memory.currentSha256,
    content_size_bytes: memory.sizeBytes,
    created_at: created.writtenAt.toISOString(),
    memory_store_id: memory.storeId,
    memory_version_id: head.id,
    path: absoluteMemoryPath(memory.path),
    type: 'memory' as const,
    updated_at: memory.updatedAt.toISOString(),
    content,
  };
}

function operationForVersion(
  version: MemoryVersionRecord,
  versions: MemoryVersionRecord[],
  memoryIsActive: boolean,
): 'created' | 'modified' | 'deleted' {
  if (!memoryIsActive && versions[0]?.id === version.id) return 'deleted';
  if (versions[versions.length - 1]?.id === version.id) return 'created';
  return 'modified';
}

async function versionToClaudeApi(
  memoryStore: MemoryStoreLib,
  workspaceId: string,
  version: MemoryVersionRecord,
  view: MemoryView,
  versions: MemoryVersionRecord[],
  memoryIsActive: boolean,
  signal?: AbortSignal,
) {
  const operation = operationForVersion(version, versions, memoryIsActive);
  const redacted = version.redactedAt !== null;
  const createdBy = createdByActor(version);
  const redactedBy = redactedByActor(version);
  let content: string | null = null;
  if (view === 'full' && operation !== 'deleted' && !redacted) {
    const opened = await memoryStore.openVersion(workspaceId, version.storeId, version.id);
    if (opened) content = await streamToUtf8(opened.stream, signal);
  }
  return {
    id: version.id,
    created_at: version.writtenAt.toISOString(),
    memory_id: version.memoryId,
    memory_store_id: version.storeId,
    operation,
    type: 'memory_version' as const,
    content,
    content_sha256: operation === 'deleted' || redacted ? null : version.sha256,
    content_size_bytes: operation === 'deleted' || redacted ? null : version.sizeBytes,
    ...(createdBy ? { created_by: createdBy } : {}),
    path: redacted ? null : absoluteMemoryPath(version.path),
    redacted_at: version.redactedAt?.toISOString() ?? null,
    ...(redactedBy ? { redacted_by: redactedBy } : {}),
  };
}

function encodeOffsetCursor(offset: number): string {
  return Buffer.from(JSON.stringify({ offset }), 'utf8').toString('base64url');
}

function decodeOffsetCursor(value: unknown): number | null {
  if (value === undefined || value === '') return 0;
  if (typeof value !== 'string') return null;
  const decoded = decodeJsonCursor(value);
  return decoded && Number.isInteger(decoded.offset) && Number(decoded.offset) >= 0
    ? Number(decoded.offset)
    : null;
}

interface MemoryCursorPosition {
  path: string;
  id: string;
}

interface MemoryVersionCursorPosition {
  writtenAt: Date;
}

function decodeJsonCursor(raw: string): Record<string, unknown> | null {
  try {
    const value = JSON.parse(Buffer.from(raw, 'base64url').toString('utf8')) as unknown;
    if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
    return value as Record<string, unknown>;
  } catch {
    return null;
  }
}

function compareStringAsc(a: string, b: string): number {
  return a < b ? -1 : a > b ? 1 : 0;
}

function compareMemoryPositionsAsc(a: MemoryCursorPosition, b: MemoryCursorPosition): number {
  return compareStringAsc(a.path, b.path) || compareStringAsc(a.id, b.id);
}

function compareMemoryVersionPositionsDesc(
  a: MemoryVersionCursorPosition,
  b: MemoryVersionCursorPosition,
): number {
  // Preserve the metadata store's insertion/database order when JavaScript
  // Date truncation collapses distinct writes into the same millisecond.
  return b.writtenAt.getTime() - a.writtenAt.getTime();
}

function claudeError(type: string, message: string, details?: Record<string, unknown>) {
  return { type: 'error' as const, error: { type, message, ...details } };
}

/**
 * Wires the `/v1/memory_stores/*` CRUD endpoints. Every route is workspace-
 * scoped via the auth middleware (`req.auth!.workspaceId`); the underlying
 * `MemoryStore` library enforces tenant isolation by including the workspace
 * predicate in every read + delete query.
 */
export function registerMemoryStoresRoutes(
  app: FastifyInstance,
  memoryStore: MemoryStoreLib,
): void {
  app.post('/v1/memory_stores', async (req, reply) => {
    const auth = req.auth!;
    const body = req.body as { name?: string; description?: string; metadata?: unknown };
    if (
      !body ||
      typeof body.name !== 'string' ||
      unicodeCodePointLength(body.name) < 1 ||
      unicodeCodePointLength(body.name) > 255 ||
      CONTROL_RE.test(body.name)
    ) {
      return reply.code(400).send({ error: 'name must be 1-255 characters without controls' });
    }
    if (
      body.description !== undefined &&
      (typeof body.description !== 'string' || body.description.length > 1024)
    ) {
      return reply.code(400).send({ error: 'description must be at most 1024 characters' });
    }
    const metadata = parseMetadata(body.metadata);
    if (!metadata.ok) return reply.code(400).send({ error: metadata.error });
    if (Object.keys(metadata.value).some((key) => key.length === 0)) {
      return reply.code(400).send({ error: 'metadata keys must be 1-64 characters' });
    }
    const created = await memoryStore.createStore({
      workspaceId: auth.workspaceId,
      name: body.name,
      ...(body.description !== undefined ? { description: body.description } : {}),
      metadata: toJsonMetadata(metadata.value),
    });
    const orcaBeta = isOrcaBetaRequest(req.headers);
    return reply.send(storeToApi(created, orcaBeta));
  });

  app.get('/v1/memory_stores', async (req, reply) => {
    const auth = req.auth!;
    const q = req.query as ListQuery;
    const parsedLimit = parsePositiveIntQueryParam(q.limit, 'limit', {
      defaultValue: 20,
      max: 100,
    });
    if (!parsedLimit.ok) return reply.code(400).send({ error: parsedLimit.error });
    const includeArchived = parseBoolean(q.include_archived, false);
    if (includeArchived === null) {
      return reply.code(400).send({ error: 'include_archived must be a boolean' });
    }
    const createdAtGte = parseDateFilter(q['created_at[gte]']);
    const createdAtLte = parseDateFilter(q['created_at[lte]']);
    if (createdAtGte === null || createdAtLte === null) {
      return reply.code(400).send({ error: 'created_at filters must be RFC 3339 timestamps' });
    }
    const offset = decodeOffsetCursor(q.page);
    if (offset === null) return reply.code(400).send({ error: 'invalid page' });
    const limit = parsedLimit.value!;
    const page = await memoryStore.listStores(auth.workspaceId, {
      limit,
      offset,
      includeArchived,
      ...(createdAtGte !== undefined ? { createdAtGte } : {}),
      ...(createdAtLte !== undefined ? { createdAtLte } : {}),
    });
    const orcaBeta = isOrcaBetaRequest(req.headers);
    return reply.send({
      data: page.items.map((item) => storeToApi(item, orcaBeta)),
      next_page: page.nextCursor ? encodeOffsetCursor(offset + limit) : null,
    });
  });

  app.get('/v1/memory_stores/:id', async (req, reply) => {
    const auth = req.auth!;
    const { id } = req.params as { id: string };
    const record = await memoryStore.getStore(auth.workspaceId, id);
    if (!record) return reply.code(404).send({ error: 'memory_store not found' });
    return reply.send(storeToApi(record, isOrcaBetaRequest(req.headers)));
  });

  app.post('/v1/memory_stores/:id', async (req, reply) => {
    const auth = req.auth!;
    const { id } = req.params as { id: string };
    const body = req.body;
    if (!body || typeof body !== 'object' || Array.isArray(body)) {
      return reply.code(400).send({ error: 'invalid memory_store update body' });
    }
    const rawBody = body as Record<string, unknown>;
    if (
      rawBody.name !== undefined &&
      rawBody.name !== null &&
      (typeof rawBody.name !== 'string' ||
        unicodeCodePointLength(rawBody.name) < 1 ||
        unicodeCodePointLength(rawBody.name) > 255 ||
        CONTROL_RE.test(rawBody.name))
    ) {
      return reply.code(400).send({ error: 'name must be 1-255 characters without controls' });
    }
    if (
      rawBody.description !== undefined &&
      rawBody.description !== null &&
      (typeof rawBody.description !== 'string' || rawBody.description.length > 1024)
    ) {
      return reply.code(400).send({ error: 'description must be at most 1024 characters' });
    }
    const metadataPatch =
      rawBody.metadata === null
        ? ({ ok: true, value: null } as const)
        : Object.prototype.hasOwnProperty.call(rawBody, 'metadata')
          ? parseMetadataPatch(rawBody.metadata)
          : undefined;
    if (metadataPatch && !metadataPatch.ok) {
      return reply.code(400).send({ error: metadataPatch.error });
    }
    const existing = await memoryStore.getStore(auth.workspaceId, id);
    if (!existing) return reply.code(404).send({ error: 'memory_store not found' });
    let metadata: Record<string, string> | undefined;
    if (metadataPatch !== undefined) {
      const patchedMetadata =
        metadataPatch.value === null
          ? {}
          : applyMetadataPatch(existing.metadata, metadataPatch.value);
      const metadataError = validateMetadataLimits(patchedMetadata);
      if (metadataError) return reply.code(400).send({ error: metadataError });
      if (Object.keys(patchedMetadata).some((key) => key.length === 0)) {
        return reply.code(400).send({ error: 'metadata keys must be 1-64 characters' });
      }
      metadata = toJsonMetadata(patchedMetadata);
    }
    const updated = await memoryStore.updateStore({
      workspaceId: auth.workspaceId,
      storeId: id,
      ...(typeof rawBody.name === 'string' ? { name: rawBody.name } : {}),
      ...(Object.prototype.hasOwnProperty.call(rawBody, 'description')
        ? { description: (rawBody.description as string | null) ?? null }
        : {}),
      ...(metadata !== undefined ? { metadata } : {}),
    });
    if (!updated) return reply.code(404).send({ error: 'memory_store not found' });
    return reply.send(storeToApi(updated, isOrcaBetaRequest(req.headers)));
  });

  app.post('/v1/memory_stores/:id/archive', async (req, reply) => {
    const auth = req.auth!;
    const { id } = req.params as { id: string };
    const existing = await memoryStore.getStore(auth.workspaceId, id);
    if (!existing) return reply.code(404).send({ error: 'memory_store not found' });
    await memoryStore.archiveStore(auth.workspaceId, id);
    const after = await memoryStore.getStore(auth.workspaceId, id);
    if (!after) {
      // Defensive: archive is a soft-update, so the row should still be there.
      return reply.code(404).send({ error: 'memory_store not found' });
    }
    return reply.send(storeToApi(after, isOrcaBetaRequest(req.headers)));
  });

  app.delete('/v1/memory_stores/:id', async (req, reply) => {
    const auth = req.auth!;
    const { id } = req.params as { id: string };
    const existing = await memoryStore.getStore(auth.workspaceId, id);
    if (!existing) return reply.code(404).send({ error: 'memory_store not found' });
    await memoryStore.deleteStore(auth.workspaceId, id);
    return reply.send({ id, type: 'memory_store_deleted' });
  });

  // ---- memories CRUD ----------------------------------------------------

  app.post('/v1/memory_stores/:id/memories', async (req, reply) => {
    const auth = req.auth!;
    const { id } = req.params as { id: string };
    const body = req.body as { path?: unknown; content?: unknown } | undefined;
    const parsedPath = parseClaudeMemoryPath(body?.path);
    if (!parsedPath.ok) return reply.code(400).send({ error: 'invalid memory path' });
    if (!body || !Object.prototype.hasOwnProperty.call(body, 'content')) {
      return reply.code(400).send({ error: 'content is required' });
    }
    if (body.content !== null && typeof body.content !== 'string') {
      return reply.code(400).send({ error: 'content must be a string or null' });
    }
    const view = parseView((req.query as { view?: unknown }).view, 'basic');
    if (!view) return reply.code(400).send({ error: 'view must be basic or full' });
    const store = await memoryStore.getStore(auth.workspaceId, id);
    if (!store) return reply.code(404).send({ error: 'memory_store not found' });
    if (store.archivedAt) {
      return reply.code(409).send(claudeError('conflict_error', 'memory_store is archived'));
    }

    const buf = Buffer.from(body.content ?? '', 'utf8');
    if (buf.length > MAX_MEMORY_BYTES) {
      return reply
        .code(413)
        .send({ error: `memory content exceeds ${MAX_MEMORY_BYTES} byte limit` });
    }
    try {
      const result = await memoryStore.writeMemory({
        workspaceId: auth.workspaceId,
        storeId: id,
        path: parsedPath.path,
        content: Readable.from(buf),
        sizeBytes: buf.length,
        sha256: sha256Hex(buf),
        createOnly: true,
        ...publicWriteAttribution(auth),
      });
      const response = await memoryToClaudeApi(memoryStore, auth.workspaceId, result.memory, view);
      return reply.send(response);
    } catch (error) {
      const conflict = error as { code?: string; conflictingMemoryId?: string };
      if (conflict.code === '23505') {
        const existing = await memoryStore.getMemoryByPath(auth.workspaceId, id, parsedPath.path);
        return reply.code(409).send(
          claudeError('memory_path_conflict_error', 'A memory already exists at this path', {
            conflicting_memory_id: conflict.conflictingMemoryId ?? existing?.id,
            conflicting_path: absoluteMemoryPath(parsedPath.path),
          }),
        );
      }
      throw error;
    }
  });

  app.get('/v1/memory_stores/:id/memories', async (req, reply) => {
    const auth = req.auth!;
    const { id } = req.params as { id: string };
    const q = req.query as {
      limit?: string;
      page?: string;
      depth?: string;
      path_prefix?: string;
      view?: string;
    };
    const view = parseView(q.view, 'basic');
    if (!view) return reply.code(400).send({ error: 'view must be basic or full' });
    const parsedLimit = parsePositiveIntQueryParam(q.limit, 'limit', {
      defaultValue: 20,
      max: view === 'full' ? 20 : 100,
    });
    if (!parsedLimit.ok) return reply.code(400).send({ error: parsedLimit.error });
    const depth = parseDepth(q.depth);
    if (depth === null) return reply.code(400).send({ error: 'depth must be 0 or 1' });
    const pathPrefix = parsePathPrefix(q.path_prefix);
    if (!pathPrefix.ok) return reply.code(400).send({ error: 'invalid path_prefix' });
    const offset = decodeOffsetCursor(q.page);
    if (offset === null) return reply.code(400).send({ error: 'invalid page' });

    const store = await memoryStore.getStore(auth.workspaceId, id);
    if (!store) return reply.code(404).send({ error: 'memory_store not found' });
    const memories = (await memoryStore.listMemories(auth.workspaceId, id))
      .filter((memory) => absoluteMemoryPath(memory.path).startsWith(pathPrefix.prefix))
      .sort(compareMemoryPositionsAsc);
    const items: Array<
      | { type: 'memory'; path: string; memory: MemoryRecord }
      | { type: 'memory_prefix'; path: string }
    > = [];
    const seenPrefixes = new Set<string>();
    for (const memory of memories) {
      const path = absoluteMemoryPath(memory.path);
      const remainder = path.slice(pathPrefix.prefix.length);
      const slash = remainder.indexOf('/');
      if (depth === 1 && slash >= 0) {
        const prefix = `${pathPrefix.prefix}${remainder.slice(0, slash + 1)}`;
        if (!seenPrefixes.has(prefix)) {
          seenPrefixes.add(prefix);
          items.push({ type: 'memory_prefix', path: prefix });
        }
      } else {
        items.push({ type: 'memory', path, memory });
      }
    }
    items.sort((left, right) => compareStringAsc(left.path, right.path));
    const limit = parsedLimit.value!;
    const page = items.slice(offset, offset + limit);
    const memoryIds = new Set(
      page.flatMap((item) => (item.type === 'memory' ? [item.memory.id] : [])),
    );
    const versionsByMemoryId = new Map<string, MemoryVersionRecord[]>();
    if (memoryIds.size > 0) {
      const versions = await memoryStore.listAllVersions(auth.workspaceId, id, {
        memoryIds: [...memoryIds],
      });
      for (const version of versions) {
        if (!memoryIds.has(version.memoryId)) continue;
        const history = versionsByMemoryId.get(version.memoryId) ?? [];
        history.push(version);
        versionsByMemoryId.set(version.memoryId, history);
      }
    }
    const signal = requestReadSignal(req, reply);
    const data = await mapReadItems(
      page,
      4,
      async (item) =>
        item.type === 'memory'
          ? memoryToClaudeApi(
              memoryStore,
              auth.workspaceId,
              item.memory,
              view,
              versionsByMemoryId.get(item.memory.id) ?? [],
              signal,
            )
          : Promise.resolve({ type: 'memory_prefix' as const, path: item.path }),
      signal,
    );
    return reply.send({
      data,
      next_page: offset + limit < items.length ? encodeOffsetCursor(offset + limit) : null,
    });
  });

  app.get('/v1/memory_stores/:id/memories/:memory_id', async (req, reply) => {
    const auth = req.auth!;
    const { id, memory_id } = req.params as { id: string; memory_id: string };
    const view = parseView((req.query as { view?: unknown }).view, 'full');
    if (!view) return reply.code(400).send({ error: 'view must be basic or full' });
    const store = await memoryStore.getStore(auth.workspaceId, id);
    if (!store) return reply.code(404).send({ error: 'memory_store not found' });
    const memory = await memoryStore.getMemory(auth.workspaceId, id, memory_id);
    if (!memory) return reply.code(404).send({ error: 'memory not found' });
    return reply.send(await memoryToClaudeApi(memoryStore, auth.workspaceId, memory, view));
  });

  const updateMemory = async (req: FastifyRequest, reply: FastifyReply) => {
    const auth = req.auth!;
    const { id, memory_id } = req.params as { id: string; memory_id: string };
    const rawBody = req.body ?? {};
    if (typeof rawBody !== 'object' || Array.isArray(rawBody)) {
      return reply.code(400).send({ error: 'invalid memory update body' });
    }
    const body = rawBody as Record<string, unknown>;
    const view = parseView((req.query as { view?: unknown }).view, 'basic');
    if (!view) return reply.code(400).send({ error: 'view must be basic or full' });
    if (
      Object.prototype.hasOwnProperty.call(body, 'content') &&
      body.content !== null &&
      typeof body.content !== 'string'
    ) {
      return reply.code(400).send({ error: 'content must be a string or null' });
    }
    let newPath: string | undefined;
    if (body.path !== undefined && body.path !== null) {
      const parsedPath = parseClaudeMemoryPath(body.path);
      if (!parsedPath.ok) return reply.code(400).send({ error: 'invalid memory path' });
      newPath = parsedPath.path;
    }
    let expectedSha256: string | undefined;
    if (body.precondition !== undefined) {
      if (
        !body.precondition ||
        typeof body.precondition !== 'object' ||
        Array.isArray(body.precondition) ||
        (body.precondition as Record<string, unknown>).type !== 'content_sha256'
      ) {
        return reply.code(400).send({ error: 'invalid memory precondition' });
      }
      const candidate = (body.precondition as Record<string, unknown>).content_sha256;
      if (candidate !== undefined) {
        if (typeof candidate !== 'string' || !/^[0-9a-f]{64}$/.test(candidate)) {
          return reply.code(400).send({ error: 'invalid precondition content_sha256' });
        }
        expectedSha256 = candidate;
      }
    }

    const store = await memoryStore.getStore(auth.workspaceId, id);
    if (!store) return reply.code(404).send({ error: 'memory_store not found' });
    if (store.archivedAt) {
      return reply.code(409).send(claudeError('conflict_error', 'memory_store is archived'));
    }
    const existing = await memoryStore.getMemory(auth.workspaceId, id, memory_id);
    if (!existing) return reply.code(404).send({ error: 'memory not found' });
    const targetPath = newPath ?? existing.path;
    let contentBuffer: Buffer | undefined;
    let targetSha256 = existing.currentSha256;
    if (Object.prototype.hasOwnProperty.call(body, 'content')) {
      contentBuffer = Buffer.from((body.content as string | null) ?? '', 'utf8');
      if (contentBuffer.length > MAX_MEMORY_BYTES) {
        return reply
          .code(413)
          .send({ error: `memory content exceeds ${MAX_MEMORY_BYTES} byte limit` });
      }
      targetSha256 = sha256Hex(contentBuffer);
    }

    // Claude treats an already-achieved target as success even with a stale
    // precondition, and an empty/no-op update must not append a Version.
    if (targetPath === existing.path && targetSha256 === existing.currentSha256) {
      return reply.send(await memoryToClaudeApi(memoryStore, auth.workspaceId, existing, view));
    }
    if (!contentBuffer) {
      const opened = await memoryStore.openMemory(auth.workspaceId, id, memory_id);
      if (!opened) return reply.code(404).send({ error: 'memory not found' });
      const content = await streamToUtf8(opened.stream);
      contentBuffer = Buffer.from(content, 'utf8');
    }

    try {
      const result = await memoryStore.writeMemory({
        workspaceId: auth.workspaceId,
        storeId: id,
        memoryId: memory_id,
        path: targetPath,
        content: Readable.from(contentBuffer),
        sizeBytes: contentBuffer.length,
        sha256: targetSha256,
        ...(expectedSha256 !== undefined ? { previousSha256: expectedSha256 } : {}),
        ...publicWriteAttribution(auth),
      });
      return reply.send(
        await memoryToClaudeApi(memoryStore, auth.workspaceId, result.memory, view),
      );
    } catch (error) {
      if (error instanceof MemoryConflictError) {
        return reply
          .code(409)
          .send(
            claudeError(
              'memory_precondition_failed_error',
              'The memory content no longer matches the supplied precondition',
            ),
          );
      }
      const conflict = error as { code?: string; conflictingMemoryId?: string };
      if (conflict.code === '23505') {
        const owner = await memoryStore.getMemoryByPath(auth.workspaceId, id, targetPath);
        return reply.code(409).send(
          claudeError('memory_path_conflict_error', 'A memory already exists at this path', {
            conflicting_memory_id: conflict.conflictingMemoryId ?? owner?.id,
            conflicting_path: absoluteMemoryPath(targetPath),
          }),
        );
      }
      throw error;
    }
  };

  app.post('/v1/memory_stores/:id/memories/:memory_id', updateMemory);

  app.delete('/v1/memory_stores/:id/memories/:memory_id', async (req, reply) => {
    const auth = req.auth!;
    const { id, memory_id } = req.params as { id: string; memory_id: string };
    const { expected_content_sha256: expected } = req.query as {
      expected_content_sha256?: unknown;
    };
    if (
      expected !== undefined &&
      (typeof expected !== 'string' || !/^[0-9a-f]{64}$/.test(expected))
    ) {
      return reply.code(400).send({ error: 'invalid expected_content_sha256' });
    }
    const store = await memoryStore.getStore(auth.workspaceId, id);
    if (!store) return reply.code(404).send({ error: 'memory_store not found' });
    const memory = await memoryStore.getMemory(auth.workspaceId, id, memory_id);
    if (!memory) return reply.code(404).send({ error: 'memory not found' });
    if (expected !== undefined && expected !== memory.currentSha256) {
      return reply
        .code(409)
        .send(
          claudeError(
            'memory_precondition_failed_error',
            'The memory content no longer matches expected_content_sha256',
          ),
        );
    }
    const deleted = await memoryStore.deleteMemory(
      auth.workspaceId,
      id,
      memory_id,
      publicActor(auth),
    );
    if (!deleted) return reply.code(404).send({ error: 'memory not found' });
    return reply.send({ id: memory_id, type: 'memory_deleted' });
  });

  // ---- memory_versions: retrieve, list, redact --------------------------

  app.get('/v1/memory_stores/:id/memory_versions', async (req, reply) => {
    const auth = req.auth!;
    const { id } = req.params as { id: string };
    const q = req.query as {
      memory_id?: string;
      api_key_id?: string;
      session_id?: string;
      operation?: string;
      'created_at[gte]'?: string;
      'created_at[lte]'?: string;
      view?: string;
      limit?: string;
      page?: string;
    };
    const view = parseView(q.view, 'basic');
    if (!view) return reply.code(400).send({ error: 'view must be basic or full' });
    const parsedLimit = parsePositiveIntQueryParam(q.limit, 'limit', {
      defaultValue: 20,
      max: view === 'full' ? 20 : 100,
    });
    if (!parsedLimit.ok) return reply.code(400).send({ error: parsedLimit.error });
    if (q.operation && !['created', 'modified', 'deleted'].includes(q.operation)) {
      return reply.code(400).send({ error: 'invalid operation' });
    }
    const createdAtGte = parseDateFilter(q['created_at[gte]']);
    const createdAtLte = parseDateFilter(q['created_at[lte]']);
    if (createdAtGte === null || createdAtLte === null) {
      return reply.code(400).send({ error: 'created_at filters must be RFC 3339 timestamps' });
    }
    const offset = decodeOffsetCursor(q.page);
    if (offset === null) return reply.code(400).send({ error: 'invalid page' });
    const store = await memoryStore.getStore(auth.workspaceId, id);
    if (!store) return reply.code(404).send({ error: 'memory_store not found' });

    const idsFilter = q.memory_id ? { memoryIds: [q.memory_id] } : undefined;
    const allVersions = (await memoryStore.listAllVersions(auth.workspaceId, id, idsFilter)).sort(
      compareMemoryVersionPositionsDesc,
    );
    const histories = new Map<string, MemoryVersionRecord[]>();
    for (const version of allVersions) {
      const history = histories.get(version.memoryId) ?? [];
      history.push(version);
      histories.set(version.memoryId, history);
    }
    const activeIds = new Set(
      (await memoryStore.listMemories(auth.workspaceId, id, idsFilter)).map((memory) => memory.id),
    );
    const filtered = allVersions.filter((version) => {
      const history = histories.get(version.memoryId)!;
      const operation = operationForVersion(version, history, activeIds.has(version.memoryId));
      return (
        (!q.memory_id || version.memoryId === q.memory_id) &&
        (q.api_key_id === undefined || version.writtenByApiKeyId === q.api_key_id) &&
        (q.session_id === undefined || version.writtenBySessionId === q.session_id) &&
        (!q.operation || operation === q.operation) &&
        (createdAtGte === undefined || version.writtenAt >= createdAtGte) &&
        (createdAtLte === undefined || version.writtenAt <= createdAtLte)
      );
    });
    const limit = parsedLimit.value!;
    const page = filtered.slice(offset, offset + limit);
    const signal = requestReadSignal(req, reply);
    const data = await mapReadItems(
      page,
      4,
      (version) =>
        versionToClaudeApi(
          memoryStore,
          auth.workspaceId,
          version,
          view,
          histories.get(version.memoryId)!,
          activeIds.has(version.memoryId),
          signal,
        ),
      signal,
    );
    return reply.send({
      data,
      next_page: offset + limit < filtered.length ? encodeOffsetCursor(offset + limit) : null,
    });
  });

  app.get('/v1/memory_stores/:id/memory_versions/:version_id', async (req, reply) => {
    const auth = req.auth!;
    const { id, version_id } = req.params as { id: string; version_id: string };
    const view = parseView((req.query as { view?: unknown }).view, 'full');
    if (!view) return reply.code(400).send({ error: 'view must be basic or full' });
    const version = await memoryStore.getVersion(auth.workspaceId, id, version_id);
    if (!version) return reply.code(404).send({ error: 'memory_version not found' });
    const history = await memoryStore.listVersions(auth.workspaceId, id, version.memoryId);
    const active = (await memoryStore.getMemory(auth.workspaceId, id, version.memoryId)) !== null;
    return reply.send(
      await versionToClaudeApi(memoryStore, auth.workspaceId, version, view, history, active),
    );
  });

  app.post('/v1/memory_stores/:id/memory_versions/:version_id/redact', async (req, reply) => {
    const auth = req.auth!;
    const { id, version_id } = req.params as { id: string; version_id: string };
    const store = await memoryStore.getStore(auth.workspaceId, id);
    if (!store) return reply.code(404).send({ error: 'memory_store not found' });
    try {
      const updated = await memoryStore.redactVersion(
        auth.workspaceId,
        id,
        version_id,
        publicActor(auth),
      );
      const history = await memoryStore.listVersions(auth.workspaceId, id, updated.memoryId);
      const active = (await memoryStore.getMemory(auth.workspaceId, id, updated.memoryId)) !== null;
      return reply.send(
        await versionToClaudeApi(memoryStore, auth.workspaceId, updated, 'basic', history, active),
      );
    } catch (error) {
      const message = (error as Error).message;
      if (message.includes('not found')) {
        return reply.code(404).send({ error: 'memory_version not found' });
      }
      throw error;
    }
  });
}
