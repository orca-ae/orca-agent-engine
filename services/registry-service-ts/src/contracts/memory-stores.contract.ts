// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { initContract } from '@ts-rest/core';
import { z } from 'zod';
import { ClaudeErrorResponse, idString, isoTimestamp, pagination } from './common.js';

const c = initContract();

const Metadata = z
  .record(z.string().min(1).max(64), z.string().max(512))
  .refine((value) => Object.keys(value).length <= 16, 'metadata must contain at most 16 pairs');
const MetadataPatch = z.record(z.string().min(1).max(64), z.string().max(512).nullable());
const MemoryView = z.enum(['basic', 'full']);
const Sha256 = z.string().regex(/^[0-9a-f]{64}$/);
const MemoryConflictErrorResponse = z.object({
  type: z.literal('error'),
  error: z.discriminatedUnion('type', [
    z.object({ type: z.literal('conflict_error'), message: z.string() }),
    z.object({ type: z.literal('memory_precondition_failed_error'), message: z.string() }),
    z.object({
      type: z.literal('memory_path_conflict_error'),
      message: z.string(),
      conflicting_memory_id: z.string().optional(),
      conflicting_path: z.string().optional(),
    }),
  ]),
  request_id: z.string().nullable(),
});

const MemoryStore = z.object({
  id: idString('mems'),
  created_at: isoTimestamp,
  name: z.string(),
  type: z.literal('memory_store'),
  updated_at: isoTimestamp,
  archived_at: isoTimestamp.nullable().optional(),
  description: z.string().optional(),
  metadata: Metadata.optional(),
});

export const MemoryStoreUpdate = z.object({
  name: z.string().min(1).max(255).nullable().optional(),
  description: z.string().max(1024).nullable().optional(),
  metadata: MetadataPatch.nullable().optional(),
});

const MemoryStoreDeleted = z.object({
  id: idString('mems'),
  type: z.literal('memory_store_deleted'),
});

const Memory = z.object({
  id: idString('mem'),
  content_sha256: Sha256,
  content_size_bytes: z.number().int().nonnegative(),
  created_at: isoTimestamp,
  memory_store_id: idString('mems'),
  memory_version_id: idString('memver'),
  path: z.string(),
  type: z.literal('memory'),
  updated_at: isoTimestamp,
  content: z.string().nullable().optional(),
});

const MemoryPrefix = z.object({
  path: z.string(),
  type: z.literal('memory_prefix'),
});

const MemoryPrecondition = z.object({
  type: z.literal('content_sha256'),
  content_sha256: Sha256.optional(),
});

const CreateMemoryBody = z.object({
  path: z.string().min(1),
  content: z.string().nullable(),
});

const UpdateMemoryBody = z.object({
  content: z.string().nullable().optional(),
  path: z.string().nullable().optional(),
  precondition: MemoryPrecondition.optional(),
});

const Actor = z.union([
  z.object({ type: z.literal('session_actor'), session_id: z.string() }),
  z.object({ type: z.literal('api_actor'), api_key_id: z.string() }),
  z.object({ type: z.literal('user_actor'), user_id: z.string() }),
]);

const MemoryVersionOperation = z.enum(['created', 'modified', 'deleted']);
const MemoryVersion = z.object({
  id: idString('memver'),
  created_at: isoTimestamp,
  memory_id: idString('mem'),
  memory_store_id: idString('mems'),
  operation: MemoryVersionOperation,
  type: z.literal('memory_version'),
  content: z.string().nullable().optional(),
  content_sha256: Sha256.nullable().optional(),
  content_size_bytes: z.number().int().nonnegative().nullable().optional(),
  created_by: Actor.optional(),
  path: z.string().nullable().optional(),
  redacted_at: isoTimestamp.nullable().optional(),
  redacted_by: Actor.optional(),
});

const MemoryPathParams = z.object({ id: idString('mems'), memory_id: idString('mem') });
const MemoryVersionPathParams = z.object({
  id: idString('mems'),
  version_id: idString('memver'),
});

export const memoryStoresContract = c.router({
  create: {
    method: 'POST',
    path: '/v1/memory_stores',
    body: z.object({
      name: z.string().min(1).max(255),
      description: z.string().max(1024).optional(),
      metadata: Metadata.optional(),
    }),
    responses: {
      200: MemoryStore,
      400: ClaudeErrorResponse,
      409: MemoryConflictErrorResponse,
    },
  },
  list: {
    method: 'GET',
    path: '/v1/memory_stores',
    query: pagination.extend({
      include_archived: z.coerce.boolean().optional(),
      'created_at[gte]': isoTimestamp.optional(),
      'created_at[lte]': isoTimestamp.optional(),
    }),
    responses: {
      200: z.object({ data: z.array(MemoryStore), next_page: z.string().nullable() }),
      400: ClaudeErrorResponse,
    },
  },
  get: {
    method: 'GET',
    path: '/v1/memory_stores/:id',
    pathParams: z.object({ id: idString('mems') }),
    responses: { 200: MemoryStore, 404: ClaudeErrorResponse },
  },
  update: {
    method: 'POST',
    path: '/v1/memory_stores/:id',
    pathParams: z.object({ id: idString('mems') }),
    body: MemoryStoreUpdate,
    responses: {
      200: MemoryStore,
      400: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
      409: MemoryConflictErrorResponse,
    },
  },
  archive: {
    method: 'POST',
    path: '/v1/memory_stores/:id/archive',
    pathParams: z.object({ id: idString('mems') }),
    body: z.object({}).strict(),
    responses: { 200: MemoryStore, 404: ClaudeErrorResponse },
  },
  delete: {
    method: 'DELETE',
    path: '/v1/memory_stores/:id',
    pathParams: z.object({ id: idString('mems') }),
    body: z.object({}).strict(),
    responses: { 200: MemoryStoreDeleted, 404: ClaudeErrorResponse },
  },
  createMemory: {
    method: 'POST',
    path: '/v1/memory_stores/:id/memories',
    pathParams: z.object({ id: idString('mems') }),
    query: z.object({ view: MemoryView.optional() }),
    body: CreateMemoryBody,
    responses: {
      200: Memory,
      400: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
      409: MemoryConflictErrorResponse,
      413: ClaudeErrorResponse,
    },
  },
  listMemories: {
    method: 'GET',
    path: '/v1/memory_stores/:id/memories',
    pathParams: z.object({ id: idString('mems') }),
    query: pagination.extend({
      depth: z.coerce.number().int().min(0).max(1).optional(),
      path_prefix: z.string().optional(),
      view: MemoryView.optional(),
    }),
    responses: {
      200: z.object({
        data: z.array(z.union([Memory, MemoryPrefix])),
        next_page: z.string().nullable(),
      }),
      400: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
    },
  },
  getMemory: {
    method: 'GET',
    path: '/v1/memory_stores/:id/memories/:memory_id',
    pathParams: MemoryPathParams,
    query: z.object({ view: MemoryView.optional() }),
    responses: { 200: Memory, 400: ClaudeErrorResponse, 404: ClaudeErrorResponse },
  },
  updateMemory: {
    method: 'POST',
    path: '/v1/memory_stores/:id/memories/:memory_id',
    pathParams: MemoryPathParams,
    query: z.object({ view: MemoryView.optional() }),
    body: UpdateMemoryBody,
    responses: {
      200: Memory,
      400: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
      409: MemoryConflictErrorResponse,
      413: ClaudeErrorResponse,
    },
  },
  deleteMemory: {
    method: 'DELETE',
    path: '/v1/memory_stores/:id/memories/:memory_id',
    pathParams: MemoryPathParams,
    query: z.object({ expected_content_sha256: Sha256.optional() }),
    body: z.object({}).strict(),
    responses: {
      200: z.object({ id: idString('mem'), type: z.literal('memory_deleted') }),
      400: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
      409: MemoryConflictErrorResponse,
    },
  },
  listVersions: {
    method: 'GET',
    path: '/v1/memory_stores/:id/memory_versions',
    pathParams: z.object({ id: idString('mems') }),
    query: pagination.extend({
      memory_id: idString('mem').optional(),
      api_key_id: z.string().optional(),
      session_id: z.string().optional(),
      operation: MemoryVersionOperation.optional(),
      'created_at[gte]': isoTimestamp.optional(),
      'created_at[lte]': isoTimestamp.optional(),
      view: MemoryView.optional(),
    }),
    responses: {
      200: z.object({ data: z.array(MemoryVersion), next_page: z.string().nullable() }),
      400: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
    },
  },
  getVersion: {
    method: 'GET',
    path: '/v1/memory_stores/:id/memory_versions/:version_id',
    pathParams: MemoryVersionPathParams,
    query: z.object({ view: MemoryView.optional() }),
    responses: {
      200: MemoryVersion,
      400: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
    },
  },
  redactVersion: {
    method: 'POST',
    path: '/v1/memory_stores/:id/memory_versions/:version_id/redact',
    pathParams: MemoryVersionPathParams,
    body: z.object({}).strict(),
    responses: {
      200: MemoryVersion,
      404: ClaudeErrorResponse,
      409: MemoryConflictErrorResponse,
    },
  },
});
