// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { initContract } from '@ts-rest/core';
import { z } from 'zod';
import { ClaudeErrorResponse, idString, isoTimestamp } from './common.js';
import { binaryDownload, binaryUpload, openApiMedia } from './openapi-media.js';

const c = initContract();

export const ClaudeFile = z.object({
  id: idString('file'),
  created_at: isoTimestamp,
  filename: z.string(),
  mime_type: z.string(),
  size_bytes: z.number().int().nonnegative(),
  type: z.literal('file'),
  downloadable: z.boolean().optional(),
  scope: z
    .object({ type: z.literal('session'), id: idString('ses') })
    .nullable()
    .optional(),
});

export const LegacyFile = z.object({
  id: idString('file'),
  filename: z.string(),
  mime_type: z.string(),
  size_bytes: z.number().int().nonnegative(),
  sha256: z.string().regex(/^[0-9a-f]{64}$/),
  metadata: z.record(z.string(), z.string()).default({}),
  // Anthropic-aligned fields. `purpose` distinguishes user uploads from agent
  // outputs; `scope_id` binds an output to its originating session (sessions
  // are the only valid scope); `downloadable` gates `getContent` so user
  // uploads cannot be retrieved verbatim per Anthropic's Files API contract.
  purpose: z.enum(['agent', 'agent_output']),
  scope_id: idString('ses').nullable(),
  downloadable: z.boolean(),
  archived_at: isoTimestamp.nullable(),
  created_at: isoTimestamp,
  updated_at: isoTimestamp,
});

export const FileResponse = z.union([ClaudeFile, LegacyFile]);

export const FilesListQuery = z.object({
  limit: z.coerce.number().int().min(1).max(1000).optional(),
  after_id: idString('file').optional(),
  before_id: idString('file').optional(),
  scope_id: idString('ses').optional(),
});

export const FilesListResponse = z.object({
  data: z.array(FileResponse),
  first_id: idString('file').nullable(),
  last_id: idString('file').nullable(),
  has_more: z.boolean(),
});

export const FileDeleted = z.object({ id: idString('file'), type: z.literal('file_deleted') });

export const filesContract = c.router({
  // create is multipart — modeled here as `unknown` body since ts-rest doesn't natively model multipart.
  // The handler reads the request stream directly via @fastify/multipart.
  // The official multipart body contains only one `file` part. Legacy public
  // purpose/scope selectors are ignored by the route; scoped outputs are
  // registered only on the internal surface.
  create: {
    method: 'POST',
    path: '/v1/files',
    body: z.unknown(),
    responses: {
      200: FileResponse,
      400: ClaudeErrorResponse,
      413: ClaudeErrorResponse,
    },
    headers: z.object({ 'idempotency-key': z.string().optional() }).passthrough(),
    contentType: 'multipart/form-data',
    metadata: openApiMedia({
      requestBody: binaryUpload('file', 'The file bytes to upload.'),
    }),
  },
  get: {
    method: 'GET',
    path: '/v1/files/:id',
    pathParams: z.object({ id: idString('file') }),
    responses: { 200: FileResponse, 404: ClaudeErrorResponse },
  },
  list: {
    method: 'GET',
    path: '/v1/files',
    query: FilesListQuery,
    responses: { 200: FilesListResponse, 400: ClaudeErrorResponse },
  },
  getContent: {
    method: 'GET',
    path: '/v1/files/:id/content',
    pathParams: z.object({ id: idString('file') }),
    responses: {
      200: z.unknown(),
      403: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
    },
    metadata: openApiMedia({
      responses: {
        200: binaryDownload('application/octet-stream', 'The stored file bytes.'),
      },
    }),
  },
  delete: {
    method: 'DELETE',
    path: '/v1/files/:id',
    pathParams: z.object({ id: idString('file') }),
    body: z.object({}).strict(),
    responses: {
      200: FileDeleted,
      404: ClaudeErrorResponse,
    },
  },
});
