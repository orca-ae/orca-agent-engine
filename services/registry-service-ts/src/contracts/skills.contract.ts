// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { initContract } from '@ts-rest/core';
import { z } from 'zod';
import { ClaudeErrorResponse, idString, isoTimestamp, pagination } from './common.js';
import { binaryDownload, binaryUploadArray, openApiMedia } from './openapi-media.js';

const c = initContract();

const SkillType = z.enum(['anthropic', 'custom']);

const Skill = z.object({
  id: idString('skill'),
  created_at: isoTimestamp,
  display_title: z.string().nullable(),
  latest_version: z.string().regex(/^\d+$/).nullable(),
  source: SkillType,
  type: z.literal('skill'),
  updated_at: isoTimestamp,
});

const SkillDeleted = z.object({ id: idString('skill'), type: z.literal('skill_deleted') });
const SkillVersion = z.object({
  id: idString('skillver'),
  created_at: isoTimestamp,
  description: z.string(),
  directory: z.string(),
  name: z.string(),
  skill_id: idString('skill'),
  type: z.literal('skill_version'),
  version: z.string().regex(/^\d+$/),
});
const SkillVersionDeleted = z.object({
  id: z.string().regex(/^\d+$/),
  type: z.literal('skill_version_deleted'),
});

export const skillsContract = c.router({
  create: {
    method: 'POST',
    path: '/v1/skills',
    body: z.unknown(),
    responses: {
      200: Skill,
      400: ClaudeErrorResponse,
      409: ClaudeErrorResponse,
      413: ClaudeErrorResponse,
    },
    headers: z.object({ 'idempotency-key': z.string().optional() }).passthrough(),
    contentType: 'multipart/form-data',
    metadata: openApiMedia({
      requestBody: binaryUploadArray('files', 'The Skill bundle files.', {
        display_title: { type: 'string', description: 'Optional human-readable title.' },
      }),
    }),
  },
  get: {
    method: 'GET',
    path: '/v1/skills/:id',
    pathParams: z.object({ id: idString('skill') }),
    responses: { 200: Skill, 404: ClaudeErrorResponse },
  },
  list: {
    method: 'GET',
    path: '/v1/skills',
    query: pagination.extend({ source: SkillType.optional() }),
    responses: {
      200: z.object({
        data: z.array(Skill),
        has_more: z.boolean(),
        next_page: z.string().nullable(),
      }),
      400: ClaudeErrorResponse,
    },
  },
  createVersion: {
    method: 'POST',
    path: '/v1/skills/:id/versions',
    pathParams: z.object({ id: idString('skill') }),
    body: z.unknown(),
    responses: {
      200: SkillVersion,
      400: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
      413: ClaudeErrorResponse,
    },
    headers: z.object({ 'idempotency-key': z.string().optional() }).passthrough(),
    contentType: 'multipart/form-data',
    metadata: openApiMedia({
      requestBody: binaryUploadArray('files', 'The Skill bundle files for the new version.'),
    }),
  },
  listVersions: {
    method: 'GET',
    path: '/v1/skills/:id/versions',
    pathParams: z.object({ id: idString('skill') }),
    query: z
      .object({
        limit: z.coerce.number().int().positive().max(1000).optional(),
        page: z.string().optional(),
      })
      .passthrough(),
    responses: {
      200: z.object({
        data: z.array(SkillVersion),
        has_more: z.boolean(),
        next_page: z.string().nullable(),
      }),
      400: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
    },
  },
  getVersion: {
    method: 'GET',
    path: '/v1/skills/:id/versions/:version',
    pathParams: z.object({ id: idString('skill'), version: z.string() }),
    responses: { 200: SkillVersion, 404: ClaudeErrorResponse },
  },
  getVersionContent: {
    method: 'GET',
    path: '/v1/skills/:id/versions/:version/content',
    pathParams: z.object({ id: idString('skill'), version: z.string() }),
    responses: {
      200: z.unknown(),
      404: ClaudeErrorResponse,
      500: ClaudeErrorResponse,
    },
    metadata: openApiMedia({
      responses: {
        200: binaryDownload('application/zip', 'The immutable Skill bundle, as a zip archive.'),
      },
    }),
  },
  deleteVersion: {
    method: 'DELETE',
    path: '/v1/skills/:id/versions/:version',
    pathParams: z.object({ id: idString('skill'), version: z.string() }),
    body: z.object({}).strict(),
    responses: {
      200: SkillVersionDeleted,
      404: ClaudeErrorResponse,
    },
  },
  delete: {
    method: 'DELETE',
    path: '/v1/skills/:id',
    pathParams: z.object({ id: idString('skill') }),
    body: z.object({}).strict(),
    responses: {
      200: SkillDeleted,
      400: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
      409: ClaudeErrorResponse,
    },
  },
});
