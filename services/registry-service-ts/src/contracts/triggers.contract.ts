// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { initContract, type AppRouter } from '@ts-rest/core';
import { z } from 'zod';
import { ClaudeErrorResponse, idString, isoTimestamp, pagination } from './common.js';
import { Metadata, MetadataOutput, MetadataPatch } from './metadata.js';
import { SessionResponseSchema } from './sessions.contract.js';

const c = initContract();
const TriggerReplicas = z.number().int().min(1).max(1);

const TriggerAgentInput = z.union([
  idString('agt'),
  z
    .object({
      type: z.literal('agent'),
      id: idString('agt'),
      version: z.number().int().positive().optional(),
    })
    .strict(),
]);

const TriggerAgent = z.object({
  type: z.literal('agent'),
  id: idString('agt'),
  version: z.number().int().positive(),
});

export const triggerSourceSchema = z
  .object({
    type: z.literal('cron'),
    schedule: z.string().min(1).max(256),
    timezone: z.string().min(1).max(128).optional(),
    payload: z.string().min(1).max(262_144),
  })
  .strict();

const triggerSourceUpdateSchema = z
  .object({
    type: z.literal('cron'),
    schedule: z.string().min(1).max(256).optional(),
    timezone: z.string().min(1).max(128).optional(),
    payload: z.string().min(1).max(262_144).optional(),
  })
  .strict();

export const triggerSessionSchema = z
  .object({
    environment_id: idString('env'),
    title_template: z.string().max(1024).nullable().optional(),
    metadata: Metadata.optional(),
    vault_ids: z.array(idString('vlt')).max(100).optional(),
  })
  .strict();

const triggerSessionUpdateSchema = z
  .object({
    environment_id: idString('env').optional(),
    title_template: z.string().max(1024).nullable().optional(),
    metadata: MetadataPatch.optional(),
    vault_ids: z.array(idString('vlt')).max(100).optional(),
  })
  .strict();

export const TriggerResponseSchema = z.object({
  id: idString('trg'),
  type: z.literal('trigger'),
  name: z.string().min(1).max(256),
  agent: TriggerAgent,
  session_mode: z.literal('SESSION_PER_EVENT'),
  source: z.object({
    type: z.literal('cron'),
    schedule: z.string(),
    timezone: z.string(),
    payload: z.string().min(1).max(262_144),
  }),
  session: z.object({
    environment_id: idString('env'),
    title_template: z.string().max(1024).nullable(),
    metadata: MetadataOutput,
    vault_ids: z.array(idString('vlt')),
  }),
  replicas: TriggerReplicas,
  status: z.enum(['active', 'paused', 'archived']),
  next_fire_at: isoTimestamp.nullable(),
  last_fired_at: isoTimestamp.nullable(),
  error: z.string().nullable(),
  archived_at: isoTimestamp.nullable(),
  created_at: isoTimestamp,
  updated_at: isoTimestamp,
});

export const triggerCreateBodySchema = z
  .object({
    name: z.string().min(1).max(256),
    agent: TriggerAgentInput,
    session_mode: z.literal('SESSION_PER_EVENT'),
    source: triggerSourceSchema,
    session: triggerSessionSchema,
    replicas: TriggerReplicas.default(1),
    paused: z.boolean().optional(),
  })
  .strict();

export const triggerUpdateBodySchema = z
  .object({
    name: z.string().min(1).max(256).optional(),
    session_mode: z.literal('SESSION_PER_EVENT').optional(),
    source: triggerSourceUpdateSchema.optional(),
    session: triggerSessionUpdateSchema.optional(),
    replicas: TriggerReplicas.optional(),
  })
  .strict();

const writeHeaders = z.object({ 'idempotency-key': z.string().optional() }).passthrough();
const triggerPath = z.object({ id: idString('trg') });

export const triggersContract: AppRouter = c.router({
  create: {
    method: 'POST',
    path: '/v1/triggers',
    body: triggerCreateBodySchema,
    responses: {
      200: TriggerResponseSchema,
      400: ClaudeErrorResponse,
      403: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
    },
    headers: writeHeaders,
  },
  list: {
    method: 'GET',
    path: '/v1/triggers',
    query: pagination.extend({
      agent_id: idString('agt').optional(),
      include_archived: z.coerce.boolean().optional(),
    }),
    responses: {
      200: z.object({ data: z.array(TriggerResponseSchema), next_page: z.string().nullable() }),
      400: ClaudeErrorResponse,
      403: ClaudeErrorResponse,
    },
  },
  get: {
    method: 'GET',
    path: '/v1/triggers/:id',
    pathParams: triggerPath,
    responses: {
      200: TriggerResponseSchema,
      403: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
    },
  },
  update: {
    method: 'POST',
    path: '/v1/triggers/:id',
    pathParams: triggerPath,
    body: triggerUpdateBodySchema,
    responses: {
      200: TriggerResponseSchema,
      400: ClaudeErrorResponse,
      403: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
    },
    headers: writeHeaders,
  },
  delete: {
    method: 'DELETE',
    path: '/v1/triggers/:id',
    pathParams: triggerPath,
    body: z.object({}).strict(),
    responses: {
      200: z.object({ id: idString('trg'), type: z.literal('trigger_deleted') }),
      403: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
    },
    headers: writeHeaders,
  },
  pause: {
    method: 'POST',
    path: '/v1/triggers/:id/pause',
    pathParams: triggerPath,
    body: z.object({}).strict(),
    responses: {
      200: TriggerResponseSchema,
      403: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
    },
    headers: writeHeaders,
  },
  unpause: {
    method: 'POST',
    path: '/v1/triggers/:id/unpause',
    pathParams: triggerPath,
    body: z.object({}).strict(),
    responses: {
      200: TriggerResponseSchema,
      400: ClaudeErrorResponse,
      403: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
    },
    headers: writeHeaders,
  },
  sessions: {
    method: 'GET',
    path: '/v1/triggers/:id/sessions',
    pathParams: triggerPath,
    query: pagination.extend({ include_archived: z.coerce.boolean().optional() }),
    responses: {
      200: z.object({ data: z.array(SessionResponseSchema), next_page: z.string().nullable() }),
      400: ClaudeErrorResponse,
      403: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
    },
  },
});
