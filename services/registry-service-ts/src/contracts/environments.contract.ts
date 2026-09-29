// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { initContract } from '@ts-rest/core';
import { z } from 'zod';
import { ClaudeErrorResponse, idString, isoTimestamp, pagination } from './common.js';
import { ApiEnvConfigSchema, EnvConfigSchema } from './environment-wire.js';
import { Metadata, MetadataOutput, MetadataPatch } from './metadata.js';

const c = initContract();
const EnvironmentScope = z.enum(['organization', 'account']);

// The published environment response is Anthropic's `BetaEnvironment` and
// nothing else. Orca's own projection fields — the legacy flat
// `packages`/`networking`/`image`/`target` and the colocated
// `egress_mode`/`llm`/`env_key_set`/`env_key_expires_at` — are served only to
// `orca-beta` callers, so they are deliberately absent here: this contract is
// what `openapi:gen` publishes and what the conformance differ compares
// against Anthropic's spec, and it describes the DEFAULT wire.
const Environment = z.object({
  id: idString('env'),
  type: z.literal('environment'),
  name: z.string().min(1).max(256),
  description: z.string(),
  metadata: MetadataOutput,
  config: ApiEnvConfigSchema,
  scope: EnvironmentScope.optional(),
  archived_at: isoTimestamp.nullable(),
  created_at: isoTimestamp,
  updated_at: isoTimestamp,
});

const EnvironmentCreate = z.object({
  name: z.string().min(1).max(256),
  description: z.string().nullable().optional(),
  metadata: Metadata.optional(),
  config: EnvConfigSchema.nullable().optional(),
  scope: EnvironmentScope.nullable().optional(),
  // Legacy Orca request extensions remain accepted unconditionally.
  packages: z.array(z.string()).optional(),
  networking: z.record(z.string(), z.unknown()).optional(),
  image: z.string().nullable().optional(),
  target: z.enum(['cloud', 'self_hosted']).nullable().optional(),
  egress_mode: z.enum(['gateway', 'sidecar']).nullable().optional(),
  llm: z.record(z.string(), z.unknown()).nullable().optional(),
});

const EnvironmentUpdate = EnvironmentCreate.omit({ metadata: true })
  .partial()
  .extend({
    name: z.string().min(1).max(256).nullable().optional(),
    metadata: MetadataPatch.optional(),
  });
const EnvironmentDeleted = z.object({
  id: idString('env'),
  type: z.literal('environment_deleted'),
});

// `rotate-key` echoes the raw env key exactly ONCE. `env_key` is a
// response-only field — it is never persisted raw and never appears on GET,
// list, or update. The holder must capture it here; it is unrecoverable after.
// (`POST /v1/environments` also echoes it once, but only to an `orca-beta`
// caller, so the published create response stays Anthropic's.)
const EnvironmentKey = z.object({
  env_key: z.string(),
  env_key_expires_at: isoTimestamp,
});

// Work-queue stats for an environment's `self_hosted` distribution backlog,
// mirroring Anthropic's claim/lease/stats shape:
//   - `depth`            — sessions pending with no worker yet (queue depth).
//   - `in_flight`        — sessions assigned to a worker but not complete
//                          (a launch in flight or a connected, running runner).
//   - `worker_connected` — whether a worker is currently connected, derived from
//                          the durable environment claim's heartbeat liveness.
const WorkStats = z.object({
  depth: z.number().int().nonnegative(),
  in_flight: z.number().int().nonnegative(),
  worker_connected: z.boolean(),
});

export const environmentsContract = c.router({
  create: {
    method: 'POST',
    path: '/v1/environments',
    body: EnvironmentCreate,
    responses: { 200: Environment, 400: ClaudeErrorResponse, 409: ClaudeErrorResponse },
    headers: z.object({ 'idempotency-key': z.string().optional() }).passthrough(),
  },
  get: {
    method: 'GET',
    path: '/v1/environments/:id',
    pathParams: z.object({ id: idString('env') }),
    responses: { 200: Environment, 404: ClaudeErrorResponse },
  },
  // Work-queue stats for the environment: pending depth, in-flight count, and
  // whether a worker is currently connected (durable-claim liveness). The
  // distribution backlog is `self_hosted`-only; a cloud environment simply
  // reports zeros + worker_connected=false (no claim, no distributed sessions).
  workStats: {
    method: 'GET',
    path: '/v1/environments/:id/work_stats',
    pathParams: z.object({ id: idString('env') }),
    responses: { 200: WorkStats, 404: z.object({ error: z.string() }) },
  },
  list: {
    method: 'GET',
    path: '/v1/environments',
    query: pagination.extend({ include_archived: z.boolean().optional() }),
    responses: {
      200: z.object({
        data: z.array(Environment),
        next_page: z.string().nullable(),
      }),
      400: ClaudeErrorResponse,
    },
  },
  update: {
    method: 'POST',
    path: '/v1/environments/:id',
    pathParams: z.object({ id: idString('env') }),
    body: EnvironmentUpdate,
    responses: {
      200: Environment,
      400: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
      409: ClaudeErrorResponse,
    },
    headers: z.object({ 'idempotency-key': z.string().optional() }).passthrough(),
  },
  rotateKey: {
    method: 'POST',
    path: '/v1/environments/:id/rotate-key',
    pathParams: z.object({ id: idString('env') }),
    body: z.object({}).strict(),
    responses: { 200: EnvironmentKey, 404: z.object({ error: z.string() }) },
    headers: z.object({ 'idempotency-key': z.string().optional() }).passthrough(),
  },
  // Explicit revoke: clear the armed env key while keeping the environment.
  // The previously issued key stops authenticating; the response reports the
  // now-unarmed state (`env_key_set: false`). The raw key is never echoed.
  revokeKey: {
    method: 'POST',
    path: '/v1/environments/:id/revoke-key',
    pathParams: z.object({ id: idString('env') }),
    body: z.object({}).strict(),
    responses: { 200: Environment, 404: z.object({ error: z.string() }) },
    headers: z.object({ 'idempotency-key': z.string().optional() }).passthrough(),
  },
  archive: {
    method: 'POST',
    path: '/v1/environments/:id/archive',
    pathParams: z.object({ id: idString('env') }),
    body: z.object({}).strict(),
    responses: { 200: Environment, 404: ClaudeErrorResponse },
    headers: z.object({ 'idempotency-key': z.string().optional() }).passthrough(),
  },
  delete: {
    method: 'DELETE',
    path: '/v1/environments/:id',
    pathParams: z.object({ id: idString('env') }),
    body: z.object({}).strict(),
    responses: {
      200: EnvironmentDeleted,
      404: ClaudeErrorResponse,
      409: ClaudeErrorResponse,
    },
  },
});
