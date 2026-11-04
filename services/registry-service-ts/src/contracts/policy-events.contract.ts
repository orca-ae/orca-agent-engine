// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { responseHeaderMetadata } from './openapi-headers.js';
import { z } from 'zod';
import { initContract } from '@ts-rest/core';

const c = initContract();
const sequence = z
  .string()
  .regex(/^(0|[1-9][0-9]*)$/)
  .max(19);
export const PolicyEntity = z
  .object({
    type: z.enum([
      'Vault',
      'Unattributed',
      'Caller',
      'Developer',
      'Agent',
      'Gateway',
      'Harness',
      'Workspace',
    ]),
    id: z.string().min(1).max(200),
    attributes: z.record(z.string(), z.unknown()).optional(),
    parents: z.array(z.object({ type: z.literal('Workspace'), id: z.string() })).optional(),
  })
  .strict();
export const PolicyCall = z
  .object({
    action: z.string().min(1).max(300),
    principal: PolicyEntity,
    resource: PolicyEntity,
    request_id: z.string().min(1).max(200),
    session_id: z.string().min(1).max(200),
    input: z.record(z.string(), z.unknown()),
    subject: z.object({ kind: z.string(), id: z.string() }).optional(),
    mcp: z.object({ server: z.string(), server_host: z.string() }).strict().optional(),
  })
  .strict();
export const PolicyEvent = PolicyCall.extend({
  kind: z.enum(['request', 'response', 'error', 'approval']),
  timestamp_ms: z.number().int().safe(),
  guardrail_id: z.string().optional(),
  output: z.record(z.string(), z.unknown()).optional(),
  error_type: z.enum(['denied', 'failed', 'interrupted']).optional(),
  result: z.enum(['allow', 'deny']).optional(),
  asked_by: z.string().optional(),
  approver: z.string().optional(),
}).strict();
export const PolicyHead = z.object({ session: sequence, principal: sequence.nullable() }).strict();
const eventRow = z.object({ seq: sequence, event: PolicyEvent }).strict();
const privateState = z
  .object({
    session: z.record(z.string(), z.unknown()),
    subject_window: z.record(z.string(), z.unknown()),
    workspace_window: z.record(z.string(), z.unknown()),
  })
  .strict();
export const PolicyRead = z
  .object({
    state_revision: sequence.optional(),
    session_id: z.string(),
    resources: z.array(PolicyEntity).optional(),
    mcp_servers: z.array(z.object({ name: z.string(), url: z.string() })).optional(),
    subject: z.object({ kind: z.string(), id: z.string() }).optional(),
    now: z.string().datetime(),
    epoch: sequence,
    bundle_id: z.string(),
    core_version: z.literal('0.1.0'),
    guardrail_state: privateState.optional(),
    head: PolicyHead,
    principal: PolicyEntity,
    resource: PolicyEntity,
    turn_event_id: z.string().nullable(),
    day_window: z.string(),
    month_window: z.string(),
    events: z.array(eventRow),
    guardrails: z.array(
      z
        .object({
          id: z.string(),
          name: z.string(),
          mode: z.enum(['observe', 'enforce']).optional(),
          rule: z.unknown(),
          subagent_id: z.string().optional(),
        })
        .passthrough(),
    ),
  })
  .strict();
export const PolicyAppend = z
  .object({
    batch_id: z.string().min(1).max(200),
    tool_use_id: z.string().min(1).max(200),
    expected_state_revision: sequence.optional(),
    day_window: z.string().optional(),
    month_window: z.string().optional(),
    expected_head: PolicyHead,
    epoch: sequence,
    bundle_id: z.string().min(1).max(200),
    decision: z
      .object({
        call: PolicyCall,
        verdict: z.enum(['allow', 'ask', 'deny']),
        dogwood: z
          .array(
            z
              .object({
                guardrail_id: z.string(),
                verdict: z.enum(['allow', 'ask', 'deny', 'abstain']),
                deciding_rules: z.array(z.string()),
                errors: z.array(z.string()),
              })
              .strict(),
          )
          .max(16),
        confirmation: z
          .object({ event_id: z.string(), tool_use_id: z.string() })
          .strict()
          .optional(),
        public_tool_use_id: z.string().max(200).optional(),
      })
      .strict()
      .optional(),
    events: z.array(PolicyEvent).max(64),
    outcome: z.enum(['response', 'failed', 'interrupted']).optional(),
    output: z.unknown().optional(),
    state_updates: z
      .array(
        z
          .object({
            scope: z.enum(['session', 'subject_window', 'workspace_window']),
            key: z.string().min(1).max(300),
            action: z.enum(['set', 'increment', 'delete', 'append']),
            value: z.unknown().optional(),
          })
          .strict(),
      )
      .max(100)
      .optional(),
    turn_event_id: z.string().optional(),
    window: z.string().optional(),
  })
  .strict()
  .refine((v) => Boolean(v.decision) !== Boolean(v.outcome), 'one decision or outcome is required');
export type { PolicyAppendBody, PolicyReadBody, PolicyAppendResponse } from '@orca/guardrails';

export const PolicyReceipt = z
  .object({
    batch_id: z.string(),
    tool_use_id: z.string(),
    epoch: sequence,
    bundle_id: z.string(),
    committed_at: z.string().datetime(),
    valid_before: z.string().datetime().nullable(),
  })
  .strict();
export const PolicyAppendResult = z
  .object({
    state_revision: sequence.optional(),
    disposition: z.enum(['created', 'replayed']),
    receipt: PolicyReceipt,
    guardrail_state: privateState.optional(),
    now: z.string().datetime(),
    head: PolicyHead,
    events: z.array(eventRow),
  })
  .strict();

export const PolicyConflict = z.object({
  error: z.string(),
  code: z.enum([
    'head_conflict',
    'stale_epoch',
    'bundle_conflict',
    'decision_changed',
    'decision_expired',
  ]),
  now: z.string().datetime(),
  head: PolicyHead,
});
const path = '/internal/v1/workspaces/:workspaceId/sessions/:id/policy-events';
const pathParams = z.object({ workspaceId: z.string(), id: z.string() });
export const PolicyWarning = z
  .object({
    epoch: sequence,
    bundle_id: z.string().min(1).max(200),
    guardrail_id: z.string().min(1).max(200),
    server: z
      .string()
      .min(1)
      .max(200)
      .regex(/^[A-Za-z0-9_-]+$/),
    code: z.literal('manifest_drift'),
  })
  .strict();
const noStore = responseHeaderMetadata({
  200: {
    'Cache-Control': {
      description: 'Fresh authenticated origin policy exchange; never cached.',
      required: true,
      schema: { type: 'string', enum: ['no-store'] },
    },
  },
  409: {
    'Cache-Control': {
      description: 'Fresh authenticated origin policy exchange; never cached.',
      required: true,
      schema: { type: 'string', enum: ['no-store'] },
    },
  },
});
export const policyEventsContract = c.router({
  warning: {
    metadata: noStore,
    method: 'POST',
    path: '/internal/v1/workspaces/:workspaceId/sessions/:id/policy-warnings',
    pathParams,
    body: PolicyWarning,
    responses: {
      200: z.object({ recorded: z.literal(true) }),
      400: z.object({ error: z.string() }),
      403: z.object({ error: z.string() }),
      409: z.object({ error: z.string() }),
      503: z.object({ error: z.string() }),
    },
  },
  read: {
    metadata: noStore,
    method: 'GET',
    path,
    pathParams,
    query: z
      .object({ history: z.enum(['session', 'principal']).optional(), since: sequence.optional() })
      .strict(),
    responses: {
      200: PolicyRead,
      400: z.object({ error: z.string() }),
      403: z.object({ error: z.string() }),
      404: z.object({ error: z.string() }),
      409: z.union([PolicyConflict, z.object({ error: z.string() })]),
      503: z.object({ error: z.string() }),
    },
  },
  append: {
    metadata: noStore,
    method: 'POST',
    path,
    pathParams,
    body: PolicyAppend,
    responses: {
      200: PolicyAppendResult,
      400: z.object({ error: z.string() }),
      403: z.object({ error: z.string() }),
      404: z.object({ error: z.string() }),
      409: z.union([PolicyConflict, z.object({ error: z.string() })]),
      503: z.object({ error: z.string() }),
    },
  },
});
