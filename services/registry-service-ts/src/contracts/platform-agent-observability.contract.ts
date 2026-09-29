// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { initContract } from '@ts-rest/core';
import { z } from 'zod';
import {
  AGENT_OBSERVABILITY_ADAPTERS,
  AGENT_OBSERVABILITY_CAPTURE_MODES,
  AGENT_OBSERVABILITY_ENDPOINT_CLASSES,
} from '../domain/agent-observability-policy.js';
import {
  isSingleStrongEntityTag,
  normalizeAgentObservabilityIdempotencyKey,
} from '../domain/agent-observability-validation.js';
import { isoTimestamp } from './common.js';

export const PLATFORM_AGENT_OBSERVABILITY_PATH = '/v1/platform/agent_observability';

/** Allowlists are sets: reject duplicates and make representation/hash order canonical. */
const adapters = z
  .array(z.enum(AGENT_OBSERVABILITY_ADAPTERS))
  .min(1)
  .max(AGENT_OBSERVABILITY_ADAPTERS.length)
  .refine((values) => new Set(values).size === values.length, 'must not contain duplicates')
  .transform((values) => [...values].sort());
const endpointClasses = z
  .array(z.enum(AGENT_OBSERVABILITY_ENDPOINT_CLASSES))
  .min(1)
  .max(AGENT_OBSERVABILITY_ENDPOINT_CLASSES.length)
  .refine((values) => new Set(values).size === values.length, 'must not contain duplicates')
  .transform((values) => [...values].sort());

export const PlatformAgentObservabilityPutRequestSchema = z
  .object({
    allowed_adapters: adapters,
    allowed_endpoint_classes: endpointClasses,
    max_capture_mode: z.enum(AGENT_OBSERVABILITY_CAPTURE_MODES),
  })
  .strict();

export const PlatformAgentObservabilityPolicySchema =
  PlatformAgentObservabilityPutRequestSchema.extend({
    type: z.literal('agent_observability_platform_policy'),
    capture_restriction_epoch: z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
    created_at: isoTimestamp,
    updated_at: isoTimestamp,
  })
    .strict()
    .refine(
      (value) => Date.parse(value.updated_at) >= Date.parse(value.created_at),
      'updated_at must not precede created_at',
    );

export const PlatformAgentObservabilityPutHeadersSchema = z
  .object({
    'idempotency-key': z
      .string()
      .refine(
        (value) => normalizeAgentObservabilityIdempotencyKey(value) !== null,
        'must be a bounded nonempty idempotency key',
      )
      .transform((value) => normalizeAgentObservabilityIdempotencyKey(value)!),
    'if-match': z.string().refine(isSingleStrongEntityTag, 'must be one strong entity tag'),
  })
  .passthrough();

const error = (message: string) => z.object({ error: z.literal(message) }).strict();
const unavailable = error('agent observability platform policy unavailable');
const unauthenticated = error('unauthenticated');

/** Platform-admin only; intentionally absent from the public Anthropic contract. */
export const platformAgentObservabilityContract = initContract().router({
  getPolicy: {
    method: 'GET',
    path: PLATFORM_AGENT_OBSERVABILITY_PATH,
    responses: {
      200: PlatformAgentObservabilityPolicySchema,
      401: unauthenticated,
      503: unavailable,
    },
  },
  putPolicy: {
    method: 'PUT',
    path: PLATFORM_AGENT_OBSERVABILITY_PATH,
    body: PlatformAgentObservabilityPutRequestSchema,
    headers: PlatformAgentObservabilityPutHeadersSchema,
    responses: {
      200: PlatformAgentObservabilityPolicySchema,
      400: error('invalid agent observability platform policy request'),
      401: unauthenticated,
      409: error('idempotency-key reused with different request'),
      412: error('agent observability platform policy is stale'),
      428: error('if-match is required'),
      503: unavailable,
    },
  },
});

export type PlatformAgentObservabilityPolicy = z.infer<
  typeof PlatformAgentObservabilityPolicySchema
>;
export type PlatformAgentObservabilityPutRequest = z.infer<
  typeof PlatformAgentObservabilityPutRequestSchema
>;
