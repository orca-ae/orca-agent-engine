// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { initContract } from '@ts-rest/core';
import { z } from 'zod';
import {
  AGENT_OBSERVABILITY_ADAPTERS,
  AGENT_OBSERVABILITY_BINDING_STATUSES,
  AGENT_OBSERVABILITY_CAPTURE_MODES,
  AGENT_OBSERVABILITY_COMPRESSIONS,
  AGENT_OBSERVABILITY_DISABLED_REASONS,
  AGENT_OBSERVABILITY_ENDPOINT_CLASSES,
  AGENT_OBSERVABILITY_ENDPOINT_KINDS,
  AGENT_OBSERVABILITY_PROTOCOLS,
  AGENT_OBSERVABILITY_SEMANTIC_PROFILES,
  AGENT_OBSERVABILITY_WORKSPACE_MODES,
} from '../domain/agent-observability-policy.js';
import {
  AGENT_OBSERVABILITY_ENDPOINT_MAX_LENGTH,
  AGENT_OBSERVABILITY_EXTERNAL_PROJECT_ID_MAX_LENGTH,
  AGENT_OBSERVABILITY_KEY_HINT_MAX_LENGTH,
  AGENT_OBSERVABILITY_SECRET_VALUE_MAX_LENGTH,
  AGENT_OBSERVABILITY_TIMEOUT_MAX_MS,
  AGENT_OBSERVABILITY_TIMEOUT_MIN_MS,
  hasAgentObservabilityControlCharacter,
  isAgentObservabilityExternalProjectId,
  isAgentObservabilityKeyHint,
  isAgentObservabilityOtlpHttpCredentials,
  isAgentObservabilitySecretValue,
  isCanonicalAgentObservabilityEndpoint,
  isSingleStrongEntityTag,
  normalizeAgentObservabilityIdempotencyKey,
  normalizeAgentObservabilityEndpoint,
} from '../domain/agent-observability-validation.js';
import { isoTimestamp } from './common.js';

const c = initContract();

export const AGENT_OBSERVABILITY_LABEL_MAX_LENGTH = 256;
export const AGENT_OBSERVABILITY_SAMPLE_RATE_SCALE = 10_000;

export const AgentObservabilityCaptureModeSchema = z.enum(AGENT_OBSERVABILITY_CAPTURE_MODES);
export const AgentObservabilityAdapterSchema = z.enum(AGENT_OBSERVABILITY_ADAPTERS);
export const AgentObservabilityEndpointKindSchema = z.enum(AGENT_OBSERVABILITY_ENDPOINT_KINDS);
export const AgentObservabilityEndpointClassSchema = z.enum(AGENT_OBSERVABILITY_ENDPOINT_CLASSES);
export const AgentObservabilityBindingStatusSchema = z.enum(AGENT_OBSERVABILITY_BINDING_STATUSES);

export const AgentObservabilityEndpointUrlSchema = z
  .string()
  .max(AGENT_OBSERVABILITY_ENDPOINT_MAX_LENGTH)
  .refine(isCanonicalAgentObservabilityEndpoint, 'must be a canonical absolute HTTP(S) endpoint');

/**
 * PUT accepts a safe endpoint spelling and canonicalizes it before hashing or
 * persistence. State responses always use {@link AgentObservabilityEndpointUrlSchema}.
 */
export const AgentObservabilityMutationEndpointUrlSchema = z
  .string()
  .max(AGENT_OBSERVABILITY_ENDPOINT_MAX_LENGTH)
  .refine(
    (value) => normalizeAgentObservabilityEndpoint(value) !== null,
    'must be an absolute HTTP(S) endpoint without userinfo, query, or fragment',
  );

export const AgentObservabilityExternalProjectIdSchema = z
  .string()
  .min(1)
  .max(AGENT_OBSERVABILITY_EXTERNAL_PROJECT_ID_MAX_LENGTH)
  .nullable()
  .refine(isAgentObservabilityExternalProjectId, 'must be trimmed and bounded');

export const AgentObservabilityKeyHintSchema = z
  .string()
  .min(1)
  .max(AGENT_OBSERVABILITY_KEY_HINT_MAX_LENGTH)
  .nullable()
  .refine(isAgentObservabilityKeyHint, 'must be printable, trimmed, and bounded');

const AgentObservabilityCredentialViewSchema = z.union([
  z
    .object({
      configured: z.literal(true),
      version: z.number().int().positive(),
      key_hint: AgentObservabilityKeyHintSchema,
      rotated_at: isoTimestamp,
    })
    .strict(),
  z
    .object({
      configured: z.literal(false),
      version: z.null(),
      key_hint: z.null(),
      rotated_at: z.null(),
    })
    .strict(),
]);

export const AgentObservabilityBindingViewSchema = z
  .object({
    id: z.string().min(1),
    scope: z.enum(['organization', 'workspace']),
    organization_id: z.string().min(1),
    workspace_id: z.string().min(1).nullable(),
    target: z
      .object({
        adapter_type: AgentObservabilityAdapterSchema,
        external_project_id: AgentObservabilityExternalProjectIdSchema,
        endpoint_kind: AgentObservabilityEndpointKindSchema,
        endpoint_class: AgentObservabilityEndpointClassSchema,
        endpoint_url: AgentObservabilityEndpointUrlSchema,
      })
      .strict(),
    status: AgentObservabilityBindingStatusSchema,
    config: z
      .object({
        version: z.number().int().positive(),
        semantic_profile: z.enum(AGENT_OBSERVABILITY_SEMANTIC_PROFILES),
        protocol: z.enum(AGENT_OBSERVABILITY_PROTOCOLS),
        compression: z.enum(AGENT_OBSERVABILITY_COMPRESSIONS),
        timeout_ms: z
          .number()
          .int()
          .min(AGENT_OBSERVABILITY_TIMEOUT_MIN_MS)
          .max(AGENT_OBSERVABILITY_TIMEOUT_MAX_MS),
        environment: z.string().nullable(),
        release: z.string().nullable(),
        capture_mode: AgentObservabilityCaptureModeSchema,
        sample_rate: z.number().min(0).max(1),
        config_schema_version: z.number().int().positive(),
      })
      .strict(),
    credential: AgentObservabilityCredentialViewSchema,
  })
  .strict();

export const AgentObservabilityDisabledReasonSchema = z.enum(AGENT_OBSERVABILITY_DISABLED_REASONS);

export const AgentObservabilityEffectiveStateSchema = z.union([
  z
    .object({
      source: z.enum(['organization_default', 'workspace_custom']),
      status: z.literal('enabled'),
      disabled_reason: z.null(),
      capture_mode: AgentObservabilityCaptureModeSchema,
      binding: AgentObservabilityBindingViewSchema,
    })
    .strict(),
  z
    .object({
      source: z.enum(['none', 'organization_default', 'workspace_custom']),
      status: z.literal('disabled'),
      disabled_reason: AgentObservabilityDisabledReasonSchema,
      capture_mode: z.literal('metadata_only'),
      binding: AgentObservabilityBindingViewSchema.nullable(),
    })
    .strict(),
]);

const OrganizationConfiguredStateSchema = z
  .object({
    capture_ceiling: AgentObservabilityCaptureModeSchema,
    default_binding: AgentObservabilityBindingViewSchema.nullable(),
  })
  .strict();

const WorkspaceConfiguredStateSchema = z
  .object({
    mode: z.enum(AGENT_OBSERVABILITY_WORKSPACE_MODES),
    capture_ceiling: AgentObservabilityCaptureModeSchema,
    binding: AgentObservabilityBindingViewSchema.nullable(),
  })
  .strict();

export const OrganizationAgentObservabilityStateSchema = z
  .object({
    type: z.literal('agent_observability'),
    scope: z.literal('organization'),
    organization_id: z.string().min(1),
    workspace_id: z.null(),
    configured: OrganizationConfiguredStateSchema,
    effective: AgentObservabilityEffectiveStateSchema,
  })
  .strict();

export const WorkspaceAgentObservabilityStateSchema = z
  .object({
    type: z.literal('agent_observability'),
    scope: z.literal('workspace'),
    organization_id: z.string().min(1),
    workspace_id: z.string().min(1),
    configured: WorkspaceConfiguredStateSchema,
    effective: AgentObservabilityEffectiveStateSchema,
  })
  .strict();

export const AgentObservabilityStateUnavailableSchema = z
  .object({ error: z.literal('agent observability state unavailable') })
  .strict();

export const AgentObservabilityWorkspaceNotFoundSchema = z
  .object({ error: z.literal('workspace not found') })
  .strict();

const AgentObservabilityOrganizationNotFoundSchema = z
  .object({ error: z.literal('organization not found') })
  .strict();

const AgentObservabilityUnauthenticatedSchema = z
  .object({ error: z.literal('unauthenticated') })
  .strict();

const AgentObservabilityForbiddenSchema = z.object({ error: z.string() }).strict();

const AgentObservabilityMutationBadRequestSchema = z
  .object({ error: z.literal('invalid agent observability request') })
  .strict();
const AgentObservabilityMutationConflictSchema = z
  .object({ error: z.literal('agent observability mutation conflict') })
  .strict();
const AgentObservabilityMutationStaleSchema = z
  .object({ error: z.literal('agent observability state is stale') })
  .strict();
const AgentObservabilityMutationPreconditionRequiredSchema = z
  .object({ error: z.literal('if-match is required') })
  .strict();
const AgentObservabilityMutationUnavailableSchema = z
  .object({ error: z.literal('agent observability unavailable') })
  .strict();

const AgentObservabilityMutationLabelSchema = z
  .string()
  .min(1)
  .max(AGENT_OBSERVABILITY_LABEL_MAX_LENGTH)
  .refine(
    (value) => value === value.trim() && !hasAgentObservabilityControlCharacter(value),
    'must be trimmed and contain no control characters',
  );

const AgentObservabilityMutationSampleRateSchema = z
  .number()
  .finite()
  .min(0)
  .max(1)
  .refine((value) => {
    const scaled = value * AGENT_OBSERVABILITY_SAMPLE_RATE_SCALE;
    return (
      Math.abs(scaled - Math.round(scaled)) <=
      Number.EPSILON * AGENT_OBSERVABILITY_SAMPLE_RATE_SCALE
    );
  }, `must use at most four decimal places`);

const AgentObservabilityMutationIdempotencyKeySchema = z
  .string()
  .refine(
    (value) => normalizeAgentObservabilityIdempotencyKey(value) !== null,
    'must normalize to a bounded opaque key',
  );

const AgentObservabilityMutationIfMatchSchema = z
  .string()
  .refine(isSingleStrongEntityTag, 'must be one strong entity tag');

export const AgentObservabilityMutationTargetSchema = z
  .object({
    adapter_type: z.literal('otlp_http'),
    endpoint_kind: AgentObservabilityEndpointKindSchema,
    endpoint_class: AgentObservabilityEndpointClassSchema,
    endpoint_url: AgentObservabilityMutationEndpointUrlSchema,
    external_project_id: AgentObservabilityExternalProjectIdSchema.optional(),
  })
  .strict();

export const AgentObservabilityMutationConfigSchema = z
  .object({
    semantic_profile: z.enum(AGENT_OBSERVABILITY_SEMANTIC_PROFILES),
    protocol: z.enum(['http/protobuf', 'http/json']),
    compression: z.enum(AGENT_OBSERVABILITY_COMPRESSIONS),
    timeout_ms: z
      .number()
      .int()
      .min(AGENT_OBSERVABILITY_TIMEOUT_MIN_MS)
      .max(AGENT_OBSERVABILITY_TIMEOUT_MAX_MS),
    environment: AgentObservabilityMutationLabelSchema.nullable().optional(),
    release: AgentObservabilityMutationLabelSchema.nullable().optional(),
    capture_mode: AgentObservabilityCaptureModeSchema,
    sample_rate: AgentObservabilityMutationSampleRateSchema,
  })
  .strict();

const AgentObservabilityCredentialSecretValueSchema = z
  .string()
  .min(1)
  .max(AGENT_OBSERVABILITY_SECRET_VALUE_MAX_LENGTH)
  .refine(isAgentObservabilitySecretValue, 'must be bounded and contain no control characters');

export const AgentObservabilityMutationCredentialsSchema = z
  .discriminatedUnion('type', [
    z
      .object({
        type: z.literal('basic'),
        username: AgentObservabilityCredentialSecretValueSchema,
        password: AgentObservabilityCredentialSecretValueSchema,
      })
      .strict(),
    z
      .object({
        type: z.literal('bearer'),
        token: AgentObservabilityCredentialSecretValueSchema,
      })
      .strict(),
    z
      .object({
        type: z.literal('custom_headers'),
        headers: z.record(z.string(), z.string()),
      })
      .strict(),
  ])
  .refine(
    isAgentObservabilityOtlpHttpCredentials,
    'must be a valid bounded OTLP HTTP credential bundle',
  );

/** Full replacement request. Server-owned response/state fields are absent by design. */
export const OrganizationAgentObservabilityPutRequestSchema = z
  .object({
    target: AgentObservabilityMutationTargetSchema,
    config: AgentObservabilityMutationConfigSchema,
    capture_ceiling: AgentObservabilityCaptureModeSchema,
    credentials: AgentObservabilityMutationCredentialsSchema.optional(),
  })
  .strict();

/**
 * Workspace replacement is deliberately a strict mode-discriminated union.
 * `inherit` and `disabled` cannot carry a caller-selected target, config, or
 * credentials; `custom` owns all three configuration axes.
 */
export const WorkspaceAgentObservabilityPutRequestSchema = z.discriminatedUnion('mode', [
  z
    .object({
      mode: z.literal('inherit'),
      capture_ceiling: AgentObservabilityCaptureModeSchema,
    })
    .strict(),
  z
    .object({
      mode: z.literal('disabled'),
      capture_ceiling: AgentObservabilityCaptureModeSchema,
    })
    .strict(),
  z
    .object({
      mode: z.literal('custom'),
      target: AgentObservabilityMutationTargetSchema,
      config: AgentObservabilityMutationConfigSchema,
      capture_ceiling: AgentObservabilityCaptureModeSchema,
      // A same-active-target policy replacement keeps its credential head.
      // Service authority decides when a credential-bearing replacement is
      // required; accepting it here would otherwise make that update impossible.
      credentials: AgentObservabilityMutationCredentialsSchema.optional(),
    })
    .strict(),
]);

/** Rotation changes only write-only credentials for the current default binding. */
export const OrganizationAgentObservabilityCredentialRotationRequestSchema = z
  .object({ credentials: AgentObservabilityMutationCredentialsSchema })
  .strict();

/** Rotation changes only write-only credentials for the current custom workspace binding. */
export const WorkspaceAgentObservabilityCredentialRotationRequestSchema = z
  .object({ credentials: AgentObservabilityMutationCredentialsSchema })
  .strict();

/** Emergency default disable deliberately accepts no caller-selected target or policy. */
export const OrganizationAgentObservabilityDisableRequestSchema = z.object({}).strict();

/** Changes only the organization capture ceiling, even without a default binding. */
export const OrganizationAgentObservabilityCaptureCeilingRequestSchema = z
  .object({ capture_ceiling: AgentObservabilityCaptureModeSchema })
  .strict();

/** Header schema is standalone because this listener is intentionally non-public. */
export const OrganizationAgentObservabilityPutHeadersSchema = z
  .object({
    'idempotency-key': AgentObservabilityMutationIdempotencyKeySchema,
    'if-match': AgentObservabilityMutationIfMatchSchema.optional(),
  })
  .passthrough();

/** Workspace settings always exist, so every workspace PUT has one ETag CAS. */
export const WorkspaceAgentObservabilityPutHeadersSchema = z
  .object({
    'idempotency-key': AgentObservabilityMutationIdempotencyKeySchema,
    'if-match': AgentObservabilityMutationIfMatchSchema,
  })
  .passthrough();

export const OrganizationAgentObservabilityCredentialRotationHeadersSchema = z
  .object({
    'idempotency-key': AgentObservabilityMutationIdempotencyKeySchema,
    'if-match': AgentObservabilityMutationIfMatchSchema,
  })
  .passthrough();

export const WorkspaceAgentObservabilityCredentialRotationHeadersSchema = z
  .object({
    'idempotency-key': AgentObservabilityMutationIdempotencyKeySchema,
    'if-match': AgentObservabilityMutationIfMatchSchema,
  })
  .passthrough();

export const OrganizationAgentObservabilityDisableHeadersSchema = z
  .object({
    'idempotency-key': AgentObservabilityMutationIdempotencyKeySchema,
    'if-match': AgentObservabilityMutationIfMatchSchema.optional(),
  })
  .passthrough();

export const AgentObservabilityWorkspacePathParamsSchema = z
  .object({ workspaceId: z.string().min(1).max(100) })
  .strict();

/**
 * Admin-listener-only contract. It intentionally stays out of `publicContract`
 * so generated public OpenAPI and Anthropic conformance remain unchanged.
 */
export const adminAgentObservabilityContract = c.router({
  putOrganizationCaptureCeiling: {
    method: 'PUT',
    path: '/v1/organizations/agent_observability/capture_ceiling',
    body: OrganizationAgentObservabilityCaptureCeilingRequestSchema,
    headers: WorkspaceAgentObservabilityPutHeadersSchema,
    responses: {
      200: OrganizationAgentObservabilityStateSchema,
      400: AgentObservabilityMutationBadRequestSchema,
      401: AgentObservabilityUnauthenticatedSchema,
      403: AgentObservabilityForbiddenSchema,
      404: AgentObservabilityOrganizationNotFoundSchema,
      409: AgentObservabilityMutationConflictSchema,
      412: AgentObservabilityMutationStaleSchema,
      428: AgentObservabilityMutationPreconditionRequiredSchema,
      503: AgentObservabilityMutationUnavailableSchema,
    },
  },
  organizationState: {
    method: 'GET',
    path: '/v1/organizations/agent_observability',
    responses: {
      200: OrganizationAgentObservabilityStateSchema,
      401: AgentObservabilityUnauthenticatedSchema,
      403: AgentObservabilityForbiddenSchema,
      404: AgentObservabilityOrganizationNotFoundSchema,
      503: AgentObservabilityStateUnavailableSchema,
    },
  },
  workspaceState: {
    method: 'GET',
    path: '/v1/organizations/workspaces/:workspaceId/agent_observability',
    pathParams: AgentObservabilityWorkspacePathParamsSchema,
    responses: {
      200: WorkspaceAgentObservabilityStateSchema,
      401: AgentObservabilityUnauthenticatedSchema,
      403: AgentObservabilityForbiddenSchema,
      404: AgentObservabilityWorkspaceNotFoundSchema,
      503: AgentObservabilityStateUnavailableSchema,
    },
  },
  putOrganizationDefault: {
    method: 'PUT',
    path: '/v1/organizations/agent_observability',
    body: OrganizationAgentObservabilityPutRequestSchema,
    headers: OrganizationAgentObservabilityPutHeadersSchema,
    responses: {
      200: OrganizationAgentObservabilityStateSchema,
      201: OrganizationAgentObservabilityStateSchema,
      400: AgentObservabilityMutationBadRequestSchema,
      401: AgentObservabilityUnauthenticatedSchema,
      403: AgentObservabilityForbiddenSchema,
      404: AgentObservabilityOrganizationNotFoundSchema,
      409: AgentObservabilityMutationConflictSchema,
      412: AgentObservabilityMutationStaleSchema,
      428: AgentObservabilityMutationPreconditionRequiredSchema,
      503: AgentObservabilityMutationUnavailableSchema,
    },
  },
  putWorkspaceObservability: {
    method: 'PUT',
    path: '/v1/organizations/workspaces/:workspaceId/agent_observability',
    pathParams: AgentObservabilityWorkspacePathParamsSchema,
    body: WorkspaceAgentObservabilityPutRequestSchema,
    headers: WorkspaceAgentObservabilityPutHeadersSchema,
    responses: {
      200: WorkspaceAgentObservabilityStateSchema,
      201: WorkspaceAgentObservabilityStateSchema,
      400: AgentObservabilityMutationBadRequestSchema,
      401: AgentObservabilityUnauthenticatedSchema,
      403: AgentObservabilityForbiddenSchema,
      404: AgentObservabilityWorkspaceNotFoundSchema,
      409: AgentObservabilityMutationConflictSchema,
      412: AgentObservabilityMutationStaleSchema,
      428: AgentObservabilityMutationPreconditionRequiredSchema,
      503: AgentObservabilityMutationUnavailableSchema,
    },
  },
  disableOrganizationDefault: {
    method: 'POST',
    path: '/v1/organizations/agent_observability:disable',
    body: OrganizationAgentObservabilityDisableRequestSchema,
    headers: OrganizationAgentObservabilityDisableHeadersSchema,
    responses: {
      200: OrganizationAgentObservabilityStateSchema,
      400: AgentObservabilityMutationBadRequestSchema,
      401: AgentObservabilityUnauthenticatedSchema,
      403: AgentObservabilityForbiddenSchema,
      404: AgentObservabilityOrganizationNotFoundSchema,
      409: AgentObservabilityMutationConflictSchema,
      412: AgentObservabilityMutationStaleSchema,
      503: AgentObservabilityMutationUnavailableSchema,
    },
  },
  rotateOrganizationCredentials: {
    method: 'POST',
    path: '/v1/organizations/agent_observability:rotate_credentials',
    body: OrganizationAgentObservabilityCredentialRotationRequestSchema,
    headers: OrganizationAgentObservabilityCredentialRotationHeadersSchema,
    responses: {
      200: OrganizationAgentObservabilityStateSchema,
      400: AgentObservabilityMutationBadRequestSchema,
      401: AgentObservabilityUnauthenticatedSchema,
      403: AgentObservabilityForbiddenSchema,
      404: AgentObservabilityOrganizationNotFoundSchema,
      409: AgentObservabilityMutationConflictSchema,
      412: AgentObservabilityMutationStaleSchema,
      428: AgentObservabilityMutationPreconditionRequiredSchema,
      503: AgentObservabilityMutationUnavailableSchema,
    },
  },
  rotateWorkspaceCredentials: {
    method: 'POST',
    path: '/v1/organizations/workspaces/:workspaceId/agent_observability:rotate_credentials',
    pathParams: AgentObservabilityWorkspacePathParamsSchema,
    body: WorkspaceAgentObservabilityCredentialRotationRequestSchema,
    headers: WorkspaceAgentObservabilityCredentialRotationHeadersSchema,
    responses: {
      200: WorkspaceAgentObservabilityStateSchema,
      400: AgentObservabilityMutationBadRequestSchema,
      401: AgentObservabilityUnauthenticatedSchema,
      403: AgentObservabilityForbiddenSchema,
      404: AgentObservabilityWorkspaceNotFoundSchema,
      409: AgentObservabilityMutationConflictSchema,
      412: AgentObservabilityMutationStaleSchema,
      428: AgentObservabilityMutationPreconditionRequiredSchema,
      503: AgentObservabilityMutationUnavailableSchema,
    },
  },
});

export type OrganizationAgentObservabilityState = z.infer<
  typeof OrganizationAgentObservabilityStateSchema
>;
export type WorkspaceAgentObservabilityState = z.infer<
  typeof WorkspaceAgentObservabilityStateSchema
>;
export type AgentObservabilityBindingView = z.infer<typeof AgentObservabilityBindingViewSchema>;
export type OrganizationAgentObservabilityPutRequest = z.infer<
  typeof OrganizationAgentObservabilityPutRequestSchema
>;
export type WorkspaceAgentObservabilityPutRequest = z.infer<
  typeof WorkspaceAgentObservabilityPutRequestSchema
>;
export type OrganizationAgentObservabilityCredentialRotationRequest = z.infer<
  typeof OrganizationAgentObservabilityCredentialRotationRequestSchema
>;
export type WorkspaceAgentObservabilityCredentialRotationRequest = z.infer<
  typeof WorkspaceAgentObservabilityCredentialRotationRequestSchema
>;
export type OrganizationAgentObservabilityDisableRequest = z.infer<
  typeof OrganizationAgentObservabilityDisableRequestSchema
>;
