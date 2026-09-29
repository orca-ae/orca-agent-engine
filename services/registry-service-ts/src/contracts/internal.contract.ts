// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { initContract } from '@ts-rest/core';
import { z } from 'zod';
import {
  LOGICAL_CREDENTIAL_ID_PATTERN,
  PROVIDER_CREDENTIAL_SCHEMES,
  isLogicalCredentialId,
} from '../domain/provider-credential.js';
import {
  AGENT_OBSERVABILITY_ADAPTERS,
  AGENT_OBSERVABILITY_BINDING_STATUSES,
  AGENT_OBSERVABILITY_CAPTURE_MODES,
  AGENT_OBSERVABILITY_COMPRESSIONS,
  AGENT_OBSERVABILITY_ENDPOINT_CLASSES,
  AGENT_OBSERVABILITY_ENDPOINT_KINDS,
  AGENT_OBSERVABILITY_PROTOCOLS,
  AGENT_OBSERVABILITY_SEMANTIC_PROFILES,
} from '../domain/agent-observability-policy.js';
import {
  isAgentObservabilityBasicUsername,
  isAgentObservabilitySecretValue,
  normalizeAgentObservabilityCustomHeaders,
} from '../domain/agent-observability-validation.js';
import { WORKSPACE_ID_RE } from '../auth/workspace-id.js';
import { idString, isoTimestamp } from './common.js';

const c = initContract();
const requestedVaultCredentialId = z.union([
  idString('vcrd'),
  z.string().refine(isLogicalCredentialId, {
    message: `must be a vcrd_… identifier or match ${LOGICAL_CREDENTIAL_ID_PATTERN.source}`,
  }),
]);

/**
 * Mirror of `files.contract.ts`'s `File` zod. Re-declared here (not imported)
 * because the response shape lives in two contracts and a shared zod object
 * would create a cyclic import between contracts. The fields MUST stay in
 * lock-step with `files.contract.ts`.
 */
const File = z.object({
  id: idString('file'),
  filename: z.string(),
  mime_type: z.string(),
  size_bytes: z.number().int().nonnegative(),
  sha256: z.string().regex(/^[0-9a-f]{64}$/),
  metadata: z.record(z.string(), z.string()).default({}),
  purpose: z.enum(['agent', 'agent_output']),
  scope_id: idString('ses').nullable(),
  downloadable: z.boolean(),
  archived_at: isoTimestamp.nullable(),
  created_at: isoTimestamp,
  updated_at: isoTimestamp,
});

/**
 * Mirror of `memory-stores.contract.ts`'s `Memory` and `MemoryVersion` shapes.
 * Re-declared (not imported) so the internal contract stays standalone — the
 * fields MUST stay in lock-step with `memory-stores.contract.ts`.
 */
const InternalMemory = z.object({
  id: idString('mem'),
  store_id: idString('mems'),
  path: z.string(),
  current_sha256: z.string().regex(/^[0-9a-f]{64}$/),
  size_bytes: z.number().int().nonnegative(),
  updated_at: isoTimestamp,
  updated_by_session_id: z.string().nullable(),
  updated_by_event_id: z.string().nullable(),
});

const InternalMemoryVersion = z.object({
  id: idString('memver'),
  store_id: idString('mems'),
  memory_id: idString('mem'),
  path: z.string(),
  sha256: z.string().regex(/^[0-9a-f]{64}$/),
  size_bytes: z.number().int().nonnegative(),
  written_by_session_id: z.string().nullable(),
  written_by_event_id: z.string().nullable(),
  written_at: isoTimestamp,
  redacted_at: isoTimestamp.nullable(),
});

const InternalSession = z
  .object({
    id: idString('ses'),
    status: z.enum(['idle', 'running', 'rescheduling', 'terminated']),
    sandbox_handle_id: z.string().nullable(),
  })
  .passthrough();

/** A non-empty worker tunnel connection id (opaque; assigned by the worker). */
const workerConnId = z.string().min(1);

const AgentObservabilityCaptureModeSchema = z.enum(AGENT_OBSERVABILITY_CAPTURE_MODES);
const AgentObservabilityAdapterSchema = z.enum(AGENT_OBSERVABILITY_ADAPTERS);
const AgentObservabilityEndpointKindSchema = z.enum(AGENT_OBSERVABILITY_ENDPOINT_KINDS);
const AgentObservabilityEndpointClassSchema = z.enum(AGENT_OBSERVABILITY_ENDPOINT_CLASSES);
const AgentObservabilityBindingStatusSchema = z.enum(AGENT_OBSERVABILITY_BINDING_STATUSES);

export const AgentObservabilitySessionContextSuppressionReasonSchema = z.enum([
  'session_archived',
  'session_deleted',
  'session_revoked',
  'organization_archived',
  'workspace_archived',
  'organization_default_revoked',
  'organization_revoked',
  'workspace_revoked',
  'binding_revoked',
  'binding_disabled',
  'binding_archived',
  'binding_configuration_invalid',
  'platform_adapter_disallowed',
  'platform_endpoint_class_disallowed',
  'credential_not_configured',
]);

const AgentObservabilitySessionContextEpochsSchema = z
  .object({
    organization_selection_epoch: z.number().int().nonnegative().safe(),
    workspace_selection_epoch: z.number().int().nonnegative().safe(),
    organization_default_revocation_epoch: z.number().int().nonnegative().safe(),
    organization_revocation_epoch: z.number().int().nonnegative().safe(),
    workspace_revocation_epoch: z.number().int().nonnegative().safe(),
    binding_revocation_epoch: z.number().int().nonnegative().safe(),
    platform_capture_restriction_epoch: z.number().int().nonnegative().safe(),
    organization_capture_restriction_epoch: z.number().int().nonnegative().safe(),
    workspace_capture_restriction_epoch: z.number().int().nonnegative().safe(),
    session_revocation_epoch: z.number().int().nonnegative().safe(),
  })
  .strict();

const AgentObservabilitySessionContextBindingSchema = z
  .object({
    id: z.string().min(1),
    version: z.number().int().positive().safe(),
    scope: z.enum(['organization', 'workspace']),
    workspace_id: z.string().min(1).nullable(),
    target: z
      .object({
        adapter_type: AgentObservabilityAdapterSchema,
        endpoint_kind: AgentObservabilityEndpointKindSchema,
        endpoint_class: AgentObservabilityEndpointClassSchema,
        endpoint_url: z.string().url(),
        external_project_id: z.string().min(1).nullable(),
      })
      .strict(),
    lifecycle_status: AgentObservabilityBindingStatusSchema,
    config: z
      .object({
        semantic_profile: z.enum(AGENT_OBSERVABILITY_SEMANTIC_PROFILES),
        protocol: z.enum(AGENT_OBSERVABILITY_PROTOCOLS),
        compression: z.enum(AGENT_OBSERVABILITY_COMPRESSIONS),
        timeout_ms: z.number().int().positive().safe(),
        environment: z.string().nullable(),
        release: z.string().nullable(),
        capture_mode: AgentObservabilityCaptureModeSchema,
        sample_rate: z.number().min(0).max(1),
        config_schema_version: z.number().int().positive().safe(),
      })
      .strict(),
    current_credential_version: z.number().int().positive().safe().nullable(),
  })
  .strict();

const AgentObservabilitySessionContextCommonSchema = z.object({
  schema_version: z.literal(1),
  organization_id: z.string().min(1),
  workspace_id: z.string().min(1),
  session_id: idString('ses'),
  agent: z
    .object({
      id: idString('agt'),
      version: z.number().int().positive().safe(),
    })
    .strict(),
  harness: z.string().nullable(),
  harness_mode: z.string().nullable(),
  selection_source: z.enum(['organization_default', 'workspace_custom', 'disabled']),
  binding: AgentObservabilitySessionContextBindingSchema.nullable(),
  capture: z
    .object({
      pinned_mode: AgentObservabilityCaptureModeSchema,
      effective_mode: AgentObservabilityCaptureModeSchema,
      current_ceilings: z
        .object({
          platform: AgentObservabilityCaptureModeSchema,
          organization: AgentObservabilityCaptureModeSchema,
          workspace: AgentObservabilityCaptureModeSchema,
        })
        .strict(),
    })
    .strict(),
  epochs: z
    .object({
      pinned: AgentObservabilitySessionContextEpochsSchema,
      current: AgentObservabilitySessionContextEpochsSchema,
    })
    .strict(),
});

/** Non-secret Session-pinned delivery context for the observability exporter. */
export const AgentObservabilitySessionContextSchema = z.discriminatedUnion('status', [
  AgentObservabilitySessionContextCommonSchema.extend({
    status: z.literal('enabled'),
    reason: z.null(),
  }).strict(),
  AgentObservabilitySessionContextCommonSchema.extend({
    status: z.literal('disabled'),
    reason: z.literal('session_pin_disabled'),
  }).strict(),
  AgentObservabilitySessionContextCommonSchema.extend({
    status: z.literal('suppressed'),
    reason: AgentObservabilitySessionContextSuppressionReasonSchema,
  }).strict(),
]);

export type AgentObservabilitySessionContext = z.infer<
  typeof AgentObservabilitySessionContextSchema
>;

const AgentObservabilityContextBadRequestSchema = z
  .object({ error: z.literal('invalid agent observability context request') })
  .strict();
const AgentObservabilityContextNotFoundSchema = z
  .object({ error: z.literal('not found') })
  .strict();
const AgentObservabilityContextUnavailableSchema = z
  .object({ error: z.literal('agent observability unavailable') })
  .strict();

const AgentObservabilitySecretValueSchema = z
  .string()
  .refine(isAgentObservabilitySecretValue, 'must be bounded secret material');
const AgentObservabilityBasicUsernameSchema = z
  .string()
  .refine(isAgentObservabilityBasicUsername, 'must be an unambiguous Basic username');

const AgentObservabilityCustomHeadersSchema = z.record(z.string(), z.string()).refine((value) => {
  const normalized = normalizeAgentObservabilityCustomHeaders(value);
  return normalized !== null && JSON.stringify(normalized) === JSON.stringify(value);
}, 'must be canonical bounded custom headers');

const AgentObservabilitySecretBundleSchema = z.discriminatedUnion('adapter_type', [
  z
    .object({
      adapter_type: z.literal('otlp_http'),
      auth: z.discriminatedUnion('type', [
        z
          .object({
            type: z.literal('basic'),
            username: AgentObservabilityBasicUsernameSchema,
            password: AgentObservabilitySecretValueSchema,
          })
          .strict(),
        z
          .object({ type: z.literal('bearer'), token: AgentObservabilitySecretValueSchema })
          .strict(),
        z
          .object({
            type: z.literal('custom_headers'),
            headers: AgentObservabilityCustomHeadersSchema,
          })
          .strict(),
      ]),
    })
    .strict(),
  z
    .object({
      adapter_type: z.literal('langfuse_sdk'),
      public_key: AgentObservabilitySecretValueSchema,
      secret_key: AgentObservabilitySecretValueSchema,
    })
    .strict(),
]);

/** Secret material released only after a fresh audit-backed authorization. */
export const AgentObservabilitySecretResolutionSchema = z
  .object({
    schema_version: z.literal(1),
    authorization_id: z.string().regex(/^obsauth_[0-9A-HJ-NP-TV-Z]{20}$/),
    binding_id: z.string().min(1),
    binding_version: z.number().int().positive().safe(),
    credential_version: z.number().int().positive().safe(),
    effective_capture_mode: AgentObservabilityCaptureModeSchema,
    bundle: AgentObservabilitySecretBundleSchema,
  })
  .strict();

export type AgentObservabilitySecretResolution = z.infer<
  typeof AgentObservabilitySecretResolutionSchema
>;

const AgentObservabilitySecretBadRequestSchema = z
  .object({ error: z.literal('invalid agent observability secret request') })
  .strict();
const AgentObservabilitySecretDeniedSchema = z
  .object({ error: z.literal('agent observability secret resolution denied') })
  .strict();

/**
 * Internal callers use Registry storage identifiers, not public wire aliases.
 * Keep these path schemas beside the resolver contract so its Fastify handler
 * validates exactly the same selector shape.
 */
export const InternalAgentObservabilityWorkspaceIdSchema = z
  .string()
  .regex(WORKSPACE_ID_RE, 'must be a valid workspace identifier');
/** Matches Fastify's bounded non-public router parameter limit. */
export const INTERNAL_AGENT_OBSERVABILITY_SESSION_ID_MAX_LENGTH = 128;
export const InternalAgentObservabilitySessionIdSchema = z
  .string()
  .max(INTERNAL_AGENT_OBSERVABILITY_SESSION_ID_MAX_LENGTH)
  .regex(/^ses_[A-Za-z0-9_-]+$/, 'must be an internal ses_… identifier');
export const InternalAgentObservabilityContextPathParamsSchema = z
  .object({
    workspaceId: InternalAgentObservabilityWorkspaceIdSchema,
    sessionId: InternalAgentObservabilitySessionIdSchema,
  })
  .strict();

/** Secret and context resolvers intentionally share one exact path selector. */
export const InternalAgentObservabilitySecretPathParamsSchema =
  InternalAgentObservabilityContextPathParamsSchema;

const AgentObservabilityResolverEmptyBodySchema = z.object({}).strict();

/**
 * A persisted environment claim — the durable lease that pins an environment's
 * worker tunnel to the single registry replica that owns it. Mirrors the
 * `EnvironmentClaim` domain shape (`src/domain/environment-claims.ts`); the
 * timestamps are ISO strings on the wire.
 */
const InternalEnvironmentClaim = z.object({
  environment_id: idString('env'),
  owner_pod: z.string(),
  worker_conn_id: z.string(),
  claimed_at: isoTimestamp,
  last_ping: isoTimestamp,
});

export const PreparedAgentSnapshotSchema = z
  .object({
    id: idString('agt'),
    name: z.string(),
    version: z.number().int().positive(),
    model: z
      .object({
        provider: z.string(),
        id: z.string(),
      })
      .passthrough(),
    system: z.string().nullable(),
    tools: z.array(z.unknown()),
    mcp_servers: z.array(z.unknown()),
    skills: z.array(z.unknown()),
    metadata: z.record(z.string(), z.unknown()),
    multiagent: z
      .object({
        type: z.literal('coordinator'),
        agents: z.array(
          z.object({
            type: z.literal('agent'),
            id: idString('agt'),
            version: z.number().int().positive(),
          }),
        ),
      })
      .nullable(),
  })
  .passthrough();

export const PreparedSkillDescriptorSchema = z
  .object({
    id: z.string().min(1),
    skill_id: z.string().min(1),
    source: z.enum(['anthropic', 'custom']),
    version_identifier: z.string().min(1),
    name: z.string().min(1),
    description: z.string().min(1),
    entrypoint: z.literal('SKILL.md'),
    package_sha256: z.string().regex(/^[0-9a-f]{64}$/),
    package_size_bytes: z.number().int().positive(),
  })
  .strict();

/**
 * One exact, immutable Skill-bundle pin as delivered to a runner — the shared
 * descriptor shape the prepared-execution join (`loadPreparedAgents`), the
 * session-wide union loader (`loadSessionSkillBundles`), and the colocated agent
 * snapshot (`AgentSnapshot.skills`) all carry.
 */
export type PreparedSkillDescriptor = z.infer<typeof PreparedSkillDescriptorSchema>;

const PreparedAgent = PreparedAgentSnapshotSchema.omit({ skills: true }).extend({
  workspace_id: z.string().min(1),
  skills: z.array(PreparedSkillDescriptorSchema),
});

const PreparedGuardrail = z
  .object({
    id: idString('grd'),
    name: z.string().min(1),
    tier: z.enum(['session', 'agent', 'workspace', 'organization']),
    phases: z.array(z.string().min(1)),
    rule: z.unknown(),
    stateful: z.boolean(),
    state_scope: z.enum(['turn', 'session', 'subject_window']).optional(),
    /** Set when the guardrail came from a dispatched subagent's own record. */
    subagent_id: z.string().min(1).optional(),
  })
  .strict();

const PreparedEnvironment = z.object({
  id: idString('env'),
  workspace_id: z.string().min(1),
  name: z.string(),
  packages: z.record(z.string(), z.array(z.string())),
  networking: z.record(z.string(), z.unknown()),
  image: z.string().nullable(),
  target: z.enum(['cloud', 'self_hosted']).nullable(),
});

const PreparedVaultCredential = z.object({
  credential_id: idString('vcrd'),
  vault_id: idString('vlt'),
  auth_type: z.string(),
  mcp_server_url: z.string().nullable(),
  secret_name: z.string().nullable(),
  networking: z.unknown(),
});

const PreparedResourceCommon = {
  id: idString('sesrsc'),
  file_id: idString('file').nullable(),
  memory_store_id: idString('mems').nullable(),
  repo_ref: z.record(z.string(), z.unknown()).nullable(),
  mount_path: z.string(),
  access: z.enum(['read_only', 'read_write']),
  mount_strategy: z.string().nullable(),
  instructions: z.string().nullable(),
  attached_at: isoTimestamp,
  detached_at: z.null(),
};

const PreparedResource = z.discriminatedUnion('type', [
  z.object({
    ...PreparedResourceCommon,
    type: z.literal('file'),
    file_id: idString('file'),
    file: z.object({
      filename: z.string(),
      mime_type: z.string(),
      size_bytes: z.number().int().nonnegative(),
      sha256: z.string().regex(/^[0-9a-f]{64}$/),
      purpose: z.enum(['agent', 'agent_output']),
    }),
  }),
  z.object({
    ...PreparedResourceCommon,
    type: z.literal('memory_store'),
    memory_store_id: idString('mems'),
    memory_store: z.object({
      id: idString('mems'),
      workspace_id: z.string().min(1),
      name: z.string(),
    }),
  }),
  z.object({
    ...PreparedResourceCommon,
    type: z.literal('github_repository'),
    repo_ref: z.object({
      url: z.string().url(),
      git_credential_id: idString('gitcred'),
      checkout: z.record(z.string(), z.unknown()).optional(),
    }),
  }),
]);

export const HarnessTurnReceiptSchema = z
  .object({
    turnId: z.string().min(1).max(512),
    sourceIds: z.array(z.string().min(1).max(512)).min(1).max(256),
    usageEventId: idString('evt'),
    guarded: z.boolean(),
    phase: z.enum(['pending', 'ready', 'settled']),
    terminalEventId: idString('evt'),
    errorEventId: idString('evt'),
    producedAt: isoTimestamp,
    error: z.string().max(4096).nullable(),
  })
  .strict();
export const HarnessTurnRequestSchema = z
  .object({
    runtimeRevision: z.number().int().positive().optional(),
    ownershipRevision: z.number().int().nonnegative().optional(),
    ownerToken: z.string().uuid().optional(),
    action: z.discriminatedUnion('type', [
      z.object({ type: z.literal('inspect') }).strict(),
      z
        .object({
          type: z.literal('claim'),
          expectedOwnershipRevision: z.number().int().nonnegative(),
        })
        .strict(),
      z.object({ type: z.literal('begin'), receipt: HarnessTurnReceiptSchema }).strict(),
      z
        .object({
          type: z.literal('accept_source'),
          turnId: z.string().min(1).max(512),
          sourceId: z.string().min(1).max(512),
        })
        .strict(),
      z
        .object({
          type: z.literal('commit'),
          turnId: z.string().min(1).max(512),
          state: z.unknown(),
          responsePersisted: z.literal(true),
          error: z.string().max(4096).nullable(),
        })
        .strict(),
      z
        .object({
          type: z.literal('abandon'),
          turnId: z.string().min(1).max(512),
          error: z.string().min(1).max(4096).optional(),
        })
        .strict(),
      z.object({ type: z.literal('settle'), turnId: z.string().min(1).max(512) }).strict(),
    ]),
  })
  .strict();

export const PreparedExecutionV2Schema = z.object({
  schema_version: z.literal(2),
  workspace_id: z.string().min(1),
  session: z.object({
    id: idString('ses'),
    workspace_id: z.string().min(1),
    runtime_revision: z.number().int().positive(),
    status: z.enum(['idle', 'running', 'rescheduling', 'terminated']),
    agent_id: idString('agt'),
    agent_version: z.number().int().positive(),
    environment_id: idString('env').nullable(),
    vault_ids: z.array(idString('vlt')),
    metadata: z.record(z.string(), z.unknown()),
    /** Private native SDK recovery state; never part of the public Session schema. */
    harness_state: z.unknown().optional(),
    harness_ownership_revision: z.number().int().nonnegative().optional(),
    harness_state_revision: z
      .string()
      .regex(/^[a-f0-9]{64}$/)
      .nullable()
      .optional(),
    /** The single producer authorized to persist this session's LLM usage. */
    usage_writer: z.enum(['harness', 'ai-gateway']).default('harness'),
  }),
  primary_agent: PreparedAgent,
  subagents: z.array(PreparedAgent),
  environment: PreparedEnvironment.nullable(),
  vault_credentials: z.array(PreparedVaultCredential),
  resources: z.array(PreparedResource),
  /**
   * Guardrails applying to this session, already ordered by authority and
   * compiled. Additive: a runtime that predates guardrails ignores the field,
   * so no `schema_version` bump is warranted.
   */
  guardrails: z.array(PreparedGuardrail).default([]),
  /**
   * Session-scoped guardrail counters, restored. Handing these back at every
   * preparation is what stops a cap being reset by forcing a runner restart.
   */
  guardrail_state: z.record(z.string(), z.unknown()).default({}),
});

export type PreparedAgentSnapshot = z.infer<typeof PreparedAgentSnapshotSchema>;
export type PreparedExecutionV2 = z.infer<typeof PreparedExecutionV2Schema>;

export const internalContract = c.router({
  effectiveGuardrails: {
    method: 'GET',
    path: '/internal/v1/guardrails/effective',
    query: z.object({
      principal_id: idString('ses'),
      request_id: z.string().min(1),
      traffic_kind: z.string().min(1),
      runtime_config_revision: z.string().min(1),
      'scope.org_id': z.string().min(1),
      'scope.workspace_id': z.string().min(1),
      'scope.session_id': idString('ses'),
      'scope.agent_id': idString('agt').optional(),
    }),
    responses: {
      200: z.object({
        schema: z.literal('1'),
        bundle_id: z.string().min(1),
        generation: z.number().int().nonnegative(),
        scope: z.record(z.string(), z.string()),
        runtime_config_revision: z.string().min(1),
        issued_at: isoTimestamp,
        expires_at: isoTimestamp,
        seed_verdict: z.literal('allow'),
        guardrails: z.array(PreparedGuardrail),
        guardrail_state: z.object({ session: z.record(z.string(), z.unknown()) }),
      }),
      400: z.object({ error: z.string() }),
      403: z.object({ error: z.string() }),
      404: z.object({ error: z.string() }),
      409: z.object({ error: z.string() }),
      503: z.object({ error: z.string() }),
    },
  },
  resolveAgentObservabilityContext: {
    method: 'POST',
    path: '/internal/v1/workspaces/:workspaceId/sessions/:sessionId/agent-observability/context/resolve',
    pathParams: InternalAgentObservabilityContextPathParamsSchema,
    body: AgentObservabilityResolverEmptyBodySchema,
    responses: {
      200: AgentObservabilitySessionContextSchema,
      400: AgentObservabilityContextBadRequestSchema,
      404: AgentObservabilityContextNotFoundSchema,
      503: AgentObservabilityContextUnavailableSchema,
    },
  },
  resolveAgentObservabilitySecret: {
    method: 'POST',
    path: '/internal/v1/workspaces/:workspaceId/sessions/:sessionId/agent-observability/secret/resolve',
    pathParams: InternalAgentObservabilitySecretPathParamsSchema,
    body: AgentObservabilityResolverEmptyBodySchema,
    responses: {
      200: AgentObservabilitySecretResolutionSchema,
      400: AgentObservabilitySecretBadRequestSchema,
      404: AgentObservabilityContextNotFoundSchema,
      409: AgentObservabilitySecretDeniedSchema,
      503: AgentObservabilityContextUnavailableSchema,
    },
  },
  resolveMcpDestination: {
    method: 'POST',
    path: '/internal/v1/workspaces/:workspaceId/sessions/:sessionId/mcp-destination/resolve',
    pathParams: z.object({
      workspaceId: z.string().min(1),
      sessionId: idString('ses'),
    }),
    body: z.object({ backend: z.string().min(1) }).strict(),
    responses: {
      200: z
        .object({
          url: z
            .string()
            .url()
            .refine((url) => /^https?:\/\//i.test(url), {
              message: 'url must use HTTP(S)',
            })
            .refine(
              (url) => {
                const parsed = new URL(url);
                return parsed.username === '' && parsed.password === '' && parsed.hash === '';
              },
              {
                message: 'url must not contain userinfo or a fragment',
              },
            ),
          credential_id: idString('vcrd').nullable(),
          revision: z.number().int().positive().safe(),
        })
        .strict(),
      400: z.object({ error: z.string() }),
      404: z.object({ error: z.string() }),
      409: z.object({
        error: z.literal('invalid_runtime_binding'),
        resource_type: z.string(),
        resource_id: z.string(),
      }),
    },
  },
  executionOwner: {
    method: 'GET',
    path: '/internal/v1/workspaces/:workspaceId/sessions/:sessionId/execution-owner',
    pathParams: z.object({ workspaceId: z.string().min(1), sessionId: idString('ses') }),
    responses: {
      200: z.object({ owner: z.enum(['registry', 'harness-server']) }),
      400: z.object({ error: z.string() }),
      404: z.object({ error: z.string() }),
      409: z.object({ error: z.string() }),
    },
  },
  prepareExecution: {
    method: 'POST',
    path: '/internal/v1/workspaces/:workspaceId/sessions/:sessionId/executions:prepare',
    pathParams: z.object({
      workspaceId: z.string().min(1),
      sessionId: idString('ses'),
    }),
    body: z.object({}).strict(),
    responses: {
      200: PreparedExecutionV2Schema,
      400: z.object({ error: z.string() }),
      404: z.object({ error: z.string() }),
      409: z.object({
        error: z.literal('invalid_runtime_binding'),
        resource_type: z.string(),
        resource_id: z.string(),
      }),
      503: z.object({ error: z.string() }),
    },
  },
  harnessTurn: {
    method: 'POST',
    path: '/internal/v1/workspaces/:workspaceId/sessions/:sessionId/harness-turn',
    pathParams: z.object({ workspaceId: z.string().min(1), sessionId: idString('ses') }),
    body: HarnessTurnRequestSchema,
    responses: {
      200: z
        .object({
          runtimeRevision: z.number().int().positive(),
          ownershipRevision: z.number().int().nonnegative(),
          state: z.unknown(),
          receipt: HarnessTurnReceiptSchema.nullable(),
          guardrailState: z.record(z.string(), z.unknown()),
        })
        .nullable(),
      400: z.object({ error: z.string() }),
      404: z.object({ error: z.string() }),
      409: z.union([
        z.object({
          error: z.literal('invalid_runtime_binding'),
          resource_type: z.string(),
          resource_id: z.string(),
        }),
        z.object({ error: z.string() }),
      ]),
    },
  },
  saveHarnessState: {
    method: 'POST',
    path: '/internal/v1/workspaces/:workspaceId/sessions/:sessionId/harness-state',
    pathParams: z.object({ workspaceId: z.string().min(1), sessionId: idString('ses') }),
    body: z
      .object({
        runtime_revision: z.number().int().positive(),
        expected_checkpoint_revision: z
          .string()
          .regex(/^[a-f0-9]{64}$/)
          .nullable(),
        state: z.unknown(),
      })
      .strict(),
    responses: {
      200: z.object({ checkpoint_revision: z.string().regex(/^[a-f0-9]{64}$/) }),
      400: z.object({ error: z.string() }),
      404: z.object({ error: z.string() }),
      409: z.object({ error: z.string() }),
    },
  },
  updateSessionState: {
    method: 'PATCH',
    path: '/internal/v1/workspaces/:workspaceId/sessions/:id/state',
    pathParams: z.object({ workspaceId: z.string().min(1), id: idString('ses') }),
    body: z.object({
      status: z.enum(['idle', 'running', 'rescheduling', 'terminated']),
      sandbox_handle_id: z.string().nullable().optional(),
    }),
    responses: {
      200: InternalSession,
      400: z.object({ error: z.string() }),
      404: z.object({ error: z.string() }),
    },
  },
  recordSessionUsage: {
    method: 'POST',
    path: '/internal/v1/workspaces/:workspaceId/sessions/:id/usage',
    pathParams: z.object({ workspaceId: z.string().min(1), id: idString('ses') }),
    body: z.object({
      usage: z.object({
        cache_creation: z
          .object({
            ephemeral_1h_input_tokens: z.number().int().nonnegative().optional(),
            ephemeral_5m_input_tokens: z.number().int().nonnegative().optional(),
          })
          .optional(),
        cache_read_input_tokens: z.number().int().nonnegative().optional(),
        input_tokens: z.number().int().nonnegative().optional(),
        output_tokens: z.number().int().nonnegative().optional(),
      }),
      /**
       * The model that produced this delta. Registry prices the delta as it
       * arrives, so a session that changes model mid-run is priced at the rate
       * that applied when each portion was incurred. Omitting it records the
       * tokens and no cost — unpriced, never $0.00.
       */
      model: z.string().min(1).optional(),
      /**
       * Which vendor served that model. Model identity is the
       * `{ provider, id }` pair, because the same id served through two
       * providers can carry different rates. Optional: a harness that names
       * only the id is priced against the deployment's default provider, which
       * is what every report before this field existed meant.
       */
      provider: z.string().min(1).optional(),
      /**
       * The thread the delta belongs to. A dispatched subagent runs on its own
       * thread; naming it is what makes per-subagent spend attributable.
       */
      thread_id: idString('sth').optional(),
      /** Managed subagent whose model call produced the delta. */
      subagent_id: idString('agt').optional(),
      /** Turn whose Registry-authenticated principal owns cross-session spend. */
      turn_event_id: idString('evt').optional(),
      /** Stable sandbox event id; duplicate delivery returns the original totals without charging twice. */
      usage_event_id: idString('evt').optional(),
    }),
    responses: {
      200: InternalSession,
      400: z.object({ error: z.string() }),
      404: z.object({ error: z.string() }),
    },
  },
  refreshGuardrailSubjectWindow: {
    method: 'POST',
    path: '/internal/v1/workspaces/:workspaceId/sessions/:id/guardrail-subject-window',
    pathParams: z.object({ workspaceId: z.string().min(1), id: idString('ses') }),
    body: z.object({ turn_event_id: idString('evt') }),
    responses: {
      200: z.object({ guardrail_subject_window_state: z.record(z.string(), z.unknown()) }),
      400: z.object({ error: z.string() }),
      404: z.object({ error: z.string() }),
    },
  },
  /**
   * Flush a batch of guardrail state updates (internal listener only, behind
   * its workload authentication in `src/auth/internal-auth.ts`). Each update is
   * applied as an atomic SQL delta — an increment is a conflicting upsert that
   * adds, so two writers cannot lose each other's update.
   *
   * `turn` scope is deliberately absent from `scope`: turn state is never
   * persisted, and accepting it here would silently drop the write. The route
   * rejects it (400) rather than acknowledging a write it will not make.
   */
  applyGuardrailState: {
    method: 'POST',
    path: '/internal/v1/workspaces/:workspaceId/sessions/:id/guardrail-state',
    pathParams: z.object({ workspaceId: z.string().min(1), id: idString('ses') }),
    body: z
      .object({
        /**
         * Who a `subject_window` counter belongs to, and which window. Both are
         * required whenever the batch carries a `subject_window` update: they
         * are part of that counter's key, and the caller — not Registry — owns
         * the window boundary its evaluation used.
         */
        subject: z.string().min(1).max(256).optional(),
        window: z.string().min(1).max(64).optional(),
        updates: z.array(
          z
            .object({
              scope: z.enum(['session', 'subject_window']),
              key: z.string().min(1).max(256),
              action: z.enum(['set', 'increment', 'delete', 'append']),
              value: z.unknown().optional(),
            })
            .strict(),
        ),
      })
      .strict(),
    responses: {
      200: z.object({ applied: z.number().int().nonnegative() }).strict(),
      400: z.object({ error: z.string() }),
      404: z.object({ error: z.string() }),
    },
  },
  resolveVaultCredential: {
    method: 'POST',
    path: '/internal/v1/workspaces/:workspaceId/sessions/:sessionId/vault-credentials/:id/resolve',
    pathParams: z.object({
      workspaceId: z.string().min(1),
      sessionId: idString('ses'),
      id: requestedVaultCredentialId,
    }),
    body: z
      .object({
        credential_id: requestedVaultCredentialId,
        // Legacy gateway alias: this repeats credential_id and is not the
        // Registry-managed vlt_* resource returned in the response.
        vault_id: requestedVaultCredentialId,
        force_refresh: z.boolean().optional(),
      })
      .strict(),
    responses: {
      200: z.union([
        z.object({
          credential_id: idString('vcrd'),
          vault_id: idString('vlt'),
          version: z.string(),
          scheme: z.enum(PROVIDER_CREDENTIAL_SCHEMES),
          secret_value: z.string(),
          ttl_seconds: z.number(),
        }),
        z.object({
          credential_id: idString('vcrd'),
          vault_id: idString('vlt'),
          version: z.string(),
          auth_type: z.literal('environment_variable'),
          secret_name: z.string(),
          secret_value: z.string(),
          networking: z.unknown(),
          ttl_seconds: z.number(),
        }),
      ]),
      400: z.object({ error: z.string() }),
      404: z.object({ error: z.string() }),
      409: z.object({ error: z.string() }),
      503: z.object({ error: z.string() }),
    },
  },
  resolveGitCredential: {
    method: 'POST',
    path: '/internal/v1/workspaces/:workspaceId/sessions/:sessionId/git-credentials/:id/resolve',
    pathParams: z.object({
      workspaceId: z.string().min(1),
      sessionId: idString('ses'),
      id: idString('gitcred'),
    }),
    body: z.object({}).strict(),
    responses: {
      200: z.object({
        git_credential_id: idString('gitcred'),
        provider: z.literal('github'),
        repo_url: z.string(),
        secret_value: z.string(),
        ttl_seconds: z.number(),
      }),
      404: z.object({ error: z.string() }),
    },
  },
  mintSessionJwt: {
    method: 'POST',
    path: '/internal/v1/workspaces/:workspaceId/sessions/:id/mint-jwt',
    pathParams: z.object({ workspaceId: z.string().min(1), id: idString('ses') }),
    body: z.object({
      mcp_server_names: z.array(z.string()).default([]),
      vault_ids: z.array(idString('vlt')).default([]),
      /**
       * Optional audience override. When set, the minter signs the JWT
       * with this `aud` claim (e.g. `'git-creds'`) instead of the configured
       * default (`'ai-gateway'`).
       */
      audience: z.string().min(1).optional(),
      /**
       * Optional `repo_urls` allowlist that the minter encodes as a
       * custom JWT claim. The `/v1/git-creds` route compares incoming repo
       * URLs against this list, so a leaked token can only fetch credentials
       * for repos the registry explicitly authorized at session-spawn.
       */
      repo_urls: z.array(z.string().url()).optional(),
    }),
    responses: {
      200: z.object({
        token: z.string(),
        expires_at: z.number(),
      }),
      404: z.object({ error: z.string() }),
    },
  },
  /**
   * Internal env-key verification: checks a raw `sk-…` env key against the
   * environment's stored digest + expiry (rejecting archived environments), the
   * same check the worker tunnel route runs in-process when a worker dials; the
   * tunnel does not call this route. Fails closed: an unknown id or a
   * wrong/expired/revoked key returns `{ valid: false }` with no workspace
   * leaked and no existence oracle. Served only on the internal listener, behind
   * its workload authentication (`src/auth/internal-auth.ts`).
   */
  verifyEnvKey: {
    method: 'POST',
    path: '/internal/environments/:id/verify-key',
    pathParams: z.object({ id: idString('env') }),
    body: z.object({ env_key: z.string().min(1) }),
    responses: {
      200: z.union([
        z.object({ valid: z.literal(true), workspace_id: z.string() }),
        z.object({ valid: z.literal(false) }),
      ]),
      400: z.object({ error: z.string() }),
    },
  },
  /**
   * Internal-listener **environment-claim** routes (workload-authenticated,
   * `src/auth/internal-auth.ts`) — the durable, multi-replica
   * equivalent of an in-memory tunnel/host registry. An environment is claimed
   * by exactly one registry pod at a time so a worker's tunnel terminates on the
   * replica that owns its environment. The decision logic + Drizzle wrappers
   * live in `src/domain/environment-claims.ts`; these declare the wire shapes.
   *
   * `claimEnvironment` is **newest-wins**: a fresh claim unconditionally
   * replaces any existing one, so a reconnecting/relocated worker takes over
   * from a pod that lagged on cleanup rather than being rejected.
   */
  claimEnvironment: {
    method: 'PUT',
    path: '/internal/environments/:id/claim',
    pathParams: z.object({ id: idString('env') }),
    body: z.object({
      owner_pod: z.string().min(1),
      worker_conn_id: workerConnId,
    }),
    responses: {
      200: InternalEnvironmentClaim,
      400: z.object({ error: z.string() }),
    },
  },
  /**
   * Advance a claim's heartbeat watermark (`last_ping`). Connection-scoped: a
   * ping from a connection that has since been taken over (newest-wins) is a
   * no-op and returns `refreshed: false` — it must not resurrect a stale owner.
   */
  heartbeatEnvironmentClaim: {
    method: 'POST',
    path: '/internal/environments/:id/claim/heartbeat',
    pathParams: z.object({ id: idString('env') }),
    body: z.object({ worker_conn_id: workerConnId }),
    responses: {
      200: z.object({ refreshed: z.boolean() }),
      400: z.object({ error: z.string() }),
    },
  },
  /**
   * Release a claim, **connection-scoped** (the worker teardown path). Drops the
   * row only when the releasing worker still owns it — symmetric with the
   * heartbeat guard. A worker whose claim was already taken over (newest-wins)
   * is a no-op (`released: false`), so its teardown can never delete the live
   * owner's claim and unclaim an environment that still has a live owner.
   */
  releaseEnvironmentClaim: {
    method: 'POST',
    path: '/internal/environments/:id/claim/release',
    pathParams: z.object({ id: idString('env') }),
    body: z.object({ worker_conn_id: workerConnId }),
    responses: {
      200: z.object({ released: z.boolean() }),
      400: z.object({ error: z.string() }),
    },
  },
  /** Read the current claim owner, or `null` when the environment is unclaimed. */
  getEnvironmentClaim: {
    method: 'GET',
    path: '/internal/environments/:id/claim',
    pathParams: z.object({ id: idString('env') }),
    responses: {
      200: z.object({ claim: InternalEnvironmentClaim.nullable() }),
      400: z.object({ error: z.string() }),
    },
  },
  /**
   * Background sweeper: bulk-delete every claim whose heartbeat is older than
   * the staleness TTL (`last_ping < now - ttl`). This is what gives the TTL
   * effect on its own — without it a dead owner's row lingers until a *new*
   * worker claims the same environment. A registry pod runs this on an interval.
   * Internal listener only.
   */
  reapEnvironmentClaims: {
    method: 'POST',
    path: '/internal/environments/claims/reap',
    body: z.object({}).strict(),
    responses: {
      200: z.object({ reaped: z.number().int().nonnegative() }),
    },
  },
  /**
   * Internal-listener endpoint used by the harness's output indexer to
   * register session-output blobs as `File` rows. Multipart body
   * mirrors the public `POST /v1/files`, but unlike that route this one
   * derives workspace, output purpose, and session scope from the path.
   */
  createFile: {
    method: 'POST',
    path: '/internal/v1/workspaces/:workspaceId/sessions/:sessionId/files',
    pathParams: z.object({
      workspaceId: z.string().min(1),
      sessionId: idString('ses'),
    }),
    body: z.unknown(),
    responses: {
      201: File,
      400: z.object({ error: z.string() }),
      413: z.object({ error: z.string() }),
    },
    contentType: 'multipart/form-data',
  },
  /**
   * Internal-listener endpoint used by the harness's `MemoryVersionWatcher` to
   * register a memory version it observed in a mounted store.
   * Unlike the public PATCH (which 409s on CAS mismatch to give callers
   * Anthropic's published precondition contract), this route falls back to
   * last-writer-wins on `MemoryConflictError` — the response carries
   * `conflict: true` so the watcher can emit a `session.memory_conflict`
   * event on the transcript stream.
   */
  recordMemoryVersion: {
    method: 'POST',
    path: '/internal/v1/workspaces/:workspaceId/sessions/:sessionId/memory-stores/:storeId/memory-versions',
    pathParams: z.object({
      workspaceId: z.string().min(1),
      sessionId: idString('ses'),
      storeId: idString('mems'),
    }),
    body: z.object({
      path: z.string().min(1).max(1024),
      content_base64: z.string(),
      content_sha256: z.string().regex(/^[0-9a-f]{64}$/),
      previous_sha256: z
        .string()
        .regex(/^[0-9a-f]{64}$/)
        .nullable(),
      version_id: idString('memver').optional(),
      written_by_event_id: z.string().optional(),
    }),
    responses: {
      201: z.object({
        memory: InternalMemory,
        version: InternalMemoryVersion,
        conflict: z.boolean(),
      }),
      400: z.object({ error: z.string() }),
      404: z.object({ error: z.string() }),
      413: z.object({ error: z.string() }),
    },
  },
});
