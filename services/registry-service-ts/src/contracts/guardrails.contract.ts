// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { responseHeaderMetadata } from './openapi-headers.js';
import { initContract } from '@ts-rest/core';
import { z } from 'zod';
import {
  MAX_EXPRESSION_LENGTH,
  PHASES,
  GUARDRAIL_MODES,
  STATE_SCOPES,
  SCOPES,
  compileGuardrailRule,
  getGuardrailType,
  isEnforced,
  supportsAsk,
  type GuardrailRule,
  type Phase,
  type Scope,
} from '@orca/guardrails';
import { ClaudeErrorResponse, idString, isoTimestamp, pagination } from './common.js';
import { Metadata, MetadataPatch } from './metadata.js';

const c = initContract();

/**
 * Guardrails are an Orca-native policy surface rather than part of the Claude
 * Managed Agents beta, so they live under their own API group instead of
 * squatting on `/v1`. Keeping them off `/v1` means a client can tell, from the
 * path alone, which requests are portable and which are ours.
 */
export const GUARDRAILS_API_PREFIX = '/apis/policy.runorca.ai/v1';

export const GuardrailPhase = z.enum(PHASES);
export const GuardrailScope = z.enum(SCOPES);
export const GuardrailMode = z.enum(GUARDRAIL_MODES);

/** The scopes a workspace principal may write. `organization` is not one. */
export const WORKSPACE_WRITABLE_SCOPES: readonly z.infer<typeof GuardrailScope>[] = [
  'workspace',
  'explicit',
];

const BuiltinRuleWire = z
  .object({
    kind: z.literal('builtin'),
    builtin: z.string().min(1).max(200),
    params: z.record(z.string(), z.unknown()).optional(),
  })
  .strict();

const ExpressionRuleWire = z
  .object({
    kind: z.literal('expression'),
    expression: z.string().min(1).max(MAX_EXPRESSION_LENGTH),
    // `allow` is absent deliberately: a guardrail that allows when its
    // predicate is false has no effect, so it is not a verdict anyone can pick.
    on_false: z.enum(['ask', 'deny']),
    reason: z.string().max(1024).optional(),
  })
  .strict();

const McpManifest = z
  .object({
    server: z.string().min(1),
    url: z.string().url(),
    tools: z.array(z.record(z.string(), z.unknown())).max(1000),
  })
  .strict();
const DogwoodRuleWire = z
  .object({
    kind: z.literal('dogwood'),
    policy: z.string().min(1).max(65536),
    applies_to: z.enum(['all_tools', 'mcp_tools']).optional(),
    history: z.enum(['session', 'principal']).optional(),
    mcp_manifests: z.array(McpManifest).max(64).optional(),
  })
  .strict();
const StoredDogwoodRule = z
  .object({
    kind: z.literal('dogwood'),
    policy: z.string(),
    appliesTo: z.enum(['all_tools', 'mcp_tools']).optional(),
    history: z.enum(['session', 'principal']).optional(),
    mcpManifests: z.array(McpManifest).optional(),
  })
  .strict();

export const GuardrailRuleWire = z.discriminatedUnion('kind', [
  BuiltinRuleWire,
  ExpressionRuleWire,
  DogwoodRuleWire,
]);

export type GuardrailRuleWireValue = z.infer<typeof GuardrailRuleWire>;

/**
 * The rule as it is persisted: exactly the shape `@orca/guardrails` evaluates.
 * Storing the evaluated shape means the harness reads a row and hands it to the
 * engine, with no second translation that could drift from this one.
 */
const StoredBuiltinRule = z
  .object({
    kind: z.literal('builtin'),
    builtin: z.string(),
    params: z.record(z.string(), z.unknown()).optional(),
  })
  .strict();

const StoredExpressionRule = z
  .object({
    kind: z.literal('expression'),
    expression: z.string(),
    onFalse: z.enum(['ask', 'deny']),
    reason: z.string().optional(),
  })
  .strict();

export const StoredGuardrailRule = z.discriminatedUnion('kind', [
  StoredBuiltinRule,
  StoredExpressionRule,
  StoredDogwoodRule,
]);

export const Guardrail = z.object({
  id: idString('grd'),
  type: z.literal('guardrail'),
  name: z.string().min(1).max(200),
  description: z.string(),
  enabled: z.boolean(),
  mode: GuardrailMode,
  phases: z.array(GuardrailPhase).min(1),
  scope: GuardrailScope,
  rule: z.union([
    GuardrailRuleWire,
    z
      .object({
        kind: z.string().refine((kind) => !['builtin', 'expression', 'dogwood'].includes(kind)),
      })
      .passthrough(),
  ]),
  warnings: z.array(z.string()).optional(),
  metadata: Metadata,
  archived_at: isoTimestamp.nullable(),
  created_at: isoTimestamp,
  updated_at: isoTimestamp,
});

export const GuardrailCreate = z
  .object({
    name: z.string().min(1).max(200),
    description: z.string().max(1024).nullable().optional(),
    enabled: z.boolean().optional(),
    mode: GuardrailMode.optional(),
    // Omitted means "wherever this rule fires", which only a builtin can
    // answer; see `resolveGuardrailAuthoring`.
    phases: z.array(GuardrailPhase).min(1).optional(),
    // Every scope parses. Whether the caller may *write* the one they asked for
    // is an authorization question answered by the listener — deciding it here
    // would report a permission failure as a malformed request.
    scope: GuardrailScope.optional(),
    rule: GuardrailRuleWire,
    metadata: Metadata.optional(),
  })
  .strict();

export const GuardrailUpdate = z
  .object({
    name: z.string().min(1).max(200).optional(),
    description: z.string().max(1024).nullable().optional(),
    enabled: z.boolean().optional(),
    mode: GuardrailMode.optional(),
    phases: z.array(GuardrailPhase).min(1).optional(),
    scope: GuardrailScope.optional(),
    rule: GuardrailRuleWire.optional(),
    metadata: MetadataPatch.optional(),
  })
  .strict();

export const GuardrailDeleted = z.object({
  id: idString('grd'),
  type: z.literal('guardrail_deleted'),
});

/** One entry of the served builtin catalog. Passed through verbatim. */
export const GuardrailType = z.object({
  name: z.string(),
  title: z.string(),
  description: z.string(),
  phases: z.array(GuardrailPhase),
  stateful: z.boolean(),
  stateScope: z.enum(STATE_SCOPES).optional(),
  verdicts: z.array(z.enum(['ask', 'deny'])),
  paramsSchema: z.record(z.string(), z.unknown()),
});

export const PreparedGuardrailWire = z
  .object({
    id: idString('grd'),
    name: z.string().min(1),
    tier: z.enum(['session', 'agent', 'workspace', 'organization']),
    phases: z.array(z.string().min(1)),
    rule: z.unknown(),
    stateful: z.boolean(),
    mode: GuardrailMode.optional(),
    state_scope: z.enum(STATE_SCOPES).optional(),
    subagent_id: z.string().min(1).optional(),
  })
  .strict();

export const WorkspaceGuardrailBundle = z
  .object({
    schema: z.literal('1'),
    bundle_id: z.string().min(1),
    generation: z.number().int().nonnegative(),
    scope: z.object({ org_id: z.string().min(1), workspace_id: z.string().min(1) }),
    runtime_config_revision: z.string().min(1),
    issued_at: isoTimestamp,
    expires_at: isoTimestamp,
    seed_verdict: z.literal('allow'),
    guardrails: z.array(PreparedGuardrailWire.extend({ mode: GuardrailMode })),
    omitted_rules: z.number().int().nonnegative(),
  })
  .strict();

export const WorkspaceBundleQuery = z
  .object({
    rule_kinds: z.string().min(1).max(200).optional(),
    guardrail_capabilities: z.string().min(1).max(512).optional(),
  })
  .strict();

export const GuardrailLeaseResponseHeaders = {
  ETag: {
    description: 'Quoted policy content identity, independent of lease renewal.',
    required: true,
    schema: { type: 'string', pattern: '^"registry-[a-f0-9]{64}"$' },
  },
  'Cache-Control': {
    description: 'Origin exchanges are never renewed by an HTTP cache.',
    required: true,
    schema: { type: 'string', enum: ['no-store'] },
  },
  'Orca-Guardrail-Issued-At': {
    description: 'Registry UTC lease issue time.',
    required: true,
    schema: { type: 'string', format: 'date-time' },
  },
  'Orca-Guardrail-Expires-At': {
    description: 'Exclusive expiry, exactly one hour after issue.',
    required: true,
    schema: { type: 'string', format: 'date-time' },
  },
  'Orca-Guardrail-Generation': {
    description: 'Issue time as integer epoch milliseconds.',
    required: true,
    schema: { type: 'string', pattern: '^[0-9]+$' },
  },
};

export const guardrailsContract = c.router({
  dogwoodSchema: {
    method: 'GET',
    path: `${GUARDRAILS_API_PREFIX}/policyschemas/dogwood`,
    responses: {
      200: z.object({
        base_schema: z.string(),
        event_schemas: z.object({ session: z.string(), principal: z.string() }),
        language_version: z.string(),
        core_version: z.string(),
        limits: z.record(z.string(), z.unknown()),
      }),
      403: ClaudeErrorResponse,
    },
  },
  effective: {
    metadata: responseHeaderMetadata({
      200: GuardrailLeaseResponseHeaders,
      304: GuardrailLeaseResponseHeaders,
    }),
    method: 'GET',
    path: `${GUARDRAILS_API_PREFIX}/effectiveguardrails`,
    query: WorkspaceBundleQuery,
    headers: z.object({ 'if-none-match': z.string().optional() }).passthrough(),
    responses: {
      200: WorkspaceGuardrailBundle,
      304: c.noBody(),
      400: ClaudeErrorResponse,
      403: ClaudeErrorResponse,
      503: ClaudeErrorResponse,
    },
  },
  create: {
    method: 'POST',
    path: `${GUARDRAILS_API_PREFIX}/guardrails`,
    body: GuardrailCreate,
    responses: { 201: Guardrail, 400: ClaudeErrorResponse, 403: ClaudeErrorResponse },
    headers: z.object({ 'idempotency-key': z.string().optional() }).passthrough(),
  },
  list: {
    method: 'GET',
    path: `${GUARDRAILS_API_PREFIX}/guardrails`,
    query: pagination.extend({ include_archived: z.boolean().optional() }),
    responses: {
      200: z.object({ data: z.array(Guardrail), next_page: z.string().nullable() }),
      400: ClaudeErrorResponse,
    },
  },
  get: {
    method: 'GET',
    path: `${GUARDRAILS_API_PREFIX}/guardrails/:id`,
    pathParams: z.object({ id: idString('grd') }),
    responses: { 200: Guardrail, 404: ClaudeErrorResponse },
  },
  update: {
    method: 'POST',
    path: `${GUARDRAILS_API_PREFIX}/guardrails/:id`,
    pathParams: z.object({ id: idString('grd') }),
    body: GuardrailUpdate,
    responses: {
      200: Guardrail,
      400: ClaudeErrorResponse,
      403: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
    },
    headers: z.object({ 'idempotency-key': z.string().optional() }).passthrough(),
  },
  archive: {
    method: 'POST',
    path: `${GUARDRAILS_API_PREFIX}/guardrails/:id/archive`,
    pathParams: z.object({ id: idString('grd') }),
    body: z.object({}).strict(),
    responses: { 200: Guardrail, 403: ClaudeErrorResponse, 404: ClaudeErrorResponse },
    headers: z.object({ 'idempotency-key': z.string().optional() }).passthrough(),
  },
  delete: {
    method: 'DELETE',
    path: `${GUARDRAILS_API_PREFIX}/guardrails/:id`,
    pathParams: z.object({ id: idString('grd') }),
    body: z.object({}).strict(),
    responses: {
      200: GuardrailDeleted,
      403: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
      409: ClaudeErrorResponse,
    },
  },
  listTypes: {
    method: 'GET',
    path: `${GUARDRAILS_API_PREFIX}/guardrailtypes`,
    responses: { 200: z.object({ data: z.array(GuardrailType) }) },
  },
});

/** Rewrite an authored rule into the shape the engine evaluates. */
export function ruleToStorage(rule: GuardrailRuleWireValue): GuardrailRule {
  if (rule.kind === 'dogwood')
    return {
      kind: 'dogwood',
      policy: rule.policy,
      ...(rule.applies_to ? { appliesTo: rule.applies_to } : {}),
      ...(rule.history ? { history: rule.history } : {}),
      ...(rule.mcp_manifests ? { mcpManifests: rule.mcp_manifests } : {}),
    };
  if (rule.kind === 'expression') {
    return {
      kind: 'expression',
      expression: rule.expression,
      onFalse: rule.on_false,
      ...(rule.reason !== undefined ? { reason: rule.reason } : {}),
    };
  }
  return {
    kind: 'builtin',
    builtin: rule.builtin,
    ...(rule.params !== undefined ? { params: rule.params } : {}),
  };
}

/**
 * Project a persisted rule back onto the wire. Parsing rather than casting: a
 * row that does not match the stored shape is a bug we want to hear about, not
 * one we want to serve half-translated.
 */
export function ruleToWire(
  stored: unknown,
): GuardrailRuleWireValue | ({ kind: string } & Record<string, unknown>) {
  if (
    stored &&
    typeof stored === 'object' &&
    'kind' in stored &&
    typeof stored.kind === 'string' &&
    !['builtin', 'expression', 'dogwood'].includes(stored.kind)
  )
    return stored as { kind: string } & Record<string, unknown>;
  const rule = StoredGuardrailRule.parse(stored);
  if (rule.kind === 'dogwood')
    return {
      kind: 'dogwood',
      policy: rule.policy,
      ...(rule.appliesTo ? { applies_to: rule.appliesTo } : {}),
      ...(rule.history ? { history: rule.history } : {}),
      ...(rule.mcpManifests ? { mcp_manifests: rule.mcpManifests } : {}),
    };
  if (rule.kind === 'expression') {
    return {
      kind: 'expression',
      expression: rule.expression,
      on_false: rule.onFalse,
      ...(rule.reason !== undefined ? { reason: rule.reason } : {}),
    };
  }
  return {
    kind: 'builtin',
    builtin: rule.builtin,
    ...(rule.params !== undefined ? { params: rule.params } : {}),
  };
}

export interface GuardrailAuthoringError {
  /** Field the problem is attributable to, when there is one. */
  path?: string;
  message: string;
}

export interface ResolvedGuardrail {
  rule: GuardrailRule;
  phases: Phase[];
}

export type GuardrailAuthoringResult =
  | { ok: true; value: ResolvedGuardrail }
  | { ok: false; errors: GuardrailAuthoringError[] };

/**
 * Authoring-time validation, shared by both listeners.
 *
 * Everything a guardrail needs to evaluate is settled here, when a human is
 * present to read the error. A rule that cannot compile, a phase a builtin
 * never fires on, or a verdict the phase cannot resolve are all rejected now
 * rather than discovered mid-session, where the only choices left are to deny
 * or to let the request through unguarded.
 */
export function resolveGuardrailAuthoring(input: {
  rule: GuardrailRule;
  /**
   * The scope the guardrail will be stored at. Required rather than optional
   * because a builtin may declare `allowedScopes`, and that check only runs
   * when the scope is known — omitting it would accept at write time a rule
   * that composition later refuses to compile.
   */
  scope: Scope;
  phases?: readonly Phase[] | undefined;
}): GuardrailAuthoringResult {
  const { rule, scope } = input;
  const type = rule.kind === 'builtin' ? getGuardrailType(rule.builtin) : undefined;
  if (rule.kind === 'builtin' && (!type || type.internal)) {
    // An internal builtin is machinery, not something anyone authors, and the
    // served catalog omits it. Reporting it as unknown keeps the API's answer
    // consistent with the catalog a client validated against.
    return { ok: false, errors: [{ message: `unknown guardrail type: ${rule.builtin}` }] };
  }

  const compiled = compileGuardrailRule(rule, scope);
  if (!compiled.ok) {
    return {
      ok: false,
      errors: compiled.errors.map((error) => ({
        ...(error.path !== undefined ? { path: error.path } : {}),
        message: error.message,
      })),
    };
  }

  const phases = resolvePhases(
    input.phases,
    rule.kind === 'dogwood' ? ['tool_call', 'tool_result'] : type?.phases,
  );
  if ('error' in phases) return { ok: false, errors: [phases.error] };

  if (rule.kind === 'expression' && rule.onFalse === 'ask') {
    const unresolvable = phases.value.filter((phase) => !supportsAsk(phase));
    if (unresolvable.length > 0) {
      return {
        ok: false,
        errors: [
          {
            path: 'on_false',
            message:
              `on_false 'ask' has no approval exchange at ${unresolvable.join(', ')}; ` +
              'use deny or drop the phase',
          },
        ],
      };
    }
  }

  return { ok: true, value: { rule, phases: phases.value } };
}

function resolvePhases(
  requested: readonly Phase[] | undefined,
  catalogPhases: readonly Phase[] | undefined,
): { value: Phase[] } | { error: GuardrailAuthoringError } {
  if (!requested) {
    if (!catalogPhases) {
      return {
        error: {
          path: 'phases',
          message: 'phases is required: an expression rule has no declared evaluation points',
        },
      };
    }
    return { value: [...catalogPhases] };
  }
  if (requested.length === 0) {
    return { error: { path: 'phases', message: 'phases must contain at least one phase' } };
  }
  if (catalogPhases) {
    const outside = requested.filter((phase) => !catalogPhases.includes(phase));
    if (outside.length > 0) {
      return {
        error: {
          path: 'phases',
          message:
            `this rule does not fire at ${outside.join(', ')}; ` +
            `it fires at ${catalogPhases.join(', ')}`,
        },
      };
    }
  }
  // Runs after the catalog check, which names where the rule *does* fire and
  // is the more useful answer whenever it applies. What reaches here is a
  // phase the rule genuinely declares — or an expression rule, which has no
  // catalog entry to be screened against at all.
  //
  // Only an *explicit* request is screened. A builtin's catalog default may
  // name a phase this tree does not fire yet, and storing it verbatim is what
  // lets the rule start working when that enforcement point lands, with no
  // row to migrate. Asking for one by hand can only ever produce a guardrail
  // that never evaluates.
  const unfired = requested.filter((phase) => !isEnforced(phase));
  if (unfired.length > 0) {
    return {
      error: {
        path: 'phases',
        message:
          `no enforcement point fires ${unfired.join(', ')}, ` +
          'so a guardrail declaring it would never evaluate',
      },
    };
  }
  return { value: [...requested] };
}
