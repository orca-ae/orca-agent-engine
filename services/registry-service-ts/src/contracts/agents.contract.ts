// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { initContract } from '@ts-rest/core';
import { z } from 'zod';
import { ClaudeErrorResponse, idString, isoTimestamp, pagination } from './common.js';
import { ModelInput, ModelOutput } from './model-wire.js';
import { Metadata, MetadataOutput, MetadataPatch } from './metadata.js';

const c = initContract();

const PermissionPolicyName = z.enum(['always_allow', 'always_ask']);
const PermissionPolicy = z.object({
  type: PermissionPolicyName,
});
// Keep in sync with ORCA_MCP_TOOL_LOGICAL_NAMES in
// services/harness-server/src/harness/claude/mcp-tools.ts. The registry cannot
// import harness-server without coupling the two service packages.
const RESERVED_CUSTOM_TOOL_NAMES = new Set([
  'bash',
  'read',
  'write',
  'edit',
  'list',
  'delete',
  'glob',
  'grep',
  'web_fetch',
  'web_search',
]);
const SKILL_REFS_ERROR =
  'skills entries must be {type:"anthropic", skill_id, version?} or {type:"custom", skill_id, version?}';
export const MAX_AGENT_SKILL_REFS = 500;
export const MAX_AGENT_SKILL_REFS_ERROR = `skills must contain at most ${MAX_AGENT_SKILL_REFS} entries`;
export const CustomSkillVersionSelector = z.string().regex(/^(?:latest|[1-9]\d*)$/, {
  message:
    'custom skill reference version must be "latest" or a decimal timestamp version identifier',
});

const ToolsetConfig = z
  .object({
    enabled: z.boolean().nullable().optional(),
    permission_policy: PermissionPolicy.nullable().optional(),
  })
  .passthrough();

const ToolsetEntryConfig = ToolsetConfig.extend({
  name: z.string(),
});

const ToolsetDefaultConfig = ToolsetConfig;
const ToolsetConfigs = z.union([z.array(ToolsetEntryConfig), z.record(z.string(), ToolsetConfig)]);
const CustomToolInputSchema = z
  .object({
    type: z.literal('object'),
    properties: z.record(z.string(), z.unknown()).nullable().optional(),
    required: z.array(z.string()).nullable().optional(),
  })
  .passthrough();

export const ToolDef = z.discriminatedUnion('type', [
  z
    .object({
      type: z.literal('agent_toolset'),
      default_config: ToolsetDefaultConfig.nullable().optional(),
      configs: ToolsetConfigs.optional(),
    })
    .passthrough(),
  z
    .object({
      type: z.literal('agent_toolset_20260401'),
      default_config: ToolsetDefaultConfig.nullable().optional(),
      configs: ToolsetConfigs.optional(),
    })
    .passthrough(),
  z
    .object({
      type: z.literal('mcp_toolset'),
      mcp_server_name: z.string(),
      default_config: ToolsetDefaultConfig.nullable().optional(),
      configs: ToolsetConfigs.optional(),
    })
    .passthrough(),
  z
    .object({
      type: z.literal('custom'),
      name: z.string().refine((name) => !RESERVED_CUSTOM_TOOL_NAMES.has(name), {
        message: 'custom tool name is reserved by agent_toolset',
      }),
      description: z.string().min(1).max(4096),
      input_schema: CustomToolInputSchema,
    })
    .passthrough(),
]);

export const McpServer = z
  .object({
    name: z.string(),
    type: z.literal('url'),
    url: z.string().url(),
  })
  .passthrough();

const McpServerCreate = z
  .object({
    name: z.string(),
    type: z.literal('url').optional(),
    url: z.string().url(),
  })
  .passthrough();

export const AgentSkillRef = z.discriminatedUnion(
  'type',
  [
    z.object({
      type: z.literal('anthropic'),
      skill_id: z.string().min(1),
      version: z.string().min(1).nullable().optional(),
    }),
    z.object({
      type: z.literal('custom'),
      skill_id: idString('skill'),
      version: CustomSkillVersionSelector.nullable().optional(),
    }),
  ],
  { errorMap: () => ({ message: SKILL_REFS_ERROR }) },
);

export const AgentSkillRefs = z
  .array(AgentSkillRef, { invalid_type_error: SKILL_REFS_ERROR })
  .max(MAX_AGENT_SKILL_REFS, { message: MAX_AGENT_SKILL_REFS_ERROR });

const ResolvedAgentSkillRef = z.discriminatedUnion('type', [
  z.object({
    type: z.literal('anthropic'),
    skill_id: z.string().min(1),
    version: z.string().min(1),
  }),
  z.object({
    type: z.literal('custom'),
    skill_id: idString('skill'),
    version: CustomSkillVersionSelector,
  }),
]);

const MultiagentRosterEntry = z.union([
  idString('agt'),
  z.object({
    type: z.literal('agent'),
    id: idString('agt'),
    version: z.number().int().positive().optional(),
  }),
  z.object({ type: z.literal('self') }),
]);

const Multiagent = z.object({
  type: z.literal('coordinator'),
  agents: z.array(MultiagentRosterEntry).min(1).max(20),
});

// Response schema. Six of these fields used to reach the generated OpenAPI as
// *optional*, because each carried a `.default(…)` — five of them here, and
// `metadata` through the shared request schema. `agentToApi` emits all six
// unconditionally and Anthropic marks all six required, so the default was
// describing a parse that never happens on this side of the wire.
const Agent = z.object({
  id: idString('agt'),
  type: z.literal('agent'),
  name: z.string().min(1).max(256),
  description: z.string().max(2048).nullable(),
  version: z.number().int().positive(),
  model: ModelOutput,
  system: z.string().nullable(),
  tools: z.array(ToolDef),
  mcp_servers: z.array(McpServer),
  skills: z.array(ResolvedAgentSkillRef),
  // Optional, unlike the six fields above: `agentToApi` emits this one only
  // under `orca-beta`, so a response genuinely may not carry it. No `.default`
  // for the same reason those lost theirs — this schema never parses input.
  guardrail_ids: z.array(idString('grd')).optional(),
  metadata: MetadataOutput,
  /**
   * Present only on a coordinator agent. The roster is snapshotted with pinned
   * versions at create, so a session started from a given agent version always
   * delegates to that immutable roster. Absent on single-agent agents.
   */
  multiagent: Multiagent.nullable(),
  archived_at: isoTimestamp.nullable(),
  created_at: isoTimestamp,
  updated_at: isoTimestamp,
});

export const AgentCreate = z
  .object({
    name: z.string().min(1).max(256),
    model: ModelInput,
    description: z.string().max(2048).nullable().optional(),
    mcp_servers: z.array(McpServerCreate).max(20).default([]),
    metadata: Metadata.optional(),
    multiagent: Multiagent.nullable().optional(),
    skills: AgentSkillRefs.default([]),
    guardrail_ids: z.array(idString('grd')).max(64).default([]),
    system: z.string().nullable().optional(),
    tools: z.array(ToolDef).max(128).default([]),
  })
  .strict();

export const AgentUpdate = z
  .object({
    description: z.string().max(2048).nullable().optional(),
    mcp_servers: z.array(McpServerCreate).max(20).nullable().optional(),
    metadata: MetadataPatch.nullable().optional(),
    model: ModelInput.optional(),
    multiagent: Multiagent.nullable().optional(),
    name: z.string().min(1).max(256).optional(),
    skills: AgentSkillRefs.nullable().optional(),
    guardrail_ids: z.array(idString('grd')).max(64).nullable().optional(),
    system: z.string().nullable().optional(),
    tools: z.array(ToolDef).max(128).nullable().optional(),
    version: z.number().int().positive().optional(),
  })
  .strict();

const AgentList = z.object({
  data: z.array(Agent),
  next_page: z.string().nullable(),
});

const AgentDeleted = z.object({ id: idString('agt'), type: z.literal('agent_deleted') });

export const agentsContract = c.router({
  create: {
    method: 'POST',
    path: '/v1/agents',
    body: AgentCreate,
    responses: {
      200: Agent,
      400: ClaudeErrorResponse,
      409: ClaudeErrorResponse,
    },
    headers: z
      .object({
        'idempotency-key': z.string().optional(),
        'orca-beta': z.string().optional(),
      })
      .passthrough(),
  },
  get: {
    method: 'GET',
    path: '/v1/agents/:id',
    pathParams: z.object({ id: idString('agt') }),
    query: z.object({ version: z.coerce.number().int().positive().optional() }),
    responses: { 200: Agent, 400: ClaudeErrorResponse, 404: ClaudeErrorResponse },
  },
  listVersions: {
    method: 'GET',
    path: '/v1/agents/:id/versions',
    pathParams: z.object({ id: idString('agt') }),
    query: z.object({
      limit: z.coerce.number().int().positive().optional(),
      page: z.string().optional(),
    }),
    responses: {
      200: z.object({ data: z.array(Agent), next_page: z.string().nullable() }),
      400: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
    },
  },
  list: {
    method: 'GET',
    path: '/v1/agents',
    query: pagination.extend({
      'created_at[gte]': z.string().datetime().optional(),
      'created_at[lte]': z.string().datetime().optional(),
      include_archived: z.boolean().optional(),
    }),
    responses: { 200: AgentList, 400: ClaudeErrorResponse },
  },
  update: {
    method: 'POST',
    path: '/v1/agents/:id',
    pathParams: z.object({ id: idString('agt') }),
    body: AgentUpdate,
    responses: {
      200: Agent,
      400: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
      409: ClaudeErrorResponse,
    },
    headers: z.object({ 'idempotency-key': z.string().optional() }).passthrough(),
  },
  archive: {
    method: 'POST',
    path: '/v1/agents/:id/archive',
    pathParams: z.object({ id: idString('agt') }),
    body: z.object({}).strict(),
    responses: { 200: Agent, 404: ClaudeErrorResponse },
    headers: z.object({ 'idempotency-key': z.string().optional() }).passthrough(),
  },
  delete: {
    method: 'DELETE',
    path: '/v1/agents/:id',
    pathParams: z.object({ id: idString('agt') }),
    body: z.object({}).strict(),
    responses: { 200: AgentDeleted, 404: ClaudeErrorResponse },
  },
});
