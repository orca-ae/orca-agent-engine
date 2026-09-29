// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { initContract, type AppRouter } from '@ts-rest/core';
import { z } from 'zod';
import { AgentEventKind } from '@orca/agent-event-contract';
import { ClaudeErrorResponse, idString, isoTimestamp } from './common.js';
import { AgentSkillRef, AgentSkillRefs, McpServer, ToolDef } from './agents.contract.js';
import {
  MAX_METADATA_KEY_LENGTH,
  MAX_METADATA_PAIRS,
  MAX_METADATA_VALUE_LENGTH,
  Metadata,
  MetadataOutput,
  MetadataPatch,
} from './metadata.js';
import { ModelInput, ModelOutput } from './model-wire.js';
import { binaryDownload, eventStream, openApiMedia } from './openapi-media.js';
import { FileDeleted, FileResponse, FilesListQuery, FilesListResponse } from './files.contract.js';

const c = initContract();
export const sessionEnvironmentIdSchema = idString('env');

const pageQuery = z.object({
  limit: z.coerce.number().int().positive().optional(),
  page: z.string().optional(),
});

const betaHeaders = z
  .object({
    'idempotency-key': z.string().optional(),
    'orca-beta': z.string().optional(),
  })
  .passthrough();

const TextBlock = z.object({ type: z.literal('text'), text: z.string() }).strict();
const ImageSource = z.discriminatedUnion('type', [
  z.object({ type: z.literal('base64'), media_type: z.string(), data: z.string() }).strict(),
  z.object({ type: z.literal('url'), url: z.string().url() }).strict(),
  z.object({ type: z.literal('file'), file_id: idString('file') }).strict(),
]);
const ImageBlock = z.object({ type: z.literal('image'), source: ImageSource }).passthrough();
const DocumentSource = z.discriminatedUnion('type', [
  z.object({ type: z.literal('base64'), media_type: z.string(), data: z.string() }).strict(),
  z
    .object({ type: z.literal('text'), data: z.string(), media_type: z.string().optional() })
    .passthrough(),
  z.object({ type: z.literal('url'), url: z.string().url() }).strict(),
  z.object({ type: z.literal('file'), file_id: idString('file') }).strict(),
]);
const DocumentBlock = z
  .object({
    type: z.literal('document'),
    source: DocumentSource,
    context: z.string().nullable().optional(),
    title: z.string().nullable().optional(),
  })
  .strict();
const SearchResultBlock = z.object({ type: z.literal('search_result') }).passthrough();
const MessageContentBlock = z.union([TextBlock, ImageBlock, DocumentBlock]);
const ToolResultContentBlock = z.union([TextBlock, ImageBlock, DocumentBlock, SearchResultBlock]);

const UserMessageEventInput = z
  .object({
    type: z.literal('user.message'),
    content: z.array(MessageContentBlock).min(1),
  })
  .strict();

const UserInterruptEventInput = z
  .object({
    type: z.literal('user.interrupt'),
    session_thread_id: idString('sth').nullable().optional(),
  })
  .strict();

const UserToolConfirmationEventInput = z
  .object({
    type: z.literal('user.tool_confirmation'),
    tool_use_id: z.string().min(1),
    result: z.enum(['allow', 'deny']),
    deny_message: z.string().nullable().optional(),
  })
  .strict()
  .superRefine((value, ctx) => {
    if (value.result === 'allow' && value.deny_message != null) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['deny_message'],
        message: 'deny_message is only valid when result is deny',
      });
    }
  });

const UserCustomToolResultEventInput = z
  .object({
    type: z.literal('user.custom_tool_result'),
    custom_tool_use_id: z.string().min(1),
    content: z.array(ToolResultContentBlock).optional(),
    is_error: z.boolean().nullable().optional(),
  })
  .strict();

const UserToolResultEventInput = z
  .object({
    type: z.literal('user.tool_result'),
    tool_use_id: z.string().min(1),
    content: z.array(ToolResultContentBlock).optional(),
    is_error: z.boolean().nullable().optional(),
  })
  .strict();

const OutcomeRubric = z.discriminatedUnion('type', [
  z.object({ type: z.literal('text'), content: z.string().max(262_144) }).strict(),
  z.object({ type: z.literal('file'), file_id: idString('file') }).strict(),
]);
const UserDefineOutcomeEventInput = z
  .object({
    type: z.literal('user.define_outcome'),
    description: z.string().min(1),
    rubric: OutcomeRubric,
    max_iterations: z.number().int().min(1).max(20).nullable().optional(),
  })
  .strict();

const SystemMessageEventInput = z
  .object({
    type: z.literal('system.message'),
    content: z.array(TextBlock).min(1),
  })
  .strict();

export const sessionEventInputSchema = z.union([
  UserMessageEventInput,
  UserInterruptEventInput,
  UserToolConfirmationEventInput,
  UserCustomToolResultEventInput,
  UserDefineOutcomeEventInput,
  UserToolResultEventInput,
  SystemMessageEventInput,
]);

const InitialEventInput = z.union([UserMessageEventInput, UserDefineOutcomeEventInput]);

const GithubCheckout = z.discriminatedUnion('type', [
  z.object({ type: z.literal('branch'), name: z.string().min(1) }).strict(),
  z.object({ type: z.literal('commit'), sha: z.string().regex(/^[0-9a-f]{7,40}$/) }).strict(),
]);

const FileResourceInput = z
  .object({
    type: z.literal('file'),
    file_id: idString('file'),
    mount_path: z.string().min(1).nullable().optional(),
    access: z.enum(['read_only', 'read_write']).nullable().optional(),
    instructions: z.string().max(4096).nullable().optional(),
    mount_strategy: z.literal('tarball_prefetch').optional(),
  })
  .strict();

const MemoryStoreResourceInput = z
  .object({
    type: z.literal('memory_store'),
    memory_store_id: idString('mems'),
    access: z.enum(['read_only', 'read_write']).nullable().optional(),
    instructions: z.string().max(4096).nullable().optional(),
  })
  .strict();

const GithubRepositoryResourceInput = z
  .object({
    type: z.literal('github_repository'),
    url: z.string().url(),
    authorization_token: z.string().min(1),
    mount_path: z.string().min(1).nullable().optional(),
    access: z.enum(['read_only', 'read_write']).nullable().optional(),
    instructions: z.string().max(4096).nullable().optional(),
    checkout: GithubCheckout.nullable().optional(),
  })
  .strict();

export const sessionResourceInputSchema = z.discriminatedUnion('type', [
  FileResourceInput,
  MemoryStoreResourceInput,
  GithubRepositoryResourceInput,
]);

const FileResourceOut = z.object({
  id: idString('sesrsc'),
  type: z.literal('file'),
  file_id: idString('file'),
  mount_path: z.string(),
  created_at: isoTimestamp,
  updated_at: isoTimestamp,
});

const MemoryStoreResourceOut = z.object({
  type: z.literal('memory_store'),
  memory_store_id: idString('mems'),
  access: z.enum(['read_only', 'read_write']).nullable().optional(),
  description: z.string().optional(),
  instructions: z.string().nullable().optional(),
  mount_path: z.string().nullable().optional(),
  name: z.string().nullable().optional(),
});

const GithubRepositoryResourceOut = z.object({
  id: idString('sesrsc'),
  type: z.literal('github_repository'),
  url: z.string().url(),
  mount_path: z.string(),
  checkout: GithubCheckout.nullable().optional(),
  created_at: isoTimestamp,
  updated_at: isoTimestamp,
});

const SessionResourceOut = z.discriminatedUnion('type', [
  FileResourceOut,
  MemoryStoreResourceOut,
  GithubRepositoryResourceOut,
]);

const SessionResourceUpdate = z.object({ authorization_token: z.string().min(1) }).strict();

const SessionAgentRef = z.union([
  idString('agt'),
  z
    .object({
      type: z.literal('agent'),
      id: idString('agt'),
      version: z.number().int().positive().optional(),
    })
    .strict(),
  z
    .object({
      type: z.literal('agent_with_overrides'),
      id: idString('agt'),
      version: z.number().int().positive().optional(),
      model: ModelInput.optional(),
      system: z.string().max(100_000).nullable().optional(),
      tools: z.array(ToolDef).max(128).optional(),
      mcp_servers: z.array(McpServer).max(20).optional(),
      skills: AgentSkillRefs.optional(),
      guardrail_ids: z.array(idString('grd')).max(64).optional(),
    })
    .strict(),
]);

const SessionStats = z.object({
  active_seconds: z.number().int().nonnegative().optional(),
  duration_seconds: z.number().int().nonnegative().optional(),
});

const SessionUsage = z.object({
  cache_creation: z
    .object({
      ephemeral_1h_input_tokens: z.number().int().nonnegative().optional(),
      ephemeral_5m_input_tokens: z.number().int().nonnegative().optional(),
    })
    .optional(),
  cache_read_input_tokens: z.number().int().nonnegative().optional(),
  input_tokens: z.number().int().nonnegative().optional(),
  output_tokens: z.number().int().nonnegative().optional(),
});

export const OutcomeEvaluation = z
  .object({
    type: z.literal('outcome_evaluation'),
    outcome_id: z.string(),
    description: z.string(),
    result: z.string(),
    explanation: z.string().nullable(),
    iteration: z.number().int().nonnegative(),
    completed_at: isoTimestamp.nullable(),
  })
  .passthrough();

const SessionThreadAgent = z.object({
  id: idString('agt'),
  type: z.literal('agent'),
  name: z.string(),
  description: z.string().nullable(),
  version: z.number().int().positive(),
  model: ModelOutput,
  system: z.string().nullable(),
  tools: z.array(ToolDef),
  mcp_servers: z.array(McpServer),
  skills: z.array(AgentSkillRef),
});

const SessionMultiagent = z.object({
  type: z.literal('coordinator'),
  agents: z.array(SessionThreadAgent),
});

const SessionAgent = SessionThreadAgent.extend({
  // `z.unknown()` accepts undefined, which made this response field optional
  // in the generated OpenAPI even though every Session mapper emits it. The
  // concrete resolved topology keeps the required wire contract aligned with
  // Anthropic and with the nested agents returned by loadResolvedAgentSnapshot.
  multiagent: SessionMultiagent.nullable(),
});

export const SessionResponseSchema = z.object({
  id: idString('ses'),
  type: z.literal('session'),
  agent: SessionAgent,
  title: z.string().nullable(),
  metadata: MetadataOutput,
  environment_id: sessionEnvironmentIdSchema,
  vault_ids: z.array(idString('vlt')),
  status: z.enum(['idle', 'running', 'rescheduling', 'terminated']),
  stats: SessionStats,
  // Orca keeps this additive field for callers that need wall-clock timestamps.
  timing: z
    .object({
      started_at: isoTimestamp.nullable(),
      last_active_at: isoTimestamp.nullable(),
      active_seconds: z.number().int().nonnegative(),
      duration_seconds: z.number().int().nonnegative(),
    })
    .optional(),
  deployment_id: z.string().nullable().optional(),
  outcome_evaluations: z.array(OutcomeEvaluation),
  usage: SessionUsage,
  archived_at: isoTimestamp.nullable(),
  resources: z.array(SessionResourceOut),
  created_at: isoTimestamp,
  updated_at: isoTimestamp,
});

const SessionThreadStats = z.object({
  active_seconds: z.number().nonnegative().optional(),
  duration_seconds: z.number().nonnegative().optional(),
  startup_seconds: z.number().nonnegative().optional(),
});

const SessionThread = z.object({
  id: idString('sth'),
  type: z.literal('session_thread'),
  session_id: idString('ses'),
  parent_thread_id: idString('sth').nullable(),
  agent: SessionThreadAgent,
  status: z.enum(['running', 'idle', 'rescheduling', 'terminated']),
  stats: SessionThreadStats.nullable(),
  usage: SessionUsage.nullable(),
  archived_at: isoTimestamp.nullable(),
  created_at: isoTimestamp,
  updated_at: isoTimestamp,
});

const SessionCreate = z
  .object({
    agent: SessionAgentRef.optional(),
    // Parsed only so an orca-beta route can preserve the pre-Claude alias.
    agent_id: idString('agt').optional(),
    title: z.string().max(1024).nullable().optional(),
    metadata: Metadata.optional(),
    environment_id: sessionEnvironmentIdSchema,
    vault_ids: z.array(idString('vlt')).optional(),
    resources: z.array(sessionResourceInputSchema).max(100).optional(),
    initial_events: z.array(InitialEventInput).max(50).optional(),
  })
  .strict()
  .superRefine((value, ctx) => {
    if ((value.agent === undefined) === (value.agent_id === undefined)) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'exactly one of `agent` or `agent_id` must be provided',
      });
    }
  });

const SessionUpdate = z
  .object({
    title: z.string().max(1024).nullable().optional(),
    metadata: MetadataPatch.nullable().optional(),
    // Reserved by Claude: accepted in the request model but the endpoint
    // currently rejects every attempt to set it.
    vault_ids: z.array(idString('vlt')).optional(),
    agent: z
      .object({
        tools: z.array(ToolDef).max(128).optional(),
        mcp_servers: z.array(McpServer).max(20).optional(),
        // Changing model mid-session is what makes a cost budget's downgrade
        // gate honest. Configured with an explicit expensive-model list, the
        // gate denies at the cap only while the session is on a matching model
        // and allows again once it moves to a cheaper one — the session
        // degrades instead of dying. Without this field that gate is a dead end
        // telling a client to do something the API does not permit.
        model: ModelInput.optional(),
      })
      .strict()
      .optional(),
  })
  .strict();

const HttpEventOut = z
  .object({
    id: idString('evt'),
    type: z.string().min(1),
    processed_at: isoTimestamp.nullable().optional(),
  })
  .passthrough();

/**
 * What one SSE frame carries, for the three streaming routes.
 *
 * They emit the same events `listEvents` returns — `src/streaming/sse.ts` writes
 * `id:` from the event's cursor, `event:` from its `type`, and the event itself
 * as `data:` — so the schema is `HttpEventOut` rather than a second description
 * of the same thing.
 */
const SSE_FRAME_DESCRIPTION =
  'One server-sent event. Each frame carries the event cursor as `id`, the event ' +
  'type as `event`, and this object as `data`. `:heartbeat` comment frames are ' +
  'sent periodically and carry no data.';

const SessionDeleted = z.object({ id: idString('ses'), type: z.literal('session_deleted') });
const SessionResourceDeleted = z.object({
  id: idString('sesrsc'),
  type: z.literal('session_resource_deleted'),
});

const dateFilters = {
  'created_at[gt]': isoTimestamp.optional(),
  'created_at[gte]': isoTimestamp.optional(),
  'created_at[lt]': isoTimestamp.optional(),
  'created_at[lte]': isoTimestamp.optional(),
};
const eventDeltasQuery = z
  .union([
    z.enum([AgentEventKind.message, AgentEventKind.thinking]),
    z.array(z.enum([AgentEventKind.message, AgentEventKind.thinking])),
  ])
  .optional();
const sessionStreamQuery = z.object({
  from_cursor: z.string().optional(),
  subpath: z.string().optional(),
  event_deltas: eventDeltasQuery,
});
const threadStreamQuery = z.object({
  from_cursor: z.string().optional(),
  event_deltas: eventDeltasQuery,
});

export const sessionUpdateBodySchema = SessionUpdate;
export const sessionCreateBodySchema = SessionCreate;
/** Extract and validate the dynamic metadata_<key> query parameters. */
export const sessionMetadataFiltersQuerySchema = z
  .record(z.unknown())
  .transform((query) =>
    Object.entries(query)
      .filter(([name]) => name.startsWith('metadata_'))
      .map(([name, value]) => [name.slice('metadata_'.length), value]),
  )
  .pipe(
    z
      .array(
        z.tuple([
          z.string().min(1).max(MAX_METADATA_KEY_LENGTH),
          z.string().max(MAX_METADATA_VALUE_LENGTH),
        ]),
      )
      .max(MAX_METADATA_PAIRS),
  );
export const sessionAppendEventsBodySchema = z
  .object({ events: z.array(sessionEventInputSchema).max(100) })
  .strict();

// The fully inferred type for this large union-heavy router exceeds the
// declaration emitter's serialization limit (TS7056). Consumers only need the
// standard ts-rest router surface; request schemas remain exported separately.
export const sessionsContract: AppRouter = c.router({
  create: {
    method: 'POST',
    path: '/v1/sessions',
    body: SessionCreate,
    responses: { 200: SessionResponseSchema, 400: ClaudeErrorResponse, 404: ClaudeErrorResponse },
    headers: betaHeaders,
  },
  get: {
    method: 'GET',
    path: '/v1/sessions/:id',
    pathParams: z.object({ id: idString('ses') }),
    responses: { 200: SessionResponseSchema, 404: ClaudeErrorResponse },
  },
  update: {
    method: 'POST',
    path: '/v1/sessions/:id',
    pathParams: z.object({ id: idString('ses') }),
    body: SessionUpdate,
    responses: {
      200: SessionResponseSchema,
      400: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
      409: ClaudeErrorResponse,
    },
    headers: betaHeaders,
  },
  list: {
    method: 'GET',
    path: '/v1/sessions',
    description:
      'Orca extension: metadata_<key>=<value> filters Session metadata by exact, case-sensitive ' +
      'string equality. Multiple filters use AND and apply before pagination and cursor validation. ' +
      'Use at most 16 filters, with keys of 1-64 characters and values of at most 512 characters. ' +
      'Repeated parameters are rejected; empty values match only existing empty strings. ' +
      'URL-encode keys and values. Example: metadata_AGENT_TRIGGER=local-trigger&metadata_team=ops. ' +
      'Available in both response dialects without orca-beta.',
    query: pageQuery
      .extend({
        agent_id: idString('agt').optional(),
        agent_version: z.coerce.number().int().positive().optional(),
        ...dateFilters,
        deployment_id: z.string().optional(),
        include_archived: z.coerce.boolean().optional(),
        memory_store_id: idString('mems').optional(),
        order: z.enum(['asc', 'desc']).optional(),
        statuses: z
          .union([
            z.enum(['rescheduling', 'running', 'idle', 'terminated']),
            z.array(z.enum(['rescheduling', 'running', 'idle', 'terminated'])),
          ])
          .optional(),
      })
      .passthrough()
      .superRefine((query, context) => {
        const parsed = sessionMetadataFiltersQuerySchema.safeParse(query);
        if (!parsed.success) {
          for (const issue of parsed.error.issues) context.addIssue(issue);
        }
      }),
    responses: {
      200: z.object({
        data: z.array(SessionResponseSchema),
        next_page: z.string().nullable(),
        prev_page: z.string().nullable(),
      }),
      400: ClaudeErrorResponse,
    },
  },
  archive: {
    method: 'POST',
    path: '/v1/sessions/:id/archive',
    pathParams: z.object({ id: idString('ses') }),
    body: z.object({}).strict(),
    responses: {
      200: SessionResponseSchema,
      400: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
    },
    headers: betaHeaders,
  },
  delete: {
    method: 'DELETE',
    path: '/v1/sessions/:id',
    pathParams: z.object({ id: idString('ses') }),
    body: z.object({}).strict(),
    responses: { 200: SessionDeleted, 400: ClaudeErrorResponse, 404: ClaudeErrorResponse },
  },
  listFiles: {
    method: 'GET',
    path: '/v1/sessions/:id/files',
    pathParams: z.object({ id: idString('ses') }),
    query: FilesListQuery.omit({ scope_id: true }),
    responses: {
      200: FilesListResponse,
      400: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
    },
  },
  getFile: {
    method: 'GET',
    path: '/v1/sessions/:id/files/:file_id',
    pathParams: z.object({ id: idString('ses'), file_id: idString('file') }),
    responses: { 200: FileResponse, 404: ClaudeErrorResponse },
  },
  getFileContent: {
    method: 'GET',
    path: '/v1/sessions/:id/files/:file_id/content',
    pathParams: z.object({ id: idString('ses'), file_id: idString('file') }),
    responses: {
      200: z.unknown(),
      403: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
    },
    metadata: openApiMedia({
      responses: {
        200: binaryDownload('application/octet-stream', 'The session-created file bytes.'),
      },
    }),
  },
  deleteFile: {
    method: 'DELETE',
    path: '/v1/sessions/:id/files/:file_id',
    pathParams: z.object({ id: idString('ses'), file_id: idString('file') }),
    body: z.object({}).strict(),
    responses: { 200: FileDeleted, 404: ClaudeErrorResponse },
  },
  attachResource: {
    method: 'POST',
    path: '/v1/sessions/:id/resources',
    pathParams: z.object({ id: idString('ses') }),
    body: sessionResourceInputSchema,
    responses: {
      200: SessionResourceOut,
      400: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
      409: ClaudeErrorResponse,
    },
    headers: betaHeaders,
  },
  listResources: {
    method: 'GET',
    path: '/v1/sessions/:id/resources',
    pathParams: z.object({ id: idString('ses') }),
    query: pageQuery,
    responses: {
      200: z.object({ data: z.array(SessionResourceOut), next_page: z.string().nullable() }),
      400: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
    },
  },
  getResource: {
    method: 'GET',
    path: '/v1/sessions/:id/resources/:resource_id',
    pathParams: z.object({ id: idString('ses'), resource_id: idString('sesrsc') }),
    responses: { 200: SessionResourceOut, 404: ClaudeErrorResponse },
  },
  updateResource: {
    method: 'POST',
    path: '/v1/sessions/:id/resources/:resource_id',
    pathParams: z.object({ id: idString('ses'), resource_id: idString('sesrsc') }),
    body: SessionResourceUpdate,
    responses: {
      200: SessionResourceOut,
      400: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
      409: ClaudeErrorResponse,
    },
    headers: betaHeaders,
  },
  detachResource: {
    method: 'DELETE',
    path: '/v1/sessions/:id/resources/:resource_id',
    pathParams: z.object({ id: idString('ses'), resource_id: idString('sesrsc') }),
    body: z.object({}).strict(),
    responses: {
      200: SessionResourceDeleted,
      400: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
    },
  },
  appendEvents: {
    method: 'POST',
    path: '/v1/sessions/:id/events',
    pathParams: z.object({ id: idString('ses') }),
    body: sessionAppendEventsBodySchema,
    responses: {
      200: z.object({ data: z.array(HttpEventOut).optional() }),
      400: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
      409: ClaudeErrorResponse,
      502: ClaudeErrorResponse,
    },
    headers: betaHeaders,
  },
  listEvents: {
    method: 'GET',
    path: '/v1/sessions/:id/events',
    pathParams: z.object({ id: idString('ses') }),
    query: pageQuery.extend({
      ...dateFilters,
      order: z.enum(['asc', 'desc']).optional(),
      types: z.union([z.string(), z.array(z.string())]).optional(),
      subpath: z.string().optional(),
    }),
    responses: {
      200: z.object({ data: z.array(HttpEventOut), next_page: z.string().nullable() }),
      400: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
    },
  },
  streamEvents: {
    method: 'GET',
    path: '/v1/sessions/:id/events/stream',
    pathParams: z.object({ id: idString('ses') }),
    query: sessionStreamQuery,
    responses: { 200: HttpEventOut, 400: ClaudeErrorResponse, 404: ClaudeErrorResponse },
    metadata: openApiMedia({ responses: { 200: eventStream(SSE_FRAME_DESCRIPTION) } }),
  },
  listThreads: {
    method: 'GET',
    path: '/v1/sessions/:id/threads',
    pathParams: z.object({ id: idString('ses') }),
    query: pageQuery,
    responses: {
      200: z.object({ data: z.array(SessionThread), next_page: z.string().nullable() }),
      400: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
    },
  },
  retrieveThread: {
    method: 'GET',
    path: '/v1/sessions/:id/threads/:thread_id',
    pathParams: z.object({ id: idString('ses'), thread_id: idString('sth') }),
    responses: { 200: SessionThread, 404: ClaudeErrorResponse },
  },
  listThreadEvents: {
    method: 'GET',
    path: '/v1/sessions/:id/threads/:thread_id/events',
    pathParams: z.object({ id: idString('ses'), thread_id: idString('sth') }),
    query: pageQuery,
    responses: {
      200: z.object({ data: z.array(HttpEventOut), next_page: z.string().nullable() }),
      400: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
    },
  },
  streamThread: {
    method: 'GET',
    path: '/v1/sessions/:id/threads/:thread_id/stream',
    pathParams: z.object({ id: idString('ses'), thread_id: idString('sth') }),
    query: threadStreamQuery,
    responses: { 200: HttpEventOut, 400: ClaudeErrorResponse, 404: ClaudeErrorResponse },
    metadata: openApiMedia({ responses: { 200: eventStream(SSE_FRAME_DESCRIPTION) } }),
  },
  interruptThread: {
    method: 'POST',
    path: '/v1/sessions/:id/threads/:thread_id/interrupt',
    pathParams: z.object({ id: idString('ses'), thread_id: idString('sth') }),
    body: z.object({}).strict(),
    responses: {
      200: SessionThread,
      400: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
      502: ClaudeErrorResponse,
    },
    headers: betaHeaders,
  },
  archiveThread: {
    method: 'POST',
    path: '/v1/sessions/:id/threads/:thread_id/archive',
    pathParams: z.object({ id: idString('ses'), thread_id: idString('sth') }),
    body: z.object({}).strict(),
    responses: {
      200: SessionThread,
      400: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
      502: ClaudeErrorResponse,
    },
    headers: betaHeaders,
  },
});
