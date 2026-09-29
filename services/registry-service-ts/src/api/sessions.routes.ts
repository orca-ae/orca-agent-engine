// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import { posix as posixPath } from 'node:path';
import { isDeepStrictEqual } from 'node:util';
import {
  eq,
  and,
  isNull,
  asc,
  desc,
  gt,
  gte,
  lt,
  lte,
  or,
  inArray,
  ne,
  sql,
  type SQL,
} from 'drizzle-orm';
import type { DbClient } from '../persistence/postgres/client.js';
import {
  agentVersions,
  sessions,
  sessionResources,
  sessionEventsIndex,
  sessionSkillBindings,
  sessionThreads,
  skills,
  skillVersions,
  agents,
  environments,
  gitCredentials,
  gitCredentialStagingIntents,
  sessionLifecycleOutbox,
} from '../persistence/postgres/schema.js';
import { newId } from '../domain/versioning.js';
import type { Event, TranscriptStore } from '@orca/transcript-store';
import type { FileRecord, FileStore } from '@orca/file-store';
import type { MemoryStore } from '@orca/memory-store';
import {
  DISTRIBUTION_PENDING,
  DISTRIBUTION_FAILED,
  type SessionDistributor,
  type DistributionSessionStore,
  type DistributionSessionRow,
  type DistributionSessionPatch,
  type DistributionState,
  type DispatchableSessionRef,
  type EnvironmentTargetLookup,
} from '../tunnel/session-distributor.js';
import { loadSessionHarnessBinding } from '../domain/session-harness-binding.js';
import { harnessToProvider, resolveExecutionOwner } from '@orca/harness-catalog';
import type { BoundSessionResolver } from '../tunnel/session-event-bridge.js';
import type { SkillStore } from '@orca/skill-store';
import {
  bindStoredHarness,
  validateHarnessModel,
  validateHarnessFeatures,
  validateHarnessDeployment,
  resolveHarnessAnnotation,
  type HarnessMode,
  type HarnessType,
} from '@orca/harness-catalog';
import { AgentEventKind, SessionThreadEventKind, SpanEventKind } from '@orca/agent-event-contract';
import {
  CLAUDE_SESSION_EVENT_TYPES,
  eventBatchIdempotencyKeys,
  httpEventToProto,
  oidcTranscriptUserId,
  protoToHttpEvent,
  toPublicHttpEvent,
} from '../domain/events.js';
import {
  indexTranscriptEvents,
  isValidSessionEventsCursor,
  listSessionEventsFromIndex,
} from '../events/session-events-index.js';
import { stableSessionThreadId } from '../domain/thread-projection.js';
import { authenticatedGuardrailSubject } from '../auth/guardrail-subject.js';
import { elapsedSeconds } from '../domain/session-time.js';
import { eventsAppendTotal, eventsReadTotal } from '../metrics.js';
import type { SseConfig } from '../server.js';
import { streamSession } from '../streaming/sse.js';
import {
  sessionEnvironmentIdSchema,
  sessionAppendEventsBodySchema,
  sessionCreateBodySchema,
  sessionResourceInputSchema,
  sessionMetadataFiltersQuerySchema,
  sessionUpdateBodySchema,
} from '../contracts/sessions.contract.js';
import { buildClaudeErrorResponse } from '../contracts/common.js';
import { toInternalId } from '../contracts/id-prefix.js';
import {
  mergeModelOverrideForStorage,
  modelToApi,
  normalizeModelForStorage,
  type StoredModel,
} from '../contracts/model-wire.js';
import type { SecretStore } from '../secrets/secret-provider.js';
import {
  discardGitCredentialStagingIntents,
  persistGitCredentialStagingIntents,
  prepareGitCredentialBinding,
  purgeGitCredentialSecretRefs,
  repositoryUrlsEqual,
  stageManagedGitCredentials,
  type ManagedGitCredentialDraft,
} from '../domain/git-credentials.js';
import { stripSessionMcpServerPermissionPolicyFields } from './mcp-servers.js';
import {
  applyMetadataPatch,
  normalizeStoredMetadata,
  parseMetadata,
  parseMetadataPatch,
  toJsonMetadata,
  validateMetadataLimits,
} from './metadata.js';
import {
  SESSION_LLM_EGRESS_KEY,
  sessionLlmEgressMetadataError,
} from '../domain/session-llm-egress.js';
import {
  mcpServerToApi,
  skillToApi,
  toolToApi,
  validateGuardrailRefs,
  validateMultiagentModelRoster,
  validateSkillRefs,
} from './agents.routes.js';
import { foldOutcomeEvaluations, getSessionOutcome } from '../domain/session-outcome.js';
import { requestReadSignal } from '../middleware/read-admission.js';
import {
  createSessionReadContext,
  type SessionReadContext,
} from '../domain/session-read-context.js';
import {
  canonicalizeAgentTools,
  isCanonicalMultiagent,
  strictAgentVersionConfiguration,
  validateAgentConfiguration,
  type AgentTool,
  type CanonicalMultiagent,
} from '../domain/agent-version-configuration.js';
import {
  newSessionArchiveOutboxRow,
  newSessionDeleteOutboxRow,
  reconcileSessionLifecycleOutbox,
} from '../domain/session-lifecycle-outbox.js';
import { parsePositiveIntQueryParam } from './query-params.js';
import {
  SessionObservabilitySelectionAvailabilityError,
  SessionObservabilitySelectionResourceUnavailableError,
} from '../domain/agent-observability-session-selection.js';
import { transitionSessionObservabilityBindingInTransaction } from '../domain/session-observability-lifecycle.js';
import { SessionSkillBindingError } from '../domain/session-skill-bindings.js';
import {
  createSessionInTransaction,
  SessionAgentSnapshotUnavailableError,
  validateSessionVaultIds,
} from '../domain/session-creation.js';
import { validateResourceMountPath } from '../domain/resource-mount-path.js';
import { fileRecordToApi } from './files.routes.js';

interface SessionResourceInput {
  type: 'file' | 'memory_store' | 'github_repository';
  file_id?: string;
  memory_store_id?: string;
  url?: string;
  authorization_token?: string;
  // File/GitHub input only. Memory-store mount_path is Registry-derived and
  // output-only; the discriminated Zod schema rejects it on create/attach.
  mount_path?: string | null;
  access?: string | null;
  instructions?: string | null;
  checkout?: Record<string, unknown> | null;
  // File-resource only. Discriminator-checked by the contract; we still
  // sanity-check at runtime so SDK clients hitting the route directly get a
  // clean 400 on bogus values.
  mount_strategy?: string;
}

interface SessionResourceUpdateBody {
  authorization_token?: string;
  mount_path?: string;
  access?: string;
  instructions?: string | null;
  mount_strategy?: string | null;
}

const ALLOWED_MOUNT_STRATEGIES = new Set(['tarball_prefetch']);
const DEFAULT_EVENTS_LIMIT = 100;
const MAX_EVENTS_LIMIT = 1_000;
const SESSION_LIST_PAGE_DEFAULT = 100;
const SESSION_LIST_PAGE_MAX = 100;
const MAX_CONCURRENT_SESSION_THREADS = 25;
/**
 * Validate the (optional) `mount_strategy` field on a session-resource input.
 *
 * Returns `null` when the field is absent OR when it is a recognized value on
 * a `file` resource. Returns an error string otherwise. The harness resolves
 * `null` at session-spawn based on runtime capabilities — that's the contract
 * surface, not `'auto'`.
 */
function validateMountStrategy(r: SessionResourceInput): string | null {
  if (r.mount_strategy === undefined) return null;
  if (r.type !== 'file') {
    return 'mount_strategy is only valid on file resources';
  }
  if (!ALLOWED_MOUNT_STRATEGIES.has(r.mount_strategy)) {
    return `invalid mount_strategy: ${r.mount_strategy}`;
  }
  return null;
}

function firstZodIssue(
  error: { issues: Array<{ message: string; path: Array<string | number> }> },
  fallback: string,
): string {
  const issue = error.issues[0];
  if (!issue) return fallback;
  const path = issue.path.join('.');
  if (!path || issue.message.toLowerCase().startsWith(`${path.toLowerCase()} `)) {
    return issue.message;
  }
  return `${path}: ${issue.message}`;
}

interface SessionCreateBody {
  /**
   * Polymorphic agent reference (managed-agents-2026-04-01): a bare string
   * agent id (latest version) OR `{type:'agent',id,version?}` to pin a
   * specific version. Mutually exclusive with the legacy `agent_id`.
   */
  agent?:
    | string
    | { type: 'agent'; id: string; version?: number }
    | {
        type: 'agent_with_overrides';
        id: string;
        version?: number;
        model?: unknown;
        system?: string | null;
        tools?: unknown[];
        mcp_servers?: unknown[];
        skills?: unknown[];
        guardrail_ids?: unknown[];
      };
  /** Legacy alias for `agent` as a bare id; pins the latest version. */
  agent_id?: string;
  title?: string | null;
  metadata?: unknown;
  // Raw Fastify bodies are not validated by the ts-rest contract yet, so keep
  // this unknown and validate it explicitly in the create handler.
  environment_id?: unknown;
  vault_ids?: string[];
  resources?: SessionResourceInput[];
  initial_events?: Array<Record<string, unknown> & { type: string }>;
}

interface SessionUpdateBody {
  title?: string | null;
  metadata?: Record<string, string | null> | null;
  agent?: {
    tools?: unknown[];
    mcp_servers?: unknown[];
    model?: unknown;
  };
}

/**
 * Normalize the polymorphic `agent` ref (or the legacy `agent_id` alias) on a
 * session-create body into `{ agentId, requestedVersion? }`. Mirrors the
 * multiagent roster resolution (agents.routes.ts:479-503) for parity. Returns
 * an `{ error }` for the exactly-one-of and bad-version cases so the caller can
 * surface a clean 400.
 */
export function resolveSessionAgentRef(body: SessionCreateBody):
  | {
      agentId: string;
      requestedVersion?: number;
      overrides?: Exclude<SessionCreateBody['agent'], string | { type: 'agent' } | undefined>;
    }
  | { error: string } {
  const hasAgent = body.agent !== undefined;
  const hasAgentId = body.agent_id !== undefined;
  if (hasAgent === hasAgentId) {
    return { error: 'exactly one of `agent` or `agent_id` must be provided' };
  }

  if (hasAgentId) {
    if (typeof body.agent_id !== 'string') {
      return { error: 'agent_id must be a string agent id' };
    }
    return { agentId: toInternalId(body.agent_id) };
  }

  const agent = body.agent;
  if (typeof agent === 'string') {
    return { agentId: toInternalId(agent) };
  }
  if (
    agent &&
    typeof agent === 'object' &&
    !Array.isArray(agent) &&
    (agent.type === 'agent' || agent.type === 'agent_with_overrides') &&
    typeof agent.id === 'string'
  ) {
    const version = agent.version;
    if (version !== undefined && (!Number.isInteger(version) || version < 1)) {
      return { error: 'agent reference version must be a positive integer' };
    }
    return {
      agentId: toInternalId(agent.id),
      ...(version !== undefined ? { requestedVersion: version } : {}),
      ...(agent.type === 'agent_with_overrides' ? { overrides: agent } : {}),
    };
  }
  return {
    error: 'agent must be a string agent id, {type:"agent", id, version?}, or agent_with_overrides',
  };
}

interface AppendEventsBody {
  events: Array<Record<string, unknown> & { type: string }>;
  request_id?: string;
}

interface StoredSessionAgentOverrides {
  model?: StoredModel;
  system?: string | null;
  skills?: unknown[];
  guardrailIds?: string[];
}

function prepareClaudeEventInput(
  input: Record<string, unknown> & { type: string },
): Record<string, unknown> & { type: string } {
  if (input.type === 'user.define_outcome') {
    const outcomeId = typeof input.outcome_id === 'string' ? input.outcome_id : newId('outc');
    // The execution harness historically reads `id`; the public API exposes
    // `outcome_id`. Persist both aliases so evaluations reference the same
    // server-generated outcome without leaking the internal alias on output.
    return { ...input, id: outcomeId, outcome_id: outcomeId };
  }
  return input;
}

const COMPANION_SYSTEM_EVENT_ID_FIELD = '_orca_companion_system_event_id';

/**
 * Persist an explicit link on the turn-driving event instead of relying on
 * adjacent transcript sequence numbers. PostgreSQL sequences may interleave
 * concurrent appends, while large broker batches may be split.
 */
function correlateCompanionSystemMessage(events: Event[]): void {
  const systemIndex = events.findIndex((event) => event.kind === 'system.message');
  if (systemIndex <= 0) return;
  const source = events[systemIndex - 1]!;
  const system = events[systemIndex]!;
  try {
    const payload = JSON.parse(Buffer.from(source.payload).toString('utf8')) as unknown;
    if (!isPlainObject(payload)) return;
    source.payload = Buffer.from(
      JSON.stringify({ ...payload, [COMPANION_SYSTEM_EVENT_ID_FIELD]: system.id }),
      'utf8',
    );
  } catch {
    // The input was already schema-validated and serialized above.
  }
}

function isOrcaBetaRequest(req: FastifyRequest): boolean {
  const value = req.headers['orca-beta'];
  return Array.isArray(value)
    ? value.some((entry) => entry.trim().length > 0)
    : typeof value === 'string' && value.trim().length > 0;
}

function isActiveSessionOutputFile(
  record: FileRecord | null,
  sessionId: string,
): record is FileRecord {
  return (
    record !== null &&
    record.archivedAt === null &&
    record.purpose === 'agent_output' &&
    record.scopeId === sessionId
  );
}

const MAX_FILE_RESOURCES = 100;
/**
 * Anthropic's Memory Tool contract caps each session at 8 attached
 * `memory_store` resources. We enforce the same cap at create + attach time so
 * the harness never sees an over-attached session. The cap is intentionally
 * separate from `MAX_FILE_RESOURCES` because the two resource families are
 * independent — an over-attached session is a user-error 400, not a 4xx
 * regression of the file resources path.
 */
const MAX_MEMORY_STORE_RESOURCES = 8;
/**
 * Anthropic's published `github_repository` contract caps each session at 8
 * attached repo resources. Mirrors the memory_store cap (the two families
 * have independent budgets but share the same 8-per-session ceiling). Files
 * stay at `MAX_FILE_RESOURCES` (999 in production, currently 100 here).
 */
const MAX_GITHUB_REPOSITORY_RESOURCES = 8;
const DUPLICATE_GITHUB_REPOSITORY_ERROR =
  'session already has an active github_repository resource for this repository URL';
const DUPLICATE_RESOURCE_MOUNT_PATH_ERROR = 'session resources must use unique mount_path values';

function defaultResourceAccess(type: SessionResourceInput['type']): 'read_only' | 'read_write' {
  return type === 'file' ? 'read_only' : 'read_write';
}

function resourceLimit(type: SessionResourceInput['type']): number {
  if (type === 'file') return MAX_FILE_RESOURCES;
  if (type === 'memory_store') return MAX_MEMORY_STORE_RESOURCES;
  return MAX_GITHUB_REPOSITORY_RESOURCES;
}

function resourceCapError(type: SessionResourceInput['type']): string {
  return `session already has ${resourceLimit(type)} ${type} resources`;
}

/**
 * Output-only mount-path template for `memory_store` resources. Anthropic's
 * request shape does not accept `mount_path`; Registry derives and persists
 * the path so subsequent GETs round-trip the canonical value. The trailing
 * slash is intentional because s3fs/FUSE consume a directory path.
 */
function defaultMemoryMountPath(storeName: string, storeId: string): string {
  const safeName = SAFE_MOUNT_SEGMENT.test(storeName) && storeName !== '.' && storeName !== '..';
  return `/mnt/memory/${safeName ? storeName : storeId}/`;
}

const SAFE_MOUNT_SEGMENT = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;
const MEMORY_MOUNT_ROOT = '/mnt/memory/';

function validateMemoryMountPath(mountPath: string): string | null {
  if (
    mountPath.length <= MEMORY_MOUNT_ROOT.length ||
    hasControlCharacter(mountPath) ||
    !mountPath.startsWith(MEMORY_MOUNT_ROOT) ||
    posixPath.normalize(mountPath) !== mountPath
  ) {
    return 'memory_store mount_path must be a canonical absolute path below /mnt/memory/';
  }
  const segments = mountPath.slice(1).split('/').filter(Boolean);
  if (segments.some((segment) => !SAFE_MOUNT_SEGMENT.test(segment))) {
    return 'memory_store mount_path contains an invalid path segment';
  }
  return null;
}

function hasControlCharacter(value: string): boolean {
  return [...value].some((character) => {
    const codePoint = character.codePointAt(0)!;
    return codePoint <= 0x1f || codePoint === 0x7f;
  });
}

/**
 * Default mount-path template for `github_repository` resources. The path is
 * `/workspace/<repo-name>/` where `<repo-name>` is the last path segment of
 * `repoUrl`, stripped of `.git` and any trailing slash. Trailing slash is
 * intentional (the harness clones into a directory). Persisted on first
 * create/attach so subsequent GETs are canonical.
 *
 * Examples:
 *   `https://github.com/org/repo`         → `/workspace/repo/`
 *   `https://github.com/org/repo.git`     → `/workspace/repo/`
 *   `https://github.com/org/repo.git/`    → `/workspace/repo/`
 *
 * Falls back to `/workspace/repo/` for unparseable / empty inputs — the
 * vault-binding check elsewhere will already have rejected anything malformed
 * by the time we land here, so this branch is defense in depth.
 */
function defaultRepoMountPath(repoUrl: string): string {
  let lastSegment: string;
  try {
    const u = new URL(repoUrl);
    const parts = u.pathname.split('/').filter((p) => p.length > 0);
    lastSegment = parts.length > 0 ? parts[parts.length - 1]! : 'repo';
  } catch {
    lastSegment = 'repo';
  }
  // Strip `.git` suffix.
  lastSegment = lastSegment.replace(/\.git$/, '');
  if (lastSegment.length === 0) lastSegment = 'repo';
  return `/workspace/${lastSegment}/`;
}

/**
 * Resolved github_repository attachment, materialized after vault-binding
 * validation. The route persists `repoRef` into `session_resources.repo_ref`
 * (JSONB) and `mountPath` into `session_resources.mount_path`. Re-derived per
 * request — no caching across calls.
 */
interface ResolvedGithubRepo {
  repoRef: { git_credential_id: string; url: string; checkout?: Record<string, unknown> };
  mountPath: string;
  managedCredential?: ManagedGitCredentialDraft;
}

/**
 * Resolve a single `github_repository` resource into a persistence-ready
 * internal credential binding + mount path.
 *
 * Validation steps (in order — first failure short-circuits):
 *   1. Raw PAT/App tokens are converted to a resource-owned credential draft.
 *      Secret bytes are staged later, after all resources validate.
 *   2. Orca's `git_cred://<id>` extension resolves a pre-provisioned
 *      workspace credential and validates its resource ownership.
 *   3. Credential/repository URL binding is enforced for reference inputs.
 */
async function resolveGithubRepoResource(
  db: DbClient,
  workspaceId: string,
  sessionResourceId: string,
  resource: SessionResourceInput,
): Promise<ResolvedGithubRepo | { error: string }> {
  if (!resource.url) {
    return { error: 'github_repository resource missing url' };
  }
  const binding = await prepareGitCredentialBinding(db, {
    workspaceId,
    sessionResourceId,
    repoUrl: resource.url,
    authorizationToken: resource.authorization_token,
  });
  if ('error' in binding) return binding;

  const repoRef: ResolvedGithubRepo['repoRef'] = {
    git_credential_id: binding.gitCredentialId,
    url: resource.url,
    ...(resource.checkout ? { checkout: resource.checkout } : {}),
  };
  const mountPath = resource.mount_path ?? defaultRepoMountPath(resource.url);
  return {
    repoRef,
    mountPath,
    ...(binding.managedCredential ? { managedCredential: binding.managedCredential } : {}),
  };
}

/**
 * Build a {@link DistributionSessionStore} over the `sessions` table.
 *
 * The session distributor reads the distribution view of a session and writes
 * its `runner_id` / `host_environment_id` / `distribution_state` transitions
 * through this seam.
 *
 * Provider and mode come from the Session-pinned Agent version, bound to the
 * immutable Agent harness identity. Archive does not erase ownership. Invalid
 * bindings fail closed rather than borrowing the latest Agent configuration.
 * The distributor and the internal ownership route share this binding loader
 * and the catalog's resolveExecutionOwner topology rule.
 *
 * The per-session workspace IS carried: it is resolved from the session's
 * attached `github_repository` resource — the directory the repo is cloned into
 * is the session's working directory — falling back to `null` (the empty-string
 * sentinel on the wire, i.e. the worker's own default cwd) when the session
 * attaches no repository. See {@link resolveSessionWorkspace}.
 */
export function buildDistributionSessionStore(db: DbClient): DistributionSessionStore {
  return {
    async load(id: string): Promise<DistributionSessionRow | null> {
      const rows = await db
        .select()
        .from(sessions)
        .where(and(isNull(sessions.deletedAt), eq(sessions.id, id)))
        .limit(1);
      const row = rows[0];
      if (!row) return null;
      const workspace = await resolveSessionWorkspace(db, id);
      const selection = await loadSessionHarnessBinding(
        db,
        row.workspaceId,
        row.agentId,
        row.agentVersion,
      );
      const agentInfo = { provider: harnessToProvider(selection.harness), ...selection };
      return toDistributionRow(row, workspace, agentInfo);
    },
    async applyDistributionPatch(id: string, patch: DistributionSessionPatch): Promise<boolean> {
      const set: Partial<typeof sessions.$inferInsert> = { updatedAt: new Date() };
      if ('runnerId' in patch) set.runnerId = patch.runnerId ?? null;
      if ('hostEnvironmentId' in patch) set.hostEnvironmentId = patch.hostEnvironmentId ?? null;
      if ('distributionState' in patch) set.distributionState = patch.distributionState ?? null;
      const updated = await db
        .update(sessions)
        .set(set)
        .where(and(isNull(sessions.deletedAt), eq(sessions.id, id)))
        .returning({ id: sessions.id });
      return updated.length > 0;
    },
    async findBoundSessionId(runnerId: string): Promise<string | null> {
      const rows = await db
        .select({ id: sessions.id })
        .from(sessions)
        .where(and(isNull(sessions.deletedAt), eq(sessions.runnerId, runnerId)))
        .limit(1);
      return rows[0]?.id ?? null;
    },
    async loadPendingForEnvironment(environmentId: string): Promise<DispatchableSessionRef[]> {
      // Create-time-stranded set: PENDING with NO runner binding, scoped to the
      // environment whose worker just connected. A session that already minted a
      // runner (runner_id set) is excluded by the isNull(runnerId) filter so a
      // worker reconnect cannot mint a second runner for it; ASSIGNED / FAILED /
      // cloud sessions are excluded by the PENDING filter. Archived sessions are
      // excluded — a deleted/archived session must not spawn a runner on connect.
      const rows = await db
        .select({ id: sessions.id })
        .from(sessions)
        .where(
          and(
            isNull(sessions.deletedAt),
            eq(sessions.environmentId, environmentId),
            eq(sessions.distributionState, DISTRIBUTION_PENDING),
            isNull(sessions.runnerId),
            isNull(sessions.archivedAt),
          ),
        );
      return rows.map((r) => ({ id: r.id }));
    },
  };
}

/**
 * Build an {@link EnvironmentTargetLookup} over the `environments` table.
 *
 * Resolves an environment's `target` for {@link SessionDistributor.onWorkerConnect},
 * so a reconnect-driven dispatch threads the environment's REAL target instead
 * of assuming one. An archived environment resolves to `null` (treated as
 * "not dispatchable" by the distributor) — archive is soft teardown, so a
 * worker that somehow still connects for it must not have launches driven for
 * it. An unknown id likewise resolves to `null`.
 */
export function buildEnvironmentTargetLookup(db: DbClient): EnvironmentTargetLookup {
  return {
    async loadTarget(environmentId: string): Promise<string | null> {
      const rows = await db
        .select({ target: environments.target, archivedAt: environments.archivedAt })
        .from(environments)
        .where(and(isNull(environments.deletedAt), eq(environments.id, environmentId)))
        .limit(1);
      const row = rows[0];
      if (!row || row.archivedAt !== null) {
        return null;
      }
      return row.target ?? 'cloud';
    },
  };
}

/**
 * Build a {@link BoundSessionResolver} over the `sessions` table.
 *
 * The owner-pod {@link SessionEventBridgeManager} uses this to resolve a runner
 * that just connected to the (workspace, session) it is bound to, so it can start
 * that session's single-writer event bridge. A runner bound to no session row
 * (cloud session, or a runner this replica did not distribute) resolves to `null`
 * and the manager makes it a no-op. The lookup keys on `runner_id` — the per-
 * session binding the distributor minted — and returns the row's tenant workspace
 * so the bridge tails / appends under the correct scope.
 *
 * A distribution_state=`failed` session is EXCLUDED (resolves to `null`), so the
 * manager no-ops for it. The composed runner-connect hook runs the distributor's
 * `onRunnerConnect` FIRST: on a connect-time capability mismatch (the runner's
 * advertised harnesses do not include the session's provider) it flips the bound
 * session FAILED. Without this exclusion the bridge manager — which resolves on
 * `runner_id` alone — would then start a single-writer bridge (and deliver an
 * agent snapshot) for a session the distributor just failed, a redundant bridge
 * for a runner that cannot serve the session. Filtering FAILED here keeps the
 * distributor's failure decision authoritative. A live binding always carries a
 * non-null distribution_state (the distributor sets PENDING when it mints the
 * runner id and only ever advances it to ASSIGNED / FAILED), so `ne(…, failed)`
 * admits exactly the PENDING/ASSIGNED bindings and never spuriously drops a bound
 * session on the NULL edge.
 */
export function buildBoundSessionResolver(db: DbClient): BoundSessionResolver {
  return {
    async resolveBoundSession(
      runnerId: string,
    ): Promise<{ workspaceId: string; sessionId: string } | null> {
      const rows = await db
        .select({ id: sessions.id, workspaceId: sessions.workspaceId })
        .from(sessions)
        .where(
          and(
            isNull(sessions.deletedAt),
            eq(sessions.runnerId, runnerId),
            ne(sessions.distributionState, DISTRIBUTION_FAILED),
          ),
        )
        .limit(1);
      const row = rows[0];
      if (!row) {
        return null;
      }
      return { workspaceId: row.workspaceId, sessionId: row.id };
    },
  };
}

/**
 * Resolve a session's working-directory workspace for the launch frame.
 *
 * The workspace is the mount path of the session's first still-attached
 * `github_repository` resource — the directory the repo is cloned into, which is
 * the natural working directory for an agent operating on that repo. Returns
 * `null` when the session attaches no repository, in which case the worker runs
 * in its own default cwd (the launch frame then carries the empty-string
 * sentinel). Deterministic ordering by
 * `attached_at` then `id` so a multi-repo session resolves the same workspace
 * across a create-time dispatch and a later reconnect-driven one.
 */
async function resolveSessionWorkspace(db: DbClient, sessionId: string): Promise<string | null> {
  const rows = await db
    .select({ mountPath: sessionResources.mountPath })
    .from(sessionResources)
    .where(
      and(
        isNull(sessionResources.deletedAt),
        eq(sessionResources.sessionId, sessionId),
        eq(sessionResources.type, 'github_repository'),
        isNull(sessionResources.detachedAt),
      ),
    )
    .orderBy(sessionResources.attachedAt, sessionResources.id)
    .limit(1);
  const mountPath = rows[0]?.mountPath;
  return mountPath !== undefined && mountPath.length > 0 ? mountPath : null;
}

/** Distribution and ownership read the same Session-pinned harness binding. */
interface SessionAgentHarnessInfo {
  provider: string | null;
  harness: HarnessType;
  mode: HarnessMode;
}
function toDistributionRow(
  row: typeof sessions.$inferSelect,
  workspace: string | null,
  agentInfo: SessionAgentHarnessInfo,
): DistributionSessionRow {
  return {
    id: row.id,
    workspaceId: row.workspaceId,
    environmentId: row.environmentId ?? null,
    runnerId: row.runnerId ?? null,
    hostEnvironmentId: row.hostEnvironmentId ?? null,
    distributionState: (row.distributionState as DistributionState | null) ?? null,
    // Both fields come from the same immutable Session-pinned version.
    agentHarness: agentInfo.provider,
    agentMode: agentInfo.mode,
    agentHarnessType: agentInfo.harness,
    // The session's repository workspace (the cloned directory), or null when the
    // session attaches no repo — the launch frame then carries the empty-string
    // sentinel and the worker runs in its own default cwd.
    workspace,
  };
}

/**
 * The subset of `EnvironmentLaunchLifecycle`
 * (`src/environment/launch/environment-launch-lifecycle.ts`) the rest of the
 * app needs — a narrow seam (not the concrete class) so a route, or a
 * deployment/test that doesn't need cloud launches, does not need the
 * launcher/token-store machinery wired. Three independent consumers share
 * this one interface (the concrete `EnvironmentLaunchLifecycle` satisfies all
 * three structurally):
 *
 *   - `ensureLaunched` — the session-create route (below): kick the
 *     background provision→mint→startWorker→wait-online pipeline for a
 *     `target=cloud` colocated session's environment when no worker is
 *     connected yet. Documented to always RESOLVE (never reject), reporting
 *     a failure via `status:'failed'`, so callers log on that resolved status
 *     rather than relying on a catch.
 *   - `terminate` — `environments.routes.ts`'s archive/delete handlers:
 *     best-effort tear down a `target=cloud` environment's launcher-level box
 *     on teardown (I3). Never expected to throw in a way callers must
 *     specially handle beyond a defensive `catch` — see that module for the
 *     single-replica caveat.
 *   - `relaunch` — `relaunch-on-disconnect.ts`'s worker-tunnel disconnect
 *     trigger (C2): re-provision a fresh generation under the same
 *     environment id when a `target=cloud` worker with stranded work drops.
 */
export interface EnvironmentLaunchTrigger {
  ensureLaunched(
    environmentId: string,
    opts: { name: string },
  ): Promise<{ status: string; error?: string }>;
  relaunch(
    environmentId: string,
    opts: { name: string },
  ): Promise<{ status: string; error?: string }>;
  terminate(environmentId: string): Promise<void>;
}

export function registerSessionsRoutes(
  app: FastifyInstance,
  db: DbClient,
  store: TranscriptStore,
  sse: SseConfig,
  fileStore: FileStore,
  _skillStore: SkillStore,
  memoryStore?: MemoryStore,
  distributor?: SessionDistributor,
  environmentLaunchLifecycle?: EnvironmentLaunchTrigger,
  secretStore?: SecretStore,
  batchReadsEnabled = true,
): void {
  async function handleSessionStream(req: FastifyRequest, reply: FastifyReply) {
    const auth = req.auth!;
    const id = toInternalId((req.params as { id: string }).id);
    const q = req.query as {
      from_cursor?: string;
      subpath?: string;
      event_deltas?: string | string[];
      'event_deltas[]'?: string | string[];
    };
    const orcaBeta = isOrcaBetaRequest(req);
    const eventDeltas = parseEventDeltas(q.event_deltas ?? q['event_deltas[]']);
    if ('error' in eventDeltas) return reply.code(400).send({ error: eventDeltas.error });
    const session = await loadSessionRow(db, auth.workspaceId, id);
    if (!session) return reply.code(404).send({ error: 'not found' });
    const lastEventId = req.headers['last-event-id'];
    const fromCursor = (typeof lastEventId === 'string' && lastEventId) || (q.from_cursor ?? '');
    await streamSession(reply, store, auth.workspaceId, id, fromCursor, q.subpath ?? '', {
      ...sse,
      eventDeltas: eventDeltas.value,
      orcaBeta,
    });
  }

  async function handleThreadStream(req: FastifyRequest, reply: FastifyReply) {
    const auth = req.auth!;
    const params = req.params as { id: string; thread_id: string };
    const id = toInternalId(params.id);
    const thread_id = toInternalId(params.thread_id);
    const q = req.query as {
      from_cursor?: string;
      event_deltas?: string | string[];
      'event_deltas[]'?: string | string[];
    };
    const eventDeltas = parseEventDeltas(q.event_deltas ?? q['event_deltas[]']);
    if ('error' in eventDeltas) return reply.code(400).send({ error: eventDeltas.error });
    const session = await loadSessionRow(db, auth.workspaceId, id);
    if (!session) return reply.code(404).send({ error: 'not found' });
    const thread = await loadSessionThreadRow(db, auth.workspaceId, id, thread_id);
    if (!thread) return reply.code(404).send({ error: 'thread not found' });
    const lastEventId = req.headers['last-event-id'];
    const fromCursor = (typeof lastEventId === 'string' && lastEventId) || (q.from_cursor ?? '');
    await streamSession(reply, store, auth.workspaceId, id, fromCursor, thread.subpath, {
      ...sse,
      eventDeltas: eventDeltas.value,
      orcaBeta: isOrcaBetaRequest(req),
    });
  }

  app.post('/v1/sessions', async (req, reply) => {
    const auth = req.auth!;
    const invalidRequest = (message: string) =>
      reply.code(400).send(buildClaudeErrorResponse(req.id, 'invalid_request_error', message));
    const notFound = (message: string) =>
      reply.code(404).send(buildClaudeErrorResponse(req.id, 'not_found_error', message));
    const observabilityUnavailable = () =>
      reply
        .code(503)
        .send(
          buildClaudeErrorResponse(req.id, 'overloaded_error', 'agent observability unavailable'),
        );
    const orcaBeta = isOrcaBetaRequest(req);
    const rawBody = req.body as SessionCreateBody;

    // Keep the longstanding, more useful missing-environment error instead of
    // exposing a generic Zod issue for this required cross-resource field.
    if (rawBody?.environment_id === undefined || rawBody?.environment_id === null) {
      return invalidRequest('environment_id is required');
    }
    const parsedEnvironmentId = sessionEnvironmentIdSchema.safeParse(rawBody.environment_id);
    if (!parsedEnvironmentId.success) {
      return invalidRequest('environment_id must be an env_… identifier');
    }
    const environmentId = parsedEnvironmentId.data;
    const metadata = parseMetadata(rawBody.metadata);
    if (!metadata.ok) {
      return invalidRequest(metadata.error);
    }
    const llmEgressError = sessionLlmEgressMetadataError(metadata.value);
    if (llmEgressError) return invalidRequest(llmEgressError);
    const parsed = sessionCreateBodySchema.safeParse(req.body);
    if (!parsed.success) {
      return invalidRequest(firstZodIssue(parsed.error, 'invalid session create body'));
    }
    const body = {
      ...(parsed.data as SessionCreateBody),
      ...(Object.prototype.hasOwnProperty.call(rawBody, 'metadata')
        ? { metadata: rawBody.metadata }
        : {}),
    };

    const resolvedRef = resolveSessionAgentRef(body);
    if ('error' in resolvedRef) {
      return invalidRequest(resolvedRef.error);
    }
    const { agentId, requestedVersion, overrides } = resolvedRef;
    // Cross-resource validation: look up agent in workspace
    const agentRows = await db
      .select()
      .from(agents)
      .where(
        and(
          isNull(agents.deletedAt),
          eq(agents.id, agentId),
          eq(agents.workspaceId, auth.workspaceId),
        ),
      )
      .limit(1);
    const agent = agentRows[0];
    if (!agent || agent.archivedAt !== null) return notFound('agent not found');

    const environmentRows = await db
      .select({ id: environments.id, target: environments.target })
      .from(environments)
      .where(
        and(
          isNull(environments.deletedAt),
          eq(environments.id, environmentId),
          eq(environments.workspaceId, auth.workspaceId),
          isNull(environments.archivedAt),
        ),
      )
      .limit(1);
    if (!environmentRows[0]) {
      return notFound('environment not found');
    }

    const vaultIdsError = await validateSessionVaultIds(db, auth.workspaceId, body.vault_ids ?? []);
    if (vaultIdsError) return invalidRequest(vaultIdsError);

    // Explicit version pin: verify the requested version snapshot exists so we
    // never pin a session to a non-existent version (mirrors the multiagent
    // roster check at agents.routes.ts:503-506).
    if (requestedVersion !== undefined) {
      const versionRows = await db
        .select({ version: agentVersions.version })
        .from(agentVersions)
        .where(
          and(
            eq(agentVersions.workspaceId, auth.workspaceId),
            eq(agentVersions.agentId, agentId),
            eq(agentVersions.version, requestedVersion),
          ),
        )
        .limit(1);
      if (!versionRows[0]) {
        return notFound(`agent ${agentId} version ${requestedVersion} not found`);
      }
    }

    const pinnedVersion = requestedVersion ?? agent.version;
    let baseConfiguration: Awaited<ReturnType<typeof loadPinnedAgentConfiguration>>;
    try {
      baseConfiguration = await loadPinnedAgentConfiguration(
        db,
        auth.workspaceId,
        agentId,
        pinnedVersion,
        agent,
        { requireVersionSnapshot: true },
      );
    } catch (error) {
      if (error instanceof SessionAgentSnapshotUnavailableError) {
        return observabilityUnavailable();
      }
      throw error;
    }

    const executionHarness = resolveHarnessAnnotation(baseConfiguration.metadata);
    if ('error' in executionHarness) return invalidRequest(executionHarness.error);
    const deploymentError = validateHarnessDeployment(
      environmentRows[0].target ?? 'cloud',
      executionHarness,
    );
    if (deploymentError) return invalidRequest(deploymentError);
    const featureError = validateHarnessFeatures(baseConfiguration.metadata, {
      skills: overrides?.skills,
    });
    if (featureError) return invalidRequest(featureError);
    const storedAgentOverrides: StoredSessionAgentOverrides = {};
    if (overrides?.model !== undefined) {
      const normalizedModel = mergeModelOverrideForStorage(
        baseConfiguration.model,
        overrides.model,
        executionHarness.harness,
      );
      if ('error' in normalizedModel) return invalidRequest(normalizedModel.error);
      const harnessModelError = validateHarnessModel(baseConfiguration.metadata, normalizedModel);
      if (harnessModelError) return invalidRequest(harnessModelError);
      storedAgentOverrides.model = normalizedModel;
    }
    if (overrides && Object.prototype.hasOwnProperty.call(overrides, 'system')) {
      storedAgentOverrides.system = overrides.system ?? null;
    }
    if (overrides?.skills !== undefined) {
      const validatedSkills = await validateSkillRefs(db, auth.workspaceId, overrides.skills);
      if ('error' in validatedSkills) return invalidRequest(validatedSkills.error);
      storedAgentOverrides.skills = validatedSkills;
    }
    if (overrides?.guardrail_ids !== undefined) {
      const validatedGuardrails = await validateGuardrailRefs(
        db,
        auth.workspaceId,
        overrides.guardrail_ids,
      );
      if ('error' in validatedGuardrails) return invalidRequest(validatedGuardrails.error);
      storedAgentOverrides.guardrailIds = validatedGuardrails;
    }
    const modelRosterError = await validateMultiagentModelRoster(
      db,
      auth.workspaceId,
      agentId,
      pinnedVersion,
      storedAgentOverrides.model ?? baseConfiguration.model,
      baseConfiguration.multiagent,
      executionHarness.harness,
    );
    if (modelRosterError) return invalidRequest(modelRosterError);
    let persistedToolsOverride: AgentTool[] | null = null;
    let persistedMcpServersOverride: unknown[] | null = null;
    if (overrides?.tools !== undefined || overrides?.mcp_servers !== undefined) {
      const overrideTools =
        overrides.tools === undefined ? undefined : canonicalizeAgentTools(overrides.tools);
      const overrideMcpServers =
        overrides.mcp_servers === undefined
          ? undefined
          : stripSessionMcpServerPermissionPolicyFields(overrides.mcp_servers);
      const configurationError = validateAgentConfiguration(
        overrideTools ?? baseConfiguration.tools,
        overrideMcpServers ?? baseConfiguration.mcpServers,
      );
      if (configurationError) return invalidRequest(configurationError);
      persistedToolsOverride = overrideTools ?? null;
      persistedMcpServersOverride = overrideMcpServers ?? null;
    }
    const persistedAgentOverrides =
      Object.keys(storedAgentOverrides).length > 0 ? storedAgentOverrides : null;

    const initialEvents = body.initial_events ?? [];
    if (initialEvents.length > 50)
      return invalidRequest('initial_events must contain at most 50 events');
    if (
      initialEvents.some(
        (event) => event.type !== 'user.message' && event.type !== 'user.define_outcome',
      )
    ) {
      return invalidRequest('initial_events only supports user.message and user.define_outcome');
    }
    const resources = body.resources ?? [];
    if (resources.length > MAX_FILE_RESOURCES) {
      return invalidRequest(`too many resources (max ${MAX_FILE_RESOURCES})`);
    }
    const fileResourceCount = resources.filter((r) => r.type === 'file').length;
    if (fileResourceCount > MAX_FILE_RESOURCES) {
      return invalidRequest(`too many file resources (max ${MAX_FILE_RESOURCES})`);
    }
    const memoryResourceCount = resources.filter((r) => r.type === 'memory_store').length;
    if (memoryResourceCount > MAX_MEMORY_STORE_RESOURCES) {
      return invalidRequest(
        `too many memory_store resources (max ${MAX_MEMORY_STORE_RESOURCES} per session)`,
      );
    }
    const repoResourceCount = resources.filter((r) => r.type === 'github_repository').length;
    if (repoResourceCount > MAX_GITHUB_REPOSITORY_RESOURCES) {
      return invalidRequest(
        `too many github_repository resources (max ${MAX_GITHUB_REPOSITORY_RESOURCES} per session)`,
      );
    }

    // Validate file_id, memory_store_id, and github_repository references
    // against the workspace before opening a tx so we don't roll back on user
    // error and we surface a clean 400. We derive memory_store mount_path
    // server-side and resolve github_repository defaults during this pass so
    // the persisted row carries the canonical path for the lifetime of the
    // attachment (subsequent GETs round-trip the canonical value).
    const resourceIds = resources.map(() => newId('sesrsc'));
    const memoryMountPaths = new Map<number, string>();
    const repoResolutions = new Map<number, ResolvedGithubRepo>();
    for (let i = 0; i < resources.length; i++) {
      const r = resources[i]!;
      const msErr = validateMountStrategy(r);
      if (msErr) return invalidRequest(msErr);
      if (r.type === 'file') {
        if (!r.file_id) {
          return invalidRequest('file resource missing file_id');
        }
        const exists = await fileStore.get(auth.workspaceId, r.file_id);
        if (!exists) {
          return invalidRequest(`file_id ${r.file_id} not found in workspace`);
        }
        const mountPathErr = validateResourceMountPath(
          r.mount_path ?? `/mnt/session/uploads/${r.file_id}`,
        );
        if (mountPathErr) return invalidRequest(mountPathErr);
      } else if (r.type === 'memory_store') {
        if (!r.memory_store_id) {
          return invalidRequest('memory_store resource missing memory_store_id');
        }
        if (!memoryStore) {
          // Server is not configured with a MemoryStore — the route was
          // mounted (file resources only) but the caller asked for a
          // memory_store. Surface as 400 so the client knows this deployment
          // doesn't support memory.
          return invalidRequest('memory_store resources are not enabled on this server');
        }
        const fetched = await memoryStore.getStore(auth.workspaceId, r.memory_store_id);
        if (!fetched) {
          return invalidRequest(`memory_store ${r.memory_store_id} not found in workspace`);
        }
        const mountPath = defaultMemoryMountPath(fetched.name, fetched.id);
        const mountPathErr = validateMemoryMountPath(mountPath);
        if (mountPathErr) return invalidRequest(mountPathErr);
        const reservedMountPathErr = validateResourceMountPath(mountPath);
        if (reservedMountPathErr) return invalidRequest(reservedMountPathErr);
        if ([...memoryMountPaths.values()].includes(mountPath)) {
          return invalidRequest(DUPLICATE_RESOURCE_MOUNT_PATH_ERROR);
        }
        memoryMountPaths.set(i, mountPath);
      } else if (r.type === 'github_repository') {
        const resolved = await resolveGithubRepoResource(db, auth.workspaceId, resourceIds[i]!, r);
        if ('error' in resolved) {
          return invalidRequest(resolved.error);
        }
        const mountPathErr = validateResourceMountPath(resolved.mountPath);
        if (mountPathErr) return invalidRequest(mountPathErr);
        const duplicate = [...repoResolutions.values()].some((existing) =>
          repositoryUrlsEqual(existing.repoRef.url, resolved.repoRef.url),
        );
        if (duplicate) {
          return invalidRequest(DUPLICATE_GITHUB_REPOSITORY_ERROR);
        }
        repoResolutions.set(i, resolved);
      }
    }

    const managedGitCredentials = [...repoResolutions.values()].flatMap((resolved) =>
      resolved.managedCredential ? [resolved.managedCredential] : [],
    );
    const id = newId('ses');
    const trustedUserId =
      auth.authMethod === 'oidc' ? oidcTranscriptUserId(auth.oidcIssuer, auth.userId) : undefined;
    const initialProtoEvents = initialEvents.map((input, index) =>
      httpEventToProto({
        workspaceId: auth.workspaceId,
        sessionId: id,
        producedBy: 'client',
        userId: trustedUserId,
        idempotencyKey: `${id}:initial:${index}`,
        input: prepareClaudeEventInput(input),
        validationMode: 'claude',
      }),
    );
    const stagedGitCredentials = await stageGitCredentialDraftsForRequest(
      db,
      req,
      reply,
      secretStore,
      managedGitCredentials,
    );
    if (stagedGitCredentials === null) return;

    const now = new Date();
    const discardStagedCredentials = async (): Promise<void> => {
      if (!secretStore || stagedGitCredentials.length === 0) return;
      await discardGitCredentialStagingIntents(db, secretStore, stagedGitCredentials).catch(() => {
        req.log.warn('failed to discard staged git credentials after session create');
      });
    };

    const resourceRows = resources.map((resource, index) => {
      const resourceId = resourceIds[index]!;
      let resolvedMountPath: string;
      let repoRefValue: Record<string, unknown> | null = null;
      if (resource.type === 'memory_store') {
        resolvedMountPath = memoryMountPaths.get(index)!;
      } else if (resource.type === 'github_repository') {
        const resolved = repoResolutions.get(index)!;
        resolvedMountPath = resolved.mountPath;
        repoRefValue = resolved.repoRef;
      } else {
        resolvedMountPath = resource.mount_path ?? `/mnt/session/uploads/${resource.file_id ?? ''}`;
      }
      return {
        id: resourceId,
        workspaceId: auth.workspaceId,
        sessionId: id,
        type: resource.type,
        fileId: resource.type === 'file' ? (resource.file_id ?? null) : null,
        memoryStoreId: resource.type === 'memory_store' ? (resource.memory_store_id ?? null) : null,
        repoRef: repoRefValue,
        mountPath: resolvedMountPath,
        access: resource.access ?? defaultResourceAccess(resource.type),
        mountStrategy: resource.type === 'file' ? (resource.mount_strategy ?? null) : null,
        instructions: resource.instructions ?? null,
        attachedAt: now,
        updatedAt: now,
        detachedAt: null,
      };
    });
    const gitCredentialRows = managedGitCredentials.map((draft) =>
      managedGitCredentialInsert(draft, now),
    );

    let creation: Awaited<ReturnType<typeof createSessionInTransaction>>;
    try {
      creation = await db.transaction(async (tx) =>
        createSessionInTransaction(tx, {
          id,
          workspaceId: auth.workspaceId,
          agentId,
          agentVersion: requestedVersion ?? agent.version,
          title: body.title ?? null,
          metadata: toJsonMetadata(metadata.value),
          environmentId,
          vaultIds: body.vault_ids ?? [],
          tools: persistedToolsOverride,
          mcpServers: persistedMcpServersOverride,
          agentOverrides: persistedAgentOverrides as Record<string, unknown> | null,
          ...(storedAgentOverrides.skills !== undefined
            ? { primarySkillRefsOverride: storedAgentOverrides.skills }
            : {}),
          resourceRows,
          gitCredentialRows,
          stagedGitCredentialIds: managedGitCredentials.map((draft) => draft.id),
          initialEvents: initialProtoEvents,
          now,
        }),
      );
    } catch (error) {
      await discardStagedCredentials();
      if (
        error instanceof SessionObservabilitySelectionAvailabilityError ||
        error instanceof SessionAgentSnapshotUnavailableError
      ) {
        return observabilityUnavailable();
      }
      if (error instanceof SessionObservabilitySelectionResourceUnavailableError) {
        return notFound('workspace not found');
      }
      if (error instanceof SessionSkillBindingError) {
        return invalidRequest(error.message);
      }
      throw error;
    }
    if (!creation.ok) {
      await discardStagedCredentials();
      if (
        creation.reason === 'vault' ||
        creation.reason === 'metadata' ||
        creation.reason === 'deployment'
      ) {
        return invalidRequest(creation.message);
      }
      return notFound(creation.message);
    }

    // The outbox row and Session commit atomically. Only now may the external
    // transcript publish become visible to a Harness consumer.
    if (creation.initialEventsOutboxId) {
      try {
        const result = await reconcileSessionLifecycleOutbox(db, store, {
          eventIds: [creation.initialEventsOutboxId],
        });
        if (result.failed > 0) {
          req.log.warn('initial session events queued for retry');
        }
      } catch (error) {
        req.log.warn({ err: error }, 'failed to publish queued initial session events');
      }

      try {
        const [outboxState] = await db
          .select({ publishedAt: sessionLifecycleOutbox.publishedAt })
          .from(sessionLifecycleOutbox)
          .where(eq(sessionLifecycleOutbox.id, creation.initialEventsOutboxId))
          .limit(1);
        if (outboxState?.publishedAt) {
          await indexTranscriptEvents(db, initialProtoEvents, {
            guardrailSubject: authenticatedGuardrailSubject(auth),
          });
        }
      } catch (error) {
        req.log.warn(
          { err: error },
          'initial transcript append succeeded but event index update failed',
        );
      }
    }

    // Claim-based distribution. When the session targets a `self_hosted` OR a
    // `cloud` environment, hand it to the distributor: it persists the
    // distribution_state PENDING and, if that environment already has a
    // connected worker on this replica, mints a binding token, pre-registers the
    // expected runner, and pushes a `worker.launch_runner` frame down the worker
    // tunnel. With no connected worker the session simply stays PENDING (surfaced
    // by the work depth). The dispatch never throws on a worker-side failure — a
    // refusal/timeout is a state on the session row, not a 500 on this create.
    //
    // `cloud` additionally needs a WORKER to exist before any of the above can
    // matter — unlike self_hosted, nothing dials in on its own. When the agent is
    // `colocated` (the only mode session-runner serves) and `dispatch` found no
    // connected worker (`outcome.runnerId === null`), kick the environment-launch
    // lifecycle's background provision→mint→startWorker→wait-online pipeline
    // (the box does not exist yet, and this returns 201 before it does).
    // `ensureLaunched` is single-flighted per environment id and a no-op if a
    // worker is already online (a benign race with `dispatch`'s own
    // connected-worker check above), so calling it here is always safe, never a
    // double-launch. Once that worker dials in, the (now target-agnostic)
    // worker-tunnel connect hook's
    // `SessionDistributor.onWorkerConnect` finds this session still PENDING with
    // no runner and dispatches it — the EXISTING distributor + tunnel machinery
    // from there drives the turn, exactly as it already does for self_hosted; see
    // `environment-launch-lifecycle.ts`'s module doc for why "bind" is
    // deliberately not this module's own job.
    if (distributor !== undefined) {
      const envRows = await db
        .select({ id: environments.id, name: environments.name, target: environments.target })
        .from(environments)
        .where(
          and(
            isNull(environments.deletedAt),
            eq(environments.id, environmentId),
            eq(environments.workspaceId, auth.workspaceId),
          ),
        )
        .limit(1);
      const env = envRows[0];
      if (env !== undefined && (env.target === 'self_hosted' || env.target === 'cloud')) {
        // Fire-and-forget the worker's launch verdict (the bounded launch-result
        // wait + FAILED flip run in the background); the create returns as soon as
        // the PENDING transition + frame send complete.
        const outcome = await distributor.dispatch({
          sessionId: id,
          environment: { id: env.id, target: env.target },
        });
        void outcome.launchSettled;

        if (
          env.target === 'cloud' &&
          outcome.runnerId === null &&
          environmentLaunchLifecycle !== undefined &&
          resolveExecutionOwner('cloud', executionHarness) === 'registry'
        ) {
          void environmentLaunchLifecycle
            .ensureLaunched(env.id, { name: env.name })
            .then((launch) => {
              if (launch.status === 'failed') {
                req.log.error(
                  { sessionId: id, environmentId: env.id, error: launch.error },
                  'environment-launch: ensureLaunched failed',
                );
              }
            })
            .catch((err: unknown) => {
              // ensureLaunched is documented to resolve (never reject) on
              // failure; a rejection here is an unexpected fault, not a normal
              // launch failure — still must not crash the fire-and-forget chain.
              req.log.error(
                { err, sessionId: id, environmentId: env.id },
                'environment-launch: ensureLaunched rejected unexpectedly',
              );
            });
        }
      }
    }

    const out = await loadSession(db, auth.workspaceId, id, orcaBeta);
    return reply.code(200).send(out);
  });

  app.get('/v1/sessions', async (req, reply) => {
    const auth = req.auth!;
    const q = req.query as {
      limit?: string;
      page?: string;
      agent_id?: string;
      agent_version?: string;
      'created_at[gt]'?: string;
      'created_at[gte]'?: string;
      'created_at[lt]'?: string;
      'created_at[lte]'?: string;
      deployment_id?: string;
      include_archived?: string | boolean;
      memory_store_id?: string;
      order?: string;
      statuses?: string | string[];
      'statuses[]'?: string | string[];
    };
    const parsedLimit = parsePositiveIntQueryParam(q.limit, 'limit', {
      defaultValue: SESSION_LIST_PAGE_DEFAULT,
      max: SESSION_LIST_PAGE_MAX,
    });
    if (!parsedLimit.ok) return reply.code(400).send({ error: parsedLimit.error });
    const includeArchived = parseBooleanQuery(q.include_archived, false);
    if ('error' in includeArchived) return reply.code(400).send({ error: includeArchived.error });
    const metadataFilters = sessionMetadataFiltersQuerySchema.safeParse(q);
    if (!metadataFilters.success) {
      return reply.code(400).send({
        error: `invalid metadata filters: ${metadataFilters.error.issues[0]?.message}`,
      });
    }
    if (q.order !== undefined && q.order !== 'asc' && q.order !== 'desc') {
      return reply.code(400).send({ error: 'order must be asc or desc' });
    }
    const cursor = parseSessionPageCursor(q.page);
    if (q.page && !cursor) return reply.code(400).send({ error: 'invalid page' });
    if (q.order !== undefined && cursor && q.order !== cursor.order) {
      return reply.code(400).send({ error: 'page cursor order does not match order' });
    }
    const order = q.order ?? cursor?.order ?? 'desc';
    const statuses = parseSessionStatuses(q.statuses ?? q['statuses[]']);
    if ('error' in statuses) return reply.code(400).send({ error: statuses.error });
    const createdGt = parseTimestampQuery(q['created_at[gt]'], 'created_at[gt]');
    const createdGte = parseTimestampQuery(q['created_at[gte]'], 'created_at[gte]');
    const createdLt = parseTimestampQuery(q['created_at[lt]'], 'created_at[lt]');
    const createdLte = parseTimestampQuery(q['created_at[lte]'], 'created_at[lte]');
    if ('error' in createdGt) return reply.code(400).send({ error: createdGt.error });
    if ('error' in createdGte) return reply.code(400).send({ error: createdGte.error });
    if ('error' in createdLt) return reply.code(400).send({ error: createdLt.error });
    if ('error' in createdLte) return reply.code(400).send({ error: createdLte.error });
    const agentId = q.agent_id ? toInternalId(q.agent_id) : undefined;
    const agentVersion = q.agent_version === undefined ? undefined : Number(q.agent_version);
    if (
      q.agent_version !== undefined &&
      (!Number.isInteger(agentVersion) || (agentVersion ?? 0) < 1)
    ) {
      return reply.code(400).send({ error: 'agent_version must be a positive integer' });
    }
    const baseConditions: Array<SQL | undefined> = [
      eq(sessions.workspaceId, auth.workspaceId),
      includeArchived.value ? undefined : isNull(sessions.archivedAt),
      agentId ? eq(sessions.agentId, agentId) : undefined,
      agentVersion ? eq(sessions.agentVersion, agentVersion) : undefined,
      ...metadataFilters.data.map(([key, value]) => sql`${sessions.metadata}->>${key} = ${value}`),
      createdGt.value ? gt(sessions.createdAt, createdGt.value) : undefined,
      createdGte.value ? gte(sessions.createdAt, createdGte.value) : undefined,
      createdLt.value ? lt(sessions.createdAt, createdLt.value) : undefined,
      createdLte.value ? lte(sessions.createdAt, createdLte.value) : undefined,
      statuses.value.length > 0 ? inArray(sessions.status, statuses.value) : undefined,
      q.deployment_id ? sql`false` : undefined,
      q.memory_store_id
        ? sql`exists (
            select 1 from ${sessionResources}
            where ${sessionResources.workspaceId} = ${sessions.workspaceId}
              and ${sessionResources.sessionId} = ${sessions.id}
              and ${sessionResources.memoryStoreId} = ${toInternalId(q.memory_store_id)}
              and ${sessionResources.detachedAt} is null
          )`
        : undefined,
    ];

    let cursorPredicate: SQL | undefined;
    if (cursor) {
      const cursorRows = await db
        .select({ id: sessions.id, createdAt: sessions.createdAt })
        .from(sessions)
        .where(and(isNull(sessions.deletedAt), ...baseConditions, eq(sessions.id, cursor.id)))
        .limit(1);
      const cursorRow = cursorRows[0];
      if (!cursorRow) return reply.code(400).send({ error: 'invalid page' });
      const moveTowardLarger =
        (order === 'asc' && cursor.direction === 'next') ||
        (order === 'desc' && cursor.direction === 'prev');
      cursorPredicate = moveTowardLarger
        ? sql`(${sessions.createdAt}, ${sessions.id}) > (${cursorRow.createdAt}, ${cursorRow.id})`
        : sql`(${sessions.createdAt}, ${sessions.id}) < (${cursorRow.createdAt}, ${cursorRow.id})`;
    }
    const limit = parsedLimit.value!;
    const reverseQuery = cursor?.direction === 'prev';
    const ascendingQuery = (order === 'asc') !== reverseQuery;
    const queriedRows = await db
      .select()
      .from(sessions)
      .where(and(isNull(sessions.deletedAt), ...baseConditions, cursorPredicate))
      .orderBy(
        ascendingQuery ? asc(sessions.createdAt) : desc(sessions.createdAt),
        ascendingQuery ? asc(sessions.id) : desc(sessions.id),
      )
      .limit(limit + 1);
    const pageRows = queriedRows.slice(0, limit);
    if (reverseQuery) pageRows.reverse();
    const orcaBeta = isOrcaBetaRequest(req);
    const items = batchReadsEnabled
      ? await loadSessionRows(
          db,
          auth.workspaceId,
          pageRows,
          orcaBeta,
          requestReadSignal(req, reply),
        )
      : (
          await Promise.all(
            pageRows.map((row) => loadSession(db, auth.workspaceId, row.id, orcaBeta)),
          )
        ).filter((item) => item !== null);
    const first = pageRows[0];
    const last = pageRows[pageRows.length - 1];
    const response = {
      data: items,
      next_page:
        last && (cursor?.direction === 'prev' || queriedRows.length > limit)
          ? encodeSessionPageCursor('next', order, last.id)
          : null,
      prev_page:
        first && (cursor?.direction === 'next' || (reverseQuery && queriedRows.length > limit))
          ? encodeSessionPageCursor('prev', order, first.id)
          : null,
    };
    // Both dialects get `prev_page`. The contract declares it required on this
    // envelope; withholding it from `orca-beta` made the declaration false for
    // the dialect, and adding a key nobody was promised the absence of is
    // additive for the clients that were reading the short form.
    reply.send(response);
  });

  app.get('/v1/sessions/:id', async (req, reply) => {
    const auth = req.auth!;
    const id = toInternalId((req.params as { id: string }).id);
    const orcaBeta = isOrcaBetaRequest(req);
    const session = await loadSession(
      db,
      auth.workspaceId,
      id,
      orcaBeta,
      batchReadsEnabled
        ? createSessionReadContext(db, auth.workspaceId, orcaBeta, requestReadSignal(req, reply))
        : undefined,
    );
    if (!session) return reply.code(404).send({ error: 'not found' });
    reply.send(session);
  });

  /** UpdateSession: metadata patch/title and full-replacement tools/MCP lists. */
  app.post('/v1/sessions/:id', async (req, reply) => {
    const auth = req.auth!;
    const id = toInternalId((req.params as { id: string }).id);
    const rawBody = (req.body ?? {}) as Record<string, unknown>;

    if (Object.prototype.hasOwnProperty.call(rawBody, 'vault_ids')) {
      return reply.code(400).send({ error: 'vault_ids updates are not yet supported' });
    }
    const parsed = sessionUpdateBodySchema.safeParse(rawBody);
    if (!parsed.success) {
      return reply.code(400).send({ error: parsed.error.issues[0]?.message ?? 'invalid body' });
    }
    const body = parsed.data as SessionUpdateBody;
    const existing = await loadSessionRow(db, auth.workspaceId, id);
    if (!existing) return reply.code(404).send({ error: 'not found' });

    const hasToolsOverride = body.agent?.tools !== undefined;
    const hasMcpOverride = body.agent?.mcp_servers !== undefined;
    const hasModelOverride = body.agent?.model !== undefined;
    if (existing.archivedAt !== null || existing.status === 'terminated') {
      return reply.code(409).send({ error: 'archived or terminated sessions cannot be updated' });
    }
    if ((hasToolsOverride || hasMcpOverride || hasModelOverride) && existing.status !== 'idle') {
      return reply.code(409).send({ error: 'session must be idle to update agent configuration' });
    }
    const hasMetadata = Object.prototype.hasOwnProperty.call(rawBody, 'metadata');
    const existingMetadata = normalizeStoredMetadata(existing.metadata);
    let nextMetadata = existingMetadata;
    if (hasMetadata) {
      if (rawBody.metadata === null) {
        nextMetadata = {};
      } else {
        const metadataPatch = parseMetadataPatch(rawBody.metadata);
        if (!metadataPatch.ok) return reply.code(400).send({ error: metadataPatch.error });
        nextMetadata = applyMetadataPatch(existing.metadata, metadataPatch.value);
      }
      const metadataError = validateMetadataLimits(nextMetadata);
      if (metadataError) return reply.code(400).send({ error: metadataError });
      const llmEgressError = sessionLlmEgressMetadataError(nextMetadata);
      if (llmEgressError) return reply.code(400).send({ error: llmEgressError });
      if (
        nextMetadata[SESSION_LLM_EGRESS_KEY] !== existingMetadata[SESSION_LLM_EGRESS_KEY] &&
        existing.status !== 'idle'
      ) {
        return reply.code(409).send({ error: 'session must be idle to change LLM egress' });
      }
    }

    const nextTitle = Object.prototype.hasOwnProperty.call(body, 'title')
      ? (body.title ?? null)
      : existing.title;
    const nextToolsOverride = hasToolsOverride
      ? canonicalizeAgentTools(body.agent!.tools ?? [])
      : existing.tools;
    const nextMcpServersOverride = hasMcpOverride
      ? stripSessionMcpServerPermissionPolicyFields(body.agent!.mcp_servers)
      : existing.mcpServers;
    if (hasToolsOverride || hasMcpOverride) {
      const baseConfiguration = await loadPinnedAgentConfiguration(
        db,
        auth.workspaceId,
        existing.agentId,
        existing.agentVersion,
      );
      const effectiveTools = Array.isArray(nextToolsOverride)
        ? canonicalizeAgentTools(nextToolsOverride)
        : baseConfiguration.tools;
      const effectiveMcpServers = Array.isArray(nextMcpServersOverride)
        ? nextMcpServersOverride
        : baseConfiguration.mcpServers;
      const configurationError = validateAgentConfiguration(effectiveTools, effectiveMcpServers);
      if (configurationError) return reply.code(400).send({ error: configurationError });
    }
    const titleChanged = nextTitle !== existing.title;
    const metadataChanged = hasMetadata && !jsonValuesEqual(nextMetadata, existing.metadata);
    const llmEgressChanged =
      nextMetadata[SESSION_LLM_EGRESS_KEY] !== existingMetadata[SESSION_LLM_EGRESS_KEY];
    const toolsChanged = hasToolsOverride && !jsonValuesEqual(nextToolsOverride, existing.tools);
    const mcpChanged =
      hasMcpOverride && !jsonValuesEqual(nextMcpServersOverride, existing.mcpServers);

    // A model override is persisted into the stored agent overrides, which
    // prepare-execution already applies. Without this the field would be
    // accepted and silently dropped — worse than the 400 it replaced, because
    // the caller would believe the switch took effect.
    const storedOverrides = isPlainObject(existing.agentOverrides)
      ? ({
          ...(existing.agentOverrides as StoredSessionAgentOverrides),
        } as StoredSessionAgentOverrides)
      : ({} as StoredSessionAgentOverrides);
    let modelChanged = false;
    if (hasModelOverride) {
      const baseConfiguration = await loadPinnedAgentConfiguration(
        db,
        auth.workspaceId,
        existing.agentId,
        existing.agentVersion,
        undefined,
        { requireVersionSnapshot: true },
      );
      const normalized = mergeModelOverrideForStorage(
        baseConfiguration.model,
        body.agent!.model,
        typeof baseConfiguration.metadata.harness === 'string'
          ? baseConfiguration.metadata.harness
          : undefined,
      );
      if ('error' in normalized) return reply.code(400).send({ error: normalized.error });
      const harnessModelError = validateHarnessModel(baseConfiguration.metadata, normalized);
      if (harnessModelError) return reply.code(400).send({ error: harnessModelError });
      storedOverrides.model = normalized;
      modelChanged = true;
    }

    if (!titleChanged && !metadataChanged && !toolsChanged && !mcpChanged && !modelChanged) {
      return reply.send((await loadSession(db, auth.workspaceId, id, isOrcaBetaRequest(req)))!);
    }

    const now = new Date();
    const update: Partial<typeof sessions.$inferInsert> = {
      updatedAt: now,
    };
    if (toolsChanged || mcpChanged || modelChanged || llmEgressChanged) {
      update.runtimeRevision = sql`${sessions.runtimeRevision} + 1` as unknown as number;
    }
    if (titleChanged) update.title = nextTitle;
    if (metadataChanged) update.metadata = toJsonMetadata(nextMetadata);
    if (toolsChanged) update.tools = nextToolsOverride;
    if (mcpChanged) update.mcpServers = nextMcpServersOverride;
    if (modelChanged) update.agentOverrides = storedOverrides;

    const updateConditions: SQL[] = [
      eq(sessions.id, id),
      eq(sessions.workspaceId, auth.workspaceId),
      // A cosmetic patch merges the earlier metadata snapshot. Do not let it
      // overwrite an execution change that committed while this request waited.
      eq(sessions.runtimeRevision, existing.runtimeRevision),
      isNull(sessions.archivedAt),
      ne(sessions.status, 'terminated'),
    ];
    if (hasToolsOverride || hasMcpOverride || hasModelOverride || llmEgressChanged) {
      updateConditions.push(eq(sessions.status, 'idle'));
    }
    const updatedRows = await db
      .update(sessions)
      .set(update)
      .where(and(isNull(sessions.deletedAt), ...updateConditions))
      .returning({ id: sessions.id });
    if (updatedRows.length === 0) {
      return reply.code(409).send({ error: 'session state changed before the update completed' });
    }

    const orcaBeta = isOrcaBetaRequest(req);
    const out = await loadSession(db, auth.workspaceId, id, orcaBeta);
    const payload: Record<string, unknown> = { type: 'session.updated' };
    if (titleChanged) payload.title = nextTitle;
    if (metadataChanged) payload.metadata = nextMetadata;
    if (toolsChanged || mcpChanged) payload.agent = (out as { agent?: unknown })?.agent ?? null;
    const updatedEvent: Event = {
      id: newId('evt'),
      workspaceId: auth.workspaceId,
      sessionId: id,
      subpath: '',
      seq: 0,
      producedAt: now.toISOString(),
      producedBy: 'registry',
      kind: 'session.updated',
      payload: Buffer.from(JSON.stringify(payload), 'utf8'),
      idempotencyKey: `${id}:updated:${now.toISOString()}`,
    };
    try {
      await store.append(auth.workspaceId, id, [updatedEvent]);
      await indexTranscriptEvents(db, [updatedEvent]);
    } catch (error) {
      req.log.warn({ err: error }, 'session updated but session.updated event append failed');
    }
    return reply.send(out);
  });

  app.post('/v1/sessions/:id/archive', async (req, reply) => {
    const auth = req.auth!;
    const id = toInternalId((req.params as { id: string }).id);
    const now = new Date();
    const lifecycleEvent = newSessionArchiveOutboxRow(auth.workspaceId, id, now);
    const archiveResult = await db.transaction(async (tx) => {
      const sessionRows = await tx
        .select({ id: sessions.id })
        .from(sessions)
        .where(
          and(
            isNull(sessions.deletedAt),
            eq(sessions.id, id),
            eq(sessions.workspaceId, auth.workspaceId),
          ),
        )
        .for('update')
        .limit(1);
      if (!sessionRows[0]) return null;

      await transitionSessionObservabilityBindingInTransaction(tx, {
        workspaceId: auth.workspaceId,
        sessionId: id,
        action: 'archive',
        now,
      });

      const resourceRows = await tx
        .select({ id: sessionResources.id })
        .from(sessionResources)
        .where(
          and(
            isNull(sessionResources.deletedAt),
            eq(sessionResources.workspaceId, auth.workspaceId),
            eq(sessionResources.sessionId, id),
          ),
        )
        .for('update');
      const resourceIds = resourceRows.map((row) => row.id);
      const managedCredentialRows =
        resourceIds.length === 0
          ? []
          : await tx
              .select()
              .from(gitCredentials)
              .where(
                and(
                  isNull(gitCredentials.deletedAt),
                  eq(gitCredentials.workspaceId, auth.workspaceId),
                  inArray(gitCredentials.sessionResourceId, resourceIds),
                ),
              );
      if (managedCredentialRows.length > 0) {
        await tx
          .update(gitCredentials)
          .set({ archivedAt: now, updatedAt: now })
          .where(
            and(
              isNull(gitCredentials.deletedAt),
              eq(gitCredentials.workspaceId, auth.workspaceId),
              inArray(
                gitCredentials.id,
                managedCredentialRows.map((row) => row.id),
              ),
            ),
          );
      }
      await tx
        .update(sessions)
        .set({ archivedAt: now, updatedAt: now })
        .where(
          and(
            isNull(sessions.deletedAt),
            eq(sessions.id, id),
            eq(sessions.workspaceId, auth.workspaceId),
          ),
        );
      await tx.insert(sessionLifecycleOutbox).values(lifecycleEvent);
      return {
        secretRefs: managedCredentialRows.map((row) => row.secretRef),
        lifecycleEventId: lifecycleEvent.id,
      };
    });
    if (archiveResult === null) return reply.code(404).send({ error: 'not found' });
    await purgeGitCredentialSecretRefs(secretStore, archiveResult.secretRefs).catch(
      (error: unknown) => {
        req.log.warn(
          { err: error },
          'failed to purge managed git credentials after session archive',
        );
      },
    );
    await reconcileSessionLifecycleOutbox(db, store, {
      eventIds: [archiveResult.lifecycleEventId],
    })
      .then((result) => {
        if (result.failed > 0) {
          req.log.warn('session archive sentinel queued for retry');
        }
      })
      .catch((error: unknown) => {
        req.log.warn({ err: error }, 'failed to publish queued session archive sentinel');
      });
    const session = await loadSession(db, auth.workspaceId, id, isOrcaBetaRequest(req));
    return reply.send(session!);
  });

  app.delete('/v1/sessions/:id', async (req, reply) => {
    const auth = req.auth!;
    const id = toInternalId((req.params as { id: string }).id);
    const now = new Date();
    const lifecycleEvent = newSessionDeleteOutboxRow(auth.workspaceId, id, now);

    const deleteResult = await db.transaction(async (tx) => {
      const sessionRows = await tx
        .select({ id: sessions.id })
        .from(sessions)
        .where(
          and(
            isNull(sessions.deletedAt),
            eq(sessions.id, id),
            eq(sessions.workspaceId, auth.workspaceId),
          ),
        )
        .for('update')
        .limit(1);
      if (!sessionRows[0]) return null;

      await transitionSessionObservabilityBindingInTransaction(tx, {
        workspaceId: auth.workspaceId,
        sessionId: id,
        action: 'delete',
        now,
      });

      const resourceRows = await tx
        .select({ id: sessionResources.id })
        .from(sessionResources)
        .where(
          and(
            isNull(sessionResources.deletedAt),
            eq(sessionResources.workspaceId, auth.workspaceId),
            eq(sessionResources.sessionId, id),
          ),
        )
        .for('update');
      const resourceIds = resourceRows.map((row) => row.id);
      const managedCredentialRows =
        resourceIds.length === 0
          ? []
          : await tx
              .select()
              .from(gitCredentials)
              .where(
                and(
                  isNull(gitCredentials.deletedAt),
                  eq(gitCredentials.workspaceId, auth.workspaceId),
                  inArray(gitCredentials.sessionResourceId, resourceIds),
                ),
              );
      if (managedCredentialRows.length > 0) {
        await tx
          .update(gitCredentials)
          .set({ deletedAt: now })
          .where(
            and(
              isNull(gitCredentials.deletedAt),
              eq(gitCredentials.workspaceId, auth.workspaceId),
              inArray(
                gitCredentials.id,
                managedCredentialRows.map((row) => row.id),
              ),
            ),
          );
      }
      await tx
        .update(sessionResources)
        .set({ deletedAt: now })
        .where(
          and(
            isNull(sessionResources.deletedAt),
            eq(sessionResources.workspaceId, auth.workspaceId),
            eq(sessionResources.sessionId, id),
          ),
        );
      await tx
        .update(sessionThreads)
        .set({ deletedAt: now })
        .where(
          and(
            isNull(sessionThreads.deletedAt),
            eq(sessionThreads.workspaceId, auth.workspaceId),
            eq(sessionThreads.sessionId, id),
          ),
        );
      await tx.insert(sessionLifecycleOutbox).values(lifecycleEvent);
      await tx
        .update(sessions)
        .set({ deletedAt: now })
        .where(
          and(
            isNull(sessions.deletedAt),
            eq(sessions.id, id),
            eq(sessions.workspaceId, auth.workspaceId),
          ),
        );

      return {
        secretRefs: managedCredentialRows.map((row) => row.secretRef),
        lifecycleEventId: lifecycleEvent.id,
      };
    });
    if (deleteResult === null) return reply.code(404).send({ error: 'not found' });
    await purgeGitCredentialSecretRefs(secretStore, deleteResult.secretRefs).catch(
      (error: unknown) => {
        req.log.warn(
          { err: error },
          'failed to purge managed git credentials after session delete',
        );
      },
    );
    await reconcileSessionLifecycleOutbox(db, store, {
      eventIds: [deleteResult.lifecycleEventId],
    })
      .then((result) => {
        if (result.failed > 0) {
          req.log.warn('session delete sentinel queued for retry');
        }
      })
      .catch((error: unknown) => {
        req.log.warn({ err: error }, 'failed to publish queued session delete sentinel');
      });
    return reply.send({ id, type: 'session_deleted' });
  });

  app.get('/v1/sessions/:id/files', async (req, reply) => {
    const auth = req.auth!;
    const id = toInternalId((req.params as { id: string }).id);
    const q = req.query as { limit?: string; after_id?: string; before_id?: string };
    const session = await loadSessionRow(db, auth.workspaceId, id);
    if (!session) return reply.code(404).send({ error: 'session not found' });

    const parsedLimit = parsePositiveIntQueryParam(q.limit, 'limit', {
      defaultValue: 20,
      max: 1000,
    });
    if (!parsedLimit.ok) return reply.code(400).send({ error: parsedLimit.error });
    if (q.after_id && q.before_id) {
      return reply.code(400).send({ error: 'after_id and before_id are mutually exclusive' });
    }

    const options: {
      limit: number;
      cursor?: string;
      beforeCursor?: string;
      scopeId: string;
      purpose: 'agent_output';
    } = {
      limit: parsedLimit.value!,
      scopeId: id,
      purpose: 'agent_output',
    };
    if (q.after_id) options.cursor = q.after_id;
    if (q.before_id) options.beforeCursor = q.before_id;

    const page = await fileStore.list(auth.workspaceId, options);
    return reply.send({
      data: page.items.map((record) => fileRecordToApi(record, isOrcaBetaRequest(req))),
      first_id: page.items[0]?.id ?? null,
      last_id: page.items[page.items.length - 1]?.id ?? null,
      has_more: page.nextCursor !== null,
    });
  });

  app.get('/v1/sessions/:id/files/:file_id', async (req, reply) => {
    const auth = req.auth!;
    const params = req.params as { id: string; file_id: string };
    const id = toInternalId(params.id);
    const session = await loadSessionRow(db, auth.workspaceId, id);
    if (!session) return reply.code(404).send({ error: 'session not found' });

    const record = await fileStore.get(auth.workspaceId, params.file_id);
    if (!isActiveSessionOutputFile(record, id)) {
      return reply.code(404).send({ error: 'file not found' });
    }
    return reply.send(fileRecordToApi(record, isOrcaBetaRequest(req)));
  });

  app.get('/v1/sessions/:id/files/:file_id/content', async (req, reply) => {
    const auth = req.auth!;
    const params = req.params as { id: string; file_id: string };
    const id = toInternalId(params.id);
    const session = await loadSessionRow(db, auth.workspaceId, id);
    if (!session) return reply.code(404).send({ error: 'session not found' });

    const record = await fileStore.get(auth.workspaceId, params.file_id);
    if (!isActiveSessionOutputFile(record, id)) {
      return reply.code(404).send({ error: 'file not found' });
    }
    if (!record.downloadable) {
      return reply.code(403).send({ error: 'file is not downloadable' });
    }

    const opened = await fileStore.open(auth.workspaceId, params.file_id);
    if (!opened) return reply.code(404).send({ error: 'file not found' });
    reply.header('content-type', 'application/octet-stream');
    reply.header('content-length', String(opened.sizeBytes));
    return reply.send(opened.stream);
  });

  app.delete('/v1/sessions/:id/files/:file_id', async (req, reply) => {
    const auth = req.auth!;
    const params = req.params as { id: string; file_id: string };
    const id = toInternalId(params.id);
    const session = await loadSessionRow(db, auth.workspaceId, id);
    if (!session) return reply.code(404).send({ error: 'session not found' });

    const record = await fileStore.get(auth.workspaceId, params.file_id);
    if (!isActiveSessionOutputFile(record, id)) {
      return reply.code(404).send({ error: 'file not found' });
    }
    try {
      await fileStore.delete(auth.workspaceId, params.file_id);
    } catch (err) {
      if ((err as Error).name === 'FileNotFoundError') {
        return reply.code(404).send({ error: 'file not found' });
      }
      throw err;
    }
    return reply.send({ id: params.file_id, type: 'file_deleted' });
  });

  app.post('/v1/sessions/:id/resources', async (req, reply) => {
    const auth = req.auth!;
    const id = toInternalId((req.params as { id: string }).id);
    const parsedBody = sessionResourceInputSchema.safeParse(req.body);
    if (!parsedBody.success) {
      return reply
        .code(400)
        .send(
          buildClaudeErrorResponse(
            req.id,
            'invalid_request_error',
            firstZodIssue(parsedBody.error, 'invalid session resource'),
          ),
        );
    }
    const body = parsedBody.data as SessionResourceInput;
    const orcaBeta = isOrcaBetaRequest(req);

    if (
      body.mount_path !== undefined &&
      body.mount_path !== null &&
      (typeof body.mount_path !== 'string' || body.mount_path.length === 0)
    ) {
      return reply.code(400).send({ error: 'mount_path must be a non-empty string or null' });
    }

    const session = await loadSessionRow(db, auth.workspaceId, id);
    if (!session) return reply.code(404).send({ error: 'session not found' });

    const msErr = validateMountStrategy(body);
    if (msErr) return reply.code(400).send({ error: msErr });

    const existingResources = await db
      .select()
      .from(sessionResources)
      .where(
        and(
          isNull(sessionResources.deletedAt),
          eq(sessionResources.workspaceId, auth.workspaceId),
          eq(sessionResources.sessionId, id),
          isNull(sessionResources.detachedAt),
        ),
      );

    // Resolved mount path. memory_store paths are always server-derived;
    // github_repository may compute a default; files round-trip the input.
    const newRowId = newId('sesrsc');
    let resolvedMountPath =
      body.type === 'file'
        ? (body.mount_path ?? `/mnt/session/uploads/${body.file_id ?? ''}`)
        : (body.mount_path ?? '');
    let resolvedRepoRef: Record<string, unknown> | null = null;
    let resolvedRepoUrl: string | undefined;
    let managedGitCredential: ManagedGitCredentialDraft | undefined;

    if (body.type === 'file') {
      if (!body.file_id) {
        return reply.code(400).send({ error: 'file resource missing file_id' });
      }
      // Cap: count existing active file resources.
      const fileCount = existingResources.filter((r) => r.type === 'file').length;
      if (fileCount >= MAX_FILE_RESOURCES) {
        return reply.code(400).send({ error: resourceCapError('file') });
      }
      const valid = await fileStore.get(auth.workspaceId, body.file_id);
      if (!valid) {
        return reply.code(400).send({ error: `file_id ${body.file_id} not found in workspace` });
      }
    } else if (body.type === 'memory_store') {
      if (!body.memory_store_id) {
        return reply.code(400).send({ error: 'memory_store resource missing memory_store_id' });
      }
      if (!memoryStore) {
        return reply
          .code(400)
          .send({ error: 'memory_store resources are not enabled on this server' });
      }
      // Cap: count existing active memory_store resources.
      const memoryCount = existingResources.filter((r) => r.type === 'memory_store').length;
      if (memoryCount >= MAX_MEMORY_STORE_RESOURCES) {
        return reply.code(400).send({ error: resourceCapError('memory_store') });
      }
      const fetched = await memoryStore.getStore(auth.workspaceId, body.memory_store_id);
      if (!fetched) {
        return reply
          .code(400)
          .send({ error: `memory_store ${body.memory_store_id} not found in workspace` });
      }
      resolvedMountPath = defaultMemoryMountPath(fetched.name, fetched.id);
      const mountPathErr = validateMemoryMountPath(resolvedMountPath);
      if (mountPathErr) return reply.code(400).send({ error: mountPathErr });
    } else if (body.type === 'github_repository') {
      // Cap: count existing active github_repository resources.
      const repoCount = existingResources.filter((r) => r.type === 'github_repository').length;
      if (repoCount >= MAX_GITHUB_REPOSITORY_RESOURCES) {
        return reply.code(400).send({ error: resourceCapError('github_repository') });
      }
      const resolved = await resolveGithubRepoResource(db, auth.workspaceId, newRowId, body);
      if ('error' in resolved) {
        return reply.code(400).send({ error: resolved.error });
      }
      resolvedMountPath = resolved.mountPath;
      resolvedRepoRef = resolved.repoRef;
      resolvedRepoUrl = resolved.repoRef.url;
      managedGitCredential = resolved.managedCredential;
      if (
        existingResources.some(
          (resource) =>
            resource.type === 'github_repository' &&
            githubRepoRefMatchesUrl(resource.repoRef, resolvedRepoUrl!),
        )
      ) {
        return reply.code(400).send({ error: DUPLICATE_GITHUB_REPOSITORY_ERROR });
      }
    }
    const reservedMountPathErr = validateResourceMountPath(resolvedMountPath);
    if (reservedMountPathErr) {
      return reply.code(400).send({ error: reservedMountPathErr });
    }
    if (
      body.type === 'memory_store' &&
      existingResources.some((resource) => resource.mountPath === resolvedMountPath)
    ) {
      return reply.code(400).send({ error: DUPLICATE_RESOURCE_MOUNT_PATH_ERROR });
    }

    const stagedGitCredentials = await stageGitCredentialDraftsForRequest(
      db,
      req,
      reply,
      secretStore,
      managedGitCredential ? [managedGitCredential] : [],
    );
    if (stagedGitCredentials === null) return;

    const now = new Date();
    let attachResult: 'attached' | 'not_found' | 'cap' | 'duplicate_repo' | 'duplicate_mount_path' =
      'not_found';
    try {
      attachResult = await db.transaction(async (tx) => {
        const sessionRows = await tx
          .select({ id: sessions.id })
          .from(sessions)
          .where(
            and(
              isNull(sessions.deletedAt),
              eq(sessions.id, id),
              eq(sessions.workspaceId, auth.workspaceId),
              isNull(sessions.archivedAt),
              ne(sessions.status, 'terminated'),
            ),
          )
          .for('update')
          .limit(1);
        if (!sessionRows[0]) return 'not_found' as const;

        const activeTypeRows = await tx
          .select({
            id: sessionResources.id,
            repoRef: sessionResources.repoRef,
            mountPath: sessionResources.mountPath,
          })
          .from(sessionResources)
          .where(
            and(
              isNull(sessionResources.deletedAt),
              eq(sessionResources.workspaceId, auth.workspaceId),
              eq(sessionResources.sessionId, id),
              eq(sessionResources.type, body.type),
              isNull(sessionResources.detachedAt),
            ),
          );
        if (activeTypeRows.length >= resourceLimit(body.type)) return 'cap' as const;
        if (
          body.type === 'github_repository' &&
          resolvedRepoUrl &&
          activeTypeRows.some((resource) =>
            githubRepoRefMatchesUrl(resource.repoRef, resolvedRepoUrl),
          )
        ) {
          return 'duplicate_repo' as const;
        }
        if (
          body.type === 'memory_store' &&
          activeTypeRows.some((resource) => resource.mountPath === resolvedMountPath)
        ) {
          return 'duplicate_mount_path' as const;
        }

        await tx.insert(sessionResources).values({
          id: newRowId,
          workspaceId: auth.workspaceId,
          sessionId: id,
          type: body.type,
          fileId: body.type === 'file' ? (body.file_id ?? null) : null,
          memoryStoreId: body.type === 'memory_store' ? (body.memory_store_id ?? null) : null,
          repoRef: resolvedRepoRef,
          mountPath: resolvedMountPath,
          access: body.access ?? defaultResourceAccess(body.type),
          mountStrategy: body.type === 'file' ? (body.mount_strategy ?? null) : null,
          instructions: body.instructions ?? null,
          attachedAt: now,
          updatedAt: now,
          detachedAt: null,
        });
        if (managedGitCredential) {
          await tx
            .insert(gitCredentials)
            .values(managedGitCredentialInsert(managedGitCredential, now));
          const consumed = await tx
            .delete(gitCredentialStagingIntents)
            .where(
              and(
                eq(gitCredentialStagingIntents.workspaceId, auth.workspaceId),
                eq(gitCredentialStagingIntents.credentialId, managedGitCredential.id),
                eq(gitCredentialStagingIntents.status, 'pending'),
              ),
            )
            .returning({ credentialId: gitCredentialStagingIntents.credentialId });
          if (consumed.length !== 1) {
            throw new Error('git credential staging intent expired or missing');
          }
        }
        await tx
          .update(sessions)
          .set({
            runtimeRevision: sql`${sessions.runtimeRevision} + 1`,
            updatedAt: now,
          })
          .where(
            and(
              isNull(sessions.deletedAt),
              eq(sessions.workspaceId, auth.workspaceId),
              eq(sessions.id, id),
            ),
          );
        return 'attached' as const;
      });
    } catch (error) {
      if (secretStore && stagedGitCredentials.length > 0) {
        await discardGitCredentialStagingIntents(db, secretStore, stagedGitCredentials).catch(
          () => {
            req.log.warn('failed to discard staged git credential after attach');
          },
        );
      }
      throw error;
    }
    if (attachResult !== 'attached') {
      if (secretStore && stagedGitCredentials.length > 0) {
        await discardGitCredentialStagingIntents(db, secretStore, stagedGitCredentials).catch(
          () => {
            req.log.warn('failed to discard rejected staged git credential after attach');
          },
        );
      }
      if (attachResult === 'cap') {
        return reply.code(400).send({ error: resourceCapError(body.type) });
      }
      if (attachResult === 'duplicate_repo') {
        return reply.code(400).send({ error: DUPLICATE_GITHUB_REPOSITORY_ERROR });
      }
      if (attachResult === 'duplicate_mount_path') {
        return reply.code(400).send({ error: DUPLICATE_RESOURCE_MOUNT_PATH_ERROR });
      }
      return reply.code(404).send({ error: 'session not found' });
    }
    const inserted = await db
      .select()
      .from(sessionResources)
      .where(
        and(
          isNull(sessionResources.deletedAt),
          eq(sessionResources.workspaceId, auth.workspaceId),
          eq(sessionResources.id, newRowId),
        ),
      )
      .limit(1);
    const r = inserted[0]!;
    return reply.code(200).send(resourceToApi(r, orcaBeta));
  });

  app.get('/v1/sessions/:id/resources', async (req, reply) => {
    const auth = req.auth!;
    const id = toInternalId((req.params as { id: string }).id);
    const q = req.query as { limit?: string; page?: string };
    const session = await loadSessionRow(db, auth.workspaceId, id);
    if (!session) return reply.code(404).send({ error: 'session not found' });

    const cursor = parseResourceCursor(q.page);
    if (q.page && !cursor) return reply.code(400).send({ error: 'invalid page' });
    const limit = parseThreadLimit(q.limit);
    const conditions = [
      eq(sessionResources.workspaceId, auth.workspaceId),
      eq(sessionResources.sessionId, id),
      isNull(sessionResources.detachedAt),
    ];
    if (cursor) {
      conditions.push(
        or(
          lt(sessionResources.attachedAt, cursor.attachedAt),
          and(
            eq(sessionResources.attachedAt, cursor.attachedAt),
            lt(sessionResources.id, cursor.id),
          ),
        )!,
      );
    }
    const rows = await db
      .select()
      .from(sessionResources)
      .where(and(isNull(sessionResources.deletedAt), ...conditions))
      .orderBy(desc(sessionResources.attachedAt), desc(sessionResources.id))
      .limit(limit + 1);
    const page = rows.slice(0, limit);
    reply.send({
      data: page.map((resource) => resourceToApi(resource, isOrcaBetaRequest(req))),
      next_page:
        rows.length > limit && page.length > 0
          ? encodeResourceCursor(page[page.length - 1]!)
          : null,
    });
  });

  app.get('/v1/sessions/:id/resources/:resource_id', async (req, reply) => {
    const auth = req.auth!;
    const params = req.params as { id: string; resource_id: string };
    const id = toInternalId(params.id);
    const { resource_id } = params;
    const session = await loadSessionRow(db, auth.workspaceId, id);
    if (!session) return reply.code(404).send({ error: 'session not found' });

    const row = await loadActiveSessionResource(db, auth.workspaceId, id, resource_id);
    if (!row) return reply.code(404).send({ error: 'resource not found or already detached' });
    reply.send(resourceToApi(row, isOrcaBetaRequest(req)));
  });

  app.post('/v1/sessions/:id/resources/:resource_id', async (req, reply) => {
    const auth = req.auth!;
    const params = req.params as { id: string; resource_id: string };
    const id = toInternalId(params.id);
    const { resource_id } = params;
    const rawBody = req.body;
    if (!isPlainObject(rawBody)) {
      return reply.code(400).send({ error: 'request body must be an object' });
    }
    const body = rawBody as SessionResourceUpdateBody;
    const orcaBeta = isOrcaBetaRequest(req);
    const bodyErr = validateResourceUpdateBody(body);
    if (bodyErr) return reply.code(400).send({ error: bodyErr });
    if (body.mount_path !== undefined) {
      const reservedMountPathErr = validateResourceMountPath(body.mount_path);
      if (reservedMountPathErr) {
        return reply.code(400).send({ error: reservedMountPathErr });
      }
    }

    const session = await loadSessionRow(db, auth.workspaceId, id);
    if (!session) return reply.code(404).send({ error: 'session not found' });

    const existing = await loadActiveSessionResource(db, auth.workspaceId, id, resource_id);
    if (!existing) {
      return reply.code(404).send({ error: 'resource not found or already detached' });
    }
    if (existing.type === 'memory_store' && body.mount_path !== undefined) {
      return reply
        .code(400)
        .send(
          buildClaudeErrorResponse(
            req.id,
            'invalid_request_error',
            'memory_store mount_path is output-only and cannot be updated',
          ),
        );
    }

    const nextMountStrategy =
      body.mount_strategy !== undefined ? body.mount_strategy : existing.mountStrategy;
    if (nextMountStrategy !== null && nextMountStrategy !== undefined) {
      const msErr = validateMountStrategy({
        type: existing.type as SessionResourceInput['type'],
        mount_strategy: nextMountStrategy,
      });
      if (msErr) return reply.code(400).send({ error: msErr });
    }

    let expectedGitCredentialId: string | undefined;
    let nextRepoResolution: ResolvedGithubRepo | undefined;
    if (body.authorization_token !== undefined) {
      if (existing.type !== 'github_repository') {
        return reply
          .code(400)
          .send({ error: 'authorization_token is only valid on github_repository resources' });
      }
      const existingRepoRef = parseGithubRepoRef(existing.repoRef);
      if (!existingRepoRef) {
        return reply
          .code(409)
          .send({ error: 'github_repository resource has no credential binding' });
      }
      expectedGitCredentialId = existingRepoRef.git_credential_id;
      const resolved = await resolveGithubRepoResource(db, auth.workspaceId, resource_id, {
        type: 'github_repository',
        url: existingRepoRef.url,
        authorization_token: body.authorization_token,
        mount_path: existing.mountPath,
        ...(existingRepoRef.checkout ? { checkout: existingRepoRef.checkout } : {}),
      });
      if ('error' in resolved) return reply.code(400).send({ error: resolved.error });
      nextRepoResolution = resolved;
    }

    const stagedGitCredentials = await stageGitCredentialDraftsForRequest(
      db,
      req,
      reply,
      secretStore,
      nextRepoResolution?.managedCredential ? [nextRepoResolution.managedCredential] : [],
    );
    if (stagedGitCredentials === null) return;

    const updates: Partial<typeof sessionResources.$inferInsert> = {};
    if (body.mount_path !== undefined) updates.mountPath = body.mount_path;
    if (body.access !== undefined) updates.access = body.access;
    if (body.instructions !== undefined) updates.instructions = body.instructions;
    if (body.mount_strategy !== undefined) {
      updates.mountStrategy = existing.type === 'file' ? body.mount_strategy : null;
    }
    updates.updatedAt = new Date();

    let rotationResult:
      | { status: 'ok'; oldManagedSecretRef?: string }
      | { status: 'not_found' | 'inactive' | 'conflict' };
    try {
      rotationResult = await db.transaction(async (tx) => {
        const activeSessionRows = await tx
          .select({ id: sessions.id })
          .from(sessions)
          .where(
            and(
              isNull(sessions.deletedAt),
              eq(sessions.id, id),
              eq(sessions.workspaceId, auth.workspaceId),
              isNull(sessions.archivedAt),
              ne(sessions.status, 'terminated'),
            ),
          )
          .for('update')
          .limit(1);
        if (!activeSessionRows[0]) return { status: 'inactive' as const };

        const lockedRows = await tx
          .select()
          .from(sessionResources)
          .where(
            and(
              isNull(sessionResources.deletedAt),
              eq(sessionResources.workspaceId, auth.workspaceId),
              eq(sessionResources.id, resource_id),
              eq(sessionResources.sessionId, id),
              isNull(sessionResources.detachedAt),
            ),
          )
          .for('update')
          .limit(1);
        const locked = lockedRows[0];
        if (!locked) return { status: 'not_found' as const };

        let oldManagedSecretRef: string | undefined;
        if (nextRepoResolution && expectedGitCredentialId) {
          const currentRepoRef = parseGithubRepoRef(locked.repoRef);
          if (!currentRepoRef || currentRepoRef.git_credential_id !== expectedGitCredentialId) {
            return { status: 'conflict' as const };
          }

          const nextGitCredentialId = nextRepoResolution.repoRef.git_credential_id;
          const ownedRows = await tx
            .select()
            .from(gitCredentials)
            .where(
              and(
                isNull(gitCredentials.deletedAt),
                eq(gitCredentials.workspaceId, auth.workspaceId),
                eq(gitCredentials.sessionResourceId, resource_id),
              ),
            )
            .limit(1);
          const ownedCredential = ownedRows[0];
          if (ownedCredential && ownedCredential.id !== nextGitCredentialId) {
            oldManagedSecretRef = ownedCredential.secretRef;
            await tx
              .update(gitCredentials)
              .set({ deletedAt: new Date() })
              .where(
                and(
                  isNull(gitCredentials.deletedAt),
                  eq(gitCredentials.workspaceId, auth.workspaceId),
                  eq(gitCredentials.id, ownedCredential.id),
                ),
              );
          }
          if (nextRepoResolution.managedCredential) {
            await tx
              .insert(gitCredentials)
              .values(
                managedGitCredentialInsert(
                  nextRepoResolution.managedCredential,
                  updates.updatedAt as Date,
                ),
              );
            const consumed = await tx
              .delete(gitCredentialStagingIntents)
              .where(
                and(
                  eq(gitCredentialStagingIntents.workspaceId, auth.workspaceId),
                  eq(
                    gitCredentialStagingIntents.credentialId,
                    nextRepoResolution.managedCredential.id,
                  ),
                  eq(gitCredentialStagingIntents.status, 'pending'),
                ),
              )
              .returning({ credentialId: gitCredentialStagingIntents.credentialId });
            if (consumed.length !== 1) {
              throw new Error('git credential staging intent expired or missing');
            }
          }
          updates.repoRef = nextRepoResolution.repoRef;
        }

        await tx
          .update(sessionResources)
          .set(updates)
          .where(
            and(
              isNull(sessionResources.deletedAt),
              eq(sessionResources.workspaceId, auth.workspaceId),
              eq(sessionResources.id, resource_id),
              eq(sessionResources.sessionId, id),
              isNull(sessionResources.detachedAt),
            ),
          );
        await tx
          .update(sessions)
          .set({
            runtimeRevision: sql`${sessions.runtimeRevision} + 1`,
            updatedAt: updates.updatedAt as Date,
          })
          .where(
            and(
              isNull(sessions.deletedAt),
              eq(sessions.workspaceId, auth.workspaceId),
              eq(sessions.id, id),
            ),
          );
        return {
          status: 'ok' as const,
          ...(oldManagedSecretRef ? { oldManagedSecretRef } : {}),
        };
      });
    } catch (error) {
      if (secretStore && stagedGitCredentials.length > 0) {
        await discardGitCredentialStagingIntents(db, secretStore, stagedGitCredentials).catch(
          () => {
            req.log.warn('failed to discard staged git credential after update');
          },
        );
      }
      throw error;
    }

    if (rotationResult.status !== 'ok') {
      if (secretStore && stagedGitCredentials.length > 0) {
        await discardGitCredentialStagingIntents(db, secretStore, stagedGitCredentials).catch(
          () => {
            req.log.warn('failed to discard rejected git credential rotation secret');
          },
        );
      }
      if (rotationResult.status === 'not_found') {
        return reply.code(404).send({ error: 'resource not found or already detached' });
      }
      if (rotationResult.status === 'inactive') {
        return reply.code(409).send({ error: 'session is not active' });
      }
      return reply
        .code(409)
        .send({ error: 'github_repository credential was rotated concurrently' });
    }

    await purgeGitCredentialSecretRefs(secretStore, [rotationResult.oldManagedSecretRef]).catch(
      (error: unknown) => {
        req.log.warn({ err: error }, 'failed to purge replaced git credential secret');
      },
    );

    const updated = await loadActiveSessionResource(db, auth.workspaceId, id, resource_id);
    return reply.send(resourceToApi(updated!, orcaBeta));
  });

  app.delete('/v1/sessions/:id/resources/:resource_id', async (req, reply) => {
    const auth = req.auth!;
    const params = req.params as { id: string; resource_id: string };
    const id = toInternalId(params.id);
    const { resource_id: rsc_id } = params;
    const session = await loadSessionRow(db, auth.workspaceId, id);
    if (!session) return reply.code(404).send({ error: 'session not found' });
    const now = new Date();
    const detachedSecretRefs = await db.transaction(async (tx) => {
      const lockedRows = await tx
        .select({ id: sessionResources.id })
        .from(sessionResources)
        .where(
          and(
            isNull(sessionResources.deletedAt),
            eq(sessionResources.workspaceId, auth.workspaceId),
            eq(sessionResources.id, rsc_id),
            eq(sessionResources.sessionId, id),
            isNull(sessionResources.detachedAt),
          ),
        )
        .for('update')
        .limit(1);
      if (!lockedRows[0]) return null;

      const managedRows = await tx
        .select()
        .from(gitCredentials)
        .where(
          and(
            isNull(gitCredentials.deletedAt),
            eq(gitCredentials.workspaceId, auth.workspaceId),
            eq(gitCredentials.sessionResourceId, rsc_id),
          ),
        );
      if (managedRows.length > 0) {
        await tx
          .update(gitCredentials)
          .set({ deletedAt: new Date() })
          .where(
            and(
              isNull(gitCredentials.deletedAt),
              eq(gitCredentials.workspaceId, auth.workspaceId),
              inArray(
                gitCredentials.id,
                managedRows.map((row) => row.id),
              ),
            ),
          );
      }
      await tx
        .update(sessionResources)
        .set({ detachedAt: now, deletedAt: now, updatedAt: now })
        .where(
          and(
            isNull(sessionResources.deletedAt),
            eq(sessionResources.workspaceId, auth.workspaceId),
            eq(sessionResources.id, rsc_id),
            eq(sessionResources.sessionId, id),
            isNull(sessionResources.detachedAt),
          ),
        );
      await tx
        .update(sessions)
        .set({
          runtimeRevision: sql`${sessions.runtimeRevision} + 1`,
          updatedAt: now,
        })
        .where(
          and(
            isNull(sessions.deletedAt),
            eq(sessions.workspaceId, auth.workspaceId),
            eq(sessions.id, id),
          ),
        );
      return managedRows.map((row) => row.secretRef);
    });
    if (detachedSecretRefs === null) {
      return reply.code(404).send({ error: 'resource not found or already detached' });
    }
    await purgeGitCredentialSecretRefs(secretStore, detachedSecretRefs).catch((error: unknown) => {
      req.log.warn({ err: error }, 'failed to purge managed git credential after detach');
    });
    return reply.send({ id: rsc_id, type: 'session_resource_deleted' });
  });

  app.post('/v1/sessions/:id/events', async (req, reply) => {
    const auth = req.auth!;
    const id = toInternalId((req.params as { id: string }).id);
    const orcaBeta = isOrcaBetaRequest(req);
    let body = req.body as AppendEventsBody;
    const legacyEventInput = orcaBeta;
    if (!orcaBeta) {
      const rawBody = isPlainObject(req.body) ? req.body : {};
      const parsed = sessionAppendEventsBodySchema.safeParse({ events: rawBody.events });
      if (!parsed.success) {
        return reply
          .code(400)
          .send({ error: parsed.error.issues[0]?.message ?? 'invalid events body' });
      } else {
        body = {
          ...parsed.data,
          ...(typeof rawBody.request_id === 'string' ? { request_id: rawBody.request_id } : {}),
        } as AppendEventsBody;
      }
      const orderingError = validateClaudeEventBatch(body.events);
      if (orderingError) return reply.code(400).send({ error: orderingError });
    }

    const session = await loadSessionRow(db, auth.workspaceId, id);
    if (!session) return reply.code(404).send({ error: 'not found' });

    if (!orcaBeta && body.events.some((event) => event.type === 'user.tool_result')) {
      const capability = await resolveUserToolResultCapability(db, auth.workspaceId, session);
      if (capability.kind === 'not_self_hosted') {
        return reply
          .code(400)
          .send({ error: 'user.tool_result is only valid for self_hosted environments' });
      }
      if (capability.kind === 'invalid_harness_annotation') {
        // Distinct from `unsupported_harness`: the annotation could not be READ,
        // so "not supported by this session's execution harness" would name the
        // wrong cause. Report what actually failed to resolve.
        return reply.code(400).send({
          error: `user.tool_result is unavailable: the agent's harness annotation is invalid (${capability.error})`,
        });
      }
      if (capability.kind === 'unsupported_harness') {
        return reply
          .code(400)
          .send({ error: "user.tool_result is not supported by this session's execution harness" });
      }
    }

    const clientEvents = orcaBeta
      ? body.events
      : body.events.map((event) => prepareClaudeEventInput(event));
    const resolvedEvents = await resolveThreadTargetedEvents(
      db,
      auth.workspaceId,
      id,
      clientEvents,
    );
    if ('error' in resolvedEvents) {
      return reply.code(resolvedEvents.statusCode).send({ error: resolvedEvents.error });
    }
    const eventIdempotencyKeys = eventBatchIdempotencyKeys(body.request_id, resolvedEvents.length);
    const trustedUserId =
      auth.authMethod === 'oidc' ? oidcTranscriptUserId(auth.oidcIssuer, auth.userId) : undefined;

    try {
      resolvedEvents.forEach((input, i) =>
        httpEventToProto({
          workspaceId: auth.workspaceId,
          sessionId: id,
          producedBy: 'client',
          userId: trustedUserId,
          idempotencyKey: eventIdempotencyKeys[i]!,
          input,
          validationMode: legacyEventInput ? 'orca' : 'claude',
        }),
      );
    } catch (err) {
      const message = err instanceof Error ? err.message : 'invalid event';
      return reply.code(400).send({ error: message });
    }

    const reservation = await reserveSessionThreadCapacity(
      db,
      auth.workspaceId,
      session,
      resolvedEvents,
    );
    if ('error' in reservation) return reply.code(409).send({ error: reservation.error });

    const protoEvents = reservation.events.map((input, i) =>
      httpEventToProto({
        workspaceId: auth.workspaceId,
        sessionId: id,
        producedBy: 'client',
        userId: trustedUserId,
        idempotencyKey: eventIdempotencyKeys[i]!,
        input,
        validationMode: legacyEventInput ? 'orca' : 'claude',
      }),
    );
    if (!orcaBeta) correlateCompanionSystemMessage(protoEvents);

    try {
      await store.append(auth.workspaceId, id, protoEvents);
      eventsAppendTotal.inc({ workspace_id: auth.workspaceId, status: 'ok' }, protoEvents.length);
    } catch (err) {
      eventsAppendTotal.inc(
        { workspace_id: auth.workspaceId, status: 'error' },
        protoEvents.length,
      );
      req.log.error({ err }, 'transcript-store append failed');
      await releaseSessionThreadReservations(db, auth.workspaceId, id, reservation.reservedIds);
      return reply.code(502).send({ error: 'upstream-unavailable' });
    }

    try {
      // Indexes every event AND (session-events-index.ts's indexTranscriptEvents)
      // projects primary-thread `session.thread_*` events into the session_threads
      // read model, so the Threads API reflects them without a separate call.
      await indexTranscriptEvents(db, protoEvents, {
        guardrailSubject: authenticatedGuardrailSubject(auth),
      });
    } catch (err) {
      req.log.warn({ err }, 'transcript-store append succeeded but event index update failed');
    }

    const hasExecutableEvent = protoEvents.some((event) => event.kind.startsWith('user.'));
    if (hasExecutableEvent) {
      const now = new Date();
      const update: Partial<typeof sessions.$inferInsert> = {
        status: 'running',
        startedAt: session.startedAt ?? now,
        updatedAt: now,
      };
      if (session.status !== 'running') {
        update.lastActiveAt = now;
      }
      await db
        .update(sessions)
        .set(update)
        .where(
          and(
            isNull(sessions.deletedAt),
            eq(sessions.id, id),
            eq(sessions.workspaceId, auth.workspaceId),
          ),
        );
    }

    const output = protoEvents.map(protoToHttpEvent);
    return reply
      .code(200)
      .send(
        orcaBeta
          ? { events: output }
          : { data: output.map((event) => toPublicHttpEvent(event, false)) },
      );
  });

  app.get('/v1/sessions/:id/events', async (req, reply) => {
    const auth = req.auth!;
    const id = toInternalId((req.params as { id: string }).id);
    const q = req.query as {
      page?: string;
      limit?: string;
      subpath?: string;
      'created_at[gt]'?: string;
      'created_at[gte]'?: string;
      'created_at[lt]'?: string;
      'created_at[lte]'?: string;
      order?: string;
      types?: string | string[];
      'types[]'?: string | string[];
    };

    const session = await loadSessionRow(db, auth.workspaceId, id);
    if (!session) return reply.code(404).send({ error: 'not found' });

    const pageCursor = q.page ?? '';
    if (!isValidSessionEventsCursor(pageCursor)) {
      return reply.code(400).send({ error: 'invalid page' });
    }

    const limit = parseEventsLimit(q.limit);
    const order = q.order ?? 'asc';
    if (order !== 'asc' && order !== 'desc') {
      return reply.code(400).send({ error: 'order must be asc or desc' });
    }
    const createdGt = parseTimestampQuery(q['created_at[gt]'], 'created_at[gt]');
    const createdGte = parseTimestampQuery(q['created_at[gte]'], 'created_at[gte]');
    const createdLt = parseTimestampQuery(q['created_at[lt]'], 'created_at[lt]');
    const createdLte = parseTimestampQuery(q['created_at[lte]'], 'created_at[lte]');
    if ('error' in createdGt) return reply.code(400).send({ error: createdGt.error });
    if ('error' in createdGte) return reply.code(400).send({ error: createdGte.error });
    if ('error' in createdLt) return reply.code(400).send({ error: createdLt.error });
    if ('error' in createdLte) return reply.code(400).send({ error: createdLte.error });
    const orcaBeta = isOrcaBetaRequest(req);
    const requestedTypes = normalizeRepeatedQuery(q.types ?? q['types[]']);
    const types = orcaBeta
      ? requestedTypes
      : requestedTypes.length > 0
        ? requestedTypes.filter((type) => CLAUDE_SESSION_EVENT_TYPES.has(type))
        : [...CLAUDE_SESSION_EVENT_TYPES];
    const page = await listSessionEventsFromIndex(db, {
      workspaceId: auth.workspaceId,
      sessionId: id,
      fromCursor: pageCursor,
      limit,
      subpath: q.subpath ?? '',
      order,
      ...(createdGt.value ? { createdAtGt: createdGt.value.toISOString() } : {}),
      ...(createdGte.value ? { createdAtGte: createdGte.value.toISOString() } : {}),
      ...(createdLt.value ? { createdAtLt: createdLt.value.toISOString() } : {}),
      ...(createdLte.value ? { createdAtLte: createdLte.value.toISOString() } : {}),
      types: !orcaBeta && requestedTypes.length > 0 && types.length === 0 ? ['__none__'] : types,
    });
    eventsReadTotal.inc({ workspace_id: auth.workspaceId }, page.events.length);
    const data = orcaBeta
      ? page.events
      : page.events.map((event) => toPublicHttpEvent(event, false));
    reply.send(
      orcaBeta
        ? { data, has_more: page.nextCursor !== null, next_page: page.nextCursor }
        : { data, next_page: page.nextCursor },
    );
  });

  app.get('/v1/sessions/:id/events/stream', handleSessionStream);

  app.get('/v1/sessions/:id/threads', async (req, reply) => {
    const auth = req.auth!;
    const id = toInternalId((req.params as { id: string }).id);
    const q = req.query as { page?: string; limit?: string };
    const session = await loadSessionRow(db, auth.workspaceId, id);
    if (!session) return reply.code(404).send({ error: 'not found' });

    const cursor = parseThreadCursor(q.page);
    if (q.page && !cursor) return reply.code(400).send({ error: 'invalid page' });
    const limit = parseThreadLimit(q.limit);
    const conditions = [
      eq(sessionThreads.workspaceId, auth.workspaceId),
      eq(sessionThreads.sessionId, id),
    ];
    if (cursor) {
      conditions.push(
        or(
          lt(sessionThreads.createdAt, cursor.createdAt),
          and(eq(sessionThreads.createdAt, cursor.createdAt), lt(sessionThreads.id, cursor.id)),
        )!,
      );
    }
    const rows = await db
      .select()
      .from(sessionThreads)
      .where(and(isNull(sessionThreads.deletedAt), ...conditions))
      .orderBy(desc(sessionThreads.createdAt), desc(sessionThreads.id))
      .limit(limit + 1);
    const page = rows.slice(0, limit);
    const orcaBeta = isOrcaBetaRequest(req);
    const context = batchReadsEnabled
      ? createSessionReadContext(db, auth.workspaceId, orcaBeta, requestReadSignal(req, reply))
      : undefined;
    const data = await Promise.all(
      page.map((row) => threadToApi(db, row, session, orcaBeta, context)),
    );
    const response = {
      data,
      has_more: rows.length > limit,
      next_page:
        rows.length > limit && page.length > 0 ? encodeThreadCursor(page[page.length - 1]!) : null,
    };
    reply.send(orcaBeta ? response : { data: response.data, next_page: response.next_page });
  });

  app.get('/v1/sessions/:id/threads/:thread_id', async (req, reply) => {
    const auth = req.auth!;
    const params = req.params as { id: string; thread_id: string };
    const id = toInternalId(params.id);
    const thread_id = toInternalId(params.thread_id);
    const session = await loadSessionRow(db, auth.workspaceId, id);
    if (!session) return reply.code(404).send({ error: 'not found' });
    const thread = await loadSessionThreadRow(db, auth.workspaceId, id, thread_id);
    if (!thread) return reply.code(404).send({ error: 'thread not found' });
    const orcaBeta = isOrcaBetaRequest(req);
    reply.send(
      await threadToApi(
        db,
        thread,
        session,
        orcaBeta,
        batchReadsEnabled
          ? createSessionReadContext(db, auth.workspaceId, orcaBeta, requestReadSignal(req, reply))
          : undefined,
      ),
    );
  });

  app.get('/v1/sessions/:id/threads/:thread_id/events', async (req, reply) => {
    const auth = req.auth!;
    const params = req.params as { id: string; thread_id: string };
    const id = toInternalId(params.id);
    const thread_id = toInternalId(params.thread_id);
    const q = req.query as { page?: string; limit?: string };

    const session = await loadSessionRow(db, auth.workspaceId, id);
    if (!session) return reply.code(404).send({ error: 'not found' });
    const thread = await loadSessionThreadRow(db, auth.workspaceId, id, thread_id);
    if (!thread) return reply.code(404).send({ error: 'thread not found' });

    const pageCursor = q.page ?? '';
    if (!isValidSessionEventsCursor(pageCursor)) {
      return reply.code(400).send({ error: 'invalid page' });
    }

    const limit = parseEventsLimit(q.limit);
    const orcaBeta = isOrcaBetaRequest(req);
    const page = await listSessionEventsFromIndex(db, {
      workspaceId: auth.workspaceId,
      sessionId: id,
      fromCursor: pageCursor,
      limit,
      subpath: thread.subpath,
      ...(orcaBeta ? {} : { types: [...CLAUDE_SESSION_EVENT_TYPES] }),
    });
    eventsReadTotal.inc({ workspace_id: auth.workspaceId }, page.events.length);
    const data = orcaBeta
      ? page.events
      : page.events.map((event) => toPublicHttpEvent(event, false));
    reply.send(
      orcaBeta
        ? { data, has_more: page.nextCursor !== null, next_page: page.nextCursor }
        : { data, next_page: page.nextCursor },
    );
  });

  app.get('/v1/sessions/:id/threads/:thread_id/stream', handleThreadStream);

  app.post('/v1/sessions/:id/threads/:thread_id/interrupt', async (req, reply) => {
    const auth = req.auth!;
    const params = req.params as { id: string; thread_id: string };
    const id = toInternalId(params.id);
    const thread_id = toInternalId(params.thread_id);
    const session = await loadSessionRow(db, auth.workspaceId, id);
    if (!session) return reply.code(404).send({ error: 'not found' });
    const existing = await loadSessionThreadRow(db, auth.workspaceId, id, thread_id);
    if (!existing) return reply.code(404).send({ error: 'thread not found' });

    const now = new Date();
    const terminatedEvent = threadInterruptEvent(auth.workspaceId, id, existing, now);
    try {
      await store.append(auth.workspaceId, id, [terminatedEvent]);
      await indexTranscriptEvents(db, [terminatedEvent]);
    } catch (err) {
      req.log.error({ err }, 'failed to append session thread interrupt event');
      return reply.code(502).send({ error: 'upstream-unavailable' });
    }
    // Unlike archive, interrupt does NOT set `archived_at`: the thread stays
    // visible (terminated, not hidden). `indexTranscriptEvents` above already
    // flipped `sessionThreads.status` to 'terminated' via the same projection
    // (`upsertSessionThreadsFromEvents` in `events/session-events-index.ts`)
    // the archive route relies on — no second write path here.
    const updated = await loadSessionThreadRow(db, auth.workspaceId, id, thread_id);
    return reply.send(await threadToApi(db, updated!, session, isOrcaBetaRequest(req)));
  });

  app.post('/v1/sessions/:id/threads/:thread_id/archive', async (req, reply) => {
    const auth = req.auth!;
    const params = req.params as { id: string; thread_id: string };
    const id = toInternalId(params.id);
    const thread_id = toInternalId(params.thread_id);
    const session = await loadSessionRow(db, auth.workspaceId, id);
    if (!session) return reply.code(404).send({ error: 'not found' });
    const existing = await loadSessionThreadRow(db, auth.workspaceId, id, thread_id);
    if (!existing) return reply.code(404).send({ error: 'thread not found' });

    const now = new Date();
    const terminatedEvent = threadArchiveEvent(auth.workspaceId, id, existing, now);
    try {
      await store.append(auth.workspaceId, id, [terminatedEvent]);
      await indexTranscriptEvents(db, [terminatedEvent]);
    } catch (err) {
      req.log.error({ err }, 'failed to append session thread archive event');
      return reply.code(502).send({ error: 'upstream-unavailable' });
    }
    await db
      .update(sessionThreads)
      .set({ status: 'archived', archivedAt: now, updatedAt: now })
      .where(
        and(
          isNull(sessionThreads.deletedAt),
          eq(sessionThreads.id, thread_id),
          eq(sessionThreads.workspaceId, auth.workspaceId),
          eq(sessionThreads.sessionId, id),
        ),
      );
    const updated = await loadSessionThreadRow(db, auth.workspaceId, id, thread_id);
    return reply.send(await threadToApi(db, updated!, session, isOrcaBetaRequest(req)));
  });
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function parseGithubRepoRef(value: unknown): {
  git_credential_id: string;
  url: string;
  checkout?: Record<string, unknown>;
} | null {
  if (!isPlainObject(value)) return null;
  if (typeof value.git_credential_id !== 'string' || typeof value.url !== 'string') return null;
  return {
    git_credential_id: value.git_credential_id,
    url: value.url,
    ...(isPlainObject(value.checkout) ? { checkout: value.checkout } : {}),
  };
}

function githubRepoRefMatchesUrl(value: unknown, repoUrl: string): boolean {
  const ref = parseGithubRepoRef(value);
  return ref !== null && repositoryUrlsEqual(ref.url, repoUrl);
}

function managedGitCredentialInsert(
  draft: ManagedGitCredentialDraft,
  now: Date,
): typeof gitCredentials.$inferInsert {
  return {
    id: draft.id,
    workspaceId: draft.workspaceId,
    provider: 'github',
    repoUrl: draft.repoUrl,
    secretRef: draft.secretRef,
    sessionResourceId: draft.sessionResourceId,
    metadata: { source: 'session_resource' },
    archivedAt: null,
    createdAt: now,
    updatedAt: now,
  };
}

async function stageGitCredentialDraftsForRequest(
  db: DbClient,
  req: FastifyRequest,
  reply: FastifyReply,
  secretStore: SecretStore | undefined,
  drafts: ManagedGitCredentialDraft[],
): Promise<ManagedGitCredentialDraft[] | null> {
  if (drafts.length === 0) return [];
  if (!secretStore) {
    reply.code(503).send({ error: 'secret store unavailable' });
    return null;
  }
  await persistGitCredentialStagingIntents(db, drafts);
  try {
    await stageManagedGitCredentials(secretStore, drafts);
    return drafts;
  } catch {
    await discardGitCredentialStagingIntents(db, secretStore, drafts).catch(() => {
      req.log.warn('failed to discard git credential staging intent after secret store failure');
    });
    // Deliberately omit the caught error: SecretStore implementations are
    // third-party boundaries and must not be able to echo token bytes into
    // request logs through an exception message.
    req.log.warn('git credential secret store write failed');
    reply.code(503).send({ error: 'secret store unavailable' });
    return null;
  }
}

function validateResourceUpdateBody(body: SessionResourceUpdateBody): string | null {
  if (
    body.authorization_token === undefined &&
    body.mount_path === undefined &&
    body.access === undefined &&
    body.instructions === undefined &&
    body.mount_strategy === undefined
  ) {
    return 'at least one resource field must be provided';
  }
  if (
    body.authorization_token !== undefined &&
    (typeof body.authorization_token !== 'string' || body.authorization_token.trim().length === 0)
  ) {
    return 'authorization_token must be a non-empty string';
  }
  if (body.mount_path !== undefined && (typeof body.mount_path !== 'string' || !body.mount_path)) {
    return 'mount_path must be a non-empty string';
  }
  if (body.access !== undefined && body.access !== 'read_only' && body.access !== 'read_write') {
    return 'access must be read_only or read_write';
  }
  if (
    body.instructions !== undefined &&
    body.instructions !== null &&
    typeof body.instructions !== 'string'
  ) {
    return 'instructions must be a string or null';
  }
  if (
    body.mount_strategy !== undefined &&
    body.mount_strategy !== null &&
    typeof body.mount_strategy !== 'string'
  ) {
    return 'mount_strategy must be a string or null';
  }
  return null;
}

function resourceToApi(r: typeof sessionResources.$inferSelect, orcaBeta: boolean) {
  if (r.type === 'github_repository') {
    const repoRef = parseGithubRepoRef(r.repoRef);
    return {
      id: r.id,
      type: 'github_repository' as const,
      url: repoRef?.url ?? '',
      mount_path: r.mountPath,
      ...(repoRef?.checkout ? { checkout: repoRef.checkout } : {}),
      created_at: r.attachedAt.toISOString(),
      updated_at: r.updatedAt.toISOString(),
    };
  }
  if (!orcaBeta && r.type === 'file') {
    return {
      id: r.id,
      type: 'file' as const,
      file_id: r.fileId!,
      mount_path: r.mountPath,
      created_at: r.attachedAt.toISOString(),
      updated_at: r.updatedAt.toISOString(),
    };
  }
  if (!orcaBeta && r.type === 'memory_store') {
    return {
      type: 'memory_store' as const,
      memory_store_id: r.memoryStoreId!,
      access: r.access,
      instructions: r.instructions ?? null,
      mount_path: r.mountPath || null,
    };
  }
  return {
    id: r.id,
    type: r.type,
    file_id: r.fileId ?? null,
    memory_store_id: r.memoryStoreId ?? null,
    repo_ref: (r.repoRef as Record<string, unknown> | null) ?? null,
    mount_path: r.mountPath,
    access: r.access,
    // null when the client didn't pick one — harness resolves at session-spawn
    // based on runtime capabilities. Persisted column doubles as the override
    // store so a later attach API can introspect what the client chose.
    mount_strategy: r.mountStrategy ?? null,
    instructions: r.instructions ?? null,
    // The Orca-beta projection keeps its operational extension fields, but it
    // still has to satisfy the public SessionResource contract.
    created_at: r.attachedAt.toISOString(),
    updated_at: r.updatedAt.toISOString(),
    attached_at: r.attachedAt.toISOString(),
    detached_at: r.detachedAt?.toISOString() ?? null,
  };
}

function resourceToInternalApi(r: typeof sessionResources.$inferSelect) {
  return {
    id: r.id,
    type: r.type,
    file_id: r.fileId ?? null,
    memory_store_id: r.memoryStoreId ?? null,
    repo_ref: (r.repoRef as Record<string, unknown> | null) ?? null,
    mount_path: r.mountPath,
    access: r.access,
    mount_strategy: r.mountStrategy ?? null,
    instructions: r.instructions ?? null,
    attached_at: r.attachedAt.toISOString(),
    updated_at: r.updatedAt.toISOString(),
    detached_at: r.detachedAt?.toISOString() ?? null,
  };
}

async function threadToApi(
  db: DbClient,
  row: typeof sessionThreads.$inferSelect,
  session?: typeof sessions.$inferSelect,
  orcaBeta = false,
  context?: SessionReadContext,
) {
  const primarySession =
    row.subpath === ''
      ? (session ?? (await loadSessionRow(db, row.workspaceId, row.sessionId)))
      : null;
  const useSessionLifecycle =
    primarySession !== null && row.archivedAt === null && row.status !== 'archived';
  const status = useSessionLifecycle
    ? primarySession.archivedAt !== null
      ? 'archived'
      : primarySession.status
    : row.status;
  const archivedAt = useSessionLifecycle ? primarySession.archivedAt : row.archivedAt;
  const updatedAt = useSessionLifecycle ? primarySession.updatedAt : row.updatedAt;
  return {
    id: row.id,
    type: 'session_thread',
    session_id: row.sessionId,
    parent_thread_id:
      row.parentThreadId ??
      (row.subpath === '' ? null : stableSessionThreadId(row.workspaceId, row.sessionId, '')),
    agent: await loadSessionThreadAgentSnapshot(db, row, primarySession, orcaBeta, context),
    status: threadStatusToApi(status),
    stats: useSessionLifecycle
      ? primaryThreadStats(primarySession, context?.now)
      : threadStats(row, context?.now),
    usage: useSessionLifecycle ? sessionUsage(primarySession) : emptyThreadUsage(),
    archived_at: archivedAt?.toISOString() ?? null,
    created_at: row.createdAt.toISOString(),
    updated_at: updatedAt.toISOString(),
  };
}

async function loadSessionThreadAgentSnapshot(
  db: DbClient,
  row: typeof sessionThreads.$inferSelect,
  session: typeof sessions.$inferSelect | null,
  orcaBeta: boolean,
  context?: SessionReadContext,
) {
  const resolved = await loadResolvedAgentSnapshot(
    db,
    row.workspaceId,
    row.agentId,
    row.agentVersion,
    row.subpath === '' ? session : null,
    orcaBeta,
    false,
    row.sessionId,
    context,
  );
  if (resolved) return resolved;
  return {
    id: row.agentId,
    type: 'agent' as const,
    name: row.agentName,
    description: null,
    version: row.agentVersion,
    model: orcaBeta ? { provider: 'anthropic', id: '' } : { id: '', speed: 'standard' },
    system: null,
    tools: [],
    mcp_servers: [],
    skills: [],
  };
}

async function loadSessionThreadAgentName(
  db: Pick<DbClient, 'select'>,
  workspaceId: string,
  agentId: string,
  agentVersion: number,
): Promise<string> {
  const versionRows = await db
    .select({ snapshot: agentVersions.snapshot })
    .from(agentVersions)
    .where(
      and(
        eq(agentVersions.workspaceId, workspaceId),
        eq(agentVersions.agentId, agentId),
        eq(agentVersions.version, agentVersion),
      ),
    )
    .limit(1);
  const snapshot = isPlainObject(versionRows[0]?.snapshot)
    ? (versionRows[0]!.snapshot as Record<string, unknown>)
    : null;
  if (typeof snapshot?.name === 'string' && snapshot.name.length > 0) return snapshot.name;

  const agentRows = await db
    .select({ name: agents.name })
    .from(agents)
    .where(
      and(isNull(agents.deletedAt), eq(agents.id, agentId), eq(agents.workspaceId, workspaceId)),
    )
    .limit(1);
  return agentRows[0]?.name ?? agentId;
}

function threadStatusToApi(status: string): 'running' | 'idle' | 'rescheduling' | 'terminated' {
  if (status === 'running') return 'running';
  if (status === 'rescheduling') return 'rescheduling';
  if (status === 'terminated' || status === 'archived') return 'terminated';
  return 'idle';
}

function threadStats(row: typeof sessionThreads.$inferSelect, now = new Date()) {
  const end =
    row.archivedAt ??
    (row.status === 'terminated' || row.status === 'archived' ? row.updatedAt : now);
  const durationSeconds = Math.max(0, Math.floor((end.getTime() - row.createdAt.getTime()) / 1000));
  return {
    active_seconds: row.status === 'running' ? durationSeconds : 0,
    duration_seconds: durationSeconds,
    startup_seconds: 0,
  };
}

function primaryThreadStats(row: typeof sessions.$inferSelect, now = new Date()) {
  return {
    ...sessionStats(row, row.archivedAt ?? now),
    startup_seconds: 0,
  };
}

function emptyThreadUsage() {
  return {
    cache_creation: {
      ephemeral_1h_input_tokens: 0,
      ephemeral_5m_input_tokens: 0,
    },
    cache_read_input_tokens: 0,
    input_tokens: 0,
    output_tokens: 0,
  };
}

/**
 * Build a `session.thread_status_terminated` event recording a terminal
 * transition for `row`, tagged with `stopReason`. Both callers below append
 * this event through the SAME transcript-store + `indexTranscriptEvents` path
 * (the single writer for `sessionThreads` projections; see
 * `upsertSessionThreadsFromEvents` in `events/session-events-index.ts`) — they
 * only differ in `stopReason`, and in whether they additionally hide the
 * thread by setting `archived_at` (archive does; interrupt does not).
 */
function threadTerminatedEvent(
  workspaceId: string,
  sessionId: string,
  row: typeof sessionThreads.$inferSelect,
  now: Date,
  stopReason: 'archived' | 'interrupted',
): Event {
  const payload = {
    type: SessionThreadEventKind.statusTerminated,
    session_thread_id: row.id,
    agent_name: row.agentName,
    status: 'terminated',
    stop_reason: stopReason,
  };
  return {
    id: newId('evt'),
    workspaceId,
    sessionId,
    subpath: row.subpath,
    seq: 0,
    producedAt: now.toISOString(),
    producedBy: 'registry',
    kind: SessionThreadEventKind.statusTerminated,
    payload: Buffer.from(JSON.stringify(payload), 'utf8'),
    idempotencyKey: `${row.id}:${stopReason}:${now.toISOString()}`,
  };
}

function threadArchiveEvent(
  workspaceId: string,
  sessionId: string,
  row: typeof sessionThreads.$inferSelect,
  now: Date,
): Event {
  return threadTerminatedEvent(workspaceId, sessionId, row, now, 'archived');
}

/**
 * The `interrupt` route's event: terminates a thread WITHOUT archiving it
 * (the caller does not set `archived_at`, so the thread stays visible — see
 * `POST /v1/sessions/:id/threads/:thread_id/interrupt` above). Records the
 * client's interrupt intent; a live runner is expected to notice this event
 * on the thread's own stream and stop driving the thread independently.
 */
function threadInterruptEvent(
  workspaceId: string,
  sessionId: string,
  row: typeof sessionThreads.$inferSelect,
  now: Date,
): Event {
  return threadTerminatedEvent(workspaceId, sessionId, row, now, 'interrupted');
}

async function resolveThreadTargetedEvents(
  db: DbClient,
  workspaceId: string,
  sessionId: string,
  inputs: Array<Record<string, unknown> & { type: string }>,
): Promise<
  Array<Record<string, unknown> & { type: string }> | { statusCode: 400 | 404; error: string }
> {
  const threadCache = new Map<string, typeof sessionThreads.$inferSelect | null>();
  const out: Array<Record<string, unknown> & { type: string }> = [];
  for (const input of inputs) {
    const threadId = input.session_thread_id;
    if (threadId === undefined) {
      out.push(input);
      continue;
    }
    if (typeof threadId !== 'string') {
      return { statusCode: 400, error: 'session_thread_id must be a string' };
    }
    let thread = threadCache.get(threadId);
    if (thread === undefined) {
      thread = await loadSessionThreadRow(db, workspaceId, sessionId, toInternalId(threadId));
      threadCache.set(threadId, thread);
    }
    if (!thread) {
      return { statusCode: 404, error: `session thread ${threadId} not found` };
    }
    if (typeof input.subpath === 'string' && input.subpath !== thread.subpath) {
      return { statusCode: 400, error: 'subpath does not match session_thread_id' };
    }
    out.push({ ...input, session_thread_id: thread.id, subpath: thread.subpath });
  }
  return out;
}

/**
 * A freshly reserved thread has recorded no usage yet. Mirrors the column
 * defaults so an in-memory stand-in for a just-inserted row matches what the
 * database holds; cost stays null and the unpriced marker stays false because
 * no usage has been reported.
 */
const ZERO_THREAD_USAGE = {
  usageInputTokens: 0,
  usageOutputTokens: 0,
  usageCacheReadInputTokens: 0,
  usageCacheCreationEphemeral1hInputTokens: 0,
  usageCacheCreationEphemeral5mInputTokens: 0,
  usageCostNanoUsd: null,
  usageHasUnpriced: false,
} as const;

async function reserveSessionThreadCapacity(
  db: DbClient,
  workspaceId: string,
  session: typeof sessions.$inferSelect,
  inputs: Array<Record<string, unknown> & { type: string }>,
): Promise<
  | { events: Array<Record<string, unknown> & { type: string }>; reservedIds: string[] }
  | { error: string }
> {
  const requestedSubpaths = new Set<string>();
  for (const input of inputs) {
    if (typeof input.subpath === 'string' && input.subpath.length > 0) {
      requestedSubpaths.add(input.subpath);
    }
  }
  if (requestedSubpaths.size === 0) return { events: inputs, reservedIds: [] };

  return await db.transaction(async (tx) => {
    const locked = await tx
      .select({ id: sessions.id })
      .from(sessions)
      .where(
        and(
          isNull(sessions.deletedAt),
          eq(sessions.workspaceId, workspaceId),
          eq(sessions.id, session.id),
        ),
      )
      .for('update')
      .limit(1);
    if (!locked[0]) return { error: 'not found' };

    const rows = await tx
      .select()
      .from(sessionThreads)
      .where(
        and(
          isNull(sessionThreads.deletedAt),
          eq(sessionThreads.workspaceId, workspaceId),
          eq(sessionThreads.sessionId, session.id),
        ),
      );
    const bySubpath = new Map(rows.map((row) => [row.subpath, row]));
    let primaryThreadId = bySubpath.get('')?.id;
    if (!primaryThreadId) {
      primaryThreadId = stableSessionThreadId(workspaceId, session.id, '');
      const primaryAgentName = await loadSessionThreadAgentName(
        tx,
        workspaceId,
        session.agentId,
        session.agentVersion,
      );
      const now = new Date();
      await tx
        .insert(sessionThreads)
        .values({
          id: primaryThreadId,
          workspaceId,
          sessionId: session.id,
          subpath: '',
          agentId: session.agentId,
          agentVersion: session.agentVersion,
          agentName: primaryAgentName,
          parentThreadId: null,
          status: session.status,
          createdAt: session.createdAt,
          updatedAt: now,
        })
        .onConflictDoNothing();
      bySubpath.set('', {
        id: primaryThreadId,
        workspaceId,
        sessionId: session.id,
        subpath: '',
        agentId: session.agentId,
        agentVersion: session.agentVersion,
        agentName: primaryAgentName,
        parentThreadId: null,
        status: session.status,
        stopReason: null,
        ...ZERO_THREAD_USAGE,
        deletedAt: null,
        archivedAt: session.archivedAt,
        createdAt: session.createdAt,
        updatedAt: now,
      });
      await tx
        .update(sessionThreads)
        .set({ parentThreadId: primaryThreadId, updatedAt: now })
        .where(
          and(
            isNull(sessionThreads.deletedAt),
            eq(sessionThreads.workspaceId, workspaceId),
            eq(sessionThreads.sessionId, session.id),
            ne(sessionThreads.subpath, ''),
            isNull(sessionThreads.parentThreadId),
          ),
        );
    }
    const activeCount = rows.filter(
      (row) =>
        row.subpath.length > 0 &&
        !row.archivedAt &&
        row.status !== 'terminated' &&
        row.status !== 'archived',
    ).length;
    const newSubpaths = [...requestedSubpaths].filter((subpath) => !bySubpath.has(subpath));
    if (activeCount + newSubpaths.length > MAX_CONCURRENT_SESSION_THREADS) {
      return {
        error: `maximum concurrent session threads exceeded (max ${MAX_CONCURRENT_SESSION_THREADS})`,
      };
    }

    const agentName =
      newSubpaths.length > 0
        ? await loadSessionThreadAgentName(tx, workspaceId, session.agentId, session.agentVersion)
        : session.agentId;

    const reservedIds: string[] = [];
    const now = new Date();
    for (const subpath of newSubpaths) {
      const id = stableSessionThreadId(workspaceId, session.id, subpath);
      await tx
        .insert(sessionThreads)
        .values({
          id,
          workspaceId,
          sessionId: session.id,
          subpath,
          agentId: session.agentId,
          agentVersion: session.agentVersion,
          agentName,
          parentThreadId: primaryThreadId,
          status: 'idle',
          createdAt: now,
          updatedAt: now,
        })
        .onConflictDoNothing();
      const reserved = {
        id,
        workspaceId,
        sessionId: session.id,
        subpath,
        agentId: session.agentId,
        agentVersion: session.agentVersion,
        agentName,
        parentThreadId: primaryThreadId,
        status: 'idle',
        stopReason: null,
        ...ZERO_THREAD_USAGE,
        deletedAt: null,
        archivedAt: null,
        createdAt: now,
        updatedAt: now,
      };
      bySubpath.set(subpath, reserved);
      reservedIds.push(id);
    }

    const events = inputs.map((input) => {
      if (typeof input.subpath !== 'string' || input.subpath.length === 0) return input;
      if (typeof input.session_thread_id === 'string') return input;
      const thread = bySubpath.get(input.subpath);
      return thread ? { ...input, session_thread_id: thread.id } : input;
    });

    return { events, reservedIds };
  });
}

async function releaseSessionThreadReservations(
  db: DbClient,
  workspaceId: string,
  sessionId: string,
  reservedIds: string[],
): Promise<void> {
  if (reservedIds.length === 0) return;
  await db
    .delete(sessionThreads)
    .where(
      and(
        isNull(sessionThreads.deletedAt),
        eq(sessionThreads.workspaceId, workspaceId),
        eq(sessionThreads.sessionId, sessionId),
        inArray(sessionThreads.id, reservedIds),
        eq(sessionThreads.status, 'idle'),
        isNull(sessionThreads.archivedAt),
      ),
    );
}

function validateClaudeEventBatch(
  events: Array<Record<string, unknown> & { type: string }>,
): string | null {
  const systemIndexes = events.flatMap((event, index) =>
    event.type === 'system.message' ? [index] : [],
  );
  if (systemIndexes.length > 1) return 'at most one system.message event may be sent per request';
  if (systemIndexes.length === 0) return null;
  const index = systemIndexes[0]!;
  if (index !== events.length - 1) return 'system.message must be the final event in the request';
  const precedingType = index > 0 ? events[index - 1]!.type : null;
  if (
    precedingType !== 'user.message' &&
    precedingType !== 'user.tool_result' &&
    precedingType !== 'user.custom_tool_result'
  ) {
    return 'system.message must immediately follow user.message, user.tool_result, or user.custom_tool_result';
  }
  return null;
}

function parseTimestampQuery(
  value: string | undefined,
  field: string,
): { value: Date | undefined } | { error: string } {
  if (value === undefined) return { value: undefined };
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return { error: `${field} must be an RFC 3339 timestamp` };
  return { value: parsed };
}

function parseBooleanQuery(
  value: string | boolean | undefined,
  fallback: boolean,
): { value: boolean } | { error: string } {
  if (value === undefined) return { value: fallback };
  if (value === true || value === 'true') return { value: true };
  if (value === false || value === 'false') return { value: false };
  return { error: 'include_archived must be true or false' };
}

function normalizeRepeatedQuery(value: string | string[] | undefined): string[] {
  const values = Array.isArray(value) ? value : value === undefined ? [] : [value];
  return values.flatMap((entry) => entry.split(',')).filter((entry) => entry.length > 0);
}

function parseSessionStatuses(
  value: string | string[] | undefined,
): { value: Array<'rescheduling' | 'running' | 'idle' | 'terminated'> } | { error: string } {
  const values = normalizeRepeatedQuery(value);
  const allowed = new Set(['rescheduling', 'running', 'idle', 'terminated']);
  if (values.some((entry) => !allowed.has(entry))) {
    return { error: 'statuses must contain only rescheduling, running, idle, or terminated' };
  }
  return {
    value: values as Array<'rescheduling' | 'running' | 'idle' | 'terminated'>,
  };
}

type EventDeltaType = typeof AgentEventKind.message | typeof AgentEventKind.thinking;

function parseEventDeltas(
  value: string | string[] | undefined,
): { value: ReadonlySet<EventDeltaType> } | { error: string } {
  const values = normalizeRepeatedQuery(value);
  if (
    values.some((entry) => entry !== AgentEventKind.message && entry !== AgentEventKind.thinking)
  ) {
    return { error: 'event_deltas must be agent.message or agent.thinking' };
  }
  return {
    value: new Set(values as EventDeltaType[]),
  };
}

interface SessionPageCursor {
  direction: 'next' | 'prev';
  order: 'asc' | 'desc';
  id: string;
}

function encodeSessionPageCursor(
  direction: SessionPageCursor['direction'],
  order: SessionPageCursor['order'],
  id: string,
): string {
  return Buffer.from(JSON.stringify({ version: 1, direction, order, id }), 'utf8').toString(
    'base64url',
  );
}

function parseSessionPageCursor(value: string | undefined): SessionPageCursor | null {
  if (!value) return null;
  try {
    const parsed = JSON.parse(Buffer.from(value, 'base64url').toString('utf8')) as {
      version?: unknown;
      direction?: unknown;
      order?: unknown;
      id?: unknown;
    };
    if (
      parsed.version !== 1 ||
      (parsed.direction !== 'next' && parsed.direction !== 'prev') ||
      (parsed.order !== 'asc' && parsed.order !== 'desc') ||
      typeof parsed.id !== 'string' ||
      !parsed.id.startsWith('ses_')
    ) {
      return null;
    }
    return {
      direction: parsed.direction,
      order: parsed.order,
      id: parsed.id,
    };
  } catch {
    return null;
  }
}

function jsonValuesEqual(left: unknown, right: unknown): boolean {
  return isDeepStrictEqual(left, right);
}

export function parseEventsLimit(raw: string | undefined): number {
  if (raw === undefined || raw === '') return DEFAULT_EVENTS_LIMIT;
  if (!/^\d+$/.test(raw)) return DEFAULT_EVENTS_LIMIT;
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed <= 0) return DEFAULT_EVENTS_LIMIT;
  return Math.min(parsed, MAX_EVENTS_LIMIT);
}

function parseThreadLimit(raw: string | undefined): number {
  if (raw === undefined || raw === '') return 100;
  if (!/^\d+$/.test(raw)) return 100;
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed <= 0) return 100;
  return Math.min(parsed, 100);
}

function encodeResourceCursor(row: typeof sessionResources.$inferSelect): string {
  return `${row.attachedAt.getTime()}_${row.id}`;
}

function parseResourceCursor(raw: string | undefined): { attachedAt: Date; id: string } | null {
  if (!raw) return null;
  const separator = raw.indexOf('_');
  if (separator <= 0) return null;
  const timestamp = Number(raw.slice(0, separator));
  const id = raw.slice(separator + 1);
  if (!Number.isSafeInteger(timestamp) || timestamp < 0 || !id.startsWith('sesrsc_')) return null;
  return { attachedAt: new Date(timestamp), id };
}

function encodeThreadCursor(row: typeof sessionThreads.$inferSelect): string {
  return `${row.createdAt.getTime()}_${row.id}`;
}

function parseThreadCursor(raw: string | undefined): { createdAt: Date; id: string } | null {
  if (!raw) return null;
  const separator = raw.indexOf('_');
  if (separator <= 0) return null;
  const timestamp = Number(raw.slice(0, separator));
  const id = raw.slice(separator + 1);
  if (!Number.isSafeInteger(timestamp) || timestamp < 0 || !id.startsWith('sth_')) return null;
  return { createdAt: new Date(timestamp), id };
}

export function toApi(
  row: typeof sessions.$inferSelect,
  resources: (typeof sessionResources.$inferSelect)[] = [],
  now?: Date,
) {
  // `type` is declared required on the Session response and is emitted by
  // `loadSession`'s Claude-dialect branch, but the `orca-beta` body left it out
  // — so a client generated from our own spec could not rely on the
  // discriminator that spec promises. Added here rather than in `sessionToApi`
  // so the internal workload body, which has its own contract, is untouched.
  // Destructured so `type` lands next to `id`, as it does everywhere else.
  const { id, ...rest } = sessionToApi(
    row,
    resources,
    (resource) => resourceToApi(resource, true),
    now,
  );
  return { id, type: 'session' as const, ...rest };
}

function toInternalApi(
  row: typeof sessions.$inferSelect,
  resources: (typeof sessionResources.$inferSelect)[] = [],
) {
  return {
    ...sessionToApi(row, resources, resourceToInternalApi),
    workspace_id: row.workspaceId,
    runtime_revision: row.runtimeRevision,
  };
}

function sessionToApi(
  row: typeof sessions.$inferSelect,
  resources: (typeof sessionResources.$inferSelect)[],
  serializeResource: (
    resource: typeof sessionResources.$inferSelect,
  ) => ReturnType<typeof resourceToApi> | ReturnType<typeof resourceToInternalApi>,
  now?: Date,
) {
  const stats = sessionStats(row, now);
  return {
    id: row.id,
    agent_id: row.agentId,
    agent_version: row.agentVersion,
    agent: { id: row.agentId, version: row.agentVersion },
    title: row.title ?? null,
    metadata: normalizeStoredMetadata(row.metadata),
    environment_id: row.environmentId ?? null,
    vault_ids: row.vaultIds,
    // Session-local overrides; null when unset (fall back to the agent).
    tools: (row.tools as unknown[] | null) ?? null,
    mcp_servers: stripSessionMcpServerPermissionPolicyFields(row.mcpServers as unknown[] | null),
    status: row.status,
    sandbox_handle_id: row.sandboxHandleId ?? null,
    // Claim-based distribution view (self_hosted environments). `null` for a
    // cloud / never-distributed session; populated once the registry dispatches
    // the session to a connected worker (runner_id + host_environment_id) and as
    // the distribution_state advances pending -> assigned / failed.
    runner_id: row.runnerId ?? null,
    host_environment_id: row.hostEnvironmentId ?? null,
    distribution_state: row.distributionState ?? null,
    stats,
    timing: {
      started_at: row.startedAt?.toISOString() ?? null,
      // End of the last active interval. For running sessions this is the turn
      // start timestamp; once harness marks the session idle it becomes the turn
      // end timestamp used to close `stats.active_seconds`.
      last_active_at: row.lastActiveAt?.toISOString() ?? null,
      active_seconds: stats.active_seconds,
      duration_seconds: stats.duration_seconds,
    },
    deployment_id: null,
    outcome_evaluations: [],
    usage: sessionUsage(row),
    outcome: null,
    archived_at: row.archivedAt?.toISOString() ?? null,
    resources: resources.map(serializeResource),
    created_at: row.createdAt.toISOString(),
    updated_at: row.updatedAt.toISOString(),
  };
}

async function loadResolvedAgentSnapshot(
  db: DbClient,
  workspaceId: string,
  agentId: string,
  version: number,
  session: typeof sessions.$inferSelect | null,
  orcaBeta: boolean,
  includeMultiagent: boolean,
  bindingSessionId: string | null = session?.id ?? null,
  context?: SessionReadContext,
): Promise<Record<string, unknown> | null> {
  const versionRows = context
    ? [await context.agentVersion({ agentId, version })]
    : await db
        .select({ snapshot: agentVersions.snapshot })
        .from(agentVersions)
        .where(
          and(
            eq(agentVersions.workspaceId, workspaceId),
            eq(agentVersions.agentId, agentId),
            eq(agentVersions.version, version),
          ),
        )
        .limit(1);
  const snapshot = isPlainObject(versionRows[0]?.snapshot)
    ? (versionRows[0]!.snapshot as Record<string, unknown>)
    : null;
  const agentRows = snapshot
    ? []
    : context
      ? [await context.agent(agentId)]
      : await db
          .select()
          .from(agents)
          .where(
            and(
              isNull(agents.deletedAt),
              eq(agents.workspaceId, workspaceId),
              eq(agents.id, agentId),
            ),
          )
          .limit(1);
  const agent = agentRows[0];
  if (!snapshot && !agent) return null;

  const baseMetadata = isPlainObject(snapshot?.metadata) ? snapshot.metadata : {};
  const rawModel =
    snapshot?.model ??
    (agent
      ? {
          provider: agent.modelProvider,
          id: agent.modelId,
          ...(agent.modelSpeed ? { speed: agent.modelSpeed } : {}),
          ...(agent.modelEffort ? { effort: agent.modelEffort } : {}),
        }
      : { provider: 'anthropic', id: '' });
  const parsedModel = normalizeModelForStorage(rawModel);
  const baseModel: StoredModel =
    'error' in parsedModel ? { provider: 'anthropic', id: '' } : parsedModel;
  const storedOverrides = isPlainObject(session?.agentOverrides)
    ? (session.agentOverrides as StoredSessionAgentOverrides)
    : {};
  const model = storedOverrides.model ?? baseModel;
  const baseTools = Array.isArray(snapshot?.tools)
    ? snapshot.tools
    : Array.isArray(agent?.tools)
      ? agent.tools
      : [];
  const baseMcpServers = Array.isArray(snapshot?.mcp_servers)
    ? snapshot.mcp_servers
    : Array.isArray(agent?.mcpServers)
      ? agent.mcpServers
      : [];
  const baseSkills = Array.isArray(snapshot?.skills)
    ? snapshot.skills
    : Array.isArray(agent?.skills)
      ? agent.skills
      : [];
  const requestedSkills = storedOverrides.skills ?? baseSkills;
  const hasSystemOverride = Object.prototype.hasOwnProperty.call(storedOverrides, 'system');
  const system = hasSystemOverride
    ? (storedOverrides.system ?? null)
    : typeof snapshot?.system === 'string' || snapshot?.system === null
      ? snapshot.system
      : (agent?.system ?? null);
  const resolvedTools =
    session?.tools !== null && session?.tools !== undefined ? session.tools : baseTools;
  const resolvedMcpServers =
    session?.mcpServers !== null && session?.mcpServers !== undefined
      ? (session.mcpServers as unknown[])
      : baseMcpServers;
  const resolvedSkills =
    bindingSessionId === null
      ? requestedSkills.map((skill) => skillToApi(skill))
      : await loadBoundSessionSkillRefs(
          db,
          workspaceId,
          bindingSessionId,
          agentId,
          version,
          requestedSkills.length,
          context,
        );

  let multiagent: Record<string, unknown> | null = null;
  const rawMultiagent = snapshot?.multiagent ?? agent?.multiagent;
  if (
    includeMultiagent &&
    isPlainObject(rawMultiagent) &&
    rawMultiagent.type === 'coordinator' &&
    Array.isArray(rawMultiagent.agents)
  ) {
    const roster = await Promise.all(
      rawMultiagent.agents.map(async (entry) => {
        if (
          !isPlainObject(entry) ||
          typeof entry.id !== 'string' ||
          typeof entry.version !== 'number'
        ) {
          return null;
        }
        return await loadResolvedAgentSnapshot(
          db,
          workspaceId,
          entry.id,
          entry.version,
          null,
          orcaBeta,
          false,
          bindingSessionId,
          context,
        );
      }),
    );
    multiagent = { type: 'coordinator', agents: roster.filter((entry) => entry !== null) };
  }

  return {
    id: agentId,
    type: 'agent',
    name: typeof snapshot?.name === 'string' ? snapshot.name : (agent?.name ?? agentId),
    description:
      typeof snapshot?.description === 'string' || snapshot?.description === null
        ? snapshot.description
        : typeof baseMetadata.description === 'string'
          ? baseMetadata.description
          : (agent?.description ?? null),
    version,
    model: modelToApi(
      model,
      orcaBeta,
      typeof snapshot?.harness_type === 'string' ? snapshot.harness_type : agent?.harnessType,
    ),
    system,
    tools: (resolvedTools as AgentTool[]).map((tool) => toolToApi(tool, orcaBeta)),
    mcp_servers: resolvedMcpServers.map((server) => mcpServerToApi(server, orcaBeta)),
    skills: resolvedSkills,
    ...(includeMultiagent ? { multiagent } : {}),
  };
}

async function loadBoundSessionSkillRefs(
  db: DbClient,
  workspaceId: string,
  sessionId: string,
  agentId: string,
  agentVersion: number,
  expectedCount: number,
  context?: SessionReadContext,
): Promise<Array<{ type: 'anthropic' | 'custom'; skill_id: string; version: string }>> {
  const rows = context
    ? await context.bindings({ sessionId, agentId, version: agentVersion })
    : await db
        .select({
          ordinal: sessionSkillBindings.ordinal,
          source: skills.type,
          skillId: skillVersions.skillId,
          versionIdentifier: skillVersions.versionIdentifier,
        })
        .from(sessionSkillBindings)
        .innerJoin(
          skillVersions,
          and(
            eq(sessionSkillBindings.workspaceId, skillVersions.workspaceId),
            eq(sessionSkillBindings.skillVersionId, skillVersions.id),
            eq(sessionSkillBindings.bundleSha256, skillVersions.packageSha256),
          ),
        )
        .innerJoin(
          skills,
          and(
            eq(skillVersions.workspaceId, skills.workspaceId),
            eq(skillVersions.skillId, skills.id),
          ),
        )
        .where(
          and(
            eq(sessionSkillBindings.workspaceId, workspaceId),
            eq(sessionSkillBindings.sessionId, sessionId),
            eq(sessionSkillBindings.agentId, agentId),
            eq(sessionSkillBindings.agentVersion, agentVersion),
          ),
        )
        .orderBy(asc(sessionSkillBindings.ordinal));
  if (rows.length !== expectedCount || rows.some((row, index) => row.ordinal !== index)) {
    throw new Error(`invalid session skill bindings for ${sessionId}/${agentId}@${agentVersion}`);
  }
  return rows.map((row) => {
    if (row.source !== 'anthropic' && row.source !== 'custom') {
      throw new Error(`invalid session skill binding source ${row.source}`);
    }
    return {
      type: row.source,
      skill_id: row.skillId,
      version: row.versionIdentifier,
    };
  });
}

async function loadCanonicalOutcomeEvaluations(
  db: DbClient,
  workspaceId: string,
  sessionId: string,
  context?: SessionReadContext,
): Promise<Array<Record<string, unknown>>> {
  const rows = context
    ? await context.outcomes(sessionId)
    : await db
        .select({
          eventId: sessionEventsIndex.eventId,
          kind: sessionEventsIndex.kind,
          payload: sessionEventsIndex.payload,
          producedAt: sessionEventsIndex.producedAt,
        })
        .from(sessionEventsIndex)
        .where(
          and(
            eq(sessionEventsIndex.workspaceId, workspaceId),
            eq(sessionEventsIndex.sessionId, sessionId),
            inArray(sessionEventsIndex.kind, [
              'user.define_outcome',
              SpanEventKind.outcomeEvaluationEnd,
            ]),
          ),
        )
        .orderBy(asc(sessionEventsIndex.seq), asc(sessionEventsIndex.projectionOrdinal));

  const evaluations = new Map<string, Record<string, unknown>>();
  for (const row of rows) {
    if (!isPlainObject(row.payload)) continue;
    const outcomeId =
      typeof row.payload.outcome_id === 'string'
        ? row.payload.outcome_id
        : row.kind === 'user.define_outcome'
          ? `outc_${row.eventId.replace(/^evt_/, '')}`
          : null;
    if (!outcomeId) continue;
    if (row.kind === 'user.define_outcome') {
      evaluations.set(outcomeId, {
        type: 'outcome_evaluation',
        outcome_id: outcomeId,
        description: typeof row.payload.description === 'string' ? row.payload.description : '',
        result: 'pending',
        explanation: null,
        iteration: 0,
        completed_at: null,
      });
      continue;
    }
    const current = evaluations.get(outcomeId);
    evaluations.set(outcomeId, {
      type: 'outcome_evaluation',
      outcome_id: outcomeId,
      description:
        typeof current?.description === 'string'
          ? current.description
          : typeof row.payload.description === 'string'
            ? row.payload.description
            : '',
      result: typeof row.payload.result === 'string' ? row.payload.result : 'failed',
      explanation: typeof row.payload.explanation === 'string' ? row.payload.explanation : null,
      iteration:
        typeof row.payload.iteration === 'number' && Number.isInteger(row.payload.iteration)
          ? row.payload.iteration
          : 0,
      completed_at: row.producedAt,
    });
  }
  return [...evaluations.values()];
}

function sessionUsage(row: typeof sessions.$inferSelect) {
  return {
    cache_creation: {
      ephemeral_1h_input_tokens: row.usageCacheCreationEphemeral1hInputTokens ?? 0,
      ephemeral_5m_input_tokens: row.usageCacheCreationEphemeral5mInputTokens ?? 0,
    },
    cache_read_input_tokens: row.usageCacheReadInputTokens ?? 0,
    input_tokens: row.usageInputTokens ?? 0,
    output_tokens: row.usageOutputTokens ?? 0,
  };
}

function sessionStats(row: typeof sessions.$inferSelect, now = new Date()) {
  const statsEnd = row.archivedAt ?? now;
  const activeBase = row.activeSeconds ?? 0;
  const activeCurrent =
    row.status === 'running' && row.lastActiveAt ? elapsedSeconds(row.lastActiveAt, statsEnd) : 0;
  const durationEnd = row.archivedAt ?? (row.status === 'terminated' ? row.updatedAt : now);
  return {
    active_seconds: activeBase + activeCurrent,
    duration_seconds: elapsedSeconds(row.createdAt, durationEnd),
  };
}

interface PinnedAgentConfigurationLoadOptions {
  requireVersionSnapshot?: boolean;
}

type PinnedAgentConfiguration = {
  metadata: Record<string, unknown>;
  model: StoredModel;
  tools: AgentTool[];
  mcpServers: unknown[];
  multiagent: CanonicalMultiagent | null;
};

async function loadPinnedAgentConfiguration(
  db: DbClient,
  workspaceId: string,
  agentId: string,
  version: number,
  fallbackAgent?: typeof agents.$inferSelect,
  options: PinnedAgentConfigurationLoadOptions = {},
): Promise<PinnedAgentConfiguration> {
  const identity =
    fallbackAgent ??
    (
      await db
        .select()
        .from(agents)
        .where(
          and(
            eq(agents.workspaceId, workspaceId),
            eq(agents.id, agentId),
            isNull(agents.deletedAt),
          ),
        )
        .limit(1)
    )[0];
  if (!identity) throw new SessionAgentSnapshotUnavailableError();
  const versionMetadata = (snapshot: Record<string, unknown>) => {
    try {
      return bindStoredHarness(
        normalizeStoredMetadata(snapshot.metadata),
        identity.harnessType,
        snapshot.harness_type,
      );
    } catch {
      throw new SessionAgentSnapshotUnavailableError();
    }
  };

  const versionRows = await db
    .select({ snapshot: agentVersions.snapshot })
    .from(agentVersions)
    .where(
      and(
        eq(agentVersions.workspaceId, workspaceId),
        eq(agentVersions.agentId, agentId),
        eq(agentVersions.version, version),
      ),
    )
    .limit(1);
  if (options.requireVersionSnapshot) {
    const configuration = strictAgentVersionConfiguration(
      versionRows[0]?.snapshot,
      agentId,
      version,
    );
    return {
      metadata: versionMetadata(versionRows[0]!.snapshot as Record<string, unknown>),
      model: configuration.model,
      tools: configuration.tools,
      mcpServers: configuration.mcpServers,
      multiagent: configuration.multiagent,
    };
  }
  const snapshot = isPlainObject(versionRows[0]?.snapshot)
    ? (versionRows[0]!.snapshot as Record<string, unknown>)
    : null;
  if (snapshot) {
    const model = normalizeModelForStorage(snapshot.model);
    if ('error' in model) throw new Error(`invalid agent version model ${agentId}@${version}`);
    return {
      metadata: versionMetadata(snapshot),
      model,
      tools: canonicalizeAgentTools(Array.isArray(snapshot.tools) ? snapshot.tools : []),
      mcpServers: Array.isArray(snapshot.mcp_servers) ? snapshot.mcp_servers : [],
      multiagent: isCanonicalMultiagent(snapshot.multiagent) ? snapshot.multiagent : null,
    };
  }

  const agent =
    fallbackAgent ??
    (
      await db
        .select()
        .from(agents)
        .where(
          and(
            isNull(agents.deletedAt),
            eq(agents.workspaceId, workspaceId),
            eq(agents.id, agentId),
          ),
        )
        .limit(1)
    )[0];
  const model = normalizeModelForStorage({
    provider: agent?.modelProvider ?? 'anthropic',
    id: agent?.modelId ?? '',
    ...(agent?.modelSpeed ? { speed: agent.modelSpeed } : {}),
    ...(agent?.modelEffort ? { effort: agent.modelEffort } : {}),
  });
  if ('error' in model) throw new Error(`invalid agent model ${agentId}@${version}`);
  return {
    metadata: bindStoredHarness(
      normalizeStoredMetadata(agent?.metadata),
      identity.harnessType,
      identity.harnessType,
    ),
    model,
    tools: canonicalizeAgentTools(Array.isArray(agent?.tools) ? agent.tools : []),
    mcpServers: Array.isArray(agent?.mcpServers) ? agent.mcpServers : [],
    multiagent: isCanonicalMultiagent(agent?.multiagent) ? agent.multiagent : null,
  };
}

export async function loadSession(
  db: DbClient,
  workspaceId: string,
  id: string,
  orcaBeta = false,
  context?: SessionReadContext,
) {
  const rows = await db
    .select()
    .from(sessions)
    .where(
      and(isNull(sessions.deletedAt), eq(sessions.id, id), eq(sessions.workspaceId, workspaceId)),
    )
    .limit(1);
  if (!rows[0]) return null;
  return loadSessionFromRow(db, workspaceId, rows[0], orcaBeta, context);
}

/** Bulk-refresh selected Sessions, preserving page order and dropping concurrent deletions. */
export async function loadSessionRows(
  db: DbClient,
  workspaceId: string,
  rows: readonly (typeof sessions.$inferSelect)[],
  orcaBeta = false,
  signal?: AbortSignal,
) {
  if (rows.some((row) => row.workspaceId !== workspaceId)) {
    throw new Error('session read workspace mismatch');
  }
  signal?.throwIfAborted();
  if (rows.length === 0) return [];
  // Match the legacy per-item re-read without N+1 SQL. A Session deleted after
  // page selection is excluded even though its historical bindings remain stored.
  // Callers retain the original page rows for their cursor boundaries.
  const currentRows = await db
    .select()
    .from(sessions)
    .where(
      and(
        isNull(sessions.deletedAt),
        eq(sessions.workspaceId, workspaceId),
        inArray(
          sessions.id,
          rows.map((row) => row.id),
        ),
      ),
    );
  signal?.throwIfAborted();
  const currentById = new Map(currentRows.map((row) => [row.id, row]));
  const survivingRows = rows.flatMap((row) => {
    const current = currentById.get(row.id);
    return current ? [current] : [];
  });
  const context = createSessionReadContext(db, workspaceId, orcaBeta, signal);
  return Promise.all(
    survivingRows.map((row) => loadSessionFromRow(db, workspaceId, row, orcaBeta, context)),
  );
}

async function loadSessionFromRow(
  db: DbClient,
  workspaceId: string,
  row: typeof sessions.$inferSelect,
  orcaBeta: boolean,
  context?: SessionReadContext,
) {
  if (context && context.workspaceId !== workspaceId)
    throw new Error('session read workspace mismatch');
  const id = row.id;
  const rscRows = context
    ? await context.resources(id)
    : await db
        .select()
        .from(sessionResources)
        .where(
          and(
            isNull(sessionResources.deletedAt),
            eq(sessionResources.workspaceId, workspaceId),
            eq(sessionResources.sessionId, id),
            isNull(sessionResources.detachedAt),
          ),
        );
  if (orcaBeta) {
    const [outcomeView, agent] = await Promise.all([
      context
        ? context
            .outcomes(id)
            .then((rows) => foldOutcomeEvaluations(rows.map((row) => row.payload)))
        : getSessionOutcome(db, workspaceId, id),
      loadResolvedAgentSnapshot(
        db,
        workspaceId,
        row.agentId,
        row.agentVersion,
        row,
        true,
        true,
        row.id,
        context,
      ),
    ]);
    return {
      ...toApi(row, rscRows, context?.now),
      agent,
      outcome: outcomeView.outcome,
      outcome_evaluations: outcomeView.outcome_evaluations,
    };
  }

  const stats = sessionStats(row, context?.now);
  const [agent, outcomeEvaluations] = await Promise.all([
    loadResolvedAgentSnapshot(
      db,
      workspaceId,
      row.agentId,
      row.agentVersion,
      row,
      false,
      true,
      row.id,
      context,
    ),
    loadCanonicalOutcomeEvaluations(db, workspaceId, id, context),
  ]);
  return {
    id: row.id,
    type: 'session',
    agent,
    title: row.title ?? null,
    metadata: normalizeStoredMetadata(row.metadata),
    environment_id: row.environmentId,
    vault_ids: row.vaultIds,
    status: row.status,
    stats,
    // Additive and intentionally retained alongside the Claude `stats` field.
    timing: {
      started_at: row.startedAt?.toISOString() ?? null,
      last_active_at: row.lastActiveAt?.toISOString() ?? null,
      active_seconds: stats.active_seconds,
      duration_seconds: stats.duration_seconds,
    },
    deployment_id: null,
    outcome_evaluations: outcomeEvaluations,
    usage: sessionUsage(row),
    archived_at: row.archivedAt?.toISOString() ?? null,
    resources: rscRows.map((resource) => resourceToApi(resource, false)),
    created_at: row.createdAt.toISOString(),
    updated_at: row.updatedAt.toISOString(),
  };
}

/** Internal representation for workspace-scoped Harness lifecycle calls. */
export async function loadInternalSession(db: DbClient, workspaceId: string, id: string) {
  const rows = await db
    .select()
    .from(sessions)
    .where(
      and(isNull(sessions.deletedAt), eq(sessions.workspaceId, workspaceId), eq(sessions.id, id)),
    )
    .limit(1);
  if (!rows[0]) return null;
  const rscRows = await db
    .select()
    .from(sessionResources)
    .where(
      and(
        isNull(sessionResources.deletedAt),
        eq(sessionResources.workspaceId, workspaceId),
        eq(sessionResources.sessionId, id),
        isNull(sessionResources.detachedAt),
      ),
    );
  return toInternalApi(rows[0], rscRows);
}

async function loadSessionRow(db: DbClient, workspaceId: string, id: string) {
  const rows = await db
    .select()
    .from(sessions)
    .where(
      and(isNull(sessions.deletedAt), eq(sessions.id, id), eq(sessions.workspaceId, workspaceId)),
    )
    .limit(1);
  return rows[0] ?? null;
}

/**
 * Whether `user.tool_result` can be accepted for a session.
 *
 * Four outcomes, deliberately keeping a RESOLUTION FAILURE apart from a
 * legitimately-unsupported harness: folding them together makes the 400 blame
 * the harness ("not supported by this session's execution harness") when the
 * real cause is an annotation the registry could not read, and discards the
 * resolver's own message.
 */
type UserToolResultCapability =
  | { kind: 'supported' }
  | { kind: 'not_self_hosted' }
  | { kind: 'unsupported_harness' }
  | { kind: 'invalid_harness_annotation'; error: string };

async function resolveUserToolResultCapability(
  db: DbClient,
  workspaceId: string,
  session: typeof sessions.$inferSelect,
): Promise<UserToolResultCapability> {
  if (!session.environmentId) return { kind: 'not_self_hosted' };
  const environmentRows = await db
    .select({ target: environments.target })
    .from(environments)
    .where(
      and(
        isNull(environments.deletedAt),
        eq(environments.workspaceId, workspaceId),
        eq(environments.id, session.environmentId),
      ),
    )
    .limit(1);
  if (environmentRows[0]?.target !== 'self_hosted') return { kind: 'not_self_hosted' };

  const versionRows = await db
    .select({ snapshot: agentVersions.snapshot })
    .from(agentVersions)
    .where(
      and(
        eq(agentVersions.workspaceId, workspaceId),
        eq(agentVersions.agentId, session.agentId),
        eq(agentVersions.version, session.agentVersion),
      ),
    )
    .limit(1);
  const snapshot = isPlainObject(versionRows[0]?.snapshot)
    ? (versionRows[0]!.snapshot as Record<string, unknown>)
    : null;
  const metadata = isPlainObject(snapshot?.metadata)
    ? (snapshot.metadata as Record<string, unknown>)
    : undefined;
  const harness = resolveHarnessAnnotation(metadata);
  if ('error' in harness) return { kind: 'invalid_harness_annotation', error: harness.error };
  return harness.harness === 'claude_agent_sdk' && harness.mode === 'separate'
    ? { kind: 'supported' }
    : { kind: 'unsupported_harness' };
}

async function loadSessionThreadRow(
  db: DbClient,
  workspaceId: string,
  sessionId: string,
  threadId: string,
) {
  const internalThreadId = toInternalId(threadId);
  const rows = await db
    .select()
    .from(sessionThreads)
    .where(
      and(
        isNull(sessionThreads.deletedAt),
        eq(sessionThreads.id, internalThreadId),
        eq(sessionThreads.workspaceId, workspaceId),
        eq(sessionThreads.sessionId, sessionId),
      ),
    )
    .limit(1);
  return rows[0] ?? null;
}

async function loadActiveSessionResource(
  db: DbClient,
  workspaceId: string,
  sessionId: string,
  resourceId: string,
) {
  const rows = await db
    .select()
    .from(sessionResources)
    .where(
      and(
        isNull(sessionResources.deletedAt),
        eq(sessionResources.workspaceId, workspaceId),
        eq(sessionResources.id, resourceId),
        eq(sessionResources.sessionId, sessionId),
        isNull(sessionResources.detachedAt),
      ),
    )
    .limit(1);
  return rows[0] ?? null;
}
