// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { piLlmRoute, validateHarnessDeployment, validateHarnessModel } from '@orca/harness-catalog';
import type { SessionJwtLlmPolicy } from '../config.js';
import { intersectLlmModels } from './llm-policy.js';
import { normalizeModelForStorage } from '../contracts/model-wire.js';
// The agent-snapshot RESOLVER — the impure orchestrator that turns a session id
// into a credential-free {@link AgentSnapshot} ready to deliver over the tunnel.
//
// It resolves the records (session, agent, skills, environment, vaults) through a
// {@link SnapshotRecordLoader} seam, mints the scoped session JWT IN-PROCESS via
// the registry's own {@link SessionJwtMinter} (no HTTP round-trip — the registry
// owns the minter), selects the credential-egress strategy by the environment's
// `egress_mode`, and hands the resolved pieces to the PURE {@link buildAgentSnapshot}.
//
// The loader seam keeps the orchestration unit-testable against in-memory fakes;
// a Drizzle-backed loader wires it to the `sessions` / `agents` / `skill_versions`
// / `environments` / `vaults` tables at the server boundary.
//
// Provider carriage. The runner's launch frame omits the provider by design, so
// the snapshot must carry it. The resolver derives it from the agent's harness
// annotation (`metadata.harness`/`metadata.mode`) via the shared harness catalog,
// mapping the harness to its provider id.

import {
  harnessToProvider,
  resolveHarnessAnnotation,
  type HarnessMode,
  type SkillSlice,
} from '@orca/harness-catalog';
import type { SessionJwtMinter } from '../auth/session-jwt.js';
import {
  buildAgentSnapshot,
  type AgentSnapshot,
  type AgentSnapshotMultiagent,
  type AgentSnapshotMultiagentMember,
  type AgentToolEntry,
  type SnapshotGuardrail,
} from './agent-snapshot.js';
import type { PreparedSkillDescriptor } from '../contracts/internal.contract.js';
import { readMultiagentSnapshot, type MultiagentSnapshot } from './multiagent.js';
import { composeGuardrails, type GuardrailRow } from './guardrail-composition.js';
import {
  selectEgressStrategy,
  type EgressConfig,
  type EgressMode,
  type SidecarBinding,
} from './credential-egress.js';

/**
 * Basic-auth username emitted for the git/basic schemes (the token rides the
 * password field, so this placeholder works for any PAT). Single source of the
 * literal, mirroring the established credential-proxy default username.
 */
export const DEFAULT_BASIC_USERNAME = 'x-access-token';

/**
 * Audience claim on the LLM-proxy JWT. Distinct from the MCP/session JWT's
 * `aud='ai-gateway'` so the external gateway's LLM route verifies it
 * independently and a leaked LLM token can't be replayed against the MCP route
 * (and vice versa). The gateway MUST verify `aud='llm-proxy'` end to end.
 */
export const LLM_PROXY_AUDIENCE = 'llm-proxy';

/**
 * Default lifetime (seconds) of the LLM-proxy JWT when no explicit TTL is wired.
 * The snapshot/egress design treats the LLM token as an independently-scoped,
 * SHORT-lived credential — deliberately shorter than the default session/MCP JWT
 * TTL (300s) so a leaked LLM token expires fast. Overridable via
 * {@link AgentSnapshotResolverOptions.llmJwtTtlSecs} (wired from
 * `AI_GATEWAY_LLM_JWT_TTL_SECS`).
 */
export const DEFAULT_LLM_PROXY_JWT_TTL_SECS = 120;

/** Env vars the GitHub CLI reads — injected for a token-scheme GitHub-API binding. */
const GH_TOKEN_ENV_VARS = ['GH_TOKEN', 'GITHUB_TOKEN'] as const;

/**
 * Fallback roster-member display name (`agent_name` on `session.thread_created`) when
 * a resolved roster agent carries no name — mirrors the runner's + read-model
 * projector's own `'agent'` default so a nameless member still names its thread.
 */
const DEFAULT_ROSTER_AGENT_NAME = 'agent';

// ── Resolved-record shapes (the loader seam returns these) ──

/** The session fields the resolver reads. */
export interface ResolverSessionRecord {
  id: string;
  workspaceId: string;
  /**
   * The owning organization id — the tenant boundary ABOVE the workspace.
   * Threaded straight through to the minted session/LLM JWTs' required
   * `org_id` claim (see `SessionJwtMinter.mint`, `src/auth/session-jwt.ts`),
   * mirroring how the `/internal/.../mint-jwt` route derives it: joined from
   * `workspaces.organization_id` for the session's workspace (see
   * `loadActiveRuntimeSession` in `internal.routes.ts` and
   * `buildSnapshotRecordLoader` in `api/snapshot-loader.ts`).
   */
  organizationId: string;
  agentId: string;
  /** Version pinned when the Session was created. */
  agentVersion?: number;
  /** Session runtime revision used to invalidate Gateway's bundle cache. */
  runtimeRevision?: number;
  harnessState?: unknown;
  agentOverrides?: Record<string, unknown> | null;
  tools?: AgentToolEntry[] | null;
  mcpServers?: Array<{ name: string; url: string }> | null;
  environmentId: string | null;
  vaultIds: string[];
  /** Guardrails attached through the session's agent overrides. */
  guardrailIds?: string[];
}

/** The agent fields the resolver reads. */
export interface ResolverAgentRecord {
  id: string;
  /**
   * The agent's display name — announced as the roster member's `agent_name` on a
   * coordinator's `session.thread_created`. Optional so an existing loader/fake that
   * predates the multiagent fold need not set it (it defaults to a stable placeholder
   * for the thread announcement); a real loader always carries it.
   */
  name?: string;
  model: { provider: string; id: string };
  system: string;
  tools: AgentToolEntry[];
  mcpServers: Array<{ name: string; url: string }>;
  skills: string[];
  metadata: Record<string, unknown>;
  /** Guardrails pinned on this agent version. */
  guardrailIds?: string[];
}

/** Guardrail definitions + durable state visible to one session. */
export interface ResolverGuardrailContext {
  visible: GuardrailRow[];
  state: Record<string, unknown>;
}

/** The environment fields the resolver reads (the egress mode drives the strategy). */
export interface ResolverEnvironmentRecord {
  id: string;
  target: 'cloud' | 'self_hosted' | null;
  egressMode: EgressMode | null;
}

/**
 * The skill-version fields the resolver reads. Under the progressive Skill
 * disclosure model a skill version is a materialized-on-demand bundle, not an
 * inline system-prompt/tool-allowlist slice, so the resolver reads only the
 * identity fields it needs for existence + workspace-correctness validation.
 */
export interface ResolverSkillVersionRecord {
  id: string;
  workspaceId: string;
}

/** The vault fields the resolver reads (URL binding for gateway; host binding for sidecar). */
export interface ResolverVaultRecord {
  id: string;
  targetUrl: string;
  /** Vault credential kind, e.g. `git_https` / `gh_basic` / `https_bearer`. */
  targetKind?: string;
  archived: boolean;
}

/**
 * Record-loader seam the resolver reads through. The unit tests pass an in-memory
 * fake; the server wires a Drizzle-backed implementation. Every loader is
 * workspace-correct: the session resolves a workspace, and agent/skill/vault
 * reads are scoped to it.
 */
export interface SnapshotRecordLoader {
  loadSession(sessionId: string): Promise<ResolverSessionRecord | null>;
  loadAgent(workspaceId: string, agentId: string): Promise<ResolverAgentRecord | null>;
  loadEnvironment(environmentId: string): Promise<ResolverEnvironmentRecord | null>;
  loadSkillVersion(skillVersionId: string): Promise<ResolverSkillVersionRecord | null>;
  loadVault(workspaceId: string, vaultId: string): Promise<ResolverVaultRecord | null>;
  /**
   * Load every non-archived guardrail visible to the workspace and the
   * session-scoped state restored for this run. Optional for older fakes; the
   * production loader always supplies it.
   */
  loadGuardrailContext?(
    workspaceId: string,
    organizationId: string,
    sessionId: string,
  ): Promise<ResolverGuardrailContext>;
  /**
   * The SESSION-WIDE union of exact Skill-bundle pins for a colocated session — the
   * pinned `session_skill_bindings` rows joined to `skill_versions` → `skills`, the
   * Drizzle twin of `loadPreparedAgents` (the separate path's join) deduplicated by
   * Skill name across the session's whole agent roster (coordinator shares ONE
   * filesystem, so the union materializes once). Fails LOUD on a binding that no
   * longer resolves (a missing / foreign / archived skill version).
   *
   * OPTIONAL: an in-memory loader/fake that predates progressive disclosure omits it,
   * and the resolver then delivers a skill-free snapshot (the field is absent). A real
   * loader implements it so a colocated session's Skills reach the runner.
   */
  loadSessionSkillBundles?(
    workspaceId: string,
    sessionId: string,
  ): Promise<PreparedSkillDescriptor[]>;
  /**
   * Load a version-PINNED agent (a coordinator roster member is pinned at
   * `{ id, version }` so the roster is immutable for the coordinator version). Reads
   * the `agent_versions` snapshot for that version. OPTIONAL: a loader that predates
   * the multiagent fold omits it, and the resolver then falls back to the roster
   * agent's CURRENT version via {@link loadAgent} (a best-effort resolution that still
   * delivers a working roster). A real loader implements it for version correctness.
   */
  loadAgentVersion?(
    workspaceId: string,
    agentId: string,
    version: number,
  ): Promise<ResolverAgentRecord | null>;
}

/** Options for {@link AgentSnapshotResolver}. */
export interface AgentSnapshotResolverOptions {
  /** The record-loader seam. */
  loader: SnapshotRecordLoader;
  /** The registry's session-JWT minter (in-process; no HTTP). */
  minter: SessionJwtMinter;
  /**
   * Public ai-gateway MCP base URL. Required for GATEWAY egress (the rewritten
   * MCP servers point at it); OPTIONAL overall, because a pure-sidecar
   * deployment runs with no gateway at all. When a session resolves to gateway
   * egress and this is undefined, {@link AgentSnapshotResolver.resolve} throws a
   * loud configuration error rather than delivering a broken snapshot. The
   * sidecar path never reads it.
   */
  gatewayMcpUrl?: string;
  /** Public ai-gateway LLM-proxy base URL (gateway egress), when configured. */
  gatewayLlmUrl?: string;
  /**
   * Lifetime (seconds) of the LLM-proxy JWT. The LLM token is an
   * independently-scoped, short-lived credential — minted with its own TTL,
   * deliberately shorter than the session/MCP JWT's default. Defaults to
   * {@link DEFAULT_LLM_PROXY_JWT_TTL_SECS}; wired from `AI_GATEWAY_LLM_JWT_TTL_SECS`.
   * A non-positive value falls back to the default (the minter never mints an
   * already-expired token).
   */
  llmJwtTtlSecs?: number;
  llmPolicy?: SessionJwtLlmPolicy;
}

/**
 * Resolves a session id into a credential-free {@link AgentSnapshot}.
 *
 * Stateless apart from its injected collaborators; one instance per registry
 * replica, called on a runner (re)connect by the snapshot-delivery wiring.
 */
export class AgentSnapshotResolver {
  private readonly loader: SnapshotRecordLoader;
  private readonly minter: SessionJwtMinter;
  private readonly gatewayMcpUrl: string | undefined;
  private readonly gatewayLlmUrl: string | undefined;
  private readonly llmJwtTtlSecs: number;
  private readonly llmPolicy: SessionJwtLlmPolicy | undefined;

  constructor(opts: AgentSnapshotResolverOptions) {
    this.loader = opts.loader;
    this.llmPolicy = opts.llmPolicy;
    this.minter = opts.minter;
    this.gatewayMcpUrl = opts.gatewayMcpUrl;
    this.gatewayLlmUrl = opts.gatewayLlmUrl;
    // A non-positive override would mint an already-expired (or default-TTL)
    // token; clamp to the short-lived default so the LLM credential is always
    // independently + tightly scoped.
    this.llmJwtTtlSecs =
      opts.llmJwtTtlSecs !== undefined && opts.llmJwtTtlSecs > 0
        ? opts.llmJwtTtlSecs
        : DEFAULT_LLM_PROXY_JWT_TTL_SECS;
  }

  /**
   * Resolve the snapshot for `sessionId`, or `null` when the session or its agent
   * is not resolvable (a deleted / cross-workspace session — nothing to deliver).
   *
   * Order:
   *   1. session → workspace + agent id + environment id + vault ids;
   *   2. agent (workspace-scoped) → model, system, tools, mcp servers, skills,
   *      harness annotation → provider;
   *   3. environment → egress mode (defaults to gateway);
   *   4. compose the agent's own snapshot ({@link composeAgentSnapshot}: tools
   *      expanded, egress built, PURE {@link buildAgentSnapshot}; Skills contribute
   *      no prompt or tool slice);
   *   5. coordinator fold ({@link resolveMultiagent}): when the agent carries a
   *      persisted `multiagent` roster, resolve each member's own sub-snapshot and fold
   *      the runtime multiagent block onto the delivered snapshot (additive; absent for
   *      a single agent).
   *
   * @throws Error when a referenced skill_version is missing or belongs to another
   *   workspace (a snapshot built on a missing/foreign skill would silently drop
   *   that Skill's bundle, so this fails loud).
   */
  async resolve(sessionId: string): Promise<AgentSnapshot | null> {
    const session = await this.loader.loadSession(sessionId);
    if (session === null) return null;
    if (session.agentVersion !== undefined && !this.loader.loadAgentVersion) {
      throw new Error('snapshot loader cannot load the pinned agent version');
    }
    const pinned =
      session.agentVersion !== undefined
        ? await this.loader.loadAgentVersion!(
            session.workspaceId,
            session.agentId,
            session.agentVersion,
          )
        : await this.loader.loadAgent(session.workspaceId, session.agentId);
    if (pinned === null) return null;
    const overrides = session.agentOverrides ?? {};
    const model = normalizeModelForStorage(overrides.model ?? pinned.model, pinned.model.provider);
    if ('error' in model) throw new Error(model.error);
    const modelError = validateHarnessModel(pinned.metadata, model);
    if (modelError) throw new Error(modelError);
    const agent: ResolverAgentRecord = {
      ...pinned,
      model,
      ...(Object.hasOwn(overrides, 'system')
        ? { system: typeof overrides.system === 'string' ? overrides.system : '' }
        : {}),
      tools: session.tools ?? pinned.tools,
      mcpServers: session.mcpServers ?? pinned.mcpServers,
    };

    // The session's egress mode (shared by the coordinator and every roster member —
    // all threads run in the ONE session, sharing its sandbox / vault credentials).
    const environment = session.environmentId
      ? await this.loader.loadEnvironment(session.environmentId)
      : null;
    const selection = resolveHarnessAnnotation(agent.metadata);
    if ('error' in selection) throw new Error(selection.error);
    const deploymentError = validateHarnessDeployment(environment?.target ?? 'cloud', selection);
    if (deploymentError) throw new Error(`snapshot: ${deploymentError}`);
    const egressMode = environment?.egressMode ?? null;

    // Compose the coordinator's (or single agent's) own snapshot.
    const snapshot = await this.composeAgentSnapshot(session, agent, egressMode);
    if (session.harnessState) snapshot.harness_state = session.harnessState;

    // COLOCATED progressive Skill disclosure (additive): attach the SESSION-WIDE union
    // of exact Skill-bundle pins to the TOP-LEVEL snapshot. Sourced from the pinned
    // `session_skill_bindings` rows (NOT `agents.skills`, which in the current schema is
    // ORDERED REFS, not version ids) via the loader seam — the same rows the separate
    // path materializes. Attached to the top-level snapshot ONLY; roster member
    // sub-snapshots stay skill-free (a coordinator shares ONE filesystem, so the union
    // materializes once). Omitted entirely when the session has no Skills, so a skill-free
    // snapshot is byte-for-byte unchanged. A missing / foreign binding fails LOUD in the
    // loader (a snapshot built on a dropped Skill would silently lose it).
    const skillBundles = this.loader.loadSessionSkillBundles
      ? await this.loader.loadSessionSkillBundles(session.workspaceId, session.id)
      : [];
    if (skillBundles.length > 0) {
      snapshot.skills = skillBundles;
    }

    // COORDINATOR FOLD (additive): when the agent carries a persisted `multiagent`
    // roster, resolve each roster member's OWN sub-snapshot and fold the runtime
    // multiagent block onto the delivered snapshot — so the runner's
    // `maybeWrapCoordinator` fires and drives the thread choreography. A single-agent
    // agent has no roster, so `resolveMultiagent` returns undefined and the snapshot is
    // unchanged. The primary_thread_id is left empty here (the registry creates the
    // primary thread row lazily, so it is not known at snapshot-delivery time).
    const resolvedMultiagent = await this.resolveMultiagent(session, agent, egressMode);
    if (resolvedMultiagent !== undefined) {
      snapshot.multiagent = resolvedMultiagent.snapshot;
    }

    // Guardrails are composed by Registry, never re-derived by the credential-
    // free runner. This mirrors prepareExecution's four-tier fold for the
    // separate harness path and carries the restored state in the SAME snapshot
    // so a reconnect cannot briefly run with an empty policy.
    if (this.loader.loadGuardrailContext !== undefined) {
      const context = await this.loader.loadGuardrailContext(
        session.workspaceId,
        session.organizationId,
        session.id,
      );
      const composed = composeGuardrails({
        sessionIds: session.guardrailIds ?? [],
        agentIds: agent.guardrailIds ?? [],
        subagentIds: resolvedMultiagent?.subagentGuardrailIds ?? {},
        visible: context.visible,
      });
      if (composed.invalid.length > 0) {
        const detail = composed.invalid
          .map((entry) => `${entry.name} (${entry.id}): ${entry.errors.join('; ')}`)
          .join(' | ');
        throw new Error(`snapshot: guardrail no longer valid: ${detail}`);
      }
      snapshot.guardrails = composed.guardrails.map(toSnapshotGuardrail);
      snapshot.guardrail_state = context.state;
    }
    return snapshot;
  }

  /**
   * Compose one agent's credential-free snapshot for a session — the per-agent
   * pipeline shared by the coordinator (the session's agent) and each resolved roster
   * member. Resolves the agent's skills, its egress (scoped to ITS mcp servers +
   * minted per this session + its vaults), and its provider, then calls the pure
   * {@link buildAgentSnapshot}. The `egressMode` is the SESSION's mode (all threads
   * share the ONE session's egress posture); vaults are the session's vaults.
   */
  private async composeAgentSnapshot(
    session: ResolverSessionRecord,
    agent: ResolverAgentRecord,
    egressMode: EgressMode | null,
  ): Promise<AgentSnapshot> {
    const provider = resolveAgentProvider(agent.metadata);
    // Progressive Skill disclosure: a skill version is a materialized-on-demand bundle,
    // not an inline system-prompt/tool-allowlist slice, so the EAGER composition gets NO
    // slices — a Skill contributes nothing to this agent's `system` / `allowed_tool_names`.
    // The SESSION-WIDE bundle union is resolved from `session_skill_bindings` and attached
    // to the TOP-LEVEL snapshot by `resolve()` (see there); the runner materializes it.
    const skills: SkillSlice[] = [];
    const vaults = await this.resolveVaults(session.workspaceId, session.vaultIds);
    const egress = await this.buildEgress({ session, agent, egressMode, vaults });
    // The generic `custom` native-CLI provider carries its declarative CLI spec on the
    // snapshot; it rides the agent's free-form `metadata.custom_spec` (the runner's
    // custom harness parses + validates it). Lift it verbatim when present so a
    // `custom`-annotated agent's spec reaches the runner; every other provider omits it.
    // Spread into the pure builder input only when present — under
    // `exactOptionalPropertyTypes` an explicit `undefined` is not assignable to the
    // optional `customSpec?` slot.
    const customSpec = agent.metadata['custom_spec'];
    return buildAgentSnapshot({
      agent: { model: agent.model, system: agent.system, tools: agent.tools },
      skills,
      provider,
      egress,
      ...(customSpec !== undefined ? { customSpec } : {}),
    });
  }

  /**
   * Resolve the coordinator's persisted `multiagent` roster into the RUNTIME
   * multiagent block folded onto the delivered snapshot, or `undefined` when the agent
   * is single-agent (no `multiagent`).
   *
   * For each roster member:
   *   - `{ type: 'self' }` → a `self` member carrying the coordinator's own display
   *     name; the runner derives its snapshot from the coordinator's (one-level);
   *   - `{ type: 'agent', id, version }` → load the version-PINNED roster agent (or,
   *     when the loader cannot pin a version, its current version — best-effort) and
   *     compose ITS own snapshot (its own model/system/tools/MCP/egress).
   *
   * A member whose agent no longer resolves (deleted / cross-workspace) is DROPPED —
   * a coordinator degrades to the roster it can still reach rather than failing the
   * whole session's snapshot. If NO member resolves, the roster is empty and the fold
   * is skipped (the coordinator runs as a plain agent).
   */
  private async resolveMultiagent(
    session: ResolverSessionRecord,
    agent: ResolverAgentRecord,
    egressMode: EgressMode | null,
  ): Promise<
    | {
        snapshot: AgentSnapshotMultiagent;
        subagentGuardrailIds: Record<string, string[]>;
      }
    | undefined
  > {
    const roster: MultiagentSnapshot | null = readMultiagentSnapshot(agent.metadata['multiagent']);
    if (roster === null) {
      return undefined;
    }
    const members: AgentSnapshotMultiagentMember[] = [];
    const subagentGuardrailIds: Record<string, string[]> = {};
    for (const member of roster.agents) {
      if (member.type === 'self') {
        // A self member reuses the coordinator's own config on the runner side (the
        // runner strips the multiagent block for one-level delegation); it carries only
        // the coordinator's display name for the thread announcement.
        members.push({ type: 'self', agent_name: agent.name ?? DEFAULT_ROSTER_AGENT_NAME });
        continue;
      }
      const memberAgent = await this.loadRosterMemberAgent(
        session.workspaceId,
        member.id,
        member.version,
      );
      if (memberAgent === null) {
        // A roster agent that no longer resolves is dropped (degrade, do not fail).
        continue;
      }
      const memberSnapshot = await this.composeAgentSnapshot(session, memberAgent, egressMode);
      members.push({
        agent_name: memberAgent.name ?? DEFAULT_ROSTER_AGENT_NAME,
        snapshot: memberSnapshot,
      });
      const ids = memberAgent.guardrailIds ?? [];
      if (ids.length > 0) subagentGuardrailIds[memberAgent.id] = [...ids];
    }
    if (members.length === 0) {
      // No member resolved — nothing to delegate to; run as a plain agent.
      return undefined;
    }
    // primary_thread_id is unknown at snapshot-delivery time (the primary thread row is
    // created lazily); the runner coordinator tolerates the empty string (null parent).
    return {
      snapshot: { type: 'coordinator', primary_thread_id: '', agents: members },
      subagentGuardrailIds,
    };
  }

  /**
   * Load a version-pinned roster member agent, falling back to its current version when
   * the loader cannot pin a version (an older loader without {@link
   * SnapshotRecordLoader.loadAgentVersion}). Both reads are workspace-scoped.
   */
  private async loadRosterMemberAgent(
    workspaceId: string,
    agentId: string,
    version: number,
  ): Promise<ResolverAgentRecord | null> {
    if (this.loader.loadAgentVersion !== undefined) {
      const pinned = await this.loader.loadAgentVersion(workspaceId, agentId, version);
      if (pinned !== null) {
        return pinned;
      }
    }
    // No pinned-version loader, or the pinned version is gone — best-effort fall back to
    // the roster agent's current definition so the coordinator still reaches it.
    return this.loader.loadAgent(workspaceId, agentId);
  }

  /**
   * Resolve the SESSION-WIDE union of exact Skill-bundle pins for a session — the
   * {@link SkillsProvider} seam the owner pod's skills delivery reads to push bundle
   * BYTES to the runner. Deliberately CHEAP (no JWT mint, no egress build): a session
   * lookup for the workspace scope, then the pinned-binding union from the loader.
   * Returns `[]` for a deleted / cross-workspace session or a loader that predates
   * progressive disclosure. Fails LOUD (propagates) on a missing / foreign binding.
   *
   * The SAME union is attached to `snapshot.skills` by {@link resolve}; a caller that
   * only needs the descriptors (the skills push) uses this without composing a whole
   * snapshot.
   */
  async resolveSkillBundles(sessionId: string): Promise<PreparedSkillDescriptor[]> {
    const session = await this.loader.loadSession(sessionId);
    if (session === null || this.loader.loadSessionSkillBundles === undefined) {
      return [];
    }
    return this.loader.loadSessionSkillBundles(session.workspaceId, session.id);
  }

  /** Resolve the session vaults, dropping archived ones, preserving order. */
  private async resolveVaults(
    workspaceId: string,
    vaultIds: string[],
  ): Promise<ResolverVaultRecord[]> {
    const resolved: ResolverVaultRecord[] = [];
    for (const id of vaultIds) {
      const vault = await this.loader.loadVault(workspaceId, id);
      if (vault === null || vault.archived) continue;
      resolved.push(vault);
    }
    return resolved;
  }

  /** Build the egress config for the selected mode. */
  private async buildEgress(args: {
    session: ResolverSessionRecord;
    agent: ResolverAgentRecord;
    egressMode: EgressMode | null;
    vaults: ResolverVaultRecord[];
  }): Promise<EgressConfig> {
    const strategy = selectEgressStrategy(args.egressMode);
    if (strategy.mode === 'gateway') {
      return this.buildGatewayConfig(args.session, args.agent, args.vaults);
    }
    // sidecar
    return strategy.build({ bindings: vaultsToBindings(args.vaults) });
  }

  /** Mint the scoped JWTs in-process and build the gateway egress config. */
  private async buildGatewayConfig(
    session: ResolverSessionRecord,
    agent: ResolverAgentRecord,
    vaults: ResolverVaultRecord[],
  ): Promise<EgressConfig> {
    // Gateway egress rewrites every MCP server to the gateway URL, so the URL is
    // mandatory here. A pure-sidecar deployment legitimately runs with no gateway
    // (`gatewayMcpUrl` undefined) — but a session that resolves to GATEWAY egress
    // in that deployment is a configuration error, so fail loud rather than emit a
    // snapshot whose servers point at `undefined`.
    if (this.gatewayMcpUrl === undefined) {
      throw new Error(
        'snapshot: gateway egress selected but no ai-gateway MCP URL is configured ' +
          '(set AI_GATEWAY_MCP_URL, or set the environment egress_mode to sidecar)',
      );
    }

    const serverNames = agent.mcpServers.map((s) => s.name);
    const guardrailClaims = {
      agent_id: agent.id,
      guardrail_ids: [...new Set([...(session.guardrailIds ?? []), ...(agent.guardrailIds ?? [])])],
      ...(session.runtimeRevision !== undefined
        ? { runtime_config_revision: String(session.runtimeRevision) }
        : {}),
    };
    // url → vault_id, first-match-wins over the (archived-filtered) session vaults.
    const vaultByUrl = new Map<string, string>();
    for (const v of vaults) {
      if (!vaultByUrl.has(v.targetUrl)) vaultByUrl.set(v.targetUrl, v.id);
    }

    // Mint the scoped session JWT ONLY when the agent has at least one MCP server
    // (the `serverNames.length > 0` guard below): with no servers
    // `rewriteMcpServers` returns `{}`, so no rewritten server would carry the
    // token and minting one would put an unused short-lived credential in the
    // snapshot. The JWT carries the FULL session vault_ids allowlist (not just the
    // matched ones) — per-call X-Orca-Vault-Id headers narrow it at request time,
    // and the gateway only accepts a vault_id present in the JWT.
    const nativeSdk = ['codex-sdk', 'pi-sdk'].includes(resolveAgentProvider(agent.metadata));
    let sessionJwt = '';
    if (serverNames.length > 0) {
      const minted = await this.minter.mint(
        {
          org_id: session.organizationId,
          workspace_id: session.workspaceId,
          session_id: session.id,
          mcp_server_names: serverNames,
          vault_ids: session.vaultIds,
          // TODO(follow-up): the snapshot-resolver loader seam has no per-credential
          // read yet (only vault-level records), so this rides empty rather than the
          // active vault-credential ids `internal.routes.ts`'s mint-jwt route derives
          // via `listActiveCredentialIds`. Satisfies `SessionJwtClaims`'s required
          // field without over-claiming a scope the resolver cannot yet compute; no
          // snapshot test pins a non-empty allowlist here.
          credential_ids: [],
          ...guardrailClaims,
        },
        nativeSdk ? { ttlSecs: 660 } : undefined,
      );
      sessionJwt = minted.token;
    }

    const strategy = selectEgressStrategy('gateway');
    if (strategy.mode !== 'gateway') throw new Error('unreachable');

    const input: Parameters<typeof strategy.build>[0] = {
      sessionId: session.id,
      gatewayMcpUrl: this.gatewayMcpUrl,
      sessionJwt,
      mcpServers: agent.mcpServers,
      vaultByUrl,
    };
    if (this.gatewayLlmUrl !== undefined) {
      input.gatewayLlmUrl = this.gatewayLlmUrl;
      // The native provider route uses the gateway's configured audience and
      // exact model/route claims. Deployment policy is an upper bound, as on
      // the internal mint-jwt endpoint; absence never grants model access.
      const nativeRoute =
        resolveAgentProvider(agent.metadata) === 'pi-sdk'
          ? piLlmRoute(agent.model.provider, agent.model.id)
          : 'llm-responses';
      const models = nativeSdk
        ? intersectLlmModels(this.llmPolicy?.models ?? [], [agent.model.id])
        : [];
      if (nativeSdk && (!this.llmPolicy?.routes.includes(nativeRoute) || models.length === 0)) {
        throw new Error(
          `${resolveAgentProvider(agent.metadata)} requires ${nativeRoute} and its model in SESSION_JWT_LLM_ROUTES/MODELS`,
        );
      }
      // Each native SDK refreshes the snapshot before each turn. Its token outlives the
      // bounded ten-minute turn, including time spent waiting for approval.
      const llmMinted = await this.minter.mint(
        {
          org_id: session.organizationId,
          workspace_id: session.workspaceId,
          session_id: session.id,
          mcp_server_names: [],
          vault_ids: [],
          credential_ids: [],
          ...guardrailClaims,
          ...(nativeSdk ? { llm_routes: [nativeRoute], llm_models: models } : {}),
        },
        nativeSdk
          ? { ttlSecs: Math.max(this.llmJwtTtlSecs, 660) }
          : { audience: LLM_PROXY_AUDIENCE, ttlSecs: this.llmJwtTtlSecs },
      );
      input.llmJwt = llmMinted.token;
    }
    return strategy.build(input);
  }
}

function toSnapshotGuardrail(
  entry: ReturnType<typeof composeGuardrails>['guardrails'][number],
): SnapshotGuardrail {
  return {
    id: entry.guardrail.id,
    name: entry.guardrail.name,
    tier: entry.tier,
    phases: [...entry.guardrail.phases],
    rule: entry.guardrail.rule,
    stateful: entry.stateful,
    ...(entry.stateScope ? { state_scope: entry.stateScope } : {}),
    ...(entry.subagentId ? { subagent_id: entry.subagentId } : {}),
  };
}

/**
 * Map an agent's harness annotation to the runner provider id.
 *
 * Resolves `metadata.harness`/`metadata.mode` via the shared catalog (an invalid
 * annotation falls back to the platform default harness, matching the runner's
 * own fallback), then maps the harness to its provider via the catalog's
 * {@link harnessToProvider} — the SINGLE source of the harness→provider mapping,
 * shared with the harness-server in-sandbox builder so the two never drift. The
 * provider is what a multi-provider runner reads from the snapshot to spin up the
 * right harness.
 *
 * Exported so the claim-based distribution path
 * (`buildDistributionSessionStore` in `sessions.routes.ts`) derives the SAME
 * provider it stamps on `DistributionSessionRow.agentHarness` — the value the
 * capability-match validates against the connected worker's advertised
 * `configuredHarnesses` and the connected runner's advertised `hello.harnesses`
 * (both `ProviderRegistry.providerNames()`). Sharing this one derivation is what
 * keeps the snapshot's `provider` and the distribution row's provider in lockstep,
 * so the pre-spawn / connect capability checks fire against the SAME provider the
 * runner is asked to construct.
 */
export function resolveAgentProvider(metadata: Record<string, unknown> | null | undefined): string {
  const annotation = resolveHarnessAnnotation(metadata ?? undefined);
  if ('error' in annotation) throw new Error(annotation.error);
  return harnessToProvider(annotation.harness);
}

/**
 * Resolve an agent's execution mode (`colocated` | `separate`) from its
 * harness annotation (`metadata.harness`/`metadata.mode`), via the SAME
 * shared-catalog {@link resolveHarnessAnnotation} call {@link resolveAgentProvider}
 * uses — one derivation, so the mode and the provider it implies never
 * disagree. Invalid annotations fail before a runner is provisioned.
 *
 * Consulted by the session-create route (`sessions.routes.ts`) to decide
 * whether a `target=cloud` session's environment needs the environment-launch
 * lifecycle kicked (only a `colocated` agent runs in the sandbox the registry
 * would provision — a `separate` agent's loop stays in harness-server and
 * never touches this path).
 */
export function resolveAgentMode(
  metadata: Record<string, unknown> | null | undefined,
): HarnessMode {
  const annotation = resolveHarnessAnnotation(metadata ?? undefined);
  if ('error' in annotation) throw new Error(annotation.error);
  return annotation.mode;
}

/**
 * Derive the sidecar credential-proxy bindings from the session vaults.
 *
 * Each vault's `target_kind` selects the scheme + username + env-injection
 * preset, mirroring the established credential-proxy presets; the host is taken
 * from the vault's `target_url`. A vault whose URL can't be parsed is skipped
 * (defensive — vault URLs are validated up-front). The secret is referenced by
 * vault id; the sidecar resolves the real value out of band.
 *
 * Bindings are deduped by HOST, first-match-wins, preserving session-vault
 * order — the exact parity the gateway path applies via its `vaultByUrl` map.
 * The credential-proxy enforces ONE exact-host rewrite rule per host (a request
 * to `host` with no `Authorization` gets that host's single binding swapped on),
 * so two session vaults resolving to the same host must collapse to one entry:
 * the FIRST vault listed for the host wins (matching the gateway's
 * first-match-wins URL map), and a later vault for the same host — even with a
 * different kind — is dropped rather than emitting a second, ambiguous entry the
 * proxy would never reach.
 */
function vaultsToBindings(vaults: ResolverVaultRecord[]): SidecarBinding[] {
  const bindings: SidecarBinding[] = [];
  const seenHosts = new Set<string>();
  for (const v of vaults) {
    const host = hostOf(v.targetUrl);
    if (host === null) continue;
    if (seenHosts.has(host)) continue;
    seenHosts.add(host);
    bindings.push(bindingForKind(v.targetKind ?? '', host, v.id));
  }
  return bindings;
}

/**
 * The credential-proxy binding for a vault `target_kind` + host.
 *
 *   - `gh_basic` — the GitHub-CLI preset: an `api.*` host emits the `token` scheme
 *     with `GH_TOKEN`/`GITHUB_TOKEN` injection (gh refuses to issue an API request
 *     without a local token), and any other host emits `basic` swap-on-access with
 *     the git-friendly username.
 *   - `git_https` / `https_basic` — `basic` swap-on-access with the git-friendly
 *     username, no env injection.
 *   - `https_bearer` (and any other / unknown kind) — `bearer` swap-on-access, no
 *     username, no env injection (the safe default: nothing credential-shaped in
 *     the runner, the sidecar injects the bearer token outbound).
 */
function bindingForKind(targetKind: string, host: string, vaultId: string): SidecarBinding {
  if (targetKind === 'gh_basic') {
    if (host.startsWith('api.')) {
      return { host, scheme: 'token', vaultId, injectEnv: [...GH_TOKEN_ENV_VARS] };
    }
    return { host, scheme: 'basic', vaultId, username: DEFAULT_BASIC_USERNAME };
  }
  if (targetKind === 'git_https' || targetKind === 'https_basic') {
    return { host, scheme: 'basic', vaultId, username: DEFAULT_BASIC_USERNAME };
  }
  // https_bearer + any unknown kind → bearer swap-on-access.
  return { host, scheme: 'bearer', vaultId };
}

/** The lower-cased hostname of a URL, or `null` when it can't be parsed. */
function hostOf(url: string): string | null {
  try {
    return new URL(url).hostname.toLowerCase();
  } catch {
    return null;
  }
}
