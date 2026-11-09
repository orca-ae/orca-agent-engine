// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import {
  DSH_PROFILE_CAPABILITY,
  bindStoredHarness,
  resolveHarnessAnnotation,
  validateHarnessDeployment,
  validateHarnessGuardrails,
  validateHarnessFeatures,
  validateHarnessModel,
  type AgentToolEntry,
} from '@orca/harness-catalog';
import {
  retainedDshProfile,
  requireDshProfileApproval,
  requireDshProfileEnvironment,
  type DshApprovedProfiles,
} from './dsh-profile.js';
import type { FileStore } from '@orca/file-store';
import type { MemoryStore } from '@orca/memory-store';
import { assertGuardrailReaderCapabilities, GUARDRAIL_READER_CAPABILITIES } from '@orca/guardrails';
import { bindPolicyTurn, isDogwood } from './policy-events.js';
import { and, eq, inArray, isNull, isNotNull, or } from 'drizzle-orm';
import {
  PreparedAgentSnapshotSchema,
  type PreparedAgentSnapshot,
  type PreparedExecutionV2,
} from '../contracts/internal.contract.js';
import { normalizeStoredPackages } from '../contracts/environment-wire.js';
import {
  loadGuardrailSubjectWindowState,
  utcMonthWindow,
  WORKSPACE_BUDGET_SUBJECT,
} from './usage-accounting.js';
import type { GuardrailRouteOptions as GuardrailRollout } from './guardrail-rollout.js';
import { composeGuardrails, type GuardrailRow } from './guardrail-composition.js';
import {
  normalizeModelForStorage,
  validateModelRosterForStorage,
} from '../contracts/model-wire.js';
import type { DbClient } from '../persistence/postgres/client.js';
import {
  agents,
  agentVersions,
  environments,
  gitCredentials,
  guardrails,
  guardrailPolicyBindings,
  guardrailState,
  sessionResources,
  sessionSkillBindings,
  sessionThreads,
  sessions,
  skills,
  skillVersions,
  vaultCredentials,
  vaults,
  workspaces,
} from '../persistence/postgres/schema.js';
import { repositoryUrlsEqual } from './git-credentials.js';
import { authoritativeUsageCallerForSnapshot } from './usage-authority.js';
import {
  harnessStateRevision,
  loadHarnessTurnState,
  HarnessTurnInvalidBindingError,
} from './harness-state.js';

type SessionStatus = PreparedExecutionV2['session']['status'];
type PreparedAgent = PreparedExecutionV2['primary_agent'];
type PreparedSkillDescriptor = PreparedAgent['skills'][number];

interface LoadedAgent {
  id: string;
  version: number;
  snapshot: PreparedAgentSnapshot;
  skillRefCount: number;
}

export interface SessionAgentBinding {
  agentId: string;
  agentVersion: number;
  agentOverrides: unknown;
}

interface AgentBinding {
  agentId: string;
  agentVersion: number;
}

export class PreparedExecutionNotFoundError extends Error {
  constructor() {
    super('session not found');
    this.name = 'PreparedExecutionNotFoundError';
  }
}

export class InvalidRuntimeBindingError extends Error {
  constructor(
    readonly resourceType: string,
    readonly resourceId: string,
  ) {
    super(`invalid runtime binding: ${resourceType} ${resourceId}`);
    this.name = 'InvalidRuntimeBindingError';
  }
}

export class RuntimeDependencyUnavailableError extends Error {
  constructor(readonly dependency: string) {
    super(`${dependency} is not configured`);
    this.name = 'RuntimeDependencyUnavailableError';
  }
}

/** Resolve the concrete models used by a session's pinned primary and subagents. */
export async function loadEffectiveSessionAgentModels(input: {
  db: DbClient;
  workspaceId: string;
  session: SessionAgentBinding;
  primarySnapshot?: PreparedAgentSnapshot;
}): Promise<string[]> {
  const { db, workspaceId, session } = input;
  const identity = input.primarySnapshot
    ? (
        await db
          .select({ harnessType: agents.harnessType })
          .from(agents)
          .where(
            and(
              eq(agents.workspaceId, workspaceId),
              eq(agents.id, session.agentId),
              isNull(agents.deletedAt),
            ),
          )
          .limit(1)
      )[0]
    : undefined;
  if (input.primarySnapshot && !identity)
    throw new InvalidRuntimeBindingError('agent', session.agentId);
  const primary = input.primarySnapshot
    ? applyAgentOverrides(
        loadedAgentFromSnapshot(
          session.agentId,
          session.agentVersion,
          input.primarySnapshot,
          identity!.harnessType,
        ),
        { tools: null, mcpServers: null, agentOverrides: session.agentOverrides },
      )
    : await loadAgent(db, workspaceId, session.agentId, session.agentVersion, {
        tools: null,
        mcpServers: null,
        agentOverrides: session.agentOverrides,
      });
  const models = new Set<string>([primary.snapshot.model.id]);

  const subagentBindings = [
    ...new Map(
      (primary.snapshot.multiagent?.agents ?? [])
        .filter((ref) => ref.id !== primary.id || ref.version !== primary.version)
        .map((ref) => [`${ref.id}@${ref.version}`, { agentId: ref.id, agentVersion: ref.version }]),
    ).values(),
  ];
  const subagents = await loadAgents(db, workspaceId, subagentBindings);
  for (const binding of subagentBindings) {
    const subagent = subagents.get(agentBindingKey(binding));
    if (!subagent) {
      throw new InvalidRuntimeBindingError('agent_version', agentBindingKey(binding));
    }
    models.add(subagent.snapshot.model.id);
  }

  return [...models];
}

export async function prepareExecution(input: {
  sandboxNetworkEnabled?: boolean;
  capabilities?: string[];
  dshApprovedProfiles?: DshApprovedProfiles;
  db: DbClient;
  fileStore: FileStore;
  memoryStore?: MemoryStore;
  workspaceId: string;
  sessionId: string;
  guardrailCapabilities?: readonly string[] | undefined;
  policyOwner?: string | undefined;
  turnEventId?: string | undefined;
  gatewayRegistryUsageEnabled?: boolean;
  guardrails?: GuardrailRollout;
}): Promise<PreparedExecutionV2> {
  // Session revision, pinned versions, vault bindings, and session resources
  // must come from one Registry snapshot. Without REPEATABLE READ, concurrent
  // attach/detach or UpdateSession commits can combine an old revision with a
  // new resource graph in one response.
  return input.db.transaction(
    async (tx) =>
      await buildPreparedExecution({
        ...input,
        // The transaction exposes the same query surface used by the
        // read-only builder but is not nominally assignable to DbClient.
        db: tx as unknown as DbClient,
      }),
    { isolationLevel: 'repeatable read' },
  );
}

/** Prepare only the session's pinned guardrails for the Gateway refresh path. */
export async function prepareGatewayGuardrails(input: {
  guardrails?: GuardrailRollout;
  db: DbClient;
  workspaceId: string;
  sessionId: string;
}): Promise<{
  workspace_id: string;
  session: { id: string; runtime_revision: number };
  policy_bundle_id?: string;
  agent_ids: string[];
  guardrails: PreparedExecutionV2['guardrails'];
  guardrail_state: Record<string, unknown>;
  guardrail_workspace_window_state?: Record<string, unknown>;
  guardrail_month_window?: string;
}> {
  return input.db.transaction(
    async (tx) => {
      const db = tx as unknown as DbClient;
      const workspaceRows = await db
        .select({ status: workspaces.status })
        .from(workspaces)
        .where(eq(workspaces.id, input.workspaceId))
        .limit(1);
      if (workspaceRows[0]?.status !== 'active') throw new PreparedExecutionNotFoundError();
      const sessionRows = await db
        .select()
        .from(sessions)
        .where(
          and(
            isNull(sessions.deletedAt),
            eq(sessions.id, input.sessionId),
            eq(sessions.workspaceId, input.workspaceId),
          ),
        )
        .limit(1);
      const session = sessionRows[0];
      if (!session) throw new PreparedExecutionNotFoundError();
      if (session.archivedAt !== null || session.status === 'terminated') {
        throw new InvalidRuntimeBindingError('session', input.sessionId);
      }

      const primary = await loadAgent(db, input.workspaceId, session.agentId, session.agentVersion);
      const loadedAgents = [primary];
      const seen = new Set([`${primary.id}@${primary.version}`]);
      for (const ref of primary.snapshot.multiagent?.agents ?? []) {
        const key = `${ref.id}@${ref.version}`;
        if (seen.has(key)) continue;
        seen.add(key);
        const member = await loadAgent(db, input.workspaceId, ref.id, ref.version);
        if (member.snapshot.multiagent !== null) {
          throw new InvalidRuntimeBindingError('nested_coordinator_agent', key);
        }
        loadedAgents.push(member);
      }
      const policy = await loadGuardrails(
        db,
        input.workspaceId,
        session.id,
        loadedAgents,
        sessionGuardrailIds(session.agentOverrides),
        input.guardrails,
      );
      const [binding] = await db
        .select()
        .from(guardrailPolicyBindings)
        .where(
          and(
            eq(guardrailPolicyBindings.workspaceId, input.workspaceId),
            eq(guardrailPolicyBindings.sessionId, input.sessionId),
          ),
        );
      if (binding) {
        const pinned = binding.data as import('./dogwood-types.js').PolicyBinding;
        if (pinned.enforcer === 'ai-gateway') policy.guardrails = pinned.guardrails;
        else policy.guardrails = policy.guardrails.filter((g) => !isDogwood(g.rule));
      } else if (policy.guardrails.some((g) => isDogwood(g.rule)))
        throw new InvalidRuntimeBindingError(
          'guardrail',
          'Dogwood has no prepared enforcer binding',
        );
      return {
        workspace_id: input.workspaceId,
        session: { id: session.id, runtime_revision: session.runtimeRevision },
        agent_ids: loadedAgents.map((agent) => agent.id),
        ...(binding ? { policy_bundle_id: (binding.data as { bundleId: string }).bundleId } : {}),
        guardrails: policy.guardrails,
        guardrail_state: policy.state,
        ...(policy.workspaceState
          ? {
              guardrail_workspace_window_state: policy.workspaceState,
              guardrail_month_window: policy.monthWindow!,
            }
          : {}),
      };
    },
    { isolationLevel: 'repeatable read' },
  );
}

async function buildPreparedExecution(input: {
  sandboxNetworkEnabled?: boolean;
  capabilities?: string[];
  dshApprovedProfiles?: DshApprovedProfiles;
  db: DbClient;
  fileStore: FileStore;
  memoryStore?: MemoryStore;
  workspaceId: string;
  sessionId: string;
  guardrailCapabilities?: readonly string[] | undefined;
  policyOwner?: string | undefined;
  turnEventId?: string | undefined;
  gatewayRegistryUsageEnabled?: boolean;
  guardrails?: GuardrailRollout;
}): Promise<PreparedExecutionV2> {
  const { db, fileStore, memoryStore, workspaceId, sessionId } = input;
  const workspaceRows = await db
    .select({ status: workspaces.status })
    .from(workspaces)
    .where(eq(workspaces.id, workspaceId))
    .limit(1);
  if (workspaceRows[0]?.status !== 'active') throw new PreparedExecutionNotFoundError();
  const sessionRows = await db
    .select()
    .from(sessions)
    .where(
      and(
        isNull(sessions.deletedAt),
        eq(sessions.id, sessionId),
        eq(sessions.workspaceId, workspaceId),
      ),
    )
    .limit(1);
  const session = sessionRows[0];
  if (!session) throw new PreparedExecutionNotFoundError();
  if (session.archivedAt !== null) {
    throw new InvalidRuntimeBindingError('session', sessionId);
  }
  if (session.status === 'terminated') {
    throw new InvalidRuntimeBindingError('session', sessionId);
  }

  const primaryLoaded = await loadAgent(db, workspaceId, session.agentId, session.agentVersion, {
    tools: session.tools,
    mcpServers: session.mcpServers,
    agentOverrides: session.agentOverrides,
  });
  const usageWriter = authoritativeUsageCallerForSnapshot(
    primaryLoaded.snapshot,
    input.gatewayRegistryUsageEnabled ?? false,
  );
  if (!usageWriter) {
    throw new InvalidRuntimeBindingError(
      'agent_version',
      `${session.agentId}@${session.agentVersion}`,
    );
  }

  const graph = new Map<string, LoadedAgent>([[agentKey(primaryLoaded), primaryLoaded]]);
  const subagentLoaded: LoadedAgent[] = [];
  for (const ref of primaryLoaded.snapshot.multiagent?.agents ?? []) {
    const key = `${ref.id}@${ref.version}`;
    if (graph.has(key)) continue;
    const loaded = await loadAgent(db, workspaceId, ref.id, ref.version);
    if (loaded.snapshot.multiagent !== null) {
      throw new InvalidRuntimeBindingError('nested_coordinator_agent', key);
    }
    graph.set(key, loaded);
    subagentLoaded.push(loaded);
  }

  const loadedAgents = [primaryLoaded, ...subagentLoaded];
  const modelRoster = loadedAgents.map((agent, index) => {
    const model = normalizeModelForStorage(agent.snapshot.model);
    if ('error' in model) {
      throw new InvalidRuntimeBindingError(
        'model_config',
        `${agent.id}@${agent.version}: ${model.error}`,
      );
    }
    const harnessModelError = validateHarnessModel(
      agent.snapshot.metadata as Record<string, unknown>,
      model,
    );
    if (harnessModelError) throw new InvalidRuntimeBindingError('model_config', harnessModelError);
    return {
      ...(typeof agent.snapshot.metadata.harness === 'string'
        ? { harness: agent.snapshot.metadata.harness }
        : {}),
      label: index === 0 ? 'primary agent' : `subagent ${agent.id}@${agent.version}`,
      model,
    };
  });
  const modelRosterError = validateModelRosterForStorage(modelRoster);
  if (modelRosterError) {
    throw new InvalidRuntimeBindingError('model_config', modelRosterError);
  }

  const preparedAgents = await loadPreparedAgents(db, workspaceId, sessionId, loadedAgents);
  const primaryAgent = preparedAgents[0]!;
  const subagents = preparedAgents.slice(1);

  const environment = session.environmentId
    ? await loadEnvironment(db, workspaceId, session.environmentId)
    : null;
  try {
    if (Object.hasOwn(asRecord(session.metadata), 'dsh_profile'))
      throw new Error('Session metadata.dsh_profile is reserved for Agents');
    const profile = retainedDshProfile(primaryLoaded.snapshot);
    if (profile) {
      if (!input.capabilities?.includes(DSH_PROFILE_CAPABILITY))
        throw new Error('Harness does not advertise dsh_profile_v1');
      requireDshProfileApproval(input.dshApprovedProfiles ?? {}, workspaceId, profile);
      requireDshProfileEnvironment(environment, input.sandboxNetworkEnabled);
    }
  } catch (error) {
    throw new InvalidRuntimeBindingError(
      'dsh_profile',
      error instanceof Error ? error.message : 'Invalid DSH profile',
    );
  }
  const vaultCredentialMetadata = await loadVaultCredentials(db, workspaceId, session.vaultIds);
  const resources = await loadResources({
    db,
    fileStore,
    ...(memoryStore ? { memoryStore } : {}),
    workspaceId,
    sessionId,
  });

  const guardrailPayload = await loadGuardrails(
    db,
    workspaceId,
    session.id,
    loadedAgents,
    sessionGuardrailIds(session.agentOverrides),
    input.guardrails,
  );
  const selection = resolveHarnessAnnotation(primaryLoaded.snapshot.metadata);
  if ('error' in selection) throw new InvalidRuntimeBindingError('harness', selection.error);
  const deploymentError = validateHarnessDeployment(environment?.target ?? 'cloud', selection);
  if (deploymentError) throw new InvalidRuntimeBindingError('harness', deploymentError);
  const featureError =
    selection.harness === 'dsh_sdk'
      ? validateHarnessFeatures(primaryLoaded.snapshot.metadata, {
          ...primaryLoaded.snapshot,
          tools: primaryLoaded.snapshot.tools.filter(
            (tool): tool is AgentToolEntry =>
              typeof tool === 'object' &&
              tool !== null &&
              !Array.isArray(tool) &&
              typeof (tool as Record<string, unknown>).type === 'string',
          ),
        })
      : null;
  if (featureError) throw new InvalidRuntimeBindingError('harness', featureError);
  const guardrailError = validateHarnessGuardrails(
    selection,
    guardrailPayload.guardrails.filter((g) => !isDogwood(g.rule)),
  );
  if (guardrailError) throw new InvalidRuntimeBindingError('guardrail', guardrailError);
  let checkpointRevision: string | null = null;
  let harnessState: unknown = null;
  let ownershipRevision = 0;
  if (
    selection.harness === 'codex_sdk' ||
    selection.harness === 'pi_sdk' ||
    selection.harness === 'dsh_sdk'
  ) {
    try {
      const stored = await loadHarnessTurnState(db, workspaceId, sessionId);
      harnessState = stored.state;
      ownershipRevision = stored.ownershipRevision;
      checkpointRevision = harnessStateRevision(harnessState);
    } catch (error) {
      if (error instanceof HarnessTurnInvalidBindingError)
        throw new InvalidRuntimeBindingError('harness_state', session.id);
      throw error;
    }
  }

  const dogwoodRules = guardrailPayload.guardrails.filter((g) => isDogwood(g.rule));
  let dogwood: PreparedExecutionV2['dogwood'];
  const [previousPolicy] = await db
    .select()
    .from(guardrailPolicyBindings)
    .where(
      and(
        eq(guardrailPolicyBindings.workspaceId, workspaceId),
        eq(guardrailPolicyBindings.sessionId, sessionId),
      ),
    );
  if (dogwoodRules.length || previousPolicy) {
    if (dogwoodRules.length > 16)
      throw new InvalidRuntimeBindingError('guardrail', 'at most 16 Dogwood guardrails');
    const harness =
      selection.harness === 'claude_agent_sdk' &&
      selection.mode === 'separate' &&
      environment?.target !== 'self_hosted';
    if (dogwoodRules.length && !harness && !input.guardrails?.gatewayDogwoodEnabled)
      throw new InvalidRuntimeBindingError('guardrail', 'Dogwood requires a qualified Gateway');
    if (
      !harness &&
      dogwoodRules.some(
        (g) => (g.rule as { appliesTo?: string }).appliesTo !== 'mcp_tools' || g.subagent_id,
      )
    )
      throw new InvalidRuntimeBindingError(
        'guardrail',
        'Gateway requires unbound mcp_tools Dogwood rules',
      );
    if (
      environment?.target === 'self_hosted' &&
      (
        await db
          .select({ mode: environments.egressMode })
          .from(environments)
          .where(
            and(eq(environments.workspaceId, workspaceId), eq(environments.id, environment.id)),
          )
      )[0]?.mode === 'sidecar'
    )
      throw new InvalidRuntimeBindingError('guardrail', 'Dogwood cannot govern sidecar egress');
    const oldBinding = previousPolicy?.data as
      | import('./dogwood-types.js').PolicyBinding
      | undefined;
    const preserve =
      oldBinding && (!input.turnEventId || oldBinding.turnEventId === input.turnEventId);
    const readerRules = preserve ? oldBinding.guardrails : guardrailPayload.guardrails;
    const readerEnforcer = preserve ? oldBinding.enforcer : harness ? 'harness' : 'ai-gateway';
    try {
      assertGuardrailReaderCapabilities(
        readerEnforcer === 'ai-gateway'
          ? readerRules.filter((g) => !isDogwood(g.rule))
          : readerRules,
        input.guardrailCapabilities ?? GUARDRAIL_READER_CAPABILITIES,
      );
    } catch (error) {
      throw new InvalidRuntimeBindingError('guardrail_reader', String(error));
    }
    const policyOwner =
      input.policyOwner ?? (previousPolicy?.data as { owner?: string } | undefined)?.owner;
    if (!policyOwner)
      throw new InvalidRuntimeBindingError(
        'guardrail',
        'Dogwood preparation requires an explicit owner',
      );
    dogwood =
      !input.policyOwner && previousPolicy
        ? {
            enforcer: (previousPolicy.data as import('./dogwood-types.js').PolicyBinding).enforcer,
            epoch: previousPolicy.epoch.toString(),
            bundle_id: (previousPolicy.data as import('./dogwood-types.js').PolicyBinding).bundleId,
          }
        : await bindPolicyTurn(
            db as unknown as import('../persistence/postgres/client.js').DbTransaction,
            {
              workspaceId,
              sessionId,
              owner: policyOwner,
              guardrails: guardrailPayload.guardrails,
              enforcer: harness ? 'harness' : 'ai-gateway',
              ...(input.turnEventId ? { turnEventId: input.turnEventId } : {}),
              resources: loadedAgents.map((a) => ({ id: a.id, name: a.snapshot.name })),
              mcpServers: primaryLoaded.snapshot.mcp_servers.flatMap((m) =>
                isRecord(m) && typeof m.name === 'string' && typeof m.url === 'string'
                  ? [{ name: m.name, url: m.url }]
                  : [],
              ),
              now: input.guardrails?.now ?? (() => new Date()),
            },
          );
  }

  if (dogwood) {
    const [pinned] = await db
      .select()
      .from(guardrailPolicyBindings)
      .where(
        and(
          eq(guardrailPolicyBindings.workspaceId, workspaceId),
          eq(guardrailPolicyBindings.sessionId, sessionId),
        ),
      );
    guardrailPayload.guardrails = (
      pinned!.data as import('./dogwood-types.js').PolicyBinding
    ).guardrails;
  }
  return {
    schema_version: 2,
    workspace_id: workspaceId,
    session: {
      id: session.id,
      workspace_id: workspaceId,
      runtime_revision: session.runtimeRevision,
      status: parseSessionStatus(session.status),
      agent_id: session.agentId,
      agent_version: session.agentVersion,
      environment_id: session.environmentId,
      vault_ids: session.vaultIds,
      metadata: asRecord(session.metadata),
      usage_writer: usageWriter,
      ...(primaryLoaded.snapshot.metadata.harness === 'codex_sdk' ||
      primaryLoaded.snapshot.metadata.harness === 'pi_sdk' ||
      primaryLoaded.snapshot.metadata.harness === 'dsh_sdk'
        ? {
            harness_state: harnessState,
            harness_ownership_revision: ownershipRevision,
            harness_state_revision: checkpointRevision,
          }
        : {}),
    },
    primary_agent: primaryAgent,
    subagents,
    environment,
    vault_credentials: vaultCredentialMetadata,
    resources,
    ...(dogwood ? { dogwood } : {}),
    guardrails:
      dogwood?.enforcer === 'ai-gateway'
        ? guardrailPayload.guardrails.filter((g) => !isDogwood(g.rule))
        : guardrailPayload.guardrails,
    guardrail_state: guardrailPayload.state,
    ...(guardrailPayload.workspaceState
      ? {
          guardrail_workspace_window_state: guardrailPayload.workspaceState,
          guardrail_month_window: guardrailPayload.monthWindow!,
        }
      : {}),
    archived_thread_subpaths: await loadArchivedThreadSubpaths(db, workspaceId, session.id),
  };
}

export async function loadArchivedThreadSubpaths(
  db: DbClient,
  workspaceId: string,
  sessionId: string,
): Promise<string[]> {
  return (
    await db
      .select({ subpath: sessionThreads.subpath })
      .from(sessionThreads)
      .where(
        and(
          eq(sessionThreads.workspaceId, workspaceId),
          eq(sessionThreads.sessionId, sessionId),
          isNotNull(sessionThreads.archivedAt),
        ),
      )
  ).map((row) => row.subpath);
}

/**
 * Resolve, compile, and order the guardrails applying to this session, and
 * restore its counters.
 *
 * State travels with the prepared runtime rather than being fetched separately
 * so a respawned runner cannot briefly run with an empty snapshot — a window in
 * which a cap would read as unreached.
 */
async function loadGuardrails(
  db: DbClient,
  workspaceId: string,
  sessionId: string,
  loadedAgents: LoadedAgent[],
  sessionIds: readonly string[],
  rollout: GuardrailRollout = {},
): Promise<{
  guardrails: PreparedExecutionV2['guardrails'];
  state: Record<string, unknown>;
  workspaceState?: Record<string, unknown>;
  monthWindow?: string;
}> {
  const workspaceRow = await db
    .select({ organizationId: workspaces.organizationId })
    .from(workspaces)
    .where(eq(workspaces.id, workspaceId))
    .limit(1);
  const organizationId = workspaceRow[0]?.organizationId ?? null;

  const visibleRows = await db
    .select({
      id: guardrails.id,
      name: guardrails.name,
      enabled: guardrails.enabled,
      mode: guardrails.mode,
      phases: guardrails.phases,
      scope: guardrails.scope,
      rule: guardrails.rule,
    })
    .from(guardrails)
    .where(
      and(
        isNull(guardrails.deletedAt),
        isNull(guardrails.archivedAt),
        organizationId === null
          ? eq(guardrails.workspaceId, workspaceId)
          : or(
              eq(guardrails.workspaceId, workspaceId),
              and(
                eq(guardrails.organizationId, organizationId),
                eq(guardrails.scope, 'organization'),
              ),
            ),
      ),
    );

  const visible: GuardrailRow[] = visibleRows.map((row) => ({
    id: row.id,
    name: row.name,
    enabled: row.enabled,
    mode: row.mode,
    phases: Array.isArray(row.phases) ? (row.phases as string[]) : [],
    scope: row.scope as GuardrailRow['scope'],
    rule: row.rule,
  }));

  const [coordinator, ...subagentEntries] = loadedAgents;
  const subagentIds: Record<string, string[]> = {};
  for (const agent of subagentEntries) {
    const ids = guardrailIdsOf(agent);
    if (ids.length > 0) subagentIds[agent.id] = ids;
  }

  const composed = composeGuardrails(
    {
      sessionIds,
      agentIds: coordinator ? guardrailIdsOf(coordinator) : [],
      subagentIds,
      visible,
    },
    rollout,
  );

  if (composed.invalid.length > 0) {
    // A stored rule that no longer compiles means the type catalog moved under
    // it. Starting the session anyway would run it without a guardrail its
    // operator believes is applied, so this fails the preparation instead.
    const detail = composed.invalid
      .map((entry) => `${entry.name} (${entry.id}): ${entry.errors.join('; ')}`)
      .join(' | ');
    throw new InvalidRuntimeBindingError('guardrail', `guardrail no longer valid: ${detail}`);
  }

  const stateRows = await db
    .select({
      key: guardrailState.key,
      valueNum: guardrailState.valueNum,
      valueJson: guardrailState.valueJson,
    })
    .from(guardrailState)
    .where(
      and(eq(guardrailState.workspaceId, workspaceId), eq(guardrailState.sessionId, sessionId)),
    );

  const state: Record<string, unknown> = {};
  for (const row of stateRows) {
    state[row.key] = row.valueNum ?? row.valueJson;
  }

  const monthWindow = rollout.monthlyBudgetsEnabled
    ? utcMonthWindow((rollout.now ?? (() => new Date()))())
    : undefined;
  return {
    guardrails: composed.guardrails.map((entry) => ({
      id: entry.guardrail.id,
      name: entry.guardrail.name,
      ...(entry.guardrail.mode ? { mode: entry.guardrail.mode } : {}),
      tier: entry.tier,
      phases: [...entry.guardrail.phases],
      rule: entry.guardrail.rule,
      stateful: entry.stateful,
      ...(entry.stateScope ? { state_scope: entry.stateScope } : {}),
      ...(entry.subagentId ? { subagent_id: entry.subagentId } : {}),
    })),
    state,
    ...(rollout.monthlyBudgetsEnabled
      ? {
          monthWindow: monthWindow!,
          workspaceState: await loadGuardrailSubjectWindowState(
            db,
            workspaceId,
            WORKSPACE_BUDGET_SUBJECT,
            monthWindow!,
          ),
        }
      : {}),
  };
}

function sessionGuardrailIds(rawOverrides: unknown): string[] {
  if (!isRecord(rawOverrides) || !Array.isArray(rawOverrides.guardrailIds)) return [];
  return rawOverrides.guardrailIds.filter((value): value is string => typeof value === 'string');
}

function guardrailIdsOf(agent: LoadedAgent): string[] {
  const snapshot = agent.snapshot as { guardrail_ids?: unknown };
  return Array.isArray(snapshot.guardrail_ids)
    ? snapshot.guardrail_ids.filter((v): v is string => typeof v === 'string')
    : [];
}

async function loadAgent(
  db: DbClient,
  workspaceId: string,
  agentId: string,
  version: number,
  overrides?: { tools: unknown; mcpServers: unknown; agentOverrides: unknown },
): Promise<LoadedAgent> {
  const loaded = (await loadAgents(db, workspaceId, [{ agentId, agentVersion: version }])).get(
    `${agentId}@${version}`,
  );
  if (!loaded) throw new InvalidRuntimeBindingError('agent_version', `${agentId}@${version}`);
  return applyAgentOverrides(loaded, overrides);
}

async function loadAgents(
  db: DbClient,
  workspaceId: string,
  bindings: AgentBinding[],
): Promise<Map<string, LoadedAgent>> {
  if (bindings.length === 0) return new Map();

  const uniqueBindings = [
    ...new Map(bindings.map((binding) => [agentBindingKey(binding), binding])).values(),
  ];
  const rows = await db
    .select({
      agentId: agentVersions.agentId,
      version: agentVersions.version,
      snapshot: agentVersions.snapshot,
      harnessType: agents.harnessType,
    })
    .from(agentVersions)
    .innerJoin(
      agents,
      and(
        isNull(agents.deletedAt),
        eq(agentVersions.workspaceId, agents.workspaceId),
        eq(agentVersions.agentId, agents.id),
      ),
    )
    .where(
      and(
        eq(agents.workspaceId, workspaceId),
        eq(agentVersions.workspaceId, workspaceId),
        isNull(agents.archivedAt),
        or(
          ...uniqueBindings.map((binding) =>
            and(
              eq(agentVersions.agentId, binding.agentId),
              eq(agentVersions.version, binding.agentVersion),
            ),
          ),
        ),
      ),
    );

  const rowsByBinding = new Map(
    rows.map((row) => [
      `${row.agentId}@${row.version}`,
      loadedAgentFromSnapshot(row.agentId, row.version, row.snapshot, row.harnessType),
    ]),
  );
  for (const binding of uniqueBindings) {
    if (!rowsByBinding.has(agentBindingKey(binding))) {
      throw new InvalidRuntimeBindingError('agent_version', agentBindingKey(binding));
    }
  }
  return rowsByBinding;
}

function loadedAgentFromSnapshot(
  agentId: string,
  version: number,
  rawSnapshot: unknown,
  harnessType: string,
): LoadedAgent {
  const parsed = PreparedAgentSnapshotSchema.safeParse(rawSnapshot);
  if (!parsed.success || parsed.data.id !== agentId || parsed.data.version !== version) {
    throw new InvalidRuntimeBindingError('agent_version', `${agentId}@${version}`);
  }
  try {
    parsed.data.metadata = bindStoredHarness(
      parsed.data.metadata,
      harnessType,
      (rawSnapshot as Record<string, unknown>).harness_type,
    );
    retainedDshProfile(parsed.data);
  } catch {
    throw new InvalidRuntimeBindingError(
      'agent_version',
      `${agentId}@${version}: harness identity mismatch`,
    );
  }
  return {
    id: agentId,
    version,
    snapshot: parsed.data,
    skillRefCount: parsed.data.skills.length,
  };
}

function applyAgentOverrides(
  agent: LoadedAgent,
  overrides?: { tools: unknown; mcpServers: unknown; agentOverrides: unknown },
): LoadedAgent {
  if (!overrides) return agent;
  const storedAgentOverrides = isRecord(overrides.agentOverrides) ? overrides.agentOverrides : {};
  const modelOverride =
    isRecord(storedAgentOverrides.model) &&
    typeof storedAgentOverrides.model.provider === 'string' &&
    typeof storedAgentOverrides.model.id === 'string'
      ? (storedAgentOverrides.model as PreparedAgentSnapshot['model'])
      : agent.snapshot.model;
  const snapshot = {
    ...agent.snapshot,
    model: modelOverride,
    ...(Object.prototype.hasOwnProperty.call(storedAgentOverrides, 'system')
      ? {
          system:
            typeof storedAgentOverrides.system === 'string' ? storedAgentOverrides.system : '',
        }
      : {}),
    tools: overrides.tools === null ? agent.snapshot.tools : requireArray(overrides.tools, 'tools'),
    mcp_servers:
      overrides.mcpServers === null
        ? agent.snapshot.mcp_servers
        : requireArray(overrides.mcpServers, 'mcp_servers'),
    ...(Array.isArray(storedAgentOverrides.skills) ? { skills: storedAgentOverrides.skills } : {}),
  };
  return { ...agent, snapshot, skillRefCount: snapshot.skills.length };
}

function agentBindingKey(binding: AgentBinding): string {
  return `${binding.agentId}@${binding.agentVersion}`;
}

async function loadPreparedAgents(
  db: DbClient,
  workspaceId: string,
  sessionId: string,
  loadedAgents: LoadedAgent[],
): Promise<PreparedAgent[]> {
  const rows = await db
    .select({
      agentId: sessionSkillBindings.agentId,
      agentVersion: sessionSkillBindings.agentVersion,
      ordinal: sessionSkillBindings.ordinal,
      bundleSha256: sessionSkillBindings.bundleSha256,
      id: skillVersions.id,
      skillId: skillVersions.skillId,
      source: skills.type,
      versionIdentifier: skillVersions.versionIdentifier,
      name: skillVersions.name,
      description: skillVersions.description,
      entrypoint: skillVersions.entrypoint,
      packageSha256: skillVersions.packageSha256,
      packageSizeBytes: skillVersions.packageSizeBytes,
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
      and(eq(skillVersions.workspaceId, skills.workspaceId), eq(skillVersions.skillId, skills.id)),
    )
    .where(
      and(
        eq(sessionSkillBindings.workspaceId, workspaceId),
        eq(sessionSkillBindings.sessionId, sessionId),
      ),
    );

  const expectedAgents = new Map(loadedAgents.map((agent) => [agentKey(agent), agent] as const));
  const rowsByAgent = new Map<string, typeof rows>();
  const packageByName = new Map<string, string>();
  for (const row of rows) {
    const key = `${row.agentId}@${row.agentVersion}`;
    if (!expectedAgents.has(key)) {
      throw new InvalidRuntimeBindingError('session_skill_binding', key);
    }
    if (row.source !== 'anthropic' && row.source !== 'custom') {
      throw new InvalidRuntimeBindingError('skill_version', row.id);
    }
    if (
      row.entrypoint !== 'SKILL.md' ||
      !/^[a-z0-9](?:[a-z0-9-]{0,62}[a-z0-9])?$/.test(row.name) ||
      row.description.length === 0 ||
      row.versionIdentifier.length === 0 ||
      !/^[0-9a-f]{64}$/.test(row.packageSha256) ||
      row.packageSizeBytes <= 0
    ) {
      throw new InvalidRuntimeBindingError('skill_version', row.id);
    }
    const priorDigest = packageByName.get(row.name);
    if (priorDigest !== undefined && priorDigest !== row.packageSha256) {
      throw new InvalidRuntimeBindingError('skill_name', row.name);
    }
    packageByName.set(row.name, row.packageSha256);
    const agentRows = rowsByAgent.get(key) ?? [];
    agentRows.push(row);
    rowsByAgent.set(key, agentRows);
  }

  return loadedAgents.map((agent) => {
    const key = agentKey(agent);
    const agentRows = (rowsByAgent.get(key) ?? []).sort((a, b) => a.ordinal - b.ordinal);
    if (
      agentRows.length !== agent.skillRefCount ||
      agentRows.some((row, index) => row.ordinal !== index)
    ) {
      throw new InvalidRuntimeBindingError('session_skill_binding', key);
    }
    const descriptors: PreparedSkillDescriptor[] = agentRows.map((row) => ({
      id: row.id,
      skill_id: row.skillId,
      source: row.source as 'anthropic' | 'custom',
      version_identifier: row.versionIdentifier,
      name: row.name,
      description: row.description,
      entrypoint: 'SKILL.md',
      package_sha256: row.packageSha256,
      package_size_bytes: row.packageSizeBytes,
    }));
    const snapshot = { ...agent.snapshot } as Record<string, unknown>;
    delete snapshot['skills'];
    delete snapshot['resolved_skills'];
    return {
      ...snapshot,
      workspace_id: workspaceId,
      skills: descriptors,
    } as PreparedAgent;
  });
}

async function loadEnvironment(
  db: DbClient,
  workspaceId: string,
  environmentId: string,
): Promise<NonNullable<PreparedExecutionV2['environment']>> {
  // Archive blocks new session bindings, but existing sessions must keep the
  // environment resolvable for later starts and reschedules.
  const rows = await db
    .select()
    .from(environments)
    .where(
      and(
        isNull(environments.deletedAt),
        eq(environments.id, environmentId),
        eq(environments.workspaceId, workspaceId),
      ),
    )
    .limit(1);
  const row = rows[0];
  if (!row) throw new InvalidRuntimeBindingError('environment', environmentId);
  if (row.target !== null && row.target !== 'cloud' && row.target !== 'self_hosted') {
    throw new Error(`invalid environment target ${environmentId}`);
  }
  return {
    id: row.id,
    workspace_id: workspaceId,
    name: row.name,
    packages: normalizeStoredPackages(row.packages),
    networking: asRecord(row.networking),
    image: row.image,
    target: row.target,
  };
}

async function loadVaultCredentials(
  db: DbClient,
  workspaceId: string,
  vaultIds: string[],
): Promise<PreparedExecutionV2['vault_credentials']> {
  const uniqueVaultIds = [...new Set(vaultIds)];
  if (uniqueVaultIds.length === 0) return [];

  const vaultRows = await db
    .select({ id: vaults.id })
    .from(vaults)
    .where(
      and(
        isNull(vaults.deletedAt),
        eq(vaults.workspaceId, workspaceId),
        inArray(vaults.id, uniqueVaultIds),
        isNull(vaults.archivedAt),
      ),
    );
  const foundVaults = new Set(vaultRows.map((row) => row.id));
  for (const vaultId of uniqueVaultIds) {
    if (!foundVaults.has(vaultId)) throw new InvalidRuntimeBindingError('vault', vaultId);
  }

  const rows = await db
    .select()
    .from(vaultCredentials)
    .where(
      and(
        isNull(vaultCredentials.deletedAt),
        eq(vaultCredentials.workspaceId, workspaceId),
        inArray(vaultCredentials.vaultId, uniqueVaultIds),
        isNull(vaultCredentials.archivedAt),
      ),
    );
  const vaultOrder = new Map(uniqueVaultIds.map((id, index) => [id, index]));
  rows.sort((a, b) => {
    const byVault = (vaultOrder.get(a.vaultId) ?? 0) - (vaultOrder.get(b.vaultId) ?? 0);
    if (byVault !== 0) return byVault;
    return a.createdAt.getTime() - b.createdAt.getTime() || a.id.localeCompare(b.id);
  });
  return rows.map((row) => ({
    credential_id: row.id,
    vault_id: row.vaultId,
    auth_type: row.authType,
    mcp_server_url: row.mcpServerUrl,
    secret_name: row.secretName,
    networking: row.networking,
  }));
}

async function loadResources(input: {
  db: DbClient;
  fileStore: FileStore;
  memoryStore?: MemoryStore;
  workspaceId: string;
  sessionId: string;
}): Promise<PreparedExecutionV2['resources']> {
  const { db, fileStore, memoryStore, workspaceId, sessionId } = input;
  const rows = await db
    .select()
    .from(sessionResources)
    .where(
      and(
        isNull(sessionResources.deletedAt),
        eq(sessionResources.workspaceId, workspaceId),
        eq(sessionResources.sessionId, sessionId),
        isNull(sessionResources.detachedAt),
      ),
    );

  const repoRefs = new Map<string, ParsedRepoRef>();
  for (const row of rows) {
    if (row.type !== 'github_repository') continue;
    const repo = parseRepoRef(row.repoRef);
    if (!repo) throw new InvalidRuntimeBindingError('session_resource', row.id);
    repoRefs.set(row.id, repo);
  }

  const gitCredentialIds = [...new Set([...repoRefs.values()].map((ref) => ref.gitCredentialId))];
  const gitById = new Map<
    string,
    { id: string; repoUrl: string; sessionResourceId: string | null }
  >();
  if (gitCredentialIds.length > 0) {
    const gitRows = await db
      .select({
        id: gitCredentials.id,
        repoUrl: gitCredentials.repoUrl,
        sessionResourceId: gitCredentials.sessionResourceId,
      })
      .from(gitCredentials)
      .where(
        and(
          isNull(gitCredentials.deletedAt),
          eq(gitCredentials.workspaceId, workspaceId),
          inArray(gitCredentials.id, gitCredentialIds),
          isNull(gitCredentials.archivedAt),
        ),
      );
    for (const row of gitRows) gitById.set(row.id, row);
  }

  const prepared: PreparedExecutionV2['resources'] = [];
  for (const row of rows) {
    if (row.access !== 'read_only' && row.access !== 'read_write') {
      throw new InvalidRuntimeBindingError('session_resource', row.id);
    }
    const access: 'read_only' | 'read_write' = row.access;
    const common = {
      id: row.id,
      file_id: row.fileId,
      memory_store_id: row.memoryStoreId,
      repo_ref: null,
      mount_path: row.mountPath,
      access,
      mount_strategy: row.mountStrategy,
      instructions: row.instructions,
      attached_at: row.attachedAt.toISOString(),
      detached_at: null,
    };
    if (row.type === 'file') {
      if (!row.fileId) throw new InvalidRuntimeBindingError('session_resource', row.id);
      const file = await fileStore.get(workspaceId, row.fileId);
      if (!file || file.archivedAt !== null) {
        throw new InvalidRuntimeBindingError('file', row.fileId);
      }
      prepared.push({
        ...common,
        type: 'file',
        file_id: row.fileId,
        file: {
          filename: file.filename,
          mime_type: file.mimeType,
          size_bytes: file.sizeBytes,
          sha256: file.sha256,
          purpose: file.purpose,
        },
      });
      continue;
    }
    if (row.type === 'memory_store') {
      if (!row.memoryStoreId) throw new InvalidRuntimeBindingError('session_resource', row.id);
      if (!memoryStore) throw new RuntimeDependencyUnavailableError('memory_store');
      const store = await memoryStore.getStore(workspaceId, row.memoryStoreId);
      if (!store || store.archivedAt !== null) {
        throw new InvalidRuntimeBindingError('memory_store', row.memoryStoreId);
      }
      prepared.push({
        ...common,
        type: 'memory_store',
        memory_store_id: row.memoryStoreId,
        memory_store: { id: store.id, workspace_id: workspaceId, name: store.name },
      });
      continue;
    }
    if (row.type === 'github_repository') {
      const repo = repoRefs.get(row.id)!;
      const credential = gitById.get(repo.gitCredentialId);
      if (
        !credential ||
        !repositoryUrlsEqual(credential.repoUrl, repo.url) ||
        (credential.sessionResourceId !== null && credential.sessionResourceId !== row.id)
      ) {
        throw new InvalidRuntimeBindingError('git_credential', repo.gitCredentialId);
      }
      prepared.push({
        ...common,
        type: 'github_repository',
        repo_ref: {
          url: repo.url,
          git_credential_id: repo.gitCredentialId,
          ...(repo.checkout ? { checkout: repo.checkout } : {}),
        },
      });
      continue;
    }
    throw new InvalidRuntimeBindingError('session_resource', row.id);
  }
  return prepared;
}

interface ParsedRepoRef {
  gitCredentialId: string;
  url: string;
  checkout?: Record<string, unknown>;
}

function parseRepoRef(value: unknown): ParsedRepoRef | null {
  if (!isRecord(value)) return null;
  if (typeof value.git_credential_id !== 'string' || typeof value.url !== 'string') return null;
  try {
    new URL(value.url);
  } catch {
    return null;
  }
  return {
    gitCredentialId: value.git_credential_id,
    url: value.url,
    ...(isRecord(value.checkout) ? { checkout: value.checkout } : {}),
  };
}

function parseSessionStatus(value: string): SessionStatus {
  if (
    value === 'idle' ||
    value === 'running' ||
    value === 'rescheduling' ||
    value === 'terminated'
  ) {
    return value;
  }
  throw new Error(`invalid session status ${value}`);
}

function requireArray(value: unknown, field: string): unknown[] {
  if (Array.isArray(value)) return value;
  throw new Error(`invalid session ${field} override`);
}

function asRecord(value: unknown): Record<string, unknown> {
  return isRecord(value) ? value : {};
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function agentKey(agent: { id: string; version: number }): string {
  return `${agent.id}@${agent.version}`;
}
