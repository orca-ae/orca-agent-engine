// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Event } from '@orca/transcript-store';
import { validateHarnessDeployment, type ResolvedHarness } from '@orca/harness-catalog';
import { and, eq, inArray, isNull } from 'drizzle-orm';
import type { DbClient, DbTransaction } from '../persistence/postgres/client.js';
import {
  agents,
  agentVersions,
  environments,
  gitCredentials,
  gitCredentialStagingIntents,
  sessionLifecycleOutbox,
  sessionObservabilityBindings,
  sessionResources,
  sessions,
  sessionSkillBindings,
  sessionThreads,
  vaults,
} from '../persistence/postgres/schema.js';
import {
  selectSessionObservabilityBindingInTransaction,
  type SessionObservabilitySelection,
} from './agent-observability-session-selection.js';
import { strictAgentVersionConfiguration } from './agent-version-configuration.js';
import { newSessionInitialEventsOutboxRow } from './session-lifecycle-outbox.js';
import { sessionLlmEgressMetadataError } from './session-llm-egress.js';
import { resolveSessionSkillBindings } from './session-skill-bindings.js';
import { stableSessionThreadId } from './thread-projection.js';

type SessionResourceInsert = typeof sessionResources.$inferInsert;
type GitCredentialInsert = typeof gitCredentials.$inferInsert;
type SessionObservabilityBindingInsert = typeof sessionObservabilityBindings.$inferInsert;

export interface CreateSessionInTransactionInput {
  id: string;
  workspaceId: string;
  agentId: string;
  agentVersion: number;
  environmentId: string;
  title: string | null;
  metadata: Record<string, string>;
  vaultIds: string[];
  tools: unknown[] | null;
  mcpServers: unknown[] | null;
  agentOverrides: Record<string, unknown> | null;
  primarySkillRefsOverride?: unknown[];
  resourceRows?: SessionResourceInsert[];
  gitCredentialRows?: GitCredentialInsert[];
  stagedGitCredentialIds?: string[];
  initialEvents: Event[];
  now: Date;
}

export type CreateSessionInTransactionResult =
  | { ok: true; initialEventsOutboxId: string | null }
  | {
      ok: false;
      reason: 'agent' | 'environment' | 'vault' | 'metadata' | 'deployment';
      message: string;
    };

export { SessionAgentSnapshotUnavailableError } from './agent-version-configuration.js';

/**
 * Materialize every non-secret authority fact needed by a new Session pin.
 * Selection has already locked the observability authority rows, while
 * `harness` comes from the locked immutable Agent-version snapshot.
 */
export function newSessionObservabilityBindingRow(
  input: Pick<
    CreateSessionInTransactionInput,
    'id' | 'workspaceId' | 'agentId' | 'agentVersion' | 'now'
  >,
  selection: SessionObservabilitySelection,
  harness: ResolvedHarness,
): SessionObservabilityBindingInsert {
  return {
    workspaceId: input.workspaceId,
    sessionId: input.id,
    organizationId: selection.organizationId,
    bindingId: selection.bindingId,
    bindingVersion: selection.bindingVersion,
    bindingScope: selection.bindingScope,
    bindingWorkspaceId: selection.bindingWorkspaceId,
    selectionSource: selection.selectionSource,
    status: selection.status,
    organizationSelectionEpoch: selection.organizationSelectionEpoch,
    workspaceSelectionEpoch: selection.workspaceSelectionEpoch,
    organizationDefaultRevocationEpoch: selection.organizationDefaultRevocationEpoch,
    organizationRevocationEpoch: selection.organizationRevocationEpoch,
    workspaceRevocationEpoch: selection.workspaceRevocationEpoch,
    bindingRevocationEpoch: selection.bindingRevocationEpoch,
    platformCaptureRestrictionEpoch: selection.platformCaptureRestrictionEpoch,
    organizationCaptureRestrictionEpoch: selection.organizationCaptureRestrictionEpoch,
    workspaceCaptureRestrictionEpoch: selection.workspaceCaptureRestrictionEpoch,
    effectiveCaptureMode: selection.effectiveCaptureMode,
    sessionRevocationEpoch: 0,
    agentId: input.agentId,
    agentVersion: input.agentVersion,
    harness: harness.harness,
    harnessMode: harness.mode,
    archivedAt: null,
    deletedAt: null,
    createdAt: input.now,
    updatedAt: input.now,
  };
}

/**
 * Create the complete Registry-side Session graph inside the caller's
 * transaction. The caller decides what else commits atomically with it (HTTP
 * create uses only this graph; Trigger dispatch also links its fire row).
 *
 * This function performs database work only. Transcript publication happens
 * after commit through the existing Session lifecycle outbox.
 */
export async function createSessionInTransaction(
  tx: DbTransaction,
  input: CreateSessionInTransactionInput,
): Promise<CreateSessionInTransactionResult> {
  const llmEgressError = sessionLlmEgressMetadataError(input.metadata);
  if (llmEgressError) return { ok: false, reason: 'metadata', message: llmEgressError };
  // This must precede every later Session-create lock (Agent, Environment,
  // Vault, Skill) and every graph write. The selector owns the authority lock
  // order that fences concurrent observability configuration changes.
  const observabilitySelection = await selectSessionObservabilityBindingInTransaction(
    tx,
    input.workspaceId,
  );

  const [agent] = await tx
    .select({ name: agents.name, snapshot: agentVersions.snapshot })
    .from(agents)
    .innerJoin(
      agentVersions,
      and(
        eq(agentVersions.workspaceId, agents.workspaceId),
        eq(agentVersions.agentId, agents.id),
        eq(agentVersions.version, input.agentVersion),
      ),
    )
    .where(
      and(
        isNull(agents.deletedAt),
        eq(agents.workspaceId, input.workspaceId),
        eq(agents.id, input.agentId),
        isNull(agents.archivedAt),
      ),
    )
    .for('share')
    .limit(1);
  if (!agent) {
    return {
      ok: false,
      reason: 'agent',
      message: `agent ${input.agentId} version ${input.agentVersion} not found`,
    };
  }

  const agentConfiguration = strictAgentVersionConfiguration(
    agent.snapshot,
    input.agentId,
    input.agentVersion,
  );
  const harness = agentConfiguration.harness;

  const [environment] = await tx
    .select({ id: environments.id, target: environments.target })
    .from(environments)
    .where(
      and(
        isNull(environments.deletedAt),
        eq(environments.workspaceId, input.workspaceId),
        eq(environments.id, input.environmentId),
        isNull(environments.archivedAt),
      ),
    )
    .for('share')
    .limit(1);
  if (!environment) {
    return { ok: false, reason: 'environment', message: 'environment not found' };
  }
  const deploymentError = validateHarnessDeployment(environment.target ?? 'cloud', harness);
  if (deploymentError) return { ok: false, reason: 'deployment', message: deploymentError };

  const vaultError = await validateSessionVaultIds(tx, input.workspaceId, input.vaultIds, true);
  if (vaultError) return { ok: false, reason: 'vault', message: vaultError };

  // Resolve every fallible, user-controlled reference before the first write.
  // Trigger dispatch may record a permanent Skill/template failure in this
  // transaction, so it must not leave a Session pin or graph fragment behind.
  const skillBindings = await resolveSessionSkillBindings(tx, {
    workspaceId: input.workspaceId,
    sessionId: input.id,
    primaryAgentId: input.agentId,
    primaryAgentVersion: input.agentVersion,
    ...(input.primarySkillRefsOverride !== undefined
      ? { primarySkillRefsOverride: input.primarySkillRefsOverride }
      : {}),
  });

  const hasInitialEvents = input.initialEvents.length > 0;
  await tx
    .insert(sessionObservabilityBindings)
    .values(newSessionObservabilityBindingRow(input, observabilitySelection, harness));
  await tx.insert(sessions).values({
    id: input.id,
    workspaceId: input.workspaceId,
    agentId: input.agentId,
    agentVersion: input.agentVersion,
    runtimeRevision: 1,
    title: input.title,
    metadata: input.metadata,
    environmentId: input.environmentId,
    vaultIds: input.vaultIds,
    tools: input.tools,
    mcpServers: input.mcpServers,
    agentOverrides: input.agentOverrides,
    status: hasInitialEvents ? 'running' : 'idle',
    lastEventSeq: 0,
    sandboxHandleId: null,
    activeSeconds: 0,
    usageInputTokens: 0,
    usageOutputTokens: 0,
    usageCacheReadInputTokens: 0,
    usageCacheCreationEphemeral1hInputTokens: 0,
    usageCacheCreationEphemeral5mInputTokens: 0,
    startedAt: hasInitialEvents ? input.now : null,
    lastActiveAt: hasInitialEvents ? input.now : null,
    archivedAt: null,
    createdAt: input.now,
    updatedAt: input.now,
  });

  if (skillBindings.length > 0) await tx.insert(sessionSkillBindings).values(skillBindings);

  await tx.insert(sessionThreads).values({
    id: stableSessionThreadId(input.workspaceId, input.id, ''),
    workspaceId: input.workspaceId,
    sessionId: input.id,
    subpath: '',
    agentId: input.agentId,
    agentVersion: input.agentVersion,
    agentName: agent.name,
    parentThreadId: null,
    status: 'idle',
    stopReason: null,
    archivedAt: null,
    createdAt: input.now,
    updatedAt: input.now,
  });

  if (input.resourceRows && input.resourceRows.length > 0) {
    await tx.insert(sessionResources).values(input.resourceRows);
  }
  if (input.gitCredentialRows && input.gitCredentialRows.length > 0) {
    await tx.insert(gitCredentials).values(input.gitCredentialRows);
  }

  if (input.stagedGitCredentialIds && input.stagedGitCredentialIds.length > 0) {
    const consumed = await tx
      .delete(gitCredentialStagingIntents)
      .where(
        and(
          eq(gitCredentialStagingIntents.workspaceId, input.workspaceId),
          inArray(gitCredentialStagingIntents.credentialId, input.stagedGitCredentialIds),
          eq(gitCredentialStagingIntents.status, 'pending'),
        ),
      )
      .returning({ credentialId: gitCredentialStagingIntents.credentialId });
    if (consumed.length !== input.stagedGitCredentialIds.length) {
      throw new Error('git credential staging intent expired or missing');
    }
  }

  let initialEventsOutboxId: string | null = null;
  if (hasInitialEvents) {
    const outbox = newSessionInitialEventsOutboxRow(
      input.workspaceId,
      input.id,
      input.initialEvents,
      input.now,
    );
    initialEventsOutboxId = outbox.id;
    await tx.insert(sessionLifecycleOutbox).values(outbox);
  }

  return { ok: true, initialEventsOutboxId };
}

export async function validateSessionVaultIds(
  db: Pick<DbClient, 'select'>,
  workspaceId: string,
  vaultIds: string[],
  lock = false,
): Promise<string | null> {
  const uniqueVaultIds = [...new Set(vaultIds)];
  if (uniqueVaultIds.length === 0) return null;

  const query = db
    .select({ id: vaults.id, archivedAt: vaults.archivedAt })
    .from(vaults)
    .where(
      and(
        isNull(vaults.deletedAt),
        eq(vaults.workspaceId, workspaceId),
        inArray(vaults.id, uniqueVaultIds),
      ),
    );
  const rows = lock ? await query.for('share') : await query;
  const byId = new Map(rows.map((row) => [row.id, row]));

  for (const vaultId of uniqueVaultIds) {
    const row = byId.get(vaultId);
    if (!row) return `vault_id ${vaultId} not found in workspace`;
    if (row.archivedAt !== null) return `vault_id ${vaultId} is archived`;
  }
  return null;
}
