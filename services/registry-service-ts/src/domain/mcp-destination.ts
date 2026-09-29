// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { and, eq, inArray, isNull } from 'drizzle-orm';
import { createHash } from 'node:crypto';
import type { DbClient } from '../persistence/postgres/client.js';
import {
  agents,
  agentVersions,
  sessions,
  vaultCredentials,
  vaults,
  workspaces,
} from '../persistence/postgres/schema.js';
import { InvalidRuntimeBindingError, PreparedExecutionNotFoundError } from './prepare-execution.js';

export interface ResolvedMcpDestination {
  url: string;
  credential_id: string | null;
  revision: number;
}

export interface CredentialCandidate {
  id: string;
  vaultId: string;
  createdAt: Date;
}

const MAX_SAFE_REVISION_MASK = (1n << 53n) - 1n;

/** Anthropic-compatible URL identity used only for credential binding. */
export function canonicalMcpServerUrl(value: string): string | null {
  try {
    const parsed = new URL(value);
    if (
      (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') ||
      parsed.hostname === '' ||
      parsed.username !== '' ||
      parsed.password !== '' ||
      parsed.hash !== ''
    ) {
      return null;
    }
    const path = parsed.pathname.replace(/\/+$/u, '');
    return `${parsed.protocol}//${parsed.host}${path}${parsed.search}`;
  } catch {
    return null;
  }
}

/** Stable, positive JSON-safe namespace for one authoritative binding tuple. */
export function mcpDestinationRevision(url: string, credentialId: string | null): number {
  const digest = createHash('sha256')
    .update('orca-managed-agents:mcp-destination-binding:v1\0')
    .update(JSON.stringify([url, credentialId]))
    .digest();
  const revision = digest.readBigUInt64BE(0) & MAX_SAFE_REVISION_MASK;
  return Number(revision === 0n ? 1n : revision);
}

export function compareCredentialCandidates(
  left: CredentialCandidate,
  right: CredentialCandidate,
  vaultOrder: ReadonlyMap<string, number>,
): number {
  const byVault = vaultOrder.get(left.vaultId)! - vaultOrder.get(right.vaultId)!;
  if (byVault !== 0) return byVault;
  const leftCreatedAt = left.createdAt.getTime();
  const rightCreatedAt = right.createdAt.getTime();
  if (leftCreatedAt !== rightCreatedAt) return leftCreatedAt < rightCreatedAt ? -1 : 1;
  return left.id === right.id ? 0 : left.id < right.id ? -1 : 1;
}

/** Resolve routing and credential metadata from one immutable Registry view. */
export async function resolveMcpDestination(input: {
  db: DbClient;
  workspaceId: string;
  sessionId: string;
  backend: string;
}): Promise<ResolvedMcpDestination> {
  return input.db.transaction(
    async (tx) =>
      buildMcpDestination({
        ...input,
        db: tx as unknown as DbClient,
      }),
    { isolationLevel: 'repeatable read' },
  );
}

async function buildMcpDestination(input: {
  db: DbClient;
  workspaceId: string;
  sessionId: string;
  backend: string;
}): Promise<ResolvedMcpDestination> {
  const { db, workspaceId, sessionId, backend } = input;
  const workspace = (
    await db
      .select({ status: workspaces.status })
      .from(workspaces)
      .where(eq(workspaces.id, workspaceId))
      .limit(1)
  )[0];
  if (workspace?.status !== 'active') throw new PreparedExecutionNotFoundError();

  const session = (
    await db
      .select()
      .from(sessions)
      .where(
        and(
          isNull(sessions.deletedAt),
          eq(sessions.workspaceId, workspaceId),
          eq(sessions.id, sessionId),
        ),
      )
      .limit(1)
  )[0];
  if (!session || session.archivedAt !== null || session.status === 'terminated') {
    throw new PreparedExecutionNotFoundError();
  }
  if (!Number.isSafeInteger(session.runtimeRevision) || session.runtimeRevision <= 0) {
    throw new InvalidRuntimeBindingError('session_revision', session.id);
  }

  const agent = (
    await db
      .select({ archivedAt: agents.archivedAt })
      .from(agents)
      .where(
        and(
          isNull(agents.deletedAt),
          eq(agents.workspaceId, workspaceId),
          eq(agents.id, session.agentId),
        ),
      )
      .limit(1)
  )[0];
  const version = (
    await db
      .select({ snapshot: agentVersions.snapshot })
      .from(agentVersions)
      .where(
        and(
          eq(agentVersions.workspaceId, workspaceId),
          eq(agentVersions.agentId, session.agentId),
          eq(agentVersions.version, session.agentVersion),
        ),
      )
      .limit(1)
  )[0];
  if (!agent || agent.archivedAt !== null || !version) {
    throw new InvalidRuntimeBindingError(
      'agent_version',
      `${session.agentId}@${session.agentVersion}`,
    );
  }

  const snapshot = version.snapshot;
  const persistedServers =
    session.mcpServers ??
    (snapshot !== null && typeof snapshot === 'object' && !Array.isArray(snapshot)
      ? (snapshot as Record<string, unknown>)['mcp_servers']
      : undefined);
  const servers = parseMcpServers(persistedServers);

  // Harness rewrite assigns into an object by name while iterating, so a
  // later duplicate logical backend replaces an earlier one.
  let destinationUrl: string | null = null;
  for (const server of servers) {
    if (server.name === backend) destinationUrl = server.url;
  }
  if (destinationUrl === null) throw new PreparedExecutionNotFoundError();

  const uniqueVaultIds = [...new Set(session.vaultIds)];
  if (uniqueVaultIds.length === 0) {
    return {
      url: destinationUrl,
      credential_id: null,
      revision: mcpDestinationRevision(destinationUrl, null),
    };
  }
  const activeVaultRows = await db
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
  const activeVaultIds = new Set(activeVaultRows.map(({ id }) => id));
  for (const vaultId of uniqueVaultIds) {
    if (!activeVaultIds.has(vaultId)) throw new InvalidRuntimeBindingError('vault', vaultId);
  }

  const credentials = await db
    .select({
      id: vaultCredentials.id,
      vaultId: vaultCredentials.vaultId,
      createdAt: vaultCredentials.createdAt,
      mcpServerUrl: vaultCredentials.mcpServerUrl,
    })
    .from(vaultCredentials)
    .where(
      and(
        isNull(vaultCredentials.deletedAt),
        eq(vaultCredentials.workspaceId, workspaceId),
        inArray(vaultCredentials.vaultId, uniqueVaultIds),
        isNull(vaultCredentials.archivedAt),
      ),
    );
  const canonicalDestinationUrl = canonicalMcpServerUrl(destinationUrl)!;
  const matchingCredentials = credentials.filter(
    (credential) =>
      credential.mcpServerUrl !== null &&
      canonicalMcpServerUrl(credential.mcpServerUrl) === canonicalDestinationUrl,
  );
  const matchingVaultIds = new Set<string>();
  for (const credential of matchingCredentials) {
    if (matchingVaultIds.has(credential.vaultId)) {
      throw new InvalidRuntimeBindingError('vault_credential_binding', credential.vaultId);
    }
    matchingVaultIds.add(credential.vaultId);
  }
  const vaultOrder = new Map(uniqueVaultIds.map((id, index) => [id, index]));
  matchingCredentials.sort((left, right) => compareCredentialCandidates(left, right, vaultOrder));
  const credentialId = matchingCredentials[0]?.id ?? null;
  return {
    url: destinationUrl,
    credential_id: credentialId,
    revision: mcpDestinationRevision(destinationUrl, credentialId),
  };
}

function parseMcpServers(value: unknown): Array<{ name: string; url: string }> {
  if (!Array.isArray(value)) {
    throw new InvalidRuntimeBindingError('mcp_server', 'configuration');
  }
  return value.map((entry, index) => {
    if (entry === null || typeof entry !== 'object' || Array.isArray(entry)) {
      throw new InvalidRuntimeBindingError('mcp_server', String(index));
    }
    const { name, url } = entry as Record<string, unknown>;
    const resourceId = typeof name === 'string' && name.length > 0 ? name : String(index);
    if (typeof name !== 'string' || name.length === 0 || typeof url !== 'string') {
      throw new InvalidRuntimeBindingError('mcp_server', resourceId);
    }
    try {
      const parsed = new URL(url);
      if (
        (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') ||
        parsed.hostname === '' ||
        parsed.username !== '' ||
        parsed.password !== '' ||
        parsed.hash !== ''
      ) {
        throw new Error('unsupported MCP URL');
      }
    } catch {
      throw new InvalidRuntimeBindingError('mcp_server', resourceId);
    }
    return { name, url };
  });
}
