// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { and, eq, isNull, ne } from 'drizzle-orm';
import { TunnelTransport } from '@orca/harness-tunnel';
import type { SessionJwtMinter } from '../auth/session-jwt.js';
import { mintGitProxyCapability } from './git-proxy-capability.js';
import type { FileStore } from '@orca/file-store';
import type { MemoryStore } from '@orca/memory-store';
import type { TranscriptStore } from '@orca/transcript-store';
import type { DbClient } from '../persistence/postgres/client.js';
import { sessions } from '../persistence/postgres/schema.js';
import type { ManagedRunnerResourcesFactory } from '../tunnel/session-event-bridge.js';
import { RUNNER_SESSION_HEADER } from '../tunnel/session-event-bridge.js';
import { SessionResourcesDelivery } from '../tunnel/session-resources-delivery.js';
import { loadSessionExecutionOwner, loadSessionHarnessBinding } from './session-harness-binding.js';
import { prepareExecution } from './prepare-execution.js';
import {
  prepareRunnerGitSnapshot,
  prepareRunnerResources,
  runnerResourceRevision,
} from './runner-resources.js';

export function buildRunnerResourcesFactory(deps: {
  db: DbClient;
  fileStore: FileStore;
  memoryStore?: MemoryStore;
  store: TranscriptStore;
  jwtMinter: SessionJwtMinter;
  registryBaseUrl?: string;
  registryLocalBaseUrl?: string;
  gatewayRegistryUsageEnabled?: boolean;
}): ManagedRunnerResourcesFactory {
  return async (context) => {
    const { workspaceId, sessionId, runnerId, registry } = context;
    const [session] = await deps.db
      .select({ agentId: sessions.agentId, agentVersion: sessions.agentVersion })
      .from(sessions)
      .where(
        and(
          eq(sessions.workspaceId, workspaceId),
          eq(sessions.id, sessionId),
          isNull(sessions.deletedAt),
        ),
      )
      .limit(1);
    if (!session) throw new Error('runner session no longer exists');
    const selection = await loadSessionHarnessBinding(
      deps.db,
      workspaceId,
      session.agentId,
      session.agentVersion,
    );
    if (
      (selection.harness !== 'codex_sdk' && selection.harness !== 'pi_sdk') ||
      selection.mode !== 'colocated'
    )
      return null;
    if (!(await runnerResourceBindingIsCurrent(deps.db, workspaceId, sessionId, runnerId)))
      throw new Error('runner session binding is no longer assigned');
    const prepare = () =>
      prepareExecution({
        db: deps.db,
        fileStore: deps.fileStore,
        workspaceId,
        sessionId,
        ...(deps.memoryStore ? { memoryStore: deps.memoryStore } : {}),
        ...(deps.gatewayRegistryUsageEnabled !== undefined
          ? { gatewayRegistryUsageEnabled: deps.gatewayRegistryUsageEnabled }
          : {}),
      });
    const transport = new TunnelTransport(registry, runnerId);
    const additionalNetworkDomains = deps.registryBaseUrl
      ? [new URL(deps.registryBaseUrl).hostname]
      : [];
    return new SessionResourcesDelivery({
      workspaceId,
      sessionId,
      fileStore: deps.fileStore,
      store: deps.store,
      ...(deps.memoryStore ? { memoryStore: deps.memoryStore } : {}),
      prepare: async (retainedManifest) =>
        prepareRunnerResources(await prepare(), {
          fileStore: deps.fileStore,
          ...(retainedManifest ? { retainedManifest } : {}),
          additionalNetworkDomains,
          ...(deps.memoryStore ? { memoryStore: deps.memoryStore } : {}),
          gitCapability: async (resource) => {
            if (!deps.registryBaseUrl) throw new Error('Git proxy public URL is not configured');
            const grant = await mintGitProxyCapability(deps.db, deps.jwtMinter, {
              registryBaseUrl: deps.registryBaseUrl,
              workspaceId,
              sessionId,
              resourceId: resource.id,
            });
            return {
              resource_id: resource.id,
              remote_url: grant.remoteUrl,
              authorization_header: grant.authorizationHeader,
              expires_at: grant.expiresAt,
            };
          },
          gitSnapshot: async (resource, capability) => {
            if (!deps.registryBaseUrl || !capability)
              throw new Error('Git proxy is not configured');
            return prepareRunnerGitSnapshot({
              url: resource.repo_ref.url,
              ...(resource.repo_ref.checkout ? { checkout: resource.repo_ref.checkout } : {}),
              source: {
                url: new URL(
                  `/v1/git-proxy/${resource.id}`,
                  deps.registryLocalBaseUrl ?? deps.registryBaseUrl,
                ).href,
                authorizationHeader: capability.authorization_header,
              },
              remoteUrl: capability.remote_url,
              proxyResourceId: resource.id,
            });
          },
        }),
      isCurrent: async (revision) => {
        if (
          !context.isCurrent() ||
          !(await runnerResourceBindingIsCurrent(deps.db, workspaceId, sessionId, runnerId)) ||
          (await loadSessionExecutionOwner(deps.db, workspaceId, sessionId)) !== 'registry'
        )
          return false;
        const prepared = await prepare();
        return (
          context.isCurrent() &&
          runnerResourceRevision(prepared, additionalNetworkDomains) === revision
        );
      },
      request: async (path, body) => {
        if (!context.isCurrent()) throw new Error('runner generation retired');
        const response = await transport.handleRequest({
          method: 'POST',
          path,
          headers: [[RUNNER_SESSION_HEADER, sessionId]],
          contentType: 'application/json',
          body: Buffer.from(JSON.stringify(body)),
          signal: AbortSignal.timeout(60_000),
        });
        const chunks: Buffer[] = [];
        let size = 0;
        try {
          for await (const chunk of response.stream) {
            size += chunk.length;
            // A 10k-entry checkpoint manifest is bounded independently from its file bytes.
            if (size > 8 * 1024 * 1024) throw new Error('runner resource response exceeds limit');
            chunks.push(Buffer.from(chunk));
          }
        } finally {
          await (response.stream as { close?: () => Promise<void> }).close?.();
        }
        if (!context.isCurrent()) throw new Error('runner generation retired');
        if (response.status < 200 || response.status >= 300)
          throw new Error('runner rejected resource request');
        return JSON.parse(Buffer.concat(chunks).toString('utf8')) as unknown;
      },
    });
  };
}

/** Topology ownership alone does not authorize a superseded runner binding. */
export async function runnerResourceBindingIsCurrent(
  db: DbClient,
  workspaceId: string,
  sessionId: string,
  runnerId: string,
): Promise<boolean> {
  const [row] = await db
    .select({ id: sessions.id })
    .from(sessions)
    .where(
      and(
        eq(sessions.workspaceId, workspaceId),
        eq(sessions.id, sessionId),
        eq(sessions.runnerId, runnerId),
        eq(sessions.distributionState, 'assigned'),
        isNull(sessions.deletedAt),
        isNull(sessions.archivedAt),
        ne(sessions.status, 'terminated'),
      ),
    )
    .limit(1);
  return row !== undefined;
}
