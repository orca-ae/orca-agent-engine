// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { and, eq, isNull, ne } from 'drizzle-orm';
import { TunnelTransport } from '@orca/harness-tunnel';
import { NATIVE_USAGE_SOURCE, supportsNativeFileCompletion } from '@orca/sandbox-runtime';
import type { SnapshotProvider } from '../tunnel/session-snapshot-delivery.js';
import { NativeCompletionCoordinator, hasNativeUsageAuthority } from './native-completion.js';
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
  snapshotProvider?: SnapshotProvider;
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
    const native = selection.harness === 'pi' && selection.mode === 'colocated';
    if (
      (!native && selection.harness !== 'codex_sdk' && selection.harness !== 'pi_sdk') ||
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
    const request = async (path: string, body: unknown): Promise<unknown> => {
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
    };
    if (native) {
      const snapshot = await deps.snapshotProvider?.resolve(sessionId);
      const retainedAuthority = await hasNativeUsageAuthority(deps.db, workspaceId, sessionId);
      if (!snapshot || !supportsNativeFileCompletion(snapshot)) {
        if (retainedAuthority)
          throw new Error('Native usage authority cannot downgrade to an unsupported tool surface');
        return null;
      }
      const statusValue = await request('/v1/runner/resources', { type: 'status' });
      const status = nativeStatus(statusValue);
      if (!status) {
        if (retainedAuthority) throw new Error('Native completion runner cannot be downgraded');
        return null;
      }
      const isCurrent = async (revision: string) =>
        context.isCurrent() &&
        (await runnerResourceBindingIsCurrent(deps.db, workspaceId, sessionId, runnerId)) &&
        (await loadSessionExecutionOwner(deps.db, workspaceId, sessionId)) === 'registry' &&
        runnerResourceRevision(await prepare()) === revision;
      const coordinator = new NativeCompletionCoordinator({
        db: deps.db,
        workspaceId,
        sessionId,
        runnerId,
        store: deps.store,
        isCurrent,
      });
      return new SessionResourcesDelivery({
        workspaceId,
        sessionId,
        fileStore: deps.fileStore,
        store: deps.store,
        isCurrent,
        request,
        native: {
          coordinator,
          probe: async () => {
            const status = nativeStatus(await request('/v1/runner/resources', { type: 'status' }));
            if (!status) throw new Error('Native completion capability disappeared');
            return status;
          },
        },
        prepare: async () => {
          const currentSnapshot = await deps.snapshotProvider?.resolve(sessionId);
          if (!currentSnapshot || !supportsNativeFileCompletion(currentSnapshot))
            throw new Error('Native file-only surface changed');
          const prepared = await prepare();
          const revision = runnerResourceRevision(prepared);
          const files = await prepareRunnerResources(
            {
              ...prepared,
              resources: prepared.resources.filter((resource) => resource.type === 'file'),
            },
            {
              fileStore: deps.fileStore,
              gitSnapshot: async () => {
                throw new Error('Native Git placement is unsupported');
              },
            },
          );
          return {
            ...files,
            manifest: { ...files.manifest, revision },
            networkAllowedDomains: [],
            networkUnrestricted: false,
          };
        },
      });
    }
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
      request,
    });
  };
}

function nativeStatus(value: unknown): {
  executionId: string;
  placementId?: string;
  revision?: string;
  resolvedPaths?: Record<string, string>;
  outputs?: string;
  turn?: { turnId: string; phase: string; checkpointId?: string; digest?: string };
} | null {
  if (!value || typeof value !== 'object' || Array.isArray(value))
    throw new Error('Malformed runner status');
  const status = value as Record<string, unknown>;
  if (
    status.supported_placements === undefined ||
    (Array.isArray(status.supported_placements) &&
      !status.supported_placements.includes('session_sandbox'))
  )
    return null;
  if (
    !Array.isArray(status.supported_placements) ||
    status.usage_source !== NATIVE_USAGE_SOURCE ||
    status.completion_version !== 1 ||
    typeof status.execution_id !== 'string' ||
    !/^[A-Za-z0-9_-]{1,128}$/.test(status.execution_id)
  )
    throw new Error('Invalid native capability');
  const result: NonNullable<ReturnType<typeof nativeStatus>> = { executionId: status.execution_id };
  if (status.committed === true) {
    if (
      typeof status.placement_id !== 'string' ||
      !/^[A-Za-z0-9_-]{1,128}$/.test(status.placement_id) ||
      typeof status.revision !== 'string' ||
      !/^[a-f0-9]{64}$/.test(status.revision) ||
      !status.resolved_paths ||
      typeof status.resolved_paths !== 'object' ||
      Array.isArray(status.resolved_paths) ||
      Object.values(status.resolved_paths).some(
        (path) => typeof path !== 'string' || !path.startsWith('/') || path.length > 4096,
      ) ||
      typeof status.outputs_directory !== 'string' ||
      !status.outputs_directory.startsWith('/')
    )
      throw new Error('Invalid native executable mapping');
    result.placementId = status.placement_id;
    result.revision = status.revision;
    result.resolvedPaths = status.resolved_paths as Record<string, string>;
    result.outputs = status.outputs_directory;
  }
  if (status.turn !== null && status.turn !== undefined) {
    const turn = status.turn as Record<string, unknown>;
    if (typeof turn.turn_id !== 'string' || typeof turn.phase !== 'string')
      throw new Error('Invalid native completion status');
    result.turn = {
      turnId: turn.turn_id,
      phase: turn.phase,
      ...(typeof turn.checkpoint_id === 'string' ? { checkpointId: turn.checkpoint_id } : {}),
      ...(typeof turn.manifest_sha256 === 'string' ? { digest: turn.manifest_sha256 } : {}),
    };
  }
  return result;
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
