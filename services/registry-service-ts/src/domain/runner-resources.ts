// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import type { FileStore } from '@orca/file-store';
import type { MemoryStore } from '@orca/memory-store';
import {
  buildSandboxWritePolicy,
  parseResourceManifest,
  type ResourceMountDescriptor,
  type ResourceManifest,
  type GitProxyCapability,
} from '@orca/sandbox-runtime';
import type { PreparedExecutionV2 } from '../contracts/internal.contract.js';
import type { PreparedRunnerResources } from '../tunnel/session-resources-delivery.js';
import { prepareRunnerGitSnapshot, type RunnerGitSnapshot } from './runner-git-snapshot.js';

export interface RunnerResourceStores {
  fileStore: FileStore;
  memoryStore?: MemoryStore;
  additionalNetworkDomains?: string[];
  /** Previous Registry manifest, only after the runner confirms this active revision. */
  retainedManifest?: ResourceManifest;
  gitCapability?(
    resource: Extract<PreparedExecutionV2['resources'][number], { type: 'github_repository' }>,
  ): Promise<GitProxyCapability>;
  gitSnapshot(
    resource: Extract<PreparedExecutionV2['resources'][number], { type: 'github_repository' }>,
    capability?: GitProxyCapability,
  ): Promise<RunnerGitSnapshot>;
}

/** Revision pins binding authority and network policy, not mutable Memory bytes. */
export function runnerResourceRevision(
  prepared: PreparedExecutionV2,
  additionalDomains: string[] = [],
): string {
  return createHash('sha256')
    .update(
      JSON.stringify({
        workspace: prepared.workspace_id,
        session: prepared.session.id,
        resources: [...prepared.resources]
          .sort((a, b) => a.id.localeCompare(b.id))
          .map((resource) => ({
            id: resource.id,
            type: resource.type,
            mount: canonicalMount(resource.mount_path),
            access: resource.access,
            file: resource.file_id,
            memory: resource.memory_store_id,
            repo: resource.repo_ref,
          })),
        domains: runnerToolNetworkDomains(prepared, additionalDomains),
        unrestricted: runnerToolNetworkUnrestricted(prepared),
      }),
    )
    .digest('hex');
}

export function runnerToolNetworkUnrestricted(prepared: PreparedExecutionV2): boolean {
  const type = prepared.environment?.networking?.type;
  if (type === undefined || type === 'unrestricted') return true;
  if (type === 'limited') return false;
  throw new Error('invalid managed tool network type');
}

/** Tool networking is independent from the trusted provider's LLM/MCP egress. */
export function runnerToolNetworkDomains(
  prepared: PreparedExecutionV2,
  additionalDomains: string[] = [],
): string[] {
  const networking = prepared.environment?.networking;
  const hosts = networking?.allowed_hosts;
  if (
    hosts !== undefined &&
    hosts !== null &&
    (!Array.isArray(hosts) || hosts.some((host) => typeof host !== 'string'))
  )
    throw new Error('invalid managed tool network hosts');
  const domains = [
    ...(prepared.resources.some((resource) => resource.type === 'github_repository')
      ? additionalDomains
      : []),
    ...(Array.isArray(hosts) ? (hosts as string[]) : []),
  ];
  if (domains.some((domain) => domain.includes('*')))
    throw new Error('managed tool network domain must be an exact host');
  const policy = buildSandboxWritePolicy([], { networkAllowedDomains: domains });
  return [...(policy.networkAllowedDomains ?? [])].sort();
}

export async function prepareRunnerResources(
  prepared: PreparedExecutionV2,
  stores: RunnerResourceStores,
): Promise<PreparedRunnerResources> {
  const revision = runnerResourceRevision(prepared, stores.additionalNetworkDomains);
  const retainedManifest =
    stores.retainedManifest?.revision === revision ? stores.retainedManifest : undefined;
  const retainedGitResourceIds: string[] = [];
  const resources: ResourceMountDescriptor[] = [];
  const memoryStores = new Map<string, string>();
  const sources = new Map<string, () => Promise<NodeJS.ReadableStream>>();
  const snapshots: RunnerGitSnapshot[] = [];
  const close = async () => {
    await Promise.all(snapshots.map((snapshot) => snapshot.close()));
  };
  try {
    for (const binding of [...prepared.resources].sort((a, b) => a.id.localeCompare(b.id))) {
      const resource: ResourceMountDescriptor = {
        resource_id: binding.id,
        kind: binding.type,
        mount_path: canonicalMount(binding.mount_path),
        access: binding.access,
        files: [],
      };
      resources.push(resource);
      if (binding.type === 'file') {
        const descriptor = {
          path: '',
          sha256: binding.file.sha256,
          size_bytes: binding.file.size_bytes,
          mode: 0o444,
        };
        resource.files.push(descriptor);
        sources.set(key(binding.id, ''), async () => {
          const opened = await stores.fileStore.open(prepared.workspace_id, binding.file_id);
          if (
            !opened ||
            opened.sha256 !== descriptor.sha256 ||
            opened.sizeBytes !== descriptor.size_bytes
          ) {
            destroySource(opened?.stream);
            throw new Error('attached File changed during resource preparation');
          }
          return opened.stream;
        });
      } else if (binding.type === 'memory_store') {
        const memoryStore = stores.memoryStore;
        if (!memoryStore) throw new Error('memory store is not configured');
        memoryStores.set(binding.id, binding.memory_store_id);
        for (const memory of await memoryStore.listMemories(
          prepared.workspace_id,
          binding.memory_store_id,
        )) {
          const descriptor = {
            path: memory.path,
            sha256: memory.currentSha256,
            size_bytes: memory.sizeBytes,
            mode: binding.access === 'read_only' ? 0o444 : 0o644,
          };
          resource.files.push(descriptor);
          sources.set(key(binding.id, memory.path), async () => {
            const opened = await memoryStore.openMemory(
              prepared.workspace_id,
              binding.memory_store_id,
              memory.id,
            );
            if (
              !opened ||
              opened.sha256 !== descriptor.sha256 ||
              opened.sizeBytes !== descriptor.size_bytes
            ) {
              destroySource(opened?.stream);
              throw new Error('attached Memory changed during resource preparation');
            }
            return opened.stream;
          });
        }
      } else {
        const retained = retainedManifest?.resources.find(
          (item) => item.resource_id === binding.id && item.kind === 'github_repository',
        );
        if (retained) {
          resource.files.push(...retained.files);
          retainedGitResourceIds.push(binding.id);
          continue;
        }
        const capability = await stores.gitCapability?.(binding);
        const snapshot = await stores.gitSnapshot(binding, capability);
        snapshots.push(snapshot);
        resource.files.push(...snapshot.files);
        for (const file of snapshot.files)
          sources.set(key(binding.id, file.path), async () => snapshot.open(file.path));
      }
    }
    return {
      manifest: parseResourceManifest({
        version: 1,
        revision,
        resources,
      }),
      networkAllowedDomains: runnerToolNetworkDomains(prepared, stores.additionalNetworkDomains),
      networkUnrestricted: runnerToolNetworkUnrestricted(prepared),
      ...(stores.gitCapability
        ? {
            refreshGitCapabilities: async () => {
              const capabilities: GitProxyCapability[] = [];
              for (const binding of prepared.resources) {
                if (binding.type === 'github_repository')
                  capabilities.push(await stores.gitCapability!(binding));
              }
              return capabilities;
            },
          }
        : {}),
      memoryStores,
      retainedGitResourceIds,
      fileIds: new Map(
        prepared.resources.flatMap((resource) =>
          resource.type === 'file' ? [[resource.id, resource.file_id]] : [],
        ),
      ),
      async open(resourceId, path) {
        const source = sources.get(key(resourceId, path));
        if (!source) throw new Error('undeclared resource source');
        return source();
      },
      close,
    };
  } catch (error) {
    await close();
    throw error;
  }
}

export { prepareRunnerGitSnapshot };

function canonicalMount(value: string): string {
  return value === '/' ? value : value.replace(/\/+$/, '');
}
function key(resource: string, path: string): string {
  return JSON.stringify([resource, path]);
}

function destroySource(stream: NodeJS.ReadableStream | undefined): void {
  (stream as (NodeJS.ReadableStream & { destroy?: () => void }) | undefined)?.destroy?.();
}
