// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { SandboxHandle } from '../sandbox-runtime.js';
import type { MountHandle, MountResource, MountStrategy, TornDownState } from './mount-strategy.js';

/**
 * v1 non-FUSE fallback for memory mounts.
 *
 * Backs the memory mount with the runtime's Files API. A temporary marker
 * creates an otherwise-empty root before the write-policy probe runs, without
 * relying on a shell `mkdir` that would target the harness host for InMemory.
 *
 * The MemoryVersionWatcher walks `sandbox.files.list` recursively to detect
 * writes; the dispatcher seeds the initial agent-visible state at session
 * start by fetching each existing memory from the registry
 * (`listSessionMemories` + `getSessionMemoryContent`) and writing it with
 * `files.write`.
 *
 * Used only by runtimes that explicitly advertise the Files API fallback:
 * AgentENV, InMemory, and Local. AgentENV keeps the agent inside its Bubblewrap
 * write policy; the strategy itself does not provide an isolation boundary.
 * OpenSandbox fails closed when FUSE is absent. The strategy factory throws if
 * `mount_strategy='memory_fuse'` is explicitly requested against a non-FUSE
 * runtime — loud failure beats silent fallback.
 *
 * `teardownForSnapshot` / `restoreAfterSnapshot`: no-ops; the tmpdir state
 * persists naturally across the runtime's pause/resume.
 */
export class LocalMemoryStrategy implements MountStrategy {
  readonly name = 'local_memory' as const;
  readonly supports = ['memory_store'] as const;

  async activate(sandbox: SandboxHandle, resource: MountResource): Promise<MountHandle> {
    if (resource.type !== 'memory_store') {
      throw new Error(`LocalMemoryStrategy: unsupported resource type ${resource.type}`);
    }
    const mountPath = resource.mountPath.replace(/\/+$/, '') || '/';
    const marker = mountPath === '/' ? '/.orca-memory-mount' : `${mountPath}/.orca-memory-mount`;
    await sandbox.files.write(marker, Buffer.alloc(0));
    await sandbox.files.delete(marker);
    return {
      id: `mh_lmem_${resource.id}_${Date.now().toString(36)}`,
      resourceId: resource.id,
      resourceType: 'memory_store',
      mountPath: resource.mountPath,
    };
  }

  async deactivate(_sandbox: SandboxHandle, _handle: MountHandle): Promise<void> {
    // No-op: tmpdir is owned by the sandbox lifecycle. The runtime's destroy
    // handler cleans it up.
  }

  async teardownForSnapshot(_sandbox: SandboxHandle, handle: MountHandle): Promise<TornDownState> {
    return {
      resourceId: handle.resourceId,
      resourceType: handle.resourceType,
      mountPath: handle.mountPath,
    };
  }

  async restoreAfterSnapshot(_sandbox: SandboxHandle, torn: TornDownState): Promise<MountHandle> {
    return {
      id: `mh_lmem_${torn.resourceId}_${Date.now().toString(36)}`,
      resourceId: torn.resourceId,
      resourceType: torn.resourceType,
      mountPath: torn.mountPath,
    };
  }
}
