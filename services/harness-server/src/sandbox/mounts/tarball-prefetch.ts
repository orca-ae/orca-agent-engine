// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { FileStore } from '@orca/file-store';
import { harnessFuseMountTotal } from '../../metrics.js';
import type { SandboxHandle } from '../sandbox-runtime.js';
import type { MountHandle, MountResource, MountStrategy, TornDownState } from './mount-strategy.js';

/**
 * v1 default file mount strategy.
 *
 * `activate`: streams the file's bytes from `@orca/file-store` and writes
 * them to `mountPath` inside the sandbox via `SandboxHandle.files.write`.
 * Read-only — there is no write-back path.
 *
 * `deactivate`: deletes the file at `mountPath`.
 *
 * `teardownForSnapshot` / `restoreAfterSnapshot`: no-ops. The bytes live in
 * the sandbox filesystem, which E2B persists across pause/resume; there is
 * no host-side daemon to clean up. The hooks exist so stateful strategies
 * can plug in without re-architecting the lifecycle.
 *
 * The strategy is workspace-bound at construction. The dispatcher builds one
 * per file resource, bound to the session's workspace, after validating the
 * prepared execution's workspace ownership.
 */
export class TarballPrefetchStrategy implements MountStrategy {
  readonly name = 'tarball_prefetch' as const;
  readonly supports = ['file'] as const;

  constructor(
    private readonly workspaceId: string,
    private readonly fileStore: FileStore,
  ) {}

  async activate(sandbox: SandboxHandle, resource: MountResource): Promise<MountHandle> {
    try {
      if (resource.type !== 'file') {
        throw new Error(`TarballPrefetchStrategy: unsupported resource type ${resource.type}`);
      }
      const opened = await this.fileStore.open(this.workspaceId, resource.fileId);
      if (!opened) {
        throw new Error(`file ${resource.fileId} not found`);
      }
      await sandbox.files.write(resource.mountPath, opened.stream);
      const handle = {
        id: `mh_${resource.id}_${Date.now().toString(36)}`,
        resourceId: resource.id,
        resourceType: 'file' as const,
        mountPath: resource.mountPath,
      };
      harnessFuseMountTotal.inc({ strategy: this.name, result: 'ok' });
      return handle;
    } catch (err) {
      harnessFuseMountTotal.inc({ strategy: this.name, result: 'error' });
      throw err;
    }
  }

  async deactivate(sandbox: SandboxHandle, handle: MountHandle): Promise<void> {
    if (handle.resourceType !== 'file') return;
    try {
      await sandbox.files.delete(handle.mountPath);
    } catch {
      // Best-effort: a snapshotted+resumed sandbox may have moved the file or
      // the path may already be gone. The caller doesn't depend on this
      // succeeding for correctness.
    }
  }

  async teardownForSnapshot(_sandbox: SandboxHandle, handle: MountHandle): Promise<TornDownState> {
    // No host-side daemon to tear down; the bytes are in the sandbox FS,
    // which the runtime persists across pause/resume.
    return {
      resourceId: handle.resourceId,
      resourceType: handle.resourceType,
      mountPath: handle.mountPath,
    };
  }

  async restoreAfterSnapshot(_sandbox: SandboxHandle, torn: TornDownState): Promise<MountHandle> {
    // No-op restore: nothing was torn down. Hand back a handle so the caller
    // can continue tracking it.
    return {
      id: `mh_${torn.resourceId}_${Date.now().toString(36)}`,
      resourceId: torn.resourceId,
      resourceType: torn.resourceType,
      mountPath: torn.mountPath,
    };
  }
}
