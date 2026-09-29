// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { SandboxHandle } from '../sandbox-runtime.js';

export type MountAccess = 'read_only' | 'read_write';

/**
 * A resource the harness has been asked to mount into the sandbox at session
 * start: a workspace `file`, a `memory_store`, or a `github_repository`.
 */
export type MountResource =
  | {
      id: string; // session_resources.id
      type: 'file';
      fileId: string;
      mountPath: string;
      access: MountAccess;
    }
  | {
      id: string;
      type: 'memory_store';
      memoryStoreId: string;
      /**
       * Human-readable store name (e.g. "prefs", "logs"). The dispatcher
       * populates this from the memory store record.
       */
      storeName: string;
      mountPath: string;
      access: MountAccess;
    }
  | {
      id: string;
      type: 'github_repository';
      url: string;
      mountPath: string;
      access: MountAccess;
      /**
       * Resolved by the dispatcher from `git_cred://<id>` syntax in the
       * registry's authorization_token. The PAT is fetched per-call by the
       * GitCloneStrategy via the resolvePat callback.
       */
      gitCredentialId: string;
      /** Optional branch or commit pin from the contract. */
      checkout?: { type: 'branch' | 'commit'; value: string };
      /**
       * Per-repo index within the session (0..n-1). Used by WorkDirManager
       * to namespace the host-side clone dir.
       */
      repoIdx: number;
    };

/**
 * Returned by `activate`; opaque to the caller, passed back into
 * `deactivate` / `teardownForSnapshot`.
 */
export interface MountHandle {
  id: string;
  resourceId: string;
  resourceType: MountResource['type'];
  mountPath: string;
}

/**
 * State captured at `teardownForSnapshot` time so `restoreAfterSnapshot` can
 * reconstruct the mount on resume. For stateless strategies (TarballPrefetch)
 * this is empty — the bytes live in the sandbox FS and persist automatically.
 *
 * Future stateful strategies (FUSE daemons, memory write-through interceptors)
 * use `serializedState` to record any host-side daemon config they need to
 * recreate.
 */
export interface TornDownState {
  resourceId: string;
  resourceType: MountResource['type'];
  mountPath: string;
  serializedState?: unknown;
}

/**
 * Strategy identifier. The registry's `session_resources.mount_strategy`
 * accepts only `tarball_prefetch` (on file resources); the harness picks the
 * others itself. The materializer writes the resolved name into each file's
 * `session.resource_mounted` event so operators can tell which path was taken.
 */
export type MountStrategyName = 'tarball_prefetch' | 'memory_fuse' | 'local_memory' | 'git_clone';

/**
 * The pluggable mount strategy. TarballPrefetchStrategy is the only file
 * strategy (sandbox creds never cover the workspace file-blob namespace);
 * memory and repository strategies implement the same interface. Every
 * strategy implements the snapshot lifecycle hooks, even as no-ops.
 */
export interface MountStrategy {
  /**
   * Stable name for this strategy. The materializer writes the resolved
   * name into the `session.resource_mounted` event.
   */
  readonly name: MountStrategyName;

  /** Resource types this strategy claims; used by the dispatcher to route. */
  readonly supports: ReadonlyArray<MountResource['type']>;

  /** Mount the resource into the sandbox at session start. */
  activate(sandbox: SandboxHandle, resource: MountResource): Promise<MountHandle>;

  /** Unmount on resource detach or session stop. */
  deactivate(sandbox: SandboxHandle, handle: MountHandle): Promise<void>;

  /** Tear down before sandbox.pause(). May be a no-op. */
  teardownForSnapshot(sandbox: SandboxHandle, handle: MountHandle): Promise<TornDownState>;

  /** Restore after sandbox.resume(). May be a no-op. */
  restoreAfterSnapshot(sandbox: SandboxHandle, torn: TornDownState): Promise<MountHandle>;
}
