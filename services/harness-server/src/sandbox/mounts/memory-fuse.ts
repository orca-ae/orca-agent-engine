// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { posix as posixPath } from 'node:path';
import { buildMemoryStoreLivePrefix, type SessionS3Creds } from '../../auth/sts-creds.js';
import type { SandboxHandle } from '../sandbox-runtime.js';
import { buildS3fsCredentialEnvironment, buildS3fsMountCommand } from '../s3fs-mount.js';
import type { MountHandle, MountResource, MountStrategy, TornDownState } from './mount-strategy.js';

/**
 * v1 memory mount strategy.
 *
 * For each attached `memory_store` resource, mounts the per-store S3 prefix
 * directly at `mountPath` (typically `/mnt/memory/{store_name}/`) via
 * `s3fs-fuse` (read-only for `read_only` resources) using the per-session
 * STS-scoped creds from `SessionCredsMinter`, whose `ReadMemoryStores` /
 * `WriteMemoryStores` policy statements cover each attached store's live
 * prefix. The FUSE mount IS the live
 * agent-visible state; there are NO symlinks and NO sha-fanout — each
 * `memory_store` is its own filesystem view.
 *
 * The S3 layout mirrors `S3MemoryBlobStore` exactly: only the live namespace
 * is mounted at
 * `{keyPrefix}workspaces/{workspaceId}/memory-stores/{storeId}/live/`.
 * Version blobs are deliberately outside the mount and cannot be read or
 * modified by the sandbox.
 *
 * The MemoryVersionWatcher is responsible for detecting
 * writes asynchronously and registering versions via
 * the scoped internal memory-version endpoint. This strategy only mounts; it does NOT
 * watch.
 *
 * Lifecycle:
 *   - The dispatcher builds one `MemoryFuseStrategy` per `memory_store`
 *     resource (via `pickStrategy`). An instance can still `activate` multiple
 *     `memory_store` resources in the same session — each gets its own
 *     `s3fs` daemon at its own mount path.
 *   - `mountedStores` tracks the set of `memoryStoreId`s already mounted so
 *     re-activating the same store id (rare; e.g. duplicate session_resources
 *     pointing at the same store) is a fast no-op.
 *
 * `teardownForSnapshot` / `restoreAfterSnapshot`: no-ops — E2B persists the FUSE mount through pause/resume; the s3fs
 * daemon runs INSIDE the sandbox so there is nothing on the harness host to
 * clean up. The hooks exist so future stateful memory strategies can plug in
 * without re-architecting.
 */
export interface MemoryFuseStrategyConfig {
  workspaceId: string;
  sessionId: string;
  bucket: string;
  /** e.g. `http://rustfs:9000` (dev) or `https://s3.us-east-1.amazonaws.com` (prod). */
  endpoint: string;
  /** e.g. `memory/` — same prefix passed to `S3MemoryBlobStore`. Must end with a slash. */
  keyPrefix: string;
  /** From {@link SessionCredsMinter.mint} — short-lived STS creds. */
  creds: SessionS3Creds;
  /** Defaults to true for MinIO compatibility. Set false for virtual-hosted addressing. */
  forcePathStyle?: boolean;
  /**
   * Bucket region (e.g. `us-east-2`). Forwarded as s3fs's `endpoint=` flag so
   * SigV4 signing uses the correct region. Without this s3fs defaults to
   * `us-east-1` and AWS rejects with `AuthorizationHeaderMalformed`.
   */
  region?: string;
}

export class MemoryFuseStrategy implements MountStrategy {
  readonly name = 'memory_fuse' as const;
  readonly supports = ['memory_store'] as const;

  /** Set of `memoryStoreId`s already mounted by this strategy instance. */
  private readonly mountedStores = new Set<string>();

  constructor(private readonly config: MemoryFuseStrategyConfig) {}

  async activate(sandbox: SandboxHandle, resource: MountResource): Promise<MountHandle> {
    if (resource.type !== 'memory_store') {
      throw new Error(`MemoryFuseStrategy: unsupported resource type ${resource.type}`);
    }
    const { memoryStoreId, mountPath } = resource;

    // Treat Registry data as untrusted at this privileged boundary. Validate
    // every caller-controlled value before mkdir or s3fs can cause a side
    // effect, even though Registry performs the same mount-path validation.
    assertSafeMemoryMountPath(mountPath);
    if (resource.access !== 'read_only' && resource.access !== 'read_write') {
      throw new Error(`MemoryFuseStrategy: unsupported access ${String(resource.access)}`);
    }
    const prefix = buildMemoryStoreLivePrefix(
      this.config.keyPrefix,
      this.config.workspaceId,
      memoryStoreId,
    );

    if (!this.mountedStores.has(memoryStoreId)) {
      // mkdir is non-privileged — the orca-default sandbox template chowns
      // `/mnt/memory` to `user`, so per-store subdirs `mkdir -p` cleanly.
      const mkdirRes = await sandbox.run({
        tool: 'bash',
        args: { command: `mkdir -p -- ${shellQuote(mountPath)}` },
      });
      if (mkdirRes.exit_code !== undefined && mkdirRes.exit_code !== 0) {
        throw new Error(
          `MemoryFuseStrategy: mkdir ${mountPath} failed (exit_code=${mkdirRes.exit_code}): ${mkdirRes.stderr ?? ''}`,
        );
      }

      // Mount only this store's live namespace. The STS policy is scoped to
      // this exact prefix; `read_only` resources also get a kernel-level
      // read-only mount in addition to a read-only IAM policy.
      // `compat_dir` skips the directory-marker `CheckBucket` probe — the
      // memory store doesn't pre-write an empty marker, only data objects, so
      // without this s3fs aborts with `NoSuchKey` and refuses to mount.
      //
      // `uid=1000,gid=1000,umask=0022`: the sandbox agent runs as `user`;
      // mount happens as root via sudo. Without these flags s3fs serves
      // root-owned files and the agent gets `Permission denied` despite
      // `allow_other`. Pinning uid/gid + umask makes the FUSE serve files
      // as `user:user`.
      const regionArg = this.config.region ? `,endpoint=${this.config.region}` : '';
      const readOnlyArg = resource.access === 'read_only' ? 'ro,' : '';
      const pathStyleArg = this.config.forcePathStyle === false ? '' : 'use_path_request_style,';
      const options =
        `${readOnlyArg}allow_other,${pathStyleArg}use_cache=,ensure_diskfree=0,` +
        `compat_dir,uid=1000,gid=1000,umask=0022,url=${this.config.endpoint}${regionArg}`;
      const mountCmd = buildS3fsMountCommand({
        bucketAndPrefix: `${this.config.bucket}:/${prefix}`,
        mountPath,
        options,
      });
      // The short-lived root shell receives transport envs, writes a mode-0600
      // AWS credentials file, launches s3fs under a scrubbed environment, and
      // unlinks the file after daemonization. Neither argv nor daemon environ
      // retains the session credentials.
      const envs = buildS3fsCredentialEnvironment(this.config.creds);
      const mountRes = await sandbox.runPrivileged(mountCmd, { envs });
      if (mountRes.exit_code !== undefined && mountRes.exit_code !== 0) {
        throw new Error(
          `MemoryFuseStrategy: s3fs mount of ${prefix} failed (CAP_SYS_ADMIN missing? sudo policy?): ${mountRes.stderr ?? ''}`,
        );
      }
      this.mountedStores.add(memoryStoreId);
    }

    return {
      id: `mh_mem_${resource.id}_${Date.now().toString(36)}`,
      resourceId: resource.id,
      resourceType: 'memory_store',
      mountPath,
    };
  }

  async deactivate(sandbox: SandboxHandle, handle: MountHandle): Promise<void> {
    if (handle.resourceType !== 'memory_store') return;
    // Best-effort umount — the sandbox is being torn down anyway, so a hung
    // umount is wasted work.
    try {
      assertSafeMemoryMountPath(handle.mountPath);
      await sandbox.runPrivileged(`umount -- ${shellQuote(handle.mountPath)}`);
    } catch {
      // Swallow.
    }
  }

  async teardownForSnapshot(_sandbox: SandboxHandle, handle: MountHandle): Promise<TornDownState> {
    // No-op: E2B persists the FUSE mount across pause/resume. The s3fs daemon
    // runs INSIDE the sandbox; nothing on the harness host needs unwinding.
    return {
      resourceId: handle.resourceId,
      resourceType: handle.resourceType,
      mountPath: handle.mountPath,
    };
  }

  async restoreAfterSnapshot(_sandbox: SandboxHandle, torn: TornDownState): Promise<MountHandle> {
    assertSafeMemoryMountPath(torn.mountPath);
    return {
      id: `mh_mem_${torn.resourceId}_${Date.now().toString(36)}`,
      resourceId: torn.resourceId,
      resourceType: torn.resourceType,
      mountPath: torn.mountPath,
    };
  }
}

const MEMORY_MOUNT_ROOT = '/mnt/memory/';
const SAFE_MOUNT_SEGMENT = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;

/** Validate the privileged memory-FUSE target without changing its spelling. */
export function assertSafeMemoryMountPath(mountPath: string): void {
  if (
    mountPath.length <= MEMORY_MOUNT_ROOT.length ||
    hasControlCharacter(mountPath) ||
    !mountPath.startsWith(MEMORY_MOUNT_ROOT) ||
    posixPath.normalize(mountPath) !== mountPath
  ) {
    throw new Error(`MemoryFuseStrategy: invalid mountPath ${JSON.stringify(mountPath)}`);
  }
  const segments = mountPath.slice(1).split('/').filter(Boolean);
  if (segments.some((segment) => !SAFE_MOUNT_SEGMENT.test(segment))) {
    throw new Error(`MemoryFuseStrategy: invalid mountPath ${JSON.stringify(mountPath)}`);
  }
}

function hasControlCharacter(value: string): boolean {
  return [...value].some((character) => {
    const codePoint = character.codePointAt(0)!;
    return codePoint <= 0x1f || codePoint === 0x7f;
  });
}

function shellQuote(value: string): string {
  return `'${value.replace(/'/g, `'\\''`)}'`;
}
